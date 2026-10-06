package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	ce "github.com/cloudevents/sdk-go/v2/event"
	kafkago "github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/claudioed/labor-performance/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/labor-performance/internal/adapters/kafka/otelkafka"
	"github.com/claudioed/labor-performance/internal/analytics/report"
)

// AnalyticsConsumerGroup is the Kafka consumer group the analytics
// projector reads under. It is distinct from the OLTP consumer group so
// the two pipelines track their offsets independently — the projector can
// be reset and replayed from the beginning without disturbing the OLTP
// consumer's position.
const AnalyticsConsumerGroup = "labor-performance-analytics"

// ProcessedEvents is the analytics consumer's idempotency gate: it
// records which CloudEvents ids have been admitted, so an at-least-once
// redelivery is a no-op.
//
// It is declared HERE — where it is consumed — rather than reused from
// internal/application/ports, so the analytics side owns its own port and
// the OLTP application layer stays entirely untouched by this data
// product (ADR-0007). The Postgres implementation lives in the
// analyticsstore outbound adapter.
type ProcessedEvents interface {
	// MarkProcessed records eventId if absent, returning true iff this
	// call newly recorded it.
	MarkProcessed(ctx context.Context, eventId string) (bool, error)
}

// analyticsData is the union of the fields the three projecting payloads
// carry. Decoding all of them into one struct keeps the decode total: a
// field absent from a given event type simply stays at its zero value,
// and only the fields that event's apply path reads are ever consulted.
type analyticsData struct {
	TaskType        string    `json:"task_type"`
	ExpectedSeconds int64     `json:"expected_seconds"`
	EffectiveFrom   time.Time `json:"effective_from"`
	TaskId          string    `json:"task_id"`
	AssociateId     string    `json:"associate_id"`
	ActualSeconds   int64     `json:"actual_seconds"`
	// EfficiencyPct is a POINTER on purpose: JSON null must decode back
	// to nil ("this task could not be scored"), never to 0.0. Decoding
	// it into a plain float64 would silently turn every unscorable task
	// into a 0% performer — exactly the fabricated number ADR-0004
	// exists to prevent.
	EfficiencyPct *float64  `json:"efficiency_pct"`
	CompletedAt   time.Time `json:"completed_at"`
}

// UnitOfWork brackets the consumer's idempotency claim and the projection
// apply in ONE transaction: Execute runs fn with a ctx that carries the
// transaction, commits when fn returns nil and rolls everything back —
// including the claim — when it returns an error. A failed apply therefore
// leaves the event un-claimed, so the redelivery is applied rather than
// short-circuited as a duplicate. The Postgres implementation lives in the
// analyticsstore outbound adapter.
type UnitOfWork interface {
	Execute(ctx context.Context, fn func(ctx context.Context) error) error
}

// messageReader is the slice of *kafkago.Reader the consumer needs. The
// consume loop uses FetchMessage + CommitMessages (never ReadMessage, which
// auto-commits the offset before the handler has run).
type messageReader interface {
	FetchMessage(ctx context.Context) (kafkago.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafkago.Message) error
	Close() error
}

const (
	// defaultRetryInitial / defaultRetryMax bound the capped exponential
	// backoff Run applies while retrying a message whose handler failed
	// with a transient (infrastructure) error.
	defaultRetryInitial = 200 * time.Millisecond
	defaultRetryMax     = 5 * time.Second
)

// AnalyticsConsumer reads this service's own domain events off the
// analytics topic and applies each to the report ProjectionStore, exactly
// once per CloudEvents id.
//
// It is a SECOND, independent inbound Kafka adapter alongside Consumer:
// Consumer feeds the OLTP write path from fulfillment-execution's
// integration topic, this one feeds the analytical read model from this
// service's own analytics topic. They share no state and run in separate
// processes (cmd/labor and cmd/labor-projector).
type AnalyticsConsumer struct {
	Reader     messageReader
	Projection report.ProjectionStore
	Processed  ProcessedEvents
	// UnitOfWork makes the Processed claim and the Projection apply one
	// atomic step. Nil runs them back to back without a shared
	// transaction, which is only correct for in-memory stores that cannot
	// fail mid-apply.
	UnitOfWork UnitOfWork
	Logger     *slog.Logger

	// retryInitial/retryMax tune the transient-failure backoff; zero
	// values use the defaults (tests shrink them).
	retryInitial time.Duration
	retryMax     time.Duration
}

// NewAnalyticsConsumer constructs an AnalyticsConsumer reading the
// analytics topic from brokers under AnalyticsConsumerGroup.
func NewAnalyticsConsumer(brokers []string, projection report.ProjectionStore, processed ProcessedEvents, uow UnitOfWork, logger *slog.Logger) *AnalyticsConsumer {
	return NewAnalyticsConsumerForTopic(brokers, AnalyticsConsumerGroup, cloudevents.TopicLaborPerformanceAnalytics, projection, processed, uow, logger)
}

// NewAnalyticsConsumerForTopic is NewAnalyticsConsumer with an explicit
// consumer group and topic, so integration tests can run against an
// isolated topic.
func NewAnalyticsConsumerForTopic(brokers []string, groupID, topic string, projection report.ProjectionStore, processed ProcessedEvents, uow UnitOfWork, logger *slog.Logger) *AnalyticsConsumer {
	if logger == nil {
		logger = slog.Default()
	}
	return &AnalyticsConsumer{
		Reader: kafkago.NewReader(kafkago.ReaderConfig{
			Brokers: brokers,
			Topic:   topic,
			GroupID: groupID,
			// Start a brand-new consumer group at the EARLIEST offset.
			// The projection is a replayable read model, not a live
			// integration reaction: a fresh projector — or a backfill
			// into a new group — must see the topic's full history
			// rather than kafka-go's default of the latest offset,
			// which would silently drop every event produced before
			// the group first committed. Once the group has committed
			// offsets those take precedence, so this only affects the
			// first join.
			StartOffset: kafkago.FirstOffset,
		}),
		Projection: projection,
		Processed:  processed,
		UnitOfWork: uow,
		Logger:     logger,
	}
}

// Close releases the underlying Kafka reader.
func (c *AnalyticsConsumer) Close() error {
	return c.Reader.Close()
}

// Run fetches and handles messages until ctx is cancelled or the reader
// returns a fatal error. The offset is committed ONLY after the handler
// returns nil (at-least-once): a handler error means a transient
// infrastructure failure (the handler returns nil for deterministic bad
// input), so the SAME message is retried with capped exponential backoff
// rather than skipped. A commit failure is fatal to the loop; the
// uncommitted offset is redelivered and the idempotency claim makes the
// replay a no-op.
func (c *AnalyticsConsumer) Run(ctx context.Context) error {
	for {
		msg, err := c.Reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || ctx.Err() != nil {
				return nil
			}
			return err
		}
		if err := c.handleWithRetry(ctx, msg); err != nil {
			// Only a cancelled ctx ends the retry loop; leave the
			// offset uncommitted so the message is redelivered.
			return nil
		}
		if err := c.Reader.CommitMessages(ctx, msg); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("analytics: commit offset %d: %w", msg.Offset, err)
		}
	}
}

// handleWithRetry calls Handle until it succeeds, sleeping a capped
// exponential backoff between attempts. It returns a non-nil error only
// when ctx is cancelled.
func (c *AnalyticsConsumer) handleWithRetry(ctx context.Context, msg kafkago.Message) error {
	delay := c.retryInitial
	if delay <= 0 {
		delay = defaultRetryInitial
	}
	maxDelay := c.retryMax
	if maxDelay <= 0 {
		maxDelay = defaultRetryMax
	}
	for attempt := 1; ; attempt++ {
		err := c.Handle(ctx, msg)
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		c.logger().ErrorContext(ctx, "analytics message handling failed; retrying",
			"topic", msg.Topic, "offset", msg.Offset, "attempt", attempt, "retry_in", delay, "error", err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
		delay *= 2
		if delay > maxDelay {
			delay = maxDelay
		}
	}
}

// Handle processes one consumed message inside a
// "kafka.consume <topic>" span whose parent is the publisher's span, read
// from the message headers. It is exported separately from Run so trace
// propagation can be tested without a live broker.
func (c *AnalyticsConsumer) Handle(ctx context.Context, msg kafkago.Message) error {
	ctx, span := otelkafka.StartConsumeSpan(otelkafka.Extract(ctx, &msg), cloudevents.TopicLaborPerformanceAnalytics)
	defer span.End()

	if err := c.HandleMessage(ctx, msg.Value); err != nil {
		recordSpanError(span, err)
		return err
	}
	return nil
}

// HandleMessage decodes raw as a CloudEvents 1.0 event (ADR 0021) and
// applies the matching projection method for its full `type`. It is
// exported separately from Run so tests can feed raw events without a
// live broker.
//
// Four behaviours are load-bearing here:
//
//   - A message that is not a valid CloudEvent (including the retired
//     flat envelope) is logged at WARN and skipped (nil error), never
//     parsed: this consumer has no DLQ, and Run commits past it.
//   - A type outside the projection contract is ignored and NOT marked
//     processed, so widening the contract later can reprocess it on a
//     replay.
//   - A payload that cannot be decoded is deterministic bad input: it is
//     logged at WARN and skipped (nil error) WITHOUT being marked
//     processed. Returning an error here would make Run retry it forever.
//   - A projecting event is claimed on the CloudEvents id and applied
//     inside ONE UnitOfWork, so an at-least-once redelivery cannot
//     double-count AND a failed apply rolls the claim back, letting the
//     retry re-apply it. A non-nil return therefore always means a
//     transient infrastructure failure.
func (c *AnalyticsConsumer) HandleMessage(ctx context.Context, raw []byte) error {
	evt, err := cloudevents.Decode(raw)
	if err != nil {
		c.logger().WarnContext(ctx, "skipping invalid cloudevent on analytics topic",
			"topic", cloudevents.TopicLaborPerformanceAnalytics, "error", err)
		return nil
	}

	// Carry the event's identity on whatever consume span is active, so
	// a projection failure is traceable to the exact message.
	trace.SpanFromContext(ctx).SetAttributes(
		attribute.String("messaging.message.id", evt.ID()),
		attribute.String("cloudevents.event_type", evt.Type()),
		attribute.String("cloudevents.event_source", evt.Source()),
		attribute.String("cloudevents.event_subject", evt.Subject()),
	)

	if !isProjecting(evt.Type()) {
		return nil
	}

	var data analyticsData
	if err := evt.DataAs(&data); err != nil {
		c.logger().WarnContext(ctx, "skipping analytics event with undecodable data",
			"topic", cloudevents.TopicLaborPerformanceAnalytics, "id", evt.ID(), "type", evt.Type(), "error", err)
		return nil
	}

	return c.atomically(ctx, func(ctx context.Context) error {
		isNew, err := c.Processed.MarkProcessed(ctx, evt.ID())
		if err != nil {
			return fmt.Errorf("analytics: mark processed: %w", err)
		}
		if !isNew {
			return nil
		}
		return c.apply(ctx, evt, data)
	})
}

// atomically runs fn inside the configured UnitOfWork, or directly when
// none is configured.
func (c *AnalyticsConsumer) atomically(ctx context.Context, fn func(ctx context.Context) error) error {
	if c.UnitOfWork == nil {
		return fn(ctx)
	}
	return c.UnitOfWork.Execute(ctx, fn)
}

// logger returns the configured logger, or slog.Default for a zero-value
// consumer built directly by a test.
func (c *AnalyticsConsumer) logger() *slog.Logger {
	if c.Logger == nil {
		return slog.Default()
	}
	return c.Logger
}

// isProjecting reports whether the full CloudEvents type moves a report
// counter.
func isProjecting(eventType string) bool {
	switch eventType {
	case cloudevents.TypeLaborStandardDefined,
		cloudevents.TypeLaborStandardRevised,
		cloudevents.TypeTaskPerformanceRecorded:
		return true
	default:
		return false
	}
}

// apply routes a decoded event to the projection method for its type,
// resolving each fact's BUSINESS time (which chooses the hour bucket)
// separately from the CloudEvents `time` attribute (the domain
// occurred-at, which feeds the freshness watermark). A zero business time
// falls back to `time` rather than bucketing the row at the zero instant.
func (c *AnalyticsConsumer) apply(ctx context.Context, evt ce.Event, data analyticsData) error {
	occurredAt := evt.Time()
	switch evt.Type() {
	case cloudevents.TypeLaborStandardDefined:
		return c.Projection.ApplyLaborStandardDefined(ctx, evt.ID(), report.StandardFact{
			TaskType:        data.TaskType,
			ExpectedSeconds: data.ExpectedSeconds,
			EffectiveFrom:   orOccurredAt(data.EffectiveFrom, occurredAt),
			OccurredAt:      occurredAt,
		})

	case cloudevents.TypeLaborStandardRevised:
		return c.Projection.ApplyLaborStandardRevised(ctx, evt.ID(), report.StandardFact{
			TaskType:        data.TaskType,
			ExpectedSeconds: data.ExpectedSeconds,
			EffectiveFrom:   orOccurredAt(data.EffectiveFrom, occurredAt),
			OccurredAt:      occurredAt,
		})

	case cloudevents.TypeTaskPerformanceRecorded:
		return c.Projection.ApplyTaskPerformanceRecorded(ctx, evt.ID(), report.TaskPerformanceFact{
			TaskType:      data.TaskType,
			ActualSeconds: data.ActualSeconds,
			EfficiencyPct: data.EfficiencyPct,
			CompletedAt:   orOccurredAt(data.CompletedAt, occurredAt),
			OccurredAt:    occurredAt,
		})

	default:
		return nil
	}
}

// orOccurredAt returns businessTime, or occurredAt when businessTime is
// the zero instant.
func orOccurredAt(businessTime, occurredAt time.Time) time.Time {
	if businessTime.IsZero() {
		return occurredAt
	}
	return businessTime
}
