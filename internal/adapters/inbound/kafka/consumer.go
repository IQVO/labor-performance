// Package kafka is the inbound Kafka adapter: it consumes
// warehouse.fulfillment.events (the shared, fan-out topic
// wes-work-planning already consumes) and feeds TaskCompleted into the
// existing RecordTaskPerformance use case, including the task's own type
// (as of fulfillment-execution's ADR-0023) so per-task-type utilization
// buckets correctly instead of collapsing into "unclassified". Every
// other event type on that topic is silently skipped — mirroring
// wes-work-planning's own consumer's skip-unrecognized-event-type
// behavior — since this is a shared topic by convention even though
// fulfillment-execution publishes no other event type to it today.
//
// As of ADR-0027 (fulfillment-execution) / ADR-0021 (wes-work-planning)
// Phase 2 Task 2d, this consumer dual-reads: a message may arrive either
// in the legacy flat envelope shape or in the CloudEvents 1.0 structured
// envelope fulfillment-execution's own apis/asyncapi.yaml has always
// specified as the target shape. Both decode paths normalize to the same
// internal envelope.Envelope before handing off to the unchanged
// handleFulfillmentEvent logic below — there is exactly one business-logic
// path, never two.
package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/claudioed/labor-performance/internal/adapters/kafka/envelope"
	"github.com/claudioed/labor-performance/internal/adapters/kafka/otelkafka"
	"github.com/claudioed/labor-performance/internal/application/usecases"
	"github.com/claudioed/labor-performance/internal/domain/shared"
)

// taskCompletedData is fulfillment-execution's TaskCompleted payload, as
// verified against fulfillment-execution's actual publisher
// (internal/adapters/outbound/kafka/publisher.go's TaskCompletedData
// struct). AssociateId, DurationSeconds, and TaskType are marked
// `omitempty` on that struct's OWN JSON tags, so an older
// fulfillment-execution payload that predates one of those enrichments
// simply omits it from the wire — this struct's zero values ("" / 0)
// already degrade gracefully to exactly the "unmeasurable"/"no
// occupant"/"unclassified" cases CLAUDE.md's aggregate invariants
// require, so no special-casing is needed here beyond ordinary Go
// zero-value JSON unmarshaling. TaskType went from a real, documented
// wire gap (see shared.ParseTaskTypeLenient's doc comment, and
// fulfillment-execution's ADR-0023) to actually present on the wire as
// of that ADR; ParseTaskTypeLenient itself is unchanged and still
// degrades an unrecognized/absent value to "" (unclassified) exactly as
// before — this consumer now simply has something real to hand it most
// of the time instead of a hardcoded "".
type taskCompletedData struct {
	TaskId          string `json:"task_id"`
	StationId       string `json:"station_id"`
	WorkUnitId      string `json:"work_unit_id"`
	AssociateId     string `json:"associate_id"`
	DurationSeconds int64  `json:"duration_seconds"`
	TaskType        string `json:"task_type"`
}

// Consumer consumes warehouse.fulfillment.events, feeding TaskCompleted
// into the existing RecordTaskPerformance use case.
type Consumer struct {
	reader                *kafkago.Reader
	recordTaskPerformance *usecases.RecordTaskPerformance
	logger                *slog.Logger
}

// NewConsumer constructs a Consumer reading TopicFulfillmentEvents on
// brokers under groupID.
func NewConsumer(brokers []string, groupID string, recordTaskPerformance *usecases.RecordTaskPerformance, logger *slog.Logger) *Consumer {
	return NewConsumerForTopic(brokers, groupID, envelope.TopicFulfillmentEvents, recordTaskPerformance, logger)
}

// NewConsumerForTopic constructs a Consumer reading topic on brokers under
// groupID. It supports isolated integration topics while NewConsumer retains
// the production fulfillment-events topic.
func NewConsumerForTopic(brokers []string, groupID, topic string, recordTaskPerformance *usecases.RecordTaskPerformance, logger *slog.Logger) *Consumer {
	return &Consumer{
		reader: kafkago.NewReader(kafkago.ReaderConfig{
			Brokers: brokers,
			GroupID: groupID,
			Topic:   topic,
		}),
		recordTaskPerformance: recordTaskPerformance,
		logger:                logger,
	}
}

// Close releases the underlying Kafka reader.
func (c *Consumer) Close() error {
	return c.reader.Close()
}

// Run consumes the topic until ctx is cancelled.
func (c *Consumer) Run(ctx context.Context) error {
	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}

		if err := c.handleMessage(ctx, msg); err != nil {
			return err
		}
	}
}

// handleMessage processes one fetched message inside a
// "kafka.consume <topic>" span whose parent is the producing service's
// publish span, recovered from the message's W3C trace-context headers.
// Unparseable or unhandleable messages are logged and committed rather
// than redelivered forever; only a commit failure aborts the consume loop,
// which is the error this returns.
func (c *Consumer) handleMessage(ctx context.Context, msg kafkago.Message) error {
	topic := c.reader.Config().Topic

	msgCtx, span := otelkafka.StartConsumeSpan(otelkafka.Extract(ctx, &msg), topic,
		semconv.MessagingKafkaOffset(int(msg.Offset)),
		semconv.MessagingDestinationPartitionID(strconv.Itoa(msg.Partition)),
	)
	defer span.End()

	env, err := decodeEnvelope(msg.Value)
	if err != nil {
		recordSpanError(span, err)
		c.log(msgCtx, "skipping unparseable kafka message", "topic", topic, "error", err)
		_ = c.reader.CommitMessages(ctx, msg)
		return nil
	}

	span.SetAttributes(
		attribute.String("messaging.message.event_id", env.EventId),
		attribute.String("messaging.message.event_type", env.EventType),
		attribute.String("messaging.message.source", env.Source),
	)

	if err := c.handleFulfillmentEvent(msgCtx, env); err != nil {
		recordSpanError(span, err)
		c.log(msgCtx, "skipping kafka event",
			"topic", topic, "event_id", env.EventId, "event_type", env.EventType, "error", err)
		_ = c.reader.CommitMessages(ctx, msg)
		return nil
	}

	if err := c.reader.CommitMessages(ctx, msg); err != nil {
		recordSpanError(span, err)
		return err
	}
	return nil
}

// cloudEventsProbe unmarshals just enough of a message to tell whether it
// is a CloudEvents 1.0 structured envelope: the flat envelope has no
// specversion key at all, so its presence (any non-empty value) is the
// dual-read discriminator ADR-0027/0021 specify. A malformed or
// unrecognized specversion (i.e. present but not "1.0") is treated as
// unhandleable and fails soft, mirroring this consumer's existing
// unparseable-message posture (log + commit, never redelivered forever).
type cloudEventsProbe struct {
	Specversion string `json:"specversion"`
}

// cloudEvent is the CloudEvents 1.0 structured envelope shape specified by
// fulfillment-execution's apis/asyncapi.yaml (ADR-0004, implemented by
// ADR-0027/0021). data is left as json.RawMessage — its shape
// (taskCompletedData) is byte-identical to the flat envelope's own data
// field, this migration is envelope-only.
type cloudEvent struct {
	Specversion     string          `json:"specversion"`
	Id              string          `json:"id"`
	Type            string          `json:"type"`
	Source          string          `json:"source"`
	Subject         string          `json:"subject"`
	Time            time.Time       `json:"time"`
	Datacontenttype string          `json:"datacontenttype"`
	Data            json.RawMessage `json:"data"`
}

// eventTypePrefix is the reverse-DNS prefix fulfillment-execution's
// CloudEvents `type` attribute carries ahead of the bare event name this
// consumer's switch/case logic keys on (e.g.
// "com.warehouse.wes.fulfillment-execution.task.TaskCompleted" ->
// "TaskCompleted"). Stripping is done by taking the last dot-delimited
// segment rather than hardcoding the whole prefix, so any bounded-context
// segment fulfillment-execution's asyncapi.yaml already documents (task.*
// or package.*) still resolves correctly without this consumer needing to
// know every segment value.
func bareEventType(cloudEventsType string) string {
	if idx := strings.LastIndex(cloudEventsType, "."); idx >= 0 {
		return cloudEventsType[idx+1:]
	}
	return cloudEventsType
}

// decodeEnvelope dual-reads a raw Kafka message value: a flat envelope
// (event_id/event_type/occurred_at/source/data) or a CloudEvents 1.0
// structured envelope (specversion/id/type/source/subject/time/data), per
// ADR-0027 (fulfillment-execution) / ADR-0021 (wes-work-planning) Phase 2
// Task 2d. Either shape normalizes to the same envelope.Envelope so the
// rest of this consumer (handleFulfillmentEvent and everything it calls)
// is completely unaware of which shape arrived on the wire.
func decodeEnvelope(raw []byte) (envelope.Envelope, error) {
	var probe cloudEventsProbe
	if err := json.Unmarshal(raw, &probe); err != nil {
		return envelope.Envelope{}, err
	}

	if probe.Specversion == "" {
		// No specversion key at all (or explicitly empty): the legacy
		// flat envelope shape.
		var flat envelope.Envelope
		if err := json.Unmarshal(raw, &flat); err != nil {
			return envelope.Envelope{}, err
		}
		return flat, nil
	}

	if probe.Specversion != "1.0" {
		return envelope.Envelope{}, fmt.Errorf("unrecognized CloudEvents specversion %q", probe.Specversion)
	}

	var ce cloudEvent
	if err := json.Unmarshal(raw, &ce); err != nil {
		return envelope.Envelope{}, err
	}
	return envelope.Envelope{
		EventId:    ce.Id,
		EventType:  bareEventType(ce.Type),
		OccurredAt: ce.Time,
		Source:     ce.Source,
		Data:       ce.Data,
	}, nil
}

// handleFulfillmentEvent filters for TaskCompleted and feeds it into the
// existing RecordTaskPerformance use case. Every other event type on this
// shared topic is silently skipped, not an error.
func (c *Consumer) handleFulfillmentEvent(ctx context.Context, env envelope.Envelope) error {
	if env.EventType != envelope.EventTypeTaskCompleted {
		return nil
	}

	var data taskCompletedData
	if err := json.Unmarshal(env.Data, &data); err != nil {
		return err
	}

	// TaskType now arrives on the wire as of fulfillment-execution's
	// ADR-0023 (this service's own gap was tracked in ADR-0014, since
	// closed). ParseTaskTypeLenient handles every case identically to
	// before: a recognized PICK/PACK/SLAM passes through, and an
	// unrecognized value (e.g. REBIN, which this service does not model
	// as an engineered-labor-standard task type) or an absent field
	// (an older fulfillment-execution payload, or the lookup-miss
	// degrade case that ADR documents) both still resolve to ""
	// (unclassified) — never a reason to reject a real completion.
	_, err := c.recordTaskPerformance.Execute(ctx, usecases.RecordTaskPerformanceRequest{
		KafkaEventId:  env.EventId,
		TaskId:        data.TaskId,
		AssociateId:   shared.AssociateId(data.AssociateId),
		TaskType:      shared.ParseTaskTypeLenient(data.TaskType),
		ActualSeconds: data.DurationSeconds,
		CompletedAt:   env.OccurredAt,
	})
	return err
}

// recordSpanError marks span as failed without changing any control flow.
func recordSpanError(span trace.Span, err error) {
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
}

// log emits a structured record through the configured logger, carrying
// the consume span's trace_id/span_id via ctx. A nil logger silences
// output, as the tests rely on.
func (c *Consumer) log(ctx context.Context, msg string, args ...any) {
	if c.logger != nil {
		c.logger.WarnContext(ctx, msg, args...)
	}
}
