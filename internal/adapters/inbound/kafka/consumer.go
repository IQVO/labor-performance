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
//
// As of ADR-0017 (Phase 2 resilience, mirroring order-management's
// RepromiseConsumer / ADR-0025 exactly), a message whose
// handleFulfillmentEvent call fails is retried in-process, with jittered
// exponential backoff, up to maxHandlerAttempts total attempts before
// being dead-lettered to topic+dlqTopicSuffix with the raw payload and
// error context — see handleMessage/handleWithRetry/dlqPublish below.
package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v4"
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

// dlqTopicSuffix names the dead-letter topic this consumer publishes a
// poison message to, relative to its OWN source topic (never a fixed
// constant): NewConsumerForTopic's isolated test topics each get their
// own matching "<topic>.dlq", mirroring order-management's
// RepromiseConsumer (ADR-0025) exactly.
const dlqTopicSuffix = ".dlq"

// maxHandlerAttempts bounds handleFulfillmentEvent's in-process retry
// before a message is dead-lettered: 1 initial attempt plus up to 2
// retries, matching the fleet plan's "up to 3" bound.
const maxHandlerAttempts = 3

const (
	retryInitialInterval = 100 * time.Millisecond
	retryMaxInterval     = 2 * time.Second
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
	// dlqWriter publishes a poison message (ADR-0017 §DLQ) to
	// topic+dlqTopicSuffix after maxHandlerAttempts in-process retries
	// of handleFulfillmentEvent all fail. nil in the zero-value struct
	// existing unit tests build directly (they call
	// handleFulfillmentEvent directly and never reach handleMessage's
	// DLQ path) — dlqPublish itself guards against a nil writer so
	// those tests keep compiling and passing unchanged.
	dlqWriter *kafkago.Writer
}

// NewConsumer constructs a Consumer reading TopicFulfillmentEvents on
// brokers under groupID.
func NewConsumer(brokers []string, groupID string, recordTaskPerformance *usecases.RecordTaskPerformance, logger *slog.Logger) *Consumer {
	return NewConsumerForTopic(brokers, groupID, envelope.TopicFulfillmentEvents, recordTaskPerformance, logger)
}

// NewConsumerForTopic constructs a Consumer reading topic on brokers under
// groupID. It supports isolated integration topics while NewConsumer retains
// the production fulfillment-events topic. The dead-letter topic is always
// derived as topic+dlqTopicSuffix, so an isolated test topic gets its own
// isolated DLQ topic for free.
func NewConsumerForTopic(brokers []string, groupID, topic string, recordTaskPerformance *usecases.RecordTaskPerformance, logger *slog.Logger) *Consumer {
	return &Consumer{
		reader: kafkago.NewReader(kafkago.ReaderConfig{
			Brokers: brokers,
			GroupID: groupID,
			Topic:   topic,
		}),
		recordTaskPerformance: recordTaskPerformance,
		logger:                logger,
		dlqWriter: &kafkago.Writer{
			Addr:  kafkago.TCP(brokers...),
			Topic: topic + dlqTopicSuffix,
		},
	}
}

// Close releases the underlying Kafka reader and, if configured, the DLQ
// writer.
func (c *Consumer) Close() error {
	readerErr := c.reader.Close()
	if c.dlqWriter == nil {
		return readerErr
	}
	return errors.Join(readerErr, c.dlqWriter.Close())
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
// An unparseable message is logged and committed rather than redelivered
// forever, mirroring this consumer's pre-existing convention.
//
// A handleFulfillmentEvent failure (ADR-0017 §DLQ, mirroring
// order-management's RepromiseConsumer/ADR-0025 exactly) is retried
// in-process, with jittered exponential backoff, up to
// maxHandlerAttempts total attempts — a transient blip (a momentary
// Postgres hiccup, a lost connection) heals itself without ever
// reaching the DLQ. Only once ALL attempts are exhausted does the
// message go to the dead-letter topic (topic+dlqTopicSuffix) with the
// raw payload and the last error's context, and the offset is committed
// anyway — one poison message must never block every task behind it on
// this partition. A commit failure, or a DLQ publish failure, is the
// only thing that still aborts the consume loop.
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
		return c.commit(ctx, msg)
	}

	span.SetAttributes(
		attribute.String("messaging.message.event_id", env.EventId),
		attribute.String("messaging.message.event_type", env.EventType),
		attribute.String("messaging.message.source", env.Source),
	)

	handleErr := c.handleWithRetry(msgCtx, env)
	if handleErr == nil {
		return c.commit(ctx, msg)
	}

	recordSpanError(span, handleErr)
	c.log(msgCtx, "exhausted retries, sending to dead-letter topic",
		"topic", topic, "dlq_topic", topic+dlqTopicSuffix,
		"event_id", env.EventId, "event_type", env.EventType, "attempts", maxHandlerAttempts, "error", handleErr)
	if dlqErr := c.dlqPublish(ctx, msg, handleErr); dlqErr != nil {
		return fmt.Errorf("kafka: publish to dead-letter topic: %w", dlqErr)
	}
	return c.commit(ctx, msg)
}

// handleWithRetry retries handleFulfillmentEvent up to maxHandlerAttempts
// times with jittered exponential backoff (ADR-0017 §DLQ), bounded by
// ctx's own deadline/cancellation.
func (c *Consumer) handleWithRetry(ctx context.Context, env envelope.Envelope) error {
	policy := backoff.NewExponentialBackOff(
		backoff.WithInitialInterval(retryInitialInterval),
		backoff.WithMaxInterval(retryMaxInterval),
	)
	bounded := backoff.WithContext(backoff.WithMaxRetries(policy, maxHandlerAttempts-1), ctx)

	return backoff.Retry(func() error {
		return c.handleFulfillmentEvent(ctx, env)
	}, bounded)
}

// dlqPublish writes the raw, unmodified message payload plus error
// context (as headers, so the raw body stays byte-identical for a
// manual replay tool) to the dead-letter topic. A nil dlqWriter (the
// zero-value Consumer several unit tests construct directly, which
// never exercises this path) is a documented no-op rather than a
// nil-pointer panic.
func (c *Consumer) dlqPublish(ctx context.Context, msg kafkago.Message, cause error) error {
	if c.dlqWriter == nil {
		return nil
	}
	headers := append([]kafkago.Header{}, msg.Headers...)
	headers = append(headers,
		kafkago.Header{Key: "x-dlq-source-topic", Value: []byte(c.reader.Config().Topic)},
		kafkago.Header{Key: "x-dlq-error", Value: []byte(cause.Error())},
		kafkago.Header{Key: "x-dlq-failed-at", Value: []byte(time.Now().UTC().Format(time.RFC3339))},
	)
	return c.dlqWriter.WriteMessages(ctx, kafkago.Message{
		Key:     msg.Key,
		Value:   msg.Value,
		Headers: headers,
	})
}

// commit acknowledges msg so it is never redelivered. Only a commit
// failure itself aborts the consume loop.
func (c *Consumer) commit(ctx context.Context, msg kafkago.Message) error {
	return c.reader.CommitMessages(ctx, msg)
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
