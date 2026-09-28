// Package kafka holds this service's outbound Kafka adapters: the
// ANALYTICS publisher, which fans this service's own past-tense domain
// events onto warehouse.labor-performance.analytics, feeding the data
// product's own projector (ADR-0007); and the INTEGRATION publisher
// (ADR-0013), which publishes TaskPerformanceRecorded onto
// warehouse.labor-performance.events, this service's first Published
// Language for other bounded contexts to consume.
//
// It is strictly ADDITIVE. The OLTP write path is untouched: the domain
// and application layers still publish through the same
// ports.EventPublisher they always have, the log publisher still exists
// and is still the default, and no existing consumer of any other topic
// is affected.
//
// Since ADR 0010 (transactional outbox) the publisher is split in two
// halves: Encode turns domain events into wire-ready messages and Publish
// writes them. In the cluster the OLTP process never calls Publish — the
// postgres.OutboxPublisher calls Encode inside the use case's transaction
// and stores the result, and the postgres.OutboxRelay later hands the
// stored messages to a RelaySink.
package kafka

import (
	"context"
	"encoding/json"
	"fmt"

	kafkago "github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel/codes"

	"github.com/claudioed/labor-performance/internal/adapters/kafka/envelope"
	"github.com/claudioed/labor-performance/internal/adapters/kafka/otelkafka"
	"github.com/claudioed/labor-performance/internal/application/ports"
	"github.com/claudioed/labor-performance/internal/domain/shared"
)

// Writer is the slice of kafka-go's *Writer this adapter needs, so tests
// can substitute a recording fake without a broker.
type Writer interface {
	WriteMessages(ctx context.Context, msgs ...kafkago.Message) error
}

// Encoded is one wire-ready Kafka message. Topic is carried explicitly —
// NOT on a kafkago.Message — because kafka-go rejects a message with its
// own Topic when the Writer also has a fixed Topic, and vice versa: the
// AnalyticsPublisher's writer pins the topic, while the RelaySink's
// writer has none and applies Encoded.Topic per message.
type Encoded struct {
	Topic     string
	EventType string
	Key       []byte
	Value     []byte
	Headers   []kafkago.Header
}

// Encoder turns domain events into their Kafka wire form without sending
// them. The transactional outbox (ADR 0010) stores what Encode returns
// and replays it later, byte-for-byte, so the outbox and the direct
// publish path can never disagree on wire format.
type Encoder interface {
	Encode(ctx context.Context, events ...shared.DomainEvent) ([]Encoded, error)
}

// AnalyticsPublisher publishes each labor-performance domain event onto
// envelope.TopicLaborPerformanceAnalytics as an
// envelope.AnalyticsEnvelope. It satisfies ports.EventPublisher, so the
// composition root can hand it the same event stream the log publisher
// gets.
//
// Unlike order-management's equivalent adapter, this one performs NO
// repository enrichment: every field the report is keyed or aggregated by
// (TaskType, EfficiencyPct, ActualSeconds, CompletedAt, EffectiveFrom) is
// already carried on the domain events themselves, so the publisher stays
// a pure serializer with no read-back into the OLTP store.
type AnalyticsPublisher struct {
	Writer Writer
	// NewID mints the envelope's event_id. It is the projection's
	// idempotency key, so it must be unique per published message —
	// notably NOT the TaskId, which CLAUDE.md explicitly rules out as a
	// dedup key.
	NewID func() string
}

// NewAnalyticsPublisher constructs an AnalyticsPublisher writing to the
// analytics topic on brokers. newID mints each envelope's event_id.
//
// Balancer is kafkago.Hash, matching IntegrationPublisher's choice (see
// its doc comment): this publisher already keys every message by
// TaskType (marshalData), but LeastBytes would have silently discarded
// that key for partition routing — Hash is what actually turns the key
// into a same-TaskType-same-partition guarantee. See ADR 0018,
// order-management PR #111, and warehouse-infra PR #42.
func NewAnalyticsPublisher(brokers []string, newID func() string) *AnalyticsPublisher {
	return &AnalyticsPublisher{
		Writer: &kafkago.Writer{
			Addr:                   kafkago.TCP(brokers...),
			Topic:                  envelope.TopicLaborPerformanceAnalytics,
			Balancer:               &kafkago.Hash{},
			AllowAutoTopicCreation: true,
		},
		NewID: newID,
	}
}

// Publish emits every event onto the analytics topic. An event type
// outside the analytics contract is skipped rather than erroring, so the
// caller can hand it the full event stream indiscriminately and a future
// domain event needs no change here to be safely ignored.
func (p *AnalyticsPublisher) Publish(ctx context.Context, events ...shared.DomainEvent) error {
	for _, event := range events {
		msgs, err := p.Encode(ctx, event)
		if err != nil {
			return err
		}
		for _, msg := range msgs {
			if err := p.write(ctx, msg); err != nil {
				return err
			}
		}
	}
	return nil
}

// Encode maps every event to its analytics message — envelope, partition
// key and the W3C trace headers of the span active in ctx — without
// writing anything. Events outside the analytics contract produce no
// message. The returned Encoded carry Topic set to the analytics topic
// and a nil kafkago.Message.Topic (see Encoded).
//
// The trace context is captured HERE, not at send time, because in the
// outbox mode the send happens later on the relay goroutine, which has
// no request span of its own: injecting now is what lets the projector's
// consume span still become a child of the originating request.
func (p *AnalyticsPublisher) Encode(ctx context.Context, events ...shared.DomainEvent) ([]Encoded, error) {
	out := make([]Encoded, 0, len(events))
	for _, event := range events {
		eventType, key, data, ok := marshalData(event)
		if !ok {
			continue
		}

		payload, err := json.Marshal(envelope.AnalyticsEnvelope{
			EventId:       p.newID(),
			EventType:     eventType,
			OccurredAt:    event.OccurredAt(),
			Source:        envelope.Source,
			SchemaVersion: envelope.AnalyticsSchemaVersion,
			Data:          data,
		})
		if err != nil {
			return nil, fmt.Errorf("kafka: marshal analytics envelope: %w", err)
		}
		headers := []kafkago.Header{}
		otelkafka.Inject(ctx, &headers)
		out = append(out, Encoded{
			Topic:     envelope.TopicLaborPerformanceAnalytics,
			EventType: eventType,
			Key:       []byte(key),
			Value:     payload,
			Headers:   headers,
		})
	}
	return out, nil
}

// newID mints an envelope event id, returning "" only when no generator
// was injected (which never happens in wiring, but keeps the zero value
// usable).
func (p *AnalyticsPublisher) newID() string {
	if p.NewID == nil {
		return ""
	}
	return p.NewID()
}

// marshalData maps a domain event to its analytics event_type, partition
// key, and snake_case JSON payload. The bool return is false for an event
// type outside the analytics contract, so Encode can skip it.
//
// The partition key is the TaskType for every event: it keeps all the
// events that fold into one report dimension on a single partition, so
// the projector applies them in the order they were published.
func marshalData(e shared.DomainEvent) (eventType, key string, data json.RawMessage, ok bool) {
	switch ev := e.(type) {
	case shared.LaborStandardDefined:
		fields := map[string]any{
			"standard_id":      string(ev.StandardId),
			"task_type":        string(ev.TaskType),
			"expected_seconds": ev.ExpectedSeconds,
			"effective_from":   ev.EffectiveFrom,
		}
		if ev.TravelComponentSeconds != nil {
			fields["travel_component_seconds"] = *ev.TravelComponentSeconds
		}
		return envelope.EventTypeLaborStandardDefined, string(ev.TaskType), mustMarshal(fields), true

	case shared.LaborStandardRevised:
		fields := map[string]any{
			"standard_id":               string(ev.StandardId),
			"task_type":                 string(ev.TaskType),
			"previous_expected_seconds": ev.PreviousExpectedSeconds,
			"expected_seconds":          ev.NewExpectedSeconds,
			"effective_from":            ev.EffectiveFrom,
		}
		if ev.NewTravelComponentSeconds != nil {
			fields["travel_component_seconds"] = *ev.NewTravelComponentSeconds
		}
		return envelope.EventTypeLaborStandardRevised, string(ev.TaskType), mustMarshal(fields), true

	case shared.TaskPerformanceRecorded:
		return envelope.EventTypeTaskPerformanceRecorded, string(ev.TaskType), mustMarshal(map[string]any{
			"task_id":      ev.TaskId,
			"associate_id": string(ev.AssociateId),
			"task_type":    string(ev.TaskType),
			// A nil EfficiencyPct marshals to JSON null, which the
			// projector decodes back to nil — the "unscorable" fact
			// travels over the wire intact rather than degrading to 0.
			"efficiency_pct": ev.EfficiencyPct,
			"actual_seconds": ev.ActualSeconds,
			// A nil IdleSecondsBefore likewise marshals to JSON null
			// — "not observed" (no prior completion, an out-of-order
			// gap, or an unattributed/robot-station task), never a
			// fabricated 0.
			"idle_seconds_before": ev.IdleSecondsBefore,
			"completed_at":        ev.CompletedAt,
		}), true

	default:
		return "", "", nil, false
	}
}

// mustMarshal marshals a map whose shape is fully controlled by
// marshalData, so an error here would be a programming mistake rather
// than a runtime condition.
func mustMarshal(v any) json.RawMessage {
	b, err := json.Marshal(v)
	if err != nil {
		panic(fmt.Sprintf("kafka: marshal analytics data: %v", err))
	}
	return b
}

// write publishes one already-encoded message inside a producer span,
// injecting that span's context into the message headers so the
// projector's consume span becomes its child.
func (p *AnalyticsPublisher) write(ctx context.Context, msg Encoded) error {
	ctx, span := otelkafka.StartPublishSpan(ctx, envelope.TopicLaborPerformanceAnalytics)
	defer span.End()

	headers := append([]kafkago.Header{}, msg.Headers...)
	otelkafka.Inject(ctx, &headers)

	if err := p.Writer.WriteMessages(ctx, kafkago.Message{
		Key:     msg.Key,
		Value:   msg.Value,
		Headers: headers,
	}); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf("kafka: publish %s analytics event: %w", msg.EventType, err)
	}
	return nil
}

// Close releases the underlying Kafka writer, if it is a real one.
func (p *AnalyticsPublisher) Close() error {
	if w, ok := p.Writer.(*kafkago.Writer); ok {
		return w.Close()
	}
	return nil
}

// Compile-time assertions that AnalyticsPublisher satisfies the outbound
// event-publishing port and the outbox's Encoder.
var (
	_ ports.EventPublisher = (*AnalyticsPublisher)(nil)
	_ Encoder              = (*AnalyticsPublisher)(nil)
)
