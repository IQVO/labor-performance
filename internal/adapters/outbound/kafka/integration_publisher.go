package kafka

import (
	"context"
	"fmt"

	kafkago "github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel/codes"

	"github.com/claudioed/labor-performance/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/labor-performance/internal/adapters/kafka/otelkafka"
	"github.com/claudioed/labor-performance/internal/application/ports"
	"github.com/claudioed/labor-performance/internal/domain/shared"
)

// IntegrationPublisher publishes this service's TaskPerformanceRecorded
// domain event onto cloudevents.TopicLaborPerformanceEvents — the
// integration topic added by ADR 0013 — as a CloudEvents 1.0 event in
// structured content mode (ADR 0021), type
// cloudevents.TypeTaskPerformanceRecorded and dataschema
// urn:warehouse:labor-performance:events:TaskPerformanceRecorded:v1.
//
// Only TaskPerformanceRecorded is part of the integration contract today.
// LaborStandardDefined/LaborStandardRevised stay analytics-only — a
// consumer needing them can be added to this publisher's marshalData in
// a later, additive change; ADR 0013 explains why the first cut is
// scoped to TaskPerformanceRecorded.
//
// It satisfies ports.EventPublisher, so the composition root can hand it
// the same event stream the log and analytics publishers get, and it
// satisfies the kafka.Encoder used by the transactional outbox (ADR
// 0010) — the exact pattern AnalyticsPublisher already follows.
type IntegrationPublisher struct {
	Writer Writer
	// NewID mints the CloudEvents `id`. It is the downstream consumer's
	// idempotency key, so it must be unique per published message; it is
	// minted once at Encode time and persisted with the outbox row, so a
	// relay redelivery carries the same id.
	NewID func() string
}

// NewIntegrationPublisher constructs an IntegrationPublisher writing to
// the integration topic on brokers. newID mints each CloudEvent's id.
//
// Balancer is kafkago.Hash (FNV-1a over Message.Key), not LeastBytes:
// kafka-go's Writer does not hash Message.Key into a partition decision
// just because a message carries one — the Balancer alone decides
// partition placement, and LeastBytes routes purely by cumulative byte
// volume, ignoring Key entirely. This publisher has always set Key to
// the event's AssociateId (see integrationData), but that only achieves
// per-associate partition affinity, and therefore in-order delivery to a
// single-partition consumer, when the Balancer is itself key-aware. See
// ADR 0018, order-management PR #111, and warehouse-infra PR #42 (the
// 1->8 partition scaleup that exposed this).
func NewIntegrationPublisher(brokers []string, newID func() string) *IntegrationPublisher {
	return &IntegrationPublisher{
		Writer: &kafkago.Writer{
			BatchTimeout:           syncWriterBatchTimeout,
			Addr:                   kafkago.TCP(brokers...),
			Topic:                  cloudevents.TopicLaborPerformanceEvents,
			Balancer:               &kafkago.Hash{},
			AllowAutoTopicCreation: true,
		},
		NewID: newID,
	}
}

// Publish emits every TaskPerformanceRecorded event onto the integration
// topic. Any other event type is skipped rather than erroring, so the
// caller can hand it the full event stream indiscriminately.
func (p *IntegrationPublisher) Publish(ctx context.Context, events ...shared.DomainEvent) error {
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

// Encode maps every TaskPerformanceRecorded event to its integration
// message — CloudEvent, partition key, the CloudEvents content-type
// header and the W3C trace headers of the span active in ctx — without
// writing anything. Every other event type
// produces no message. The trace context is captured HERE, not at send
// time, for the same reason AnalyticsPublisher.Encode does: in outbox
// mode the send happens later on the relay goroutine, which has no
// request span of its own.
func (p *IntegrationPublisher) Encode(ctx context.Context, events ...shared.DomainEvent) ([]Encoded, error) {
	out := make([]Encoded, 0, len(events))
	for _, event := range events {
		eventType, key, subject, data, ok := integrationData(event)
		if !ok {
			continue
		}

		payload, err := cloudevents.New(cloudevents.Spec{
			ID:        p.newID(),
			Entity:    cloudevents.EntityPerformance,
			EventName: eventType,
			Subject:   subject,
			Time:      event.OccurredAt(),
			Stream:    cloudevents.StreamEvents,
			Version:   1,
			Data:      data,
		})
		if err != nil {
			return nil, fmt.Errorf("kafka: encode integration cloudevent: %w", err)
		}
		headers := []kafkago.Header{cloudevents.ContentTypeHeader()}
		otelkafka.Inject(ctx, &headers)
		out = append(out, Encoded{
			Topic:     cloudevents.TopicLaborPerformanceEvents,
			EventType: cloudevents.Type(cloudevents.EntityPerformance, eventType),
			Key:       []byte(key),
			Value:     payload,
			Headers:   headers,
		})
	}
	return out, nil
}

// newID mints a CloudEvent id, returning "" only when no generator
// was injected (which never happens in wiring, but keeps the zero value
// usable).
func (p *IntegrationPublisher) newID() string {
	if p.NewID == nil {
		return ""
	}
	return p.NewID()
}

// integrationData maps a domain event to its integration event name,
// partition key, CloudEvents subject, and snake_case JSON payload. The
// bool return is false for an event type outside the integration
// contract, so Encode can skip it.
//
// The partition key is the AssociateId — not the TaskType the analytics
// stream keys on — because the intended first consumer (workforce-
// management, ADR 0013) builds a per-associate read cache and needs
// every event for one associate applied in publish order on a single
// partition. A robot-station task with no associate keys on the empty
// string, which is a legitimate, expected key (every such task lands on
// the same partition as every other unattributed one; still in order
// relative to each other). The subject is performanceSubject (the
// associate id, falling back to the task id when there is no associate,
// since a CloudEvents subject is never empty in this fleet).
func integrationData(e shared.DomainEvent) (eventName, key, subject string, data map[string]any, ok bool) {
	switch ev := e.(type) {
	case shared.TaskPerformanceRecorded:
		return cloudevents.EventTaskPerformanceRecorded, string(ev.AssociateId), performanceSubject(ev), map[string]any{
			"task_id":      ev.TaskId,
			"associate_id": string(ev.AssociateId),
			"task_type":    string(ev.TaskType),
			// A nil EfficiencyPct marshals to JSON null, which a
			// consumer must not coerce to 0 — the "unscorable" fact
			// travels over the wire intact, exactly as it does on the
			// analytics stream.
			"efficiency_pct": ev.EfficiencyPct,
			"actual_seconds": ev.ActualSeconds,
			// A nil IdleSecondsBefore likewise marshals to JSON null
			// — "not observed", never a fabricated 0. ADDITIVE field:
			// a Conformist unmarshaling unknown-field-tolerant JSON
			// (e.g. workforce-management's laborperformancecache) is
			// unaffected by its presence.
			"idle_seconds_before": ev.IdleSecondsBefore,
			"completed_at":        ev.CompletedAt,
		}, true

	default:
		return "", "", "", nil, false
	}
}

// performanceSubject is the CloudEvents subject of a
// TaskPerformanceRecorded: the associate the performance belongs to (the
// same id the integration stream keys on), or — for an unattributed
// robot-station task with no associate — the task id, so the subject is
// never empty.
func performanceSubject(ev shared.TaskPerformanceRecorded) string {
	if ev.AssociateId != "" {
		return string(ev.AssociateId)
	}
	return ev.TaskId
}

// write publishes one already-encoded message inside a producer span,
// injecting that span's context into the message headers so a
// downstream consumer's span becomes its child.
func (p *IntegrationPublisher) write(ctx context.Context, msg Encoded) error {
	ctx, span := otelkafka.StartPublishSpan(ctx, cloudevents.TopicLaborPerformanceEvents)
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
		return fmt.Errorf("kafka: publish %s integration event: %w", msg.EventType, err)
	}
	return nil
}

// Close releases the underlying Kafka writer, if it is a real one.
func (p *IntegrationPublisher) Close() error {
	if w, ok := p.Writer.(*kafkago.Writer); ok {
		return w.Close()
	}
	return nil
}

// Compile-time assertions that IntegrationPublisher satisfies the
// outbound event-publishing port and the outbox's Encoder.
var (
	_ ports.EventPublisher = (*IntegrationPublisher)(nil)
	_ Encoder              = (*IntegrationPublisher)(nil)
)
