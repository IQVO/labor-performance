package kafka

import (
	"context"
	"fmt"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/claudioed/labor-performance/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/labor-performance/internal/adapters/kafka/otelkafka"
	"github.com/claudioed/labor-performance/internal/domain/shared"
)

// dataBuilder maps a domain event to one stream's event name, `type`
// entity segment, partition key, CloudEvents subject and snake_case JSON
// payload; ok is false for an event outside that stream's contract.
// marshalData (analytics) and integrationData (integration) are the two.
type dataBuilder func(shared.DomainEvent) (eventName, entity, key, subject string, data map[string]any, ok bool)

// streamEncoding is everything that differs between the analytics and the
// integration copy of an event: the dataschema stream segment, the topic,
// the payload builder and the label used in error messages. The envelope,
// the headers and the trace capture are the same for both (ADR 0021).
type streamEncoding struct {
	stream string // cloudevents.StreamAnalytics | cloudevents.StreamEvents
	topic  string
	label  string
	build  dataBuilder
}

// encodeStream maps every event of the stream's contract to its message —
// CloudEvent, partition key, the CloudEvents content-type header and the
// W3C trace headers of the span active in ctx — without writing anything.
// Events outside the contract produce no message. newID mints each
// CloudEvent's id.
func encodeStream(ctx context.Context, s streamEncoding, newID func() string, events []shared.DomainEvent) ([]Encoded, error) {
	out := make([]Encoded, 0, len(events))
	for _, event := range events {
		eventType, entity, key, subject, data, ok := s.build(event)
		if !ok {
			continue
		}

		payload, err := cloudevents.New(cloudevents.Spec{
			ID:        newID(),
			Entity:    entity,
			EventName: eventType,
			Subject:   subject,
			Time:      event.OccurredAt(),
			Stream:    s.stream,
			Version:   1,
			Data:      data,
		})
		if err != nil {
			return nil, fmt.Errorf("kafka: encode %s cloudevent: %w", s.label, err)
		}
		headers := []kafkago.Header{cloudevents.ContentTypeHeader()}
		otelkafka.Inject(ctx, &headers)
		out = append(out, Encoded{
			Topic:     s.topic,
			EventType: cloudevents.Type(entity, eventType),
			Key:       []byte(key),
			Value:     payload,
			Headers:   headers,
		})
	}
	return out, nil
}
