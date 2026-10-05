package kafka

import (
	"context"
	"fmt"

	kafkago "github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel/codes"

	"github.com/claudioed/labor-performance/internal/adapters/kafka/otelkafka"
)

// RelaySink is where the transactional outbox relay (ADR 0010) forwards
// already-encoded messages. It wraps a topic-less kafka-go Writer and
// sets each message's Topic from Encoded.Topic, so one sink can serve
// every topic the outbox carries — both the analytics topic (ADR 0007)
// and the integration topic (ADR 0013) — without the sink needing to
// know the list.
type RelaySink struct {
	Writer Writer
}

// NewRelaySink constructs a RelaySink over brokers. The writer
// deliberately has NO Topic: kafka-go rejects a per-message Topic when
// the Writer also has one, and the relay must set the topic per message.
//
// Balancer is kafkago.Hash, matching every other writer in this package
// (see NewIntegrationPublisher's doc comment): the relay forwards
// already-encoded messages whose Key was set at Encode time by the
// outbox path, and LeastBytes would silently ignore that key for
// partition routing. See ADR 0018, order-management PR #111, and
// warehouse-infra PR #42.
func NewRelaySink(brokers []string) *RelaySink {
	return &RelaySink{
		Writer: &kafkago.Writer{
			BatchTimeout:           syncWriterBatchTimeout,
			RequiredAcks:           syncWriterRequiredAcks,
			Addr:                   kafkago.TCP(brokers...),
			Balancer:               &kafkago.Hash{},
			AllowAutoTopicCreation: true,
		},
	}
}

// Send writes msgs to their respective topics in one WriteMessages call,
// preserving the order given. The stored headers (the originating
// request's trace context, captured at Encode time) travel unchanged;
// the relay's own publish span is additionally recorded so a broker
// failure is visible on the trace of the pass that hit it.
func (s *RelaySink) Send(ctx context.Context, msgs ...Encoded) error {
	if len(msgs) == 0 {
		return nil
	}
	out := make([]kafkago.Message, 0, len(msgs))
	for _, m := range msgs {
		out = append(out, kafkago.Message{
			Topic:   m.Topic,
			Key:     m.Key,
			Value:   m.Value,
			Headers: append([]kafkago.Header{}, m.Headers...),
		})
	}

	ctx, span := otelkafka.StartPublishSpan(ctx, msgs[0].Topic)
	defer span.End()

	if err := s.Writer.WriteMessages(ctx, out...); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf("kafka: relay %d outbox message(s) starting with %s: %w", len(msgs), msgs[0].EventType, err)
	}
	return nil
}

// Close releases the underlying Kafka writer, if it is a real one.
func (s *RelaySink) Close() error {
	if w, ok := s.Writer.(*kafkago.Writer); ok {
		return w.Close()
	}
	return nil
}
