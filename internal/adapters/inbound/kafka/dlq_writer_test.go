package kafka

import (
	"io"
	"log/slog"
	"testing"
)

// TestNewConsumerForTopic_DLQWriterAutoCreatesTopic pins the dead-letter
// writer config: a missing "<topic>.dlq" must be auto-created on first
// publish (fleet convention), not fail and stop the consumer.
func TestNewConsumerForTopic_DLQWriterAutoCreatesTopic(t *testing.T) {
	c := NewConsumerForTopic([]string{"localhost:9092"}, "g", "warehouse.fulfillment.events", nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	t.Cleanup(func() { _ = c.Close() })
	if c.dlqWriter.Topic != "warehouse.fulfillment.events.dlq" {
		t.Fatalf("DLQ topic = %q", c.dlqWriter.Topic)
	}
	if !c.dlqWriter.AllowAutoTopicCreation {
		t.Fatal("DLQ writer must set AllowAutoTopicCreation")
	}
}
