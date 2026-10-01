package kafka

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	kafkago "github.com/segmentio/kafka-go"
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

type scriptedDLQWriter struct {
	errs  []error
	calls int
}

func (w *scriptedDLQWriter) WriteMessages(_ context.Context, _ ...kafkago.Message) error {
	w.calls++
	if len(w.errs) == 0 {
		return nil
	}
	err := w.errs[0]
	w.errs = w.errs[1:]
	return err
}

func TestWriteDLQ_RetriesOnlyWhileTheTopicIsBeingCreated(t *testing.T) {
	boom := errors.New("broker on fire")
	tests := []struct {
		name      string
		errs      []error
		wantErr   error
		wantCalls int
	}{
		{"first write succeeds", nil, nil, 1},
		{"unknown topic then success", []error{kafkago.UnknownTopicOrPartition, kafkago.LeaderNotAvailable}, nil, 3},
		{"per-message write errors that are all not-ready are retried", []error{kafkago.WriteErrors{kafkago.UnknownTopicOrPartition}}, nil, 2},
		{"other errors are returned immediately", []error{boom}, boom, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &scriptedDLQWriter{errs: tt.errs}
			err := writeDLQ(context.Background(), w, kafkago.Message{})
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if w.calls != tt.wantCalls {
				t.Fatalf("calls = %d, want %d", w.calls, tt.wantCalls)
			}
		})
	}
}

func TestWriteDLQ_GivesUpWhenContextIsCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	w := &scriptedDLQWriter{errs: []error{kafkago.UnknownTopicOrPartition, kafkago.UnknownTopicOrPartition}}
	if err := writeDLQ(ctx, w, kafkago.Message{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
}
