package kafka

import (
	"context"
	"sync"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/claudioed/labor-performance/internal/adapters/kafka/cloudevents"
)

// A projection failure must roll the consumer's idempotency claim back, so
// the retry re-applies the event instead of short-circuiting it as an
// already-processed duplicate (the original defect: claim committed first,
// failed apply never retried).
func TestAnalyticsConsumerFailedApplyRollsBackClaimAndRetryConverges(t *testing.T) {
	proj := &recordingProjection{failFirst: 1}
	gate := newFakeProcessedEvents()
	c := newAtomicTestConsumer(proj, gate)

	raw := analyticsMessage(t, "evt-1", cloudevents.TypeTaskPerformanceRecorded, ts(9, 0), map[string]any{
		"task_type": "PICK", "actual_seconds": 45, "completed_at": ts(9, 0),
	})
	ctx := context.Background()

	if err := c.HandleMessage(ctx, raw); err == nil {
		t.Fatal("a failed projection must surface as an error so Run retries the message")
	}
	if _, claimed := gate.seen["evt-1"]; claimed {
		t.Fatal("the claim survived a failed apply; the redelivery would be skipped as a duplicate and the event lost")
	}
	if len(proj.tasks) != 0 {
		t.Fatalf("a failed apply recorded %d tasks, want 0", len(proj.tasks))
	}

	if err := c.HandleMessage(ctx, raw); err != nil {
		t.Fatalf("retry after the transient failure: %v", err)
	}
	if len(proj.tasks) != 1 {
		t.Fatalf("after the retry the projection holds %d tasks, want exactly 1", len(proj.tasks))
	}
	if _, claimed := gate.seen["evt-1"]; !claimed {
		t.Fatal("the successful retry must claim the event")
	}

	// A further redelivery of the now-applied event is a no-op.
	if err := c.HandleMessage(ctx, raw); err != nil {
		t.Fatal(err)
	}
	if len(proj.tasks) != 1 {
		t.Fatalf("duplicate delivery double-counted: %d tasks", len(proj.tasks))
	}
}

// fakeReader serves a fixed list of messages and records commits.
type fakeReader struct {
	mu       sync.Mutex
	msgs     []kafkago.Message
	next     int
	commits  []kafkago.Message
	onCommit func(kafkago.Message)
}

func (r *fakeReader) FetchMessage(ctx context.Context) (kafkago.Message, error) {
	r.mu.Lock()
	if r.next < len(r.msgs) {
		m := r.msgs[r.next]
		r.next++
		r.mu.Unlock()
		return m, nil
	}
	r.mu.Unlock()
	<-ctx.Done()
	return kafkago.Message{}, ctx.Err()
}

func (r *fakeReader) CommitMessages(_ context.Context, msgs ...kafkago.Message) error {
	r.mu.Lock()
	r.commits = append(r.commits, msgs...)
	r.mu.Unlock()
	if r.onCommit != nil {
		for _, m := range msgs {
			r.onCommit(m)
		}
	}
	return nil
}

func (r *fakeReader) Close() error { return nil }

func (r *fakeReader) commitCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.commits)
}

// Handler fails twice then succeeds: the SAME message is retried, and the
// offset is committed exactly once, after the success — never on a failure.
func TestAnalyticsConsumerRunRetriesTransientFailureThenCommitsOnce(t *testing.T) {
	proj := &recordingProjection{failFirst: 2}
	gate := newFakeProcessedEvents()
	c := newAtomicTestConsumer(proj, gate)
	c.retryInitial, c.retryMax = time.Millisecond, 2*time.Millisecond

	committed := make(chan struct{})
	var appliedAtCommit int
	reader := &fakeReader{
		msgs: []kafkago.Message{{Offset: 7, Value: analyticsMessage(t, "evt-1", cloudevents.TypeTaskPerformanceRecorded, ts(9, 0), map[string]any{
			"task_type": "PICK", "actual_seconds": 45, "completed_at": ts(9, 0),
		})}},
	}
	reader.onCommit = func(kafkago.Message) {
		appliedAtCommit = len(proj.tasks)
		close(committed)
	}
	c.Reader = reader

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()

	select {
	case <-committed:
	case <-time.After(5 * time.Second):
		t.Fatal("the message was never committed after the transient failures cleared")
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}

	if proj.callCount != 3 {
		t.Errorf("projection attempts = %d, want 3 (two failures + one success)", proj.callCount)
	}
	if got := reader.commitCount(); got != 1 {
		t.Errorf("commits = %d, want exactly 1", got)
	}
	if appliedAtCommit != 1 {
		t.Errorf("tasks applied at commit time = %d, want 1: the offset must be committed only AFTER a successful apply", appliedAtCommit)
	}
}

// A handler that keeps failing never commits: cancelling the context
// leaves the offset uncommitted so the message is redelivered.
func TestAnalyticsConsumerRunNeverCommitsAFailingMessage(t *testing.T) {
	proj := &recordingProjection{failFirst: 1 << 30}
	gate := newFakeProcessedEvents()
	c := newAtomicTestConsumer(proj, gate)
	c.retryInitial, c.retryMax = time.Millisecond, 2*time.Millisecond
	reader := &fakeReader{
		msgs: []kafkago.Message{{Offset: 1, Value: analyticsMessage(t, "evt-1", cloudevents.TypeTaskPerformanceRecorded, ts(9, 0), map[string]any{
			"task_type": "PICK", "completed_at": ts(9, 0),
		})}},
	}
	c.Reader = reader

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()

	time.Sleep(50 * time.Millisecond)
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got := reader.commitCount(); got != 0 {
		t.Errorf("commits = %d, want 0: a message that never applied must not be committed", got)
	}
	if len(gate.seen) != 0 {
		t.Errorf("claims left behind by failed applies: %v", gate.seen)
	}
}

// Deterministic bad input (not a CloudEvent) is committed past, not retried.
func TestAnalyticsConsumerRunCommitsPastInvalidCloudEvent(t *testing.T) {
	proj := &recordingProjection{}
	c := newAtomicTestConsumer(proj, newFakeProcessedEvents())
	c.retryInitial, c.retryMax = time.Millisecond, 2*time.Millisecond

	committed := make(chan struct{})
	reader := &fakeReader{msgs: []kafkago.Message{{Offset: 1, Value: []byte("{not json")}}}
	reader.onCommit = func(kafkago.Message) { close(committed) }
	c.Reader = reader

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()

	select {
	case <-committed:
	case <-time.After(5 * time.Second):
		t.Fatal("an invalid CloudEvent was not committed past")
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
	if proj.callCount != 0 {
		t.Errorf("projection called %d times for an invalid message", proj.callCount)
	}
}
