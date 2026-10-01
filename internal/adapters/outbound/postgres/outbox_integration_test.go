//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/claudioed/labor-performance/internal/adapters/kafka/cloudevents"
	outboundkafka "github.com/claudioed/labor-performance/internal/adapters/outbound/kafka"
	"github.com/claudioed/labor-performance/internal/adapters/outbound/memory"
	"github.com/claudioed/labor-performance/internal/adapters/outbound/postgres"
	"github.com/claudioed/labor-performance/internal/application/usecases"
	"github.com/claudioed/labor-performance/internal/domain/shared"
)

// outboxDB boots a throwaway Postgres (testcontainers — the test owns its
// own database, never an external DATABASE_URL) and runs the OLTP
// migrations against it.
func outboxDB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()
	container, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("labor"),
		tcpostgres.WithUsername("labor"),
		tcpostgres.WithPassword("labor"),
		tcpostgres.BasicWaitStrategies(),
	)
	if err != nil {
		t.Fatalf("start postgres container: %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(container) })

	url, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("connection string: %v", err)
	}
	if err := postgres.RunMigrations(url, migrationsDir(t)); err != nil {
		t.Fatalf("run migrations: %v", err)
	}
	pool, err := postgres.NewPool(ctx, url)
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// recordingSink records what the relay hands it and can be told to fail
// on one specific partition key.
type recordingSink struct {
	sent    []outboundkafka.Encoded
	failOn  string // Key to fail on, "" for never
	failErr error
}

func (s *recordingSink) Send(_ context.Context, msgs ...outboundkafka.Encoded) error {
	for _, m := range msgs {
		if s.failOn != "" && string(m.Key) == s.failOn {
			return s.failErr
		}
		s.sent = append(s.sent, m)
	}
	return nil
}

// failingEncoder stands in for a broken wire encoder, so the outbox
// insert fails INSIDE the use case's transaction.
type failingEncoder struct{ err error }

func (f failingEncoder) Encode(context.Context, ...shared.DomainEvent) ([]outboundkafka.Encoded, error) {
	return nil, f.err
}

func countOutbox(t *testing.T, pool *pgxpool.Pool, where string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), "SELECT count(*) FROM outbox_events WHERE "+where).Scan(&n); err != nil {
		t.Fatalf("count outbox: %v", err)
	}
	return n
}

func countRows(t *testing.T, pool *pgxpool.Pool, table, where string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), fmt.Sprintf("SELECT count(*) FROM %s WHERE %s", table, where)).Scan(&n); err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	return n
}

func analyticsEncoder() *outboundkafka.AnalyticsPublisher {
	return &outboundkafka.AnalyticsPublisher{NewID: uuid.NewString}
}

func integrationEncoder() *outboundkafka.IntegrationPublisher {
	return &outboundkafka.IntegrationPublisher{NewID: uuid.NewString}
}

func TestOutbox_DefineStandard_CommitsAggregateAndEventTogether(t *testing.T) {
	pool := outboxDB(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	standards := postgres.NewStandardRepo(pool)
	uc := &usecases.DefineStandard{
		Standards:  standards,
		Events:     postgres.NewOutboxPublisher(pool, analyticsEncoder()),
		Clock:      memory.FixedClock{At: now},
		UnitOfWork: postgres.NewUnitOfWork(pool),
	}

	if _, err := uc.Execute(ctx, shared.Pick, 45, nil); err != nil {
		t.Fatalf("define: %v", err)
	}
	// Revise: closes the prior and inserts a new one in the same scope.
	uc.Clock = memory.FixedClock{At: now.Add(time.Minute)}
	if _, err := uc.Execute(ctx, shared.Pick, 40, nil); err != nil {
		t.Fatalf("revise: %v", err)
	}

	if got := countRows(t, pool, "labor_standards", "task_type = 'PICK'"); got != 2 {
		t.Fatalf("expected 2 labor_standards rows for PICK (closed prior + active), got %d", got)
	}
	if got := countOutbox(t, pool, fmt.Sprintf("published_at IS NULL AND topic = '%s' AND event_type = '%s' AND key = 'PICK'", cloudevents.TopicLaborPerformanceAnalytics, cloudevents.TypeLaborStandardDefined)); got != 1 {
		t.Fatalf("expected 1 unpublished LaborStandardDefined row keyed PICK, got %d", got)
	}
	if got := countOutbox(t, pool, fmt.Sprintf("published_at IS NULL AND event_type = '%s' AND key = 'PICK'", cloudevents.TypeLaborStandardRevised)); got != 1 {
		t.Fatalf("expected 1 unpublished LaborStandardRevised row keyed PICK, got %d", got)
	}
	var headers string
	if err := pool.QueryRow(ctx, "SELECT headers::text FROM outbox_events ORDER BY id LIMIT 1").Scan(&headers); err != nil {
		t.Fatalf("read headers: %v", err)
	}
	if !strings.HasPrefix(headers, "[") {
		t.Fatalf("expected headers stored as a JSON array, got %q", headers)
	}
}

func TestOutbox_RecordTaskPerformance_CommitsMarkerRowAndEventTogether(t *testing.T) {
	pool := outboxDB(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	uc := &usecases.RecordTaskPerformance{
		Performances: postgres.NewPerformanceRepo(pool),
		Standards:    postgres.NewStandardRepo(pool),
		Processed:    postgres.NewProcessedEventRepo(pool),
		Events:       postgres.NewOutboxPublisher(pool, analyticsEncoder()),
		Clock:        memory.FixedClock{At: now},
		UnitOfWork:   postgres.NewUnitOfWork(pool),
	}
	req := usecases.RecordTaskPerformanceRequest{KafkaEventId: "evt-1", TaskId: "task-1", AssociateId: "assoc-1", TaskType: shared.Pack, ActualSeconds: 52, CompletedAt: now}

	if p, err := uc.Execute(ctx, req); err != nil || p == nil {
		t.Fatalf("record: p=%v err=%v", p, err)
	}
	if got := countRows(t, pool, "task_performances", "event_id = 'evt-1'"); got != 1 {
		t.Fatalf("expected the task_performances row, got %d", got)
	}
	if got := countRows(t, pool, "processed_events", "event_id = 'evt-1'"); got != 1 {
		t.Fatalf("expected the processed_events marker, got %d", got)
	}
	if got := countOutbox(t, pool, fmt.Sprintf("published_at IS NULL AND event_type = '%s' AND key = 'PACK'", cloudevents.TypeTaskPerformanceRecorded)); got != 1 {
		t.Fatalf("expected 1 unpublished TaskPerformanceRecorded row keyed PACK, got %d", got)
	}

	// Redelivery: benign no-op, no second row anywhere.
	if p, err := uc.Execute(ctx, req); err != nil || p != nil {
		t.Fatalf("redelivery: p=%v err=%v", p, err)
	}
	if got := countOutbox(t, pool, "true"); got != 1 {
		t.Fatalf("redelivery must not enqueue a second event, got %d rows", got)
	}
}

// The whole point of the outbox: if the event cannot be enqueued the
// aggregate change must not survive either.
func TestOutbox_PublishFailure_RollsBackAggregate(t *testing.T) {
	pool := outboxDB(t)
	ctx := context.Background()
	now := time.Now().UTC()
	broken := postgres.NewOutboxPublisher(pool, failingEncoder{err: errors.New("encoder broken")})
	uow := postgres.NewUnitOfWork(pool)

	define := &usecases.DefineStandard{Standards: postgres.NewStandardRepo(pool), Events: broken, Clock: memory.FixedClock{At: now}, UnitOfWork: uow}
	if _, err := define.Execute(ctx, shared.Slam, 30, nil); err == nil {
		t.Fatal("expected the failing encoder to fail the publish")
	}
	if got := countRows(t, pool, "labor_standards", "task_type = 'SLAM'"); got != 0 {
		t.Fatalf("labor_standards row survived a failed publish: the unit of work did not roll back (%d rows)", got)
	}

	record := &usecases.RecordTaskPerformance{
		Performances: postgres.NewPerformanceRepo(pool), Standards: postgres.NewStandardRepo(pool), Processed: postgres.NewProcessedEventRepo(pool),
		Events: broken, Clock: memory.FixedClock{At: now}, UnitOfWork: uow,
	}
	req := usecases.RecordTaskPerformanceRequest{KafkaEventId: "evt-rb", TaskId: "task-1", TaskType: shared.Pick, ActualSeconds: 10, CompletedAt: now}
	if _, err := record.Execute(ctx, req); err == nil {
		t.Fatal("expected the failing encoder to fail the publish")
	}
	if got := countRows(t, pool, "task_performances", "event_id = 'evt-rb'"); got != 0 {
		t.Fatalf("task_performances row survived a failed publish (%d rows)", got)
	}
	// The idempotency marker must roll back too, or the redelivery would
	// be dropped as a duplicate of a row that never existed.
	if got := countRows(t, pool, "processed_events", "event_id = 'evt-rb'"); got != 0 {
		t.Fatalf("processed_events marker survived a failed publish (%d rows)", got)
	}
	if got := countOutbox(t, pool, "true"); got != 0 {
		t.Fatalf("expected no outbox rows, got %d", got)
	}

	// With a working publisher the same message is now recorded.
	record.Events = postgres.NewOutboxPublisher(pool, analyticsEncoder())
	if p, err := record.Execute(ctx, req); err != nil || p == nil {
		t.Fatalf("redelivery after rollback must be recorded: p=%v err=%v", p, err)
	}
}

func TestOutboxRelay_PublishesInOrderAndMarksRows(t *testing.T) {
	pool := outboxDB(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	pub := postgres.NewOutboxPublisher(pool, analyticsEncoder())
	uow := postgres.NewUnitOfWork(pool)

	define := &usecases.DefineStandard{Standards: postgres.NewStandardRepo(pool), Events: pub, Clock: memory.FixedClock{At: now}, UnitOfWork: uow}
	if _, err := define.Execute(ctx, shared.Pick, 45, nil); err != nil {
		t.Fatalf("define: %v", err)
	}
	define.Clock = memory.FixedClock{At: now.Add(time.Second)}
	if _, err := define.Execute(ctx, shared.Pick, 40, nil); err != nil {
		t.Fatalf("revise: %v", err)
	}
	record := &usecases.RecordTaskPerformance{
		Performances: postgres.NewPerformanceRepo(pool), Standards: postgres.NewStandardRepo(pool), Processed: postgres.NewProcessedEventRepo(pool),
		Events: pub, Clock: memory.FixedClock{At: now.Add(2 * time.Second)}, UnitOfWork: uow,
	}
	if _, err := record.Execute(ctx, usecases.RecordTaskPerformanceRequest{KafkaEventId: "evt-1", TaskId: "task-1", TaskType: shared.Pick, ActualSeconds: 50, CompletedAt: now.Add(2 * time.Second)}); err != nil {
		t.Fatalf("record: %v", err)
	}

	sink := &recordingSink{}
	relay := postgres.NewOutboxRelay(pool, sink, slog.Default())
	n, err := relay.RelayOnce(ctx)
	if err != nil {
		t.Fatalf("relay: %v", err)
	}
	if n != 3 || len(sink.sent) != 3 {
		t.Fatalf("expected 3 published, got n=%d sent=%d", n, len(sink.sent))
	}
	want := []string{cloudevents.TypeLaborStandardDefined, cloudevents.TypeLaborStandardRevised, cloudevents.TypeTaskPerformanceRecorded}
	for i, w := range want {
		if sink.sent[i].EventType != w || string(sink.sent[i].Key) != "PICK" || sink.sent[i].Topic != cloudevents.TopicLaborPerformanceAnalytics {
			t.Fatalf("event %d: want %s keyed PICK on %s, got %s keyed %s on %s", i, w, cloudevents.TopicLaborPerformanceAnalytics, sink.sent[i].EventType, sink.sent[i].Key, sink.sent[i].Topic)
		}
		if sink.sent[i].Headers == nil {
			t.Fatalf("event %d: headers must be decoded to a non-nil slice", i)
		}
	}
	if got := countOutbox(t, pool, "published_at IS NULL"); got != 0 {
		t.Fatalf("expected every row marked published, %d still pending", got)
	}
	if got := countOutbox(t, pool, "attempts = 1 AND last_error IS NULL"); got != 3 {
		t.Fatalf("expected attempts=1,last_error=NULL on all 3 rows, got %d", got)
	}
	// A second pass finds nothing and republishes nothing.
	n, err = relay.RelayOnce(ctx)
	if err != nil || n != 0 || len(sink.sent) != 3 {
		t.Fatalf("second pass should be a no-op, got n=%d err=%v sent=%d", n, err, len(sink.sent))
	}
}

func TestOutboxRelay_SinkFailure_StopsAtFailedRowAndRetriesLater(t *testing.T) {
	pool := outboxDB(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	pub := postgres.NewOutboxPublisher(pool, analyticsEncoder())
	uow := postgres.NewUnitOfWork(pool)
	define := &usecases.DefineStandard{Standards: postgres.NewStandardRepo(pool), Events: pub, Clock: memory.FixedClock{At: now}, UnitOfWork: uow}
	for _, tt := range []shared.TaskType{shared.Pick, shared.Pack, shared.Slam} {
		if _, err := define.Execute(ctx, tt, 30, nil); err != nil {
			t.Fatalf("define %s: %v", tt, err)
		}
	}

	sink := &recordingSink{failOn: "PACK", failErr: errors.New("broker down")}
	relay := postgres.NewOutboxRelay(pool, sink, slog.Default())
	n, err := relay.RelayOnce(ctx)
	if err == nil {
		t.Fatal("expected the failing row to surface an error")
	}
	if n != 1 || len(sink.sent) != 1 || string(sink.sent[0].Key) != "PICK" {
		t.Fatalf("expected only PICK published before the failure, got n=%d sent=%v", n, sink.sent)
	}
	if got := countOutbox(t, pool, "published_at IS NULL"); got != 2 {
		t.Fatalf("expected PACK and SLAM still pending (ordering preserved), got %d pending", got)
	}
	var attempts int
	var lastErr string
	if err := pool.QueryRow(ctx, "SELECT attempts, coalesce(last_error,'') FROM outbox_events WHERE key = 'PACK'").Scan(&attempts, &lastErr); err != nil {
		t.Fatalf("read PACK: %v", err)
	}
	if attempts != 1 || !strings.Contains(lastErr, "broker down") {
		t.Fatalf("expected PACK to record the failed attempt, got attempts=%d last_error=%q", attempts, lastErr)
	}
	if got := countOutbox(t, pool, "key = 'SLAM' AND attempts = 0"); got != 1 {
		t.Fatal("SLAM must not have been attempted after PACK failed")
	}

	// Broker recovers: the next pass drains the rest, in order.
	sink.failOn = ""
	n, err = relay.RelayOnce(ctx)
	if err != nil || n != 2 {
		t.Fatalf("recovery pass: n=%d err=%v", n, err)
	}
	if string(sink.sent[1].Key) != "PACK" || string(sink.sent[2].Key) != "SLAM" {
		t.Fatalf("expected PACK then SLAM after recovery, got %v", sink.sent)
	}
	if got := countOutbox(t, pool, "published_at IS NULL"); got != 0 {
		t.Fatalf("expected outbox drained, %d pending", got)
	}
	if got := countOutbox(t, pool, "key = 'PACK' AND attempts = 2 AND last_error IS NULL"); got != 1 {
		t.Fatal("expected PACK's last_error cleared and attempts=2 after the successful retry")
	}
}

func TestOutbox_RecordTaskPerformance_FansOutToBothAnalyticsAndIntegrationTopics(t *testing.T) {
	pool := outboxDB(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	uc := &usecases.RecordTaskPerformance{
		Performances: postgres.NewPerformanceRepo(pool),
		Standards:    postgres.NewStandardRepo(pool),
		Processed:    postgres.NewProcessedEventRepo(pool),
		Events:       postgres.NewOutboxPublisher(pool, analyticsEncoder(), integrationEncoder()),
		Clock:        memory.FixedClock{At: now},
		UnitOfWork:   postgres.NewUnitOfWork(pool),
	}
	req := usecases.RecordTaskPerformanceRequest{KafkaEventId: "evt-fanout", TaskId: "task-fanout", AssociateId: "assoc-fanout", TaskType: shared.Pick, ActualSeconds: 41, CompletedAt: now}

	if p, err := uc.Execute(ctx, req); err != nil || p == nil {
		t.Fatalf("record: p=%v err=%v", p, err)
	}

	// ONE TaskPerformanceRecorded event, encoded through BOTH encoders,
	// must produce ONE outbox row per topic — the fan-out variant of
	// ADR 0010, extended by ADR 0013's integration topic.
	if got := countOutbox(t, pool, fmt.Sprintf("published_at IS NULL AND topic = '%s' AND event_type = '%s' AND key = 'PICK'", cloudevents.TopicLaborPerformanceAnalytics, cloudevents.TypeTaskPerformanceRecorded)); got != 1 {
		t.Fatalf("expected 1 unpublished analytics-topic row keyed PICK, got %d", got)
	}
	if got := countOutbox(t, pool, fmt.Sprintf("published_at IS NULL AND topic = '%s' AND event_type = '%s' AND key = 'assoc-fanout'", cloudevents.TopicLaborPerformanceEvents, cloudevents.TypeTaskPerformanceRecorded)); got != 1 {
		t.Fatalf("expected 1 unpublished integration-topic row keyed assoc-fanout, got %d", got)
	}
	if got := countOutbox(t, pool, "true"); got != 2 {
		t.Fatalf("expected exactly 2 outbox rows total (one per topic), got %d", got)
	}

	sink := &recordingSink{}
	relay := postgres.NewOutboxRelay(pool, sink, slog.Default())
	n, err := relay.RelayOnce(ctx)
	if err != nil {
		t.Fatalf("relay: %v", err)
	}
	if n != 2 || len(sink.sent) != 2 {
		t.Fatalf("expected 2 published, got n=%d sent=%d", n, len(sink.sent))
	}
	gotTopics := map[string]bool{}
	for _, m := range sink.sent {
		gotTopics[m.Topic] = true
	}
	if !gotTopics[cloudevents.TopicLaborPerformanceAnalytics] || !gotTopics[cloudevents.TopicLaborPerformanceEvents] {
		t.Fatalf("expected the relay to forward to both topics, got %v", sink.sent)
	}
}

func TestOutboxRelay_Run_DrainsUntilCancelled(t *testing.T) {
	pool := outboxDB(t)
	ctx := context.Background()
	pub := postgres.NewOutboxPublisher(pool, analyticsEncoder())
	define := &usecases.DefineStandard{Standards: postgres.NewStandardRepo(pool), Events: pub, Clock: memory.FixedClock{At: time.Now().UTC()}, UnitOfWork: postgres.NewUnitOfWork(pool)}
	if _, err := define.Execute(ctx, shared.Pick, 45, nil); err != nil {
		t.Fatalf("define: %v", err)
	}

	sink := &recordingSink{}
	relay := postgres.NewOutboxRelay(pool, sink, slog.Default(), postgres.WithInterval(20*time.Millisecond), postgres.WithBatchSize(1))
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- relay.Run(runCtx) }()

	deadline := time.Now().Add(5 * time.Second)
	for countOutbox(t, pool, "published_at IS NULL") != 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run should return the cancellation, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not stop after cancel")
	}
	if got := countOutbox(t, pool, "published_at IS NULL"); got != 0 {
		t.Fatalf("expected Run to drain the outbox, %d pending", got)
	}
}
