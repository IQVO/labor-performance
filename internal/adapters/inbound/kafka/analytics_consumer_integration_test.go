//go:build integration

package kafka_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	ce "github.com/cloudevents/sdk-go/v2/event"
	"github.com/jackc/pgx/v5/pgxpool"
	kafkago "github.com/segmentio/kafka-go"
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	inboundkafka "github.com/claudioed/labor-performance/internal/adapters/inbound/kafka"
	"github.com/claudioed/labor-performance/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/labor-performance/internal/adapters/outbound/analyticsstore"
	"github.com/claudioed/labor-performance/internal/adapters/outbound/postgres"
	"github.com/claudioed/labor-performance/internal/analytics/report"
)

// flakyProjection delegates to the real Postgres projection but fails the
// FIRST task-performance apply AFTER it has already written (claim +
// rollup upsert) — the worst case for atomicity: every write of that
// attempt must be rolled back with the consumer's own claim.
type flakyProjection struct {
	report.ProjectionStore
	calls atomic.Int32
}

func (f *flakyProjection) ApplyTaskPerformanceRecorded(ctx context.Context, eventId string, fact report.TaskPerformanceFact) error {
	err := f.ProjectionStore.ApplyTaskPerformanceRecorded(ctx, eventId, fact)
	if err != nil {
		return err
	}
	if f.calls.Add(1) == 1 {
		return errors.New("injected mid-apply failure")
	}
	return nil
}

// TestAnalyticsConsumer_FailedApplyIsRetriedNotLost runs the real analytics
// consumer against testcontainers Kafka AND Postgres. The first apply
// fails after writing; the consumer must roll back, NOT commit the offset,
// and redeliver the same message so the retry converges to exactly one
// counted task. With the old ReadMessage-auto-commit + claim-first code the
// event was lost (rollup stays empty).
func TestAnalyticsConsumer_FailedApplyIsRetriedNotLost(t *testing.T) {
	testCtx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
	defer cancel()

	// --- Postgres (analytical schema) ---
	pgContainer, err := tcpostgres.Run(testCtx, "postgres:16-alpine",
		tcpostgres.WithDatabase("analytics"),
		tcpostgres.WithUsername("analytics"),
		tcpostgres.WithPassword("analytics"),
		tcpostgres.BasicWaitStrategies(),
	)
	if err != nil {
		t.Fatalf("start postgres container: %v", err)
	}
	defer func() { _ = testcontainers.TerminateContainer(pgContainer) }()
	url, err := pgContainer.ConnectionString(testCtx, "sslmode=disable")
	if err != nil {
		t.Fatalf("connection string: %v", err)
	}
	_, thisFile, _, _ := runtime.Caller(0)
	migrations := filepath.Join(filepath.Dir(thisFile), "..", "..", "..", "..", "migrations", "analytics")
	if err := postgres.RunMigrations(url, migrations); err != nil {
		t.Fatalf("run analytics migrations: %v", err)
	}
	pool, err := analyticsstore.NewPool(testCtx, url)
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	defer pool.Close()

	// --- Kafka ---
	kContainer, err := tckafka.Run(testCtx, "confluentinc/confluent-local:7.6.1",
		tckafka.WithClusterID(fmt.Sprintf("labor-performance-itest-analytics-%d", time.Now().UnixNano())))
	if err != nil {
		t.Fatalf("start Kafka container: %v", err)
	}
	defer func() { _ = testcontainers.TerminateContainer(kContainer) }()
	brokers, err := kContainer.Brokers(testCtx)
	if err != nil {
		t.Fatalf("get Kafka brokers: %v", err)
	}
	topic := fmt.Sprintf("labor-performance-analytics-itest-%d", time.Now().UnixNano())
	createTopicAndWaitForLeader(t, testCtx, brokers[0], topic)

	flaky := &flakyProjection{ProjectionStore: analyticsstore.NewPostgresProjection(pool)}
	consumer := inboundkafka.NewAnalyticsConsumerForTopic(
		brokers,
		fmt.Sprintf("labor-performance-analytics-itest-%d", time.Now().UnixNano()),
		topic,
		flaky,
		analyticsstore.NewConsumedEventsRepo(pool),
		analyticsstore.NewUnitOfWork(pool),
		nil,
	)
	defer consumer.Close()

	runCtx, runCancel := context.WithCancel(testCtx)
	runErr := make(chan error, 1)
	go func() { runErr <- consumer.Run(runCtx) }()

	completedAt := time.Now().UTC().Truncate(time.Hour)
	eventID := fmt.Sprintf("analytics-itest-evt-%d", time.Now().UnixNano())
	e := ce.New(cloudevents.SpecVersion)
	e.SetID(eventID)
	e.SetSource("/warehouse/labor-performance")
	e.SetType(cloudevents.TypeTaskPerformanceRecorded)
	e.SetSubject("assoc-1")
	e.SetTime(completedAt)
	e.SetDataSchema("urn:warehouse:labor-performance:analytics:TaskPerformanceRecorded:v1")
	if err := e.SetData(cloudevents.DataContentType, map[string]any{
		"task_id": "task-1", "associate_id": "assoc-1", "task_type": "PICK",
		"efficiency_pct": 90.0, "actual_seconds": 50, "completed_at": completedAt,
	}); err != nil {
		t.Fatalf("set data: %v", err)
	}
	body, err := json.Marshal(e)
	if err != nil {
		t.Fatalf("marshal cloudevent: %v", err)
	}
	writer := &kafkago.Writer{Addr: kafkago.TCP(brokers...), Topic: topic}
	defer writer.Close()
	if err := writer.WriteMessages(testCtx, kafkago.Message{
		Key: []byte(eventID), Value: body, Headers: []kafkago.Header{cloudevents.ContentTypeHeader()},
	}); err != nil {
		t.Fatalf("publish analytics event: %v", err)
	}

	// The retry (after the injected failure) must land exactly one task.
	waitForRollupTasks(t, testCtx, pool, 1)

	// Let any (buggy) extra delivery surface before asserting the final state.
	time.Sleep(2 * time.Second)

	if got := flaky.calls.Load(); got != 2 {
		t.Errorf("projection attempts = %d, want 2 (the injected failure + the redelivered success)", got)
	}
	if got := countRows(t, testCtx, pool, "SELECT COALESCE(SUM(tasks_recorded),0) FROM labor_performance_rollup"); got != 1 {
		t.Errorf("tasks_recorded = %d, want exactly 1 (no loss, no double count)", got)
	}
	if got := countRows(t, testCtx, pool, "SELECT count(*) FROM analytics_consumed_events"); got != 1 {
		t.Errorf("analytics_consumed_events rows = %d, want 1", got)
	}
	if got := countRows(t, testCtx, pool, "SELECT count(*) FROM analytics_processed_events"); got != 1 {
		t.Errorf("analytics_processed_events rows = %d, want 1", got)
	}

	runCancel()
	select {
	case err := <-runErr:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("run consumer: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Error("consumer did not stop after context cancellation")
	}
}

func countRows(t *testing.T, ctx context.Context, pool *pgxpool.Pool, query string) int64 {
	t.Helper()
	var n int64
	if err := pool.QueryRow(ctx, query).Scan(&n); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	return n
}

func waitForRollupTasks(t *testing.T, ctx context.Context, pool *pgxpool.Pool, want int64) {
	t.Helper()
	for {
		var n int64
		if err := pool.QueryRow(ctx, "SELECT COALESCE(SUM(tasks_recorded),0) FROM labor_performance_rollup").Scan(&n); err == nil && n >= want {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("the analytics projection never applied the event after the injected failure: %v", ctx.Err())
		case <-time.After(200 * time.Millisecond):
		}
	}
}
