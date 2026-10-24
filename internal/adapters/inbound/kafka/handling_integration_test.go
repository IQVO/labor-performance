//go:build integration

package kafka_test

import (
	"context"
	"fmt"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	inboundkafka "github.com/claudioed/labor-performance/internal/adapters/inbound/kafka"
	"github.com/claudioed/labor-performance/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/labor-performance/internal/adapters/outbound/events"
	"github.com/claudioed/labor-performance/internal/adapters/outbound/memory"
	"github.com/claudioed/labor-performance/internal/adapters/outbound/postgres"
	"github.com/claudioed/labor-performance/internal/adapters/outbound/system"
	"github.com/claudioed/labor-performance/internal/application/usecases"
	"github.com/claudioed/labor-performance/internal/domain/shared"
)

// TestConsumer_ScoresHandlingUnitCompletionsAgainstTheirOwnStandard drives
// the real consumer from a real Kafka broker into real Postgres repos: a
// TaskCompleted carrying handling_unit/weight_g (ADR 0035) is scored
// against the CASE standard, or its declared team-lift time when the case
// is heavier than the threshold, while a completion carrying neither is
// scored against the task-type-only standard exactly as before.
func TestConsumer_ScoresHandlingUnitCompletionsAgainstTheirOwnStandard(t *testing.T) {
	testCtx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	pg, err := tcpostgres.Run(testCtx, "postgres:16-alpine",
		tcpostgres.WithDatabase("labor"), tcpostgres.WithUsername("labor"), tcpostgres.WithPassword("labor"),
		tcpostgres.BasicWaitStrategies())
	if err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	defer func() { _ = testcontainers.TerminateContainer(pg) }()
	url, err := pg.ConnectionString(testCtx, "sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	_, thisFile, _, _ := runtime.Caller(0)
	if err := postgres.RunMigrations(url, filepath.Join(filepath.Dir(thisFile), "..", "..", "..", "..", "migrations")); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	pool, err := postgres.NewPool(testCtx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	kc, err := tckafka.Run(testCtx, "confluentinc/confluent-local:7.6.1",
		tckafka.WithClusterID(fmt.Sprintf("labor-hu-itest-%d", time.Now().UnixNano())))
	if err != nil {
		t.Fatalf("start kafka: %v", err)
	}
	defer func() { _ = testcontainers.TerminateContainer(kc) }()
	brokers, err := kc.Brokers(testCtx)
	if err != nil {
		t.Fatal(err)
	}
	topic := fmt.Sprintf("labor-hu-itest-%d", time.Now().UnixNano())
	createTopicAndWaitForLeader(t, testCtx, brokers[0], topic)

	standards := postgres.NewStandardRepo(pool)
	define := &usecases.DefineStandard{Standards: standards, Events: events.NewLogPublisher(nil), Clock: memory.FixedClock{At: time.Now().UTC().Add(-time.Hour)}}
	if _, err := define.Execute(testCtx, shared.Pick, 45, nil); err != nil {
		t.Fatalf("define task-type-only: %v", err)
	}
	if _, err := define.ExecuteSpec(testCtx, usecases.DefineStandardSpec{
		TaskType: shared.Pick, HandlingUnit: shared.Case, ExpectedSeconds: 30,
		TeamLift: &usecases.TeamLiftSpec{ThresholdG: 18000, ExpectedSeconds: 75},
	}); err != nil {
		t.Fatalf("define CASE: %v", err)
	}

	record := &usecases.RecordTaskPerformance{
		Performances: postgres.NewPerformanceRepo(pool), Standards: standards, Processed: postgres.NewProcessedEventRepo(pool),
		Events: events.NewLogPublisher(nil), Clock: system.Clock{}, UnitOfWork: postgres.NewUnitOfWork(pool),
	}
	consumer := inboundkafka.NewConsumerForTopic(brokers, fmt.Sprintf("labor-hu-itest-%d", time.Now().UnixNano()), topic, record, nil)
	defer consumer.Close()
	runCtx, stop := context.WithCancel(testCtx)
	defer stop()
	go func() { _ = consumer.Run(runCtx) }()

	writer := &kafkago.Writer{Addr: kafkago.TCP(brokers...), Topic: topic}
	defer writer.Close()
	send := func(id string, extra map[string]any) {
		t.Helper()
		data := map[string]any{"task_id": id, "station_id": "s1", "work_unit_id": "wu", "associate_id": "assoc-" + id, "duration_seconds": 60, "task_type": "PICK"}
		for k, v := range extra {
			data[k] = v
		}
		if err := writer.WriteMessages(testCtx, kafkago.Message{
			Key: []byte(id), Value: mustTaskCompletedCloudEvent(t, id, data), Headers: []kafkago.Header{cloudevents.ContentTypeHeader()},
		}); err != nil {
			t.Fatalf("publish %s: %v", id, err)
		}
	}
	send("plain", nil)
	send("light", map[string]any{"handling_unit": "CASE", "weight_g": 18000})
	send("heavy", map[string]any{"handling_unit": "CASE", "weight_g": 18001})
	send("each", map[string]any{"handling_unit": "EACH"})
	send("junk", map[string]any{"handling_unit": 7, "weight_g": "heavy"})

	want := map[string]struct {
		std  int64
		unit string
		lift bool
	}{
		"plain": {45, "", false},
		"light": {30, "CASE", false},
		"heavy": {75, "CASE", true},
		"each":  {45, "EACH", false},
		"junk":  {45, "", false},
	}
	deadline := time.Now().Add(90 * time.Second)
	for id, w := range want {
		for {
			var std int64
			var unit string
			var lift bool
			err := pool.QueryRow(testCtx, `SELECT standard_seconds_at_completion, handling_unit, team_lift FROM task_performances WHERE task_id = $1`, id).Scan(&std, &unit, &lift)
			if err == nil {
				if std != w.std || unit != w.unit || lift != w.lift {
					t.Fatalf("%s: standard=%d unit=%q lift=%v, want %d %q %v", id, std, unit, lift, w.std, w.unit, w.lift)
				}
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s was not recorded in time: %v", id, err)
			}
			time.Sleep(200 * time.Millisecond)
		}
	}
}
