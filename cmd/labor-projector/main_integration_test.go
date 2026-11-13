//go:build integration

package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"syscall"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"

	"github.com/claudioed/labor-performance/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/labor-performance/internal/adapters/outbound/analyticsstore"
	outboundkafka "github.com/claudioed/labor-performance/internal/adapters/outbound/kafka"
	"github.com/claudioed/labor-performance/internal/domain/shared"
	"github.com/claudioed/labor-performance/internal/testing/pgtest"
)

func TestMain(m *testing.M) {
	code := m.Run()
	pgtest.Shutdown()
	os.Exit(code)
}

// The real entry point, the way the fleet's projector template proves it: an
// EMPTY analytical database that the binary itself migrates (it is the sole
// writer and schema owner, ADR-0007), a real testcontainers Kafka carrying
// this repo's OWN encoder output on warehouse.labor-performance.analytics
// (ADR-0021), the documented projection answering — including an
// at-least-once redelivery of one CloudEvents id projecting exactly once
// (ADR-0031) — and a SIGTERM draining cleanly.
//
// Consumer-group note: unlike the last-mile-delivery template binary, this
// projector reads under the CONSTANT group inboundkafka.AnalyticsConsumerGroup
// ("labor-performance-analytics") and exposes no group env knob (cmd/
// labor-projector/config.go reads only ANALYTICS_DATABASE_URL and
// KAFKA_BROKERS). Isolation comes from the per-run Kafka container and its
// per-run cluster id below, so a fresh group starts at the earliest offset
// on a broker that holds only this test's messages.
func TestProjectorBootsMigratesProjectsAndStopsCleanly(t *testing.T) {
	url := pgtest.EmptyURL(t)
	brokers := startKafka(t)
	createTopicAndWaitForLeader(t, cloudevents.TopicLaborPerformanceAnalytics, brokers[0])

	// The events the projector consumes, shaped by the repo's own encoder
	// (AnalyticsPublisher.Encode): the domain events of the write side,
	// serialised exactly as the outbox relay would relay them. The business
	// times are fixed and land in two DIFFERENT hour buckets, so the row
	// each event owns is unambiguous.
	occurredAt := time.Date(2026, 11, 13, 11, 50, 0, 0, time.UTC)
	effectiveFrom := time.Date(2026, 11, 13, 10, 0, 0, 0, time.UTC)
	completedAt := time.Date(2026, 11, 13, 11, 47, 0, 0, time.UTC)
	// The rollup keys rows on the UTC HOUR of the business time
	// (report.HourBucket), so the completion at 11:47 lands in the 11:00
	// bucket — the assertion must compare against the bucket, not the raw
	// completion instant.
	taskBucket := completedAt.Truncate(time.Hour)
	efficiency := 50.0 // a 60s standard against a 120s actual
	nextID := 0
	encoder := &outboundkafka.AnalyticsPublisher{NewID: func() string {
		nextID++
		return fmt.Sprintf("projector-itest-evt-%d", nextID)
	}}
	msgs, err := encoder.Encode(context.Background(),
		shared.NewLaborStandardDefined(occurredAt, shared.StandardId("STD-PROJECTOR-ITEST-1"),
			shared.Pick, 60, nil, effectiveFrom),
		shared.NewTaskPerformanceRecorded(occurredAt, "TASK-PROJECTOR-ITEST-1",
			shared.AssociateId("assoc-projector-itest"), shared.Pick, 120, &efficiency, nil, completedAt),
	)
	if err != nil {
		t.Fatalf("encode analytics events: %v", err)
	}

	ctx := context.Background()
	writer := &kafkago.Writer{Addr: kafkago.TCP(brokers...), Topic: cloudevents.TopicLaborPerformanceAnalytics, Balancer: &kafkago.Hash{}}
	t.Cleanup(func() { _ = writer.Close() })
	for _, m := range msgs {
		if err := writer.WriteMessages(ctx, kafkago.Message{Key: m.Key, Value: m.Value, Headers: m.Headers}); err != nil {
			t.Fatalf("produce %s: %v", m.EventType, err)
		}
	}
	// Kafka is at-least-once: redeliver the TaskPerformanceRecorded message
	// verbatim (same CloudEvents id) and require the rollup to stay at one.
	if err := writer.WriteMessages(ctx, kafkago.Message{Key: msgs[1].Key, Value: msgs[1].Value, Headers: msgs[1].Headers}); err != nil {
		t.Fatalf("redeliver TaskPerformanceRecorded: %v", err)
	}

	addr := freeAddr(t)
	t.Setenv("ANALYTICS_DATABASE_URL", url)
	t.Setenv("KAFKA_BROKERS", brokers[0])
	t.Setenv("ADMIN_ADDR", addr)
	// The documented knob (main.go) pointing at the repo's analytics
	// migrations: go test's working directory is this package, so the
	// default relative path would not resolve.
	t.Setenv("ANALYTICS_MIGRATIONS_PATH", analyticsMigrationsDir(t))
	t.Setenv("LOG_LEVEL", "debug")

	done := launchRun(t)
	waitUntil(t, "the admin /healthz probe to answer 200", func() bool {
		resp, err := http.Get("http://" + addr + "/healthz")
		if err != nil {
			return false
		}
		_ = resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	})

	pool, err := analyticsstore.NewPool(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	waitUntil(t, "the standard and the redelivered performance projected exactly once", func() bool {
		var n int
		err := pool.QueryRow(ctx, `SELECT
			(SELECT count(*) FROM labor_performance_rollup
			  WHERE task_type = 'PICK' AND hour_bucket = $1 AND standards_defined = 1)
			+ (SELECT count(*) FROM labor_performance_rollup
			  WHERE task_type = 'PICK' AND hour_bucket = $2
			    AND tasks_recorded = 1 AND tasks_scored = 1 AND efficiency_pct_sum = 50
			    AND tasks_measured = 1 AND actual_seconds_sum = 120)
			+ (SELECT count(*) FROM analytics_processed_events)`,
			effectiveFrom, taskBucket).Scan(&n)
		return err == nil && n == 4
	})

	stopBySIGTERM(t, done)
}

// startKafka boots one isolated Kafka container for this package's test,
// mirroring this repo's own recipe in
// internal/adapters/inbound/kafka/consumer_integration_test.go: a unique
// cluster id per run, confluent-local image, brokers returned directly.
func startKafka(t *testing.T) []string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	container, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.6.1",
		tckafka.WithClusterID(fmt.Sprintf("labor-projector-itest-%d", time.Now().UnixNano())))
	if err != nil {
		t.Fatalf("start Kafka container: %v", err)
	}
	t.Cleanup(func() {
		_ = testcontainers.TerminateContainer(container)
	})
	brokers, err := container.Brokers(ctx)
	if err != nil {
		t.Fatalf("get Kafka brokers: %v", err)
	}
	return brokers
}

// createTopicAndWaitForLeader creates the topic and waits for its partition
// leader, exactly as the repo's own Kafka integration recipe does, so the
// first produce cannot race topic propagation.
func createTopicAndWaitForLeader(t *testing.T, topic, broker string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conn, err := kafkago.DialContext(ctx, "tcp", broker)
	if err != nil {
		t.Fatalf("dial Kafka broker: %v", err)
	}
	defer conn.Close()
	controller, err := conn.Controller()
	if err != nil {
		t.Fatalf("get Kafka controller: %v", err)
	}
	controllerConn, err := kafkago.DialContext(ctx, "tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		t.Fatalf("dial Kafka controller: %v", err)
	}
	defer controllerConn.Close()
	if err := controllerConn.CreateTopics(kafkago.TopicConfig{Topic: topic, NumPartitions: 1, ReplicationFactor: 1}); err != nil {
		t.Fatalf("create Kafka topic %q: %v", topic, err)
	}
	for deadline := time.Now().Add(20 * time.Second); ; {
		partitions, err := conn.ReadPartitions(topic)
		if err == nil && len(partitions) > 0 && partitions[0].Leader.ID >= 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("Kafka topic %q did not receive a partition leader: %v", topic, err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// launchRun starts the real signal-driven run() and registers THIS test's
// SIGTERM handler before the first signal can ever fly, so stopping the
// binary later can never fall back to Go's default disposition.
func launchRun(t *testing.T) <-chan error {
	t.Helper()
	sig := make(chan os.Signal, 4)
	signal.Notify(sig, syscall.SIGTERM)
	done := make(chan error, 1)
	go func() { done <- run() }()
	return done
}

// stopBySIGTERM stops run() the only way its contract allows: SIGTERM,
// retried on a timer until run() returns, then requiring a clean nil
// return (the projector's documented graceful drain: a SIGTERM'd run()
// returns its admin server's Shutdown, not an error). The handler
// registered in launchRun is NEVER unregistered: run()'s own
// signal.NotifyContext stop has already run by the time run() returns, so
// removing the last handler would restore the default disposition while a
// just-sent retry can still be in flight and kill the test binary itself.
func stopBySIGTERM(t *testing.T, done <-chan error) {
	t.Helper()
	_ = syscall.Kill(os.Getpid(), syscall.SIGTERM)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("a SIGTERM'd projector must drain cleanly, got %v", err)
			}
			return
		case <-ticker.C:
			_ = syscall.Kill(os.Getpid(), syscall.SIGTERM)
		}
	}
}

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	return l.Addr().String()
}

func waitUntil(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for deadline := time.Now().Add(90 * time.Second); time.Now().Before(deadline); time.Sleep(200 * time.Millisecond) {
		if cond() {
			return
		}
	}
	t.Fatalf("timed out waiting for %s", what)
}

// analyticsMigrationsDir resolves the repo's analytics migrations directory
// relative to this file, so the suite does not depend on the working
// directory go test was invoked from.
func analyticsMigrationsDir(t *testing.T) string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("unable to resolve test source path")
	}
	return filepath.Join(filepath.Dir(thisFile), "..", "..", "migrations", "analytics")
}
