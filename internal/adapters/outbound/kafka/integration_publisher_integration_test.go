//go:build integration

package kafka_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	kafkago "github.com/segmentio/kafka-go"
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"

	"github.com/claudioed/labor-performance/internal/adapters/kafka/envelope"
	outboundkafka "github.com/claudioed/labor-performance/internal/adapters/outbound/kafka"
	"github.com/claudioed/labor-performance/internal/domain/shared"
)

// TestIntegrationPublisher_PublishesTaskPerformanceRecordedToRealBroker
// proves that, with EVENT_PUBLISHER=kafka wiring (the direct, no-DB path
// this test exercises via outboundkafka.NewIntegrationPublisher directly,
// mirroring how cmd/labor/main.go's buildEventPublisher constructs it), a
// TaskPerformanceRecorded domain event actually lands on
// warehouse.labor-performance.events — the integration topic added by
// ADR 0013 — on a real Kafka broker, with the exact wire envelope a
// downstream consumer (e.g. workforce-management) would decode.
func TestIntegrationPublisher_PublishesTaskPerformanceRecordedToRealBroker(t *testing.T) {
	testCtx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	container, err := tckafka.Run(testCtx, "confluentinc/confluent-local:7.6.1",
		tckafka.WithClusterID(fmt.Sprintf("labor-performance-integration-itest-%d", time.Now().UnixNano())))
	if err != nil {
		t.Fatalf("start Kafka container: %v", err)
	}
	defer func() {
		if err := testcontainers.TerminateContainer(container); err != nil {
			t.Errorf("terminate Kafka container: %v", err)
		}
	}()

	brokers, err := container.Brokers(testCtx)
	if err != nil {
		t.Fatalf("get Kafka brokers: %v", err)
	}

	// Use a throwaway, uniquely-named topic rather than the real
	// envelope.TopicLaborPerformanceEvents constant, so this test never
	// collides with another run against the same container/topic name
	// convention. The publisher under test is otherwise wired exactly
	// as production (NewIntegrationPublisher), just pointed at this
	// topic via a fresh Writer below to keep topic name test-local.
	topic := fmt.Sprintf("labor-performance-events-itest-%d", time.Now().UnixNano())
	createTopicAndWaitForLeaderIntegration(t, testCtx, brokers[0], topic)

	publisher := &outboundkafka.IntegrationPublisher{
		Writer: &kafkago.Writer{
			Addr:                   kafkago.TCP(brokers...),
			Topic:                  topic,
			Balancer:               &kafkago.Hash{},
			AllowAutoTopicCreation: true,
		},
		NewID: uuid.NewString,
	}
	defer publisher.Close()

	pct := 91.2
	taskID := fmt.Sprintf("integration-task-%d", time.Now().UnixNano())
	associateID := fmt.Sprintf("integration-assoc-%d", time.Now().UnixNano())
	completedAt := time.Now().UTC().Truncate(time.Second)
	event := shared.NewTaskPerformanceRecorded(
		time.Now().UTC(), taskID, shared.AssociateId(associateID), shared.Pick, 41, &pct, nil, completedAt)

	if err := publisher.Publish(testCtx, event); err != nil {
		t.Fatalf("publish TaskPerformanceRecorded: %v", err)
	}

	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:   brokers,
		Topic:     topic,
		Partition: 0,
		MinBytes:  1,
		MaxBytes:  10e6,
	})
	defer reader.Close()
	if err := reader.SetOffset(0); err != nil {
		t.Fatalf("set reader offset: %v", err)
	}

	readCtx, readCancel := context.WithTimeout(testCtx, 30*time.Second)
	defer readCancel()
	msg, err := reader.ReadMessage(readCtx)
	if err != nil {
		t.Fatalf("read message from %s: %v", topic, err)
	}

	if string(msg.Key) != associateID {
		t.Errorf("partition key = %q, want %q (AssociateId)", msg.Key, associateID)
	}

	var env envelope.Envelope
	if err := json.Unmarshal(msg.Value, &env); err != nil {
		t.Fatalf("decode envelope: %v", err)
	}
	if env.EventType != envelope.EventTypeTaskPerformanceRecorded {
		t.Errorf("event_type = %q, want %q", env.EventType, envelope.EventTypeTaskPerformanceRecorded)
	}
	if env.Source != envelope.Source {
		t.Errorf("source = %q, want %q", env.Source, envelope.Source)
	}
	if env.EventId == "" {
		t.Error("event_id is empty")
	}

	var data map[string]any
	if err := json.Unmarshal(env.Data, &data); err != nil {
		t.Fatalf("decode data: %v", err)
	}
	if data["task_id"] != taskID {
		t.Errorf("task_id = %v, want %v", data["task_id"], taskID)
	}
	if data["associate_id"] != associateID {
		t.Errorf("associate_id = %v, want %v", data["associate_id"], associateID)
	}
	if data["task_type"] != "PICK" {
		t.Errorf("task_type = %v, want PICK", data["task_type"])
	}
	if data["efficiency_pct"] != 91.2 {
		t.Errorf("efficiency_pct = %v, want 91.2", data["efficiency_pct"])
	}
	if data["actual_seconds"] != float64(41) {
		t.Errorf("actual_seconds = %v, want 41", data["actual_seconds"])
	}
}

func createTopicAndWaitForLeaderIntegration(t *testing.T, ctx context.Context, broker, topic string) {
	t.Helper()
	createTopicWithPartitionsAndWaitForLeaderIntegration(t, ctx, broker, topic, 1)
}

// createTopicWithPartitionsAndWaitForLeaderIntegration creates topic with
// the given partition count. Used by
// TestIntegrationPublisher_KeysMessagesForSameAssociateOntoTheSamePartition
// to exercise the exact "1->8 partitions" scaleup scenario (warehouse-infra
// PR #42) that exposed the missing key-aware Balancer bug fleet-wide (see
// order-management PR #111, ADR 0018).
func createTopicWithPartitionsAndWaitForLeaderIntegration(t *testing.T, ctx context.Context, broker, topic string, numPartitions int) {
	t.Helper()

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

	if err := controllerConn.CreateTopics(kafkago.TopicConfig{Topic: topic, NumPartitions: numPartitions, ReplicationFactor: 1}); err != nil {
		t.Fatalf("create Kafka topic %q: %v", topic, err)
	}

	deadline := time.Now().Add(20 * time.Second)
	for {
		partitions, err := conn.ReadPartitions(topic)
		if err == nil && len(partitions) >= numPartitions {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("Kafka topic %q did not become ready with %d partitions: %v", topic, numPartitions, err)
		}
		select {
		case <-ctx.Done():
			t.Fatalf("wait for Kafka topic leader: %v", ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// TestIntegrationPublisher_KeysMessagesForSameAssociateOntoTheSamePartition
// is the real-Kafka-level guarantee behind ADR 0018: on an 8-partition
// topic (mirroring the Phase 3 partition scaleup, warehouse-infra PR #42),
// every TaskPerformanceRecorded event published for the SAME AssociateId
// must land on the SAME partition, while a DIFFERENT associate's events
// are free to land elsewhere. This is exactly what Kafka's default
// partitioner provides once a non-nil Key is set AND the Writer's
// Balancer actually hashes it (kafkago.Hash) — the bug this ADR fixes is
// that IntegrationPublisher, AnalyticsPublisher and RelaySink all used
// &kafkago.LeastBytes{}, which ignores Message.Key entirely for routing.
// A fake-writer unit test (integration_publisher_test.go) proves the key
// is SET; only this real-broker test proves it is actually USED to route
// same-key messages onto the same partition — see order-management PR
// #111 for the reference case where this exact class of bug was found.
func TestIntegrationPublisher_KeysMessagesForSameAssociateOntoTheSamePartition(t *testing.T) {
	testCtx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	container, err := tckafka.Run(testCtx, "confluentinc/confluent-local:7.6.1",
		tckafka.WithClusterID(fmt.Sprintf("labor-performance-partitioning-itest-%d", time.Now().UnixNano())))
	if err != nil {
		t.Fatalf("start Kafka container: %v", err)
	}
	defer func() {
		if err := testcontainers.TerminateContainer(container); err != nil {
			t.Errorf("terminate Kafka container: %v", err)
		}
	}()

	brokers, err := container.Brokers(testCtx)
	if err != nil {
		t.Fatalf("get Kafka brokers: %v", err)
	}

	const numPartitions = 8
	topic := fmt.Sprintf("labor-performance-events-itest-part-%d", time.Now().UnixNano())
	createTopicWithPartitionsAndWaitForLeaderIntegration(t, testCtx, brokers[0], topic, numPartitions)

	publisher := &outboundkafka.IntegrationPublisher{
		Writer: &kafkago.Writer{
			Addr:                   kafkago.TCP(brokers...),
			Topic:                  topic,
			Balancer:               &kafkago.Hash{},
			AllowAutoTopicCreation: true,
		},
		NewID: uuid.NewString,
	}
	defer publisher.Close()

	completedAt := time.Now().UTC().Truncate(time.Second)
	sameAssociate := shared.AssociateId(fmt.Sprintf("itest-assoc-same-%d", time.Now().UnixNano()))
	otherAssociate := shared.AssociateId(fmt.Sprintf("itest-assoc-other-%d", time.Now().UnixNano()))
	pct := 90.0

	// Publish 3 events for sameAssociate (mirrors a real associate's task
	// stream) plus 1 for a different associate, to prove the key — not
	// accident — drives partition placement.
	events := []shared.DomainEvent{
		shared.NewTaskPerformanceRecorded(time.Now().UTC(), fmt.Sprintf("itest-task-same-1-%d", time.Now().UnixNano()), sameAssociate, shared.Pick, 40, &pct, nil, completedAt),
		shared.NewTaskPerformanceRecorded(time.Now().UTC(), fmt.Sprintf("itest-task-same-2-%d", time.Now().UnixNano()), sameAssociate, shared.Pick, 42, &pct, nil, completedAt.Add(time.Minute)),
		shared.NewTaskPerformanceRecorded(time.Now().UTC(), fmt.Sprintf("itest-task-same-3-%d", time.Now().UnixNano()), sameAssociate, shared.Pick, 44, &pct, nil, completedAt.Add(2*time.Minute)),
		shared.NewTaskPerformanceRecorded(time.Now().UTC(), fmt.Sprintf("itest-task-other-%d", time.Now().UnixNano()), otherAssociate, shared.Pick, 40, &pct, nil, completedAt),
	}
	for _, e := range events {
		if err := publisher.Publish(testCtx, e); err != nil {
			t.Fatalf("publish event: %v", err)
		}
	}

	// Read every message back with its partition using one reader per
	// partition — a plain single-partition Reader would only see that
	// partition's slice, so every partition must be scanned explicitly.
	partitionOf := map[string]int{}
	countByKeyAndPartition := map[string]map[int]int{}
	for p := 0; p < numPartitions; p++ {
		reader := kafkago.NewReader(kafkago.ReaderConfig{
			Brokers:   brokers,
			Topic:     topic,
			Partition: p,
			MaxWait:   2 * time.Second,
		})
		func() {
			defer func() { _ = reader.Close() }()
			readCtx, readCancel := context.WithTimeout(testCtx, 5*time.Second)
			defer readCancel()
			for {
				msg, err := reader.ReadMessage(readCtx)
				if err != nil {
					return // timeout: no more messages on this partition
				}
				key := string(msg.Key)
				partitionOf[key] = p
				if countByKeyAndPartition[key] == nil {
					countByKeyAndPartition[key] = map[int]int{}
				}
				countByKeyAndPartition[key][p]++
			}
		}()
	}

	if len(partitionOf) != 2 {
		t.Fatalf("observed keys->partition = %v, want exactly 2 distinct keys (sameAssociate, otherAssociate)", partitionOf)
	}
	samePartition, ok := partitionOf[string(sameAssociate)]
	if !ok {
		t.Fatalf("no message observed with key %q; partitionOf = %v", sameAssociate, partitionOf)
	}
	if _, ok := partitionOf[string(otherAssociate)]; !ok {
		t.Fatalf("no message observed with key %q; partitionOf = %v", otherAssociate, partitionOf)
	}

	// All 3 of sameAssociate's messages must be on ONE partition — not
	// merely that the first happened to land somewhere.
	partitionsSeenForSameAssociate := countByKeyAndPartition[string(sameAssociate)]
	if len(partitionsSeenForSameAssociate) != 1 {
		t.Fatalf("sameAssociate's messages spread across %d partitions (%v), want exactly 1", len(partitionsSeenForSameAssociate), partitionsSeenForSameAssociate)
	}
	if got := partitionsSeenForSameAssociate[samePartition]; got != 3 {
		t.Errorf("found %d of sameAssociate's 3 messages on partition %d, want 3 (all events for one associate must share a partition)", got, samePartition)
	}
}
