//go:build integration

package postgres_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/claudioed/labor-performance/internal/adapters/outbound/postgres"
	"github.com/claudioed/labor-performance/internal/domain/performance"
	"github.com/claudioed/labor-performance/internal/domain/shared"
)

func TestPostgres_PerformanceRoundTrip(t *testing.T) {
	pool := testDB(t)
	repo := postgres.NewPerformanceRepo(pool)
	ctx := context.Background()

	eventId := fmt.Sprintf("it-evt-%d", time.Now().UnixNano())
	taskId := fmt.Sprintf("it-task-%d", time.Now().UnixNano())
	associateId := shared.AssociateId(fmt.Sprintf("it-assoc-%d", time.Now().UnixNano()))
	completedAt := time.Now().UTC().Truncate(time.Microsecond)

	p, err := performance.New(eventId, taskId, associateId, shared.Pick, 52, 45, completedAt)
	if err != nil {
		t.Fatalf("unexpected error building performance: %v", err)
	}

	if err := repo.Save(ctx, p); err != nil {
		t.Fatalf("unexpected error saving performance: %v", err)
	}

	// Idempotency at the storage layer: ON CONFLICT (event_id) DO NOTHING
	// means saving the same event_id twice must not error and must not
	// double the row (RecordTaskPerformance's application-layer idempotency
	// gate is the primary defense, but the storage constraint is a real,
	// independently-verifiable backstop).
	if err := repo.Save(ctx, p); err != nil {
		t.Fatalf("unexpected error on duplicate save (expected idempotent no-op): %v", err)
	}

	exists, err := repo.ExistsByAssociateID(ctx, associateId)
	if err != nil {
		t.Fatalf("unexpected error checking ExistsByAssociateID: %v", err)
	}
	if !exists {
		t.Fatal("expected ExistsByAssociateID to report true after a save")
	}

	sc, err := repo.ScorecardFor(ctx, associateId)
	if err != nil {
		t.Fatalf("unexpected error fetching scorecard: %v", err)
	}
	if sc.TaskCount != 1 {
		t.Fatalf("expected TaskCount=1 (not 2, confirming the duplicate save was a no-op), got %d", sc.TaskCount)
	}
	if sc.MeanEfficiencyPct == nil {
		t.Fatal("expected a non-nil MeanEfficiencyPct (45/52*100)")
	}

	tt, err := repo.TaskTypePerformanceFor(ctx, shared.Pick)
	if err != nil {
		t.Fatalf("unexpected error fetching task-type performance: %v", err)
	}
	if tt.TaskCount < 1 {
		t.Fatalf("expected TaskCount>=1 for PICK, got %d", tt.TaskCount)
	}
	if tt.MeanActualSeconds == nil {
		t.Fatal("expected a non-nil MeanActualSeconds -- the row saved above has ActualSeconds=52")
	}
}

func TestPostgres_ProcessedEventIdempotency(t *testing.T) {
	pool := testDB(t)
	repo := postgres.NewProcessedEventRepo(pool)
	ctx := context.Background()
	eventId := fmt.Sprintf("it-processed-%d", time.Now().UnixNano())

	firstTime, err := repo.MarkProcessed(ctx, eventId)
	if err != nil {
		t.Fatalf("unexpected error on first MarkProcessed: %v", err)
	}
	if !firstTime {
		t.Fatal("expected the first MarkProcessed call to report true (newly recorded)")
	}

	secondTime, err := repo.MarkProcessed(ctx, eventId)
	if err != nil {
		t.Fatalf("unexpected error on second MarkProcessed: %v", err)
	}
	if secondTime {
		t.Fatal("expected the second MarkProcessed call for the same event_id to report false (duplicate)")
	}
}
