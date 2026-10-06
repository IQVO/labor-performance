//go:build integration

package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/claudioed/labor-performance/internal/adapters/outbound/postgres"
	"github.com/claudioed/labor-performance/internal/application/ports"
	"github.com/claudioed/labor-performance/internal/domain/shared"
	"github.com/claudioed/labor-performance/internal/domain/standard"
)

func TestPostgres_StandardRoundTrip(t *testing.T) {
	pool := testDB(t)
	repo := postgres.NewStandardRepo(pool)
	ctx := context.Background()

	id, err := repo.NextID(ctx)
	if err != nil {
		t.Fatalf("unexpected error generating ID: %v", err)
	}

	taskType := shared.Pick
	effectiveFrom := time.Now().UTC().Truncate(time.Microsecond)

	// Round-trip a freshly-active standard (EffectiveTo nil).
	fresh := mustStandard(t, id, taskType, 45, effectiveFrom)
	if err := repo.Save(ctx, fresh); err != nil {
		t.Fatalf("unexpected error saving standard: %v", err)
	}

	found, err := repo.FindCurrentlyActive(ctx, taskType)
	if err != nil {
		t.Fatalf("unexpected error finding currently active standard: %v", err)
	}
	if found == nil {
		t.Fatal("expected to find the currently active standard")
	}
	if found.ExpectedSeconds() != 45 {
		t.Fatalf("expected ExpectedSeconds=45, got %d", found.ExpectedSeconds())
	}
	if found.EffectiveTo() != nil {
		t.Fatalf("expected EffectiveTo=nil for a freshly active standard, got %v", found.EffectiveTo())
	}
	if found.Version() != 1 {
		t.Fatalf("expected version 1 after the initial save, got %d", found.Version())
	}

	// FindActiveAsOf at effectiveFrom itself must also resolve it.
	asOf, err := repo.FindActiveAsOf(ctx, taskType, effectiveFrom)
	if err != nil {
		t.Fatalf("unexpected error finding active-as-of standard: %v", err)
	}
	if asOf == nil {
		t.Fatal("expected FindActiveAsOf to resolve the standard active at effectiveFrom")
	}

	// Close it (simulating a revision) and verify the persisted EffectiveTo
	// round-trips, and FindCurrentlyActive no longer resolves it.
	closedAt := effectiveFrom.Add(time.Hour)
	found.Close(closedAt)
	if err := repo.Save(ctx, found); err != nil {
		t.Fatalf("unexpected error saving closed standard: %v", err)
	}

	stillActive, err := repo.FindCurrentlyActive(ctx, taskType)
	if err != nil {
		t.Fatalf("unexpected error re-checking currently active standard: %v", err)
	}
	if stillActive != nil {
		t.Fatalf("expected no currently-active standard after closing the only one, got %+v", stillActive)
	}

	// A lookup "as of" a time before the close still resolves the closed
	// record — this is the frozen-history guarantee RecordTaskPerformance
	// depends on for a possibly out-of-order/replayed Kafka message.
	beforeClose, err := repo.FindActiveAsOf(ctx, taskType, effectiveFrom.Add(time.Minute))
	if err != nil {
		t.Fatalf("unexpected error finding active-as-of before close: %v", err)
	}
	if beforeClose == nil {
		t.Fatal("expected FindActiveAsOf before the close time to still resolve the closed record")
	}
	if beforeClose.ExpectedSeconds() != 45 {
		t.Fatalf("expected the frozen ExpectedSeconds=45, got %d", beforeClose.ExpectedSeconds())
	}
	if beforeClose.Version() != 2 {
		t.Fatalf("expected the close to advance the version to 2, got %d", beforeClose.Version())
	}
}

// TestStandardRepo_Save_StaleVersionFails is the optimistic-concurrency
// guard (ADR 0022) against a real testcontainers Postgres: two loads of
// the same row, the first Save wins and advances the version, the second
// (stale) Save is rejected with ports.ErrConcurrentModification and its
// change is verifiably absent on reload — the first writer's close is
// never clobbered.
func TestStandardRepo_Save_StaleVersionFails(t *testing.T) {
	pool := testDB(t)
	repo := postgres.NewStandardRepo(pool)
	ctx := context.Background()

	from := time.Now().UTC().Truncate(time.Microsecond)
	first := mustStandard(t, "std-occ-1", shared.Pick, 45, from)
	if err := repo.Save(ctx, first); err != nil {
		t.Fatalf("initial save: %v", err)
	}

	loadedA, err := repo.FindCurrentlyActive(ctx, shared.Pick)
	if err != nil {
		t.Fatalf("load A: %v", err)
	}
	loadedB, err := repo.FindCurrentlyActive(ctx, shared.Pick)
	if err != nil {
		t.Fatalf("load B: %v", err)
	}

	// Writer A closes the standard first.
	loadedA.Close(from.Add(time.Hour))
	if err := repo.Save(ctx, loadedA); err != nil {
		t.Fatalf("save A (current version): %v", err)
	}

	// Writer B still holds version 1; the row is now at version 2.
	loadedB.Close(from.Add(2 * time.Hour))
	err = repo.Save(ctx, loadedB)
	if err != ports.ErrConcurrentModification {
		t.Fatalf("expected ErrConcurrentModification on stale-version save, got %v", err)
	}

	// The first writer's close survived at its own instant; the second
	// writer's was rejected, not applied. As-of a time before A's close
	// (+1h) the record still resolves; a stale B save would have moved
	// the close to +2h, so querying at +90min distinguishes them.
	reloaded, err := repo.FindActiveAsOf(ctx, shared.Pick, from.Add(30*time.Minute))
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if reloaded == nil {
		t.Fatal("expected the closed record to still resolve as-of before the close")
	}
	if reloaded.Version() != 2 {
		t.Fatalf("expected exactly one version advance, got %d", reloaded.Version())
	}
}

// TestStandardRepo_ConcurrentDefine_ExactlyOneRevision proves the
// two-writers race DefineStandard's read-modify-write shape used to lose:
// two goroutines load the SAME currently-active standard, each closes it
// and saves. Exactly one wins; the loser gets ErrConcurrentModification
// and the row advances by exactly one version — never two closes applied
// to the same record.
func TestStandardRepo_ConcurrentDefine_ExactlyOneRevision(t *testing.T) {
	pool := testDB(t)
	repo := postgres.NewStandardRepo(pool)
	ctx := context.Background()

	from := time.Now().UTC().Truncate(time.Microsecond)
	if err := repo.Save(ctx, mustStandard(t, "std-race-0", shared.Pick, 45, from)); err != nil {
		t.Fatalf("seed open standard: %v", err)
	}

	loadedA, err := repo.FindCurrentlyActive(ctx, shared.Pick)
	if err != nil {
		t.Fatalf("load A: %v", err)
	}
	loadedB, err := repo.FindCurrentlyActive(ctx, shared.Pick)
	if err != nil {
		t.Fatalf("load B: %v", err)
	}
	loadedA.Close(from.Add(time.Hour))
	loadedB.Close(from.Add(2 * time.Hour))

	results := make(chan error, 2)
	start := make(chan struct{})
	go func() { <-start; results <- repo.Save(ctx, loadedA) }()
	go func() { <-start; results <- repo.Save(ctx, loadedB) }()
	close(start)

	successes, conflicts := 0, 0
	for i := 0; i < 2; i++ {
		rerr := <-results
		switch {
		case rerr == nil:
			successes++
		case rerr == ports.ErrConcurrentModification:
			conflicts++
		default:
			t.Fatalf("unexpected error from concurrent Save: %v", rerr)
		}
	}
	if successes != 1 || conflicts != 1 {
		t.Fatalf("expected exactly one success and one conflict, got successes=%d conflicts=%d", successes, conflicts)
	}

	reloaded, err := repo.FindActiveAsOf(ctx, shared.Pick, from.Add(time.Minute))
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if reloaded.Version() != 2 {
		t.Fatalf("expected exactly one version increment despite two concurrent closes, got %d", reloaded.Version())
	}
}

// TestStandardRepo_OneOpenStandardPerTaskType proves the partial unique
// index (migration 0006) is a real database-level backstop: a second open
// standard for the same task_type — inserted directly, bypassing the use
// case — is rejected with ports.ErrOpenStandardConflict (mapped from the
// 23505 on idx_labor_standards_one_open_per_task_type), while a second
// open standard for a DIFFERENT task type is fine, and a new one for the
// same type is fine once the prior one is closed.
func TestStandardRepo_OneOpenStandardPerTaskType(t *testing.T) {
	pool := testDB(t)
	repo := postgres.NewStandardRepo(pool)
	ctx := context.Background()

	from := time.Now().UTC().Truncate(time.Microsecond)
	if err := repo.Save(ctx, mustStandard(t, "std-open-1", shared.Pick, 45, from)); err != nil {
		t.Fatalf("seed first open standard: %v", err)
	}

	// Same task type, still open: must conflict.
	err := repo.Save(ctx, mustStandard(t, "std-open-2", shared.Pick, 50, from.Add(time.Minute)))
	if err != ports.ErrOpenStandardConflict {
		t.Fatalf("expected ErrOpenStandardConflict for a second open standard of the same task type, got %v", err)
	}

	// Different task type: no conflict.
	if err := repo.Save(ctx, mustStandard(t, "std-open-3", shared.Pack, 60, from.Add(time.Minute))); err != nil {
		t.Fatalf("expected a second open standard of a DIFFERENT task type to be allowed, got %v", err)
	}

	// Close the first, then a new open standard for PICK is fine again.
	loaded, err := repo.FindCurrentlyActive(ctx, shared.Pick)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	loaded.Close(from.Add(2 * time.Hour))
	if err := repo.Save(ctx, loaded); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := repo.Save(ctx, mustStandard(t, "std-open-4", shared.Pick, 55, from.Add(2*time.Hour))); err != nil {
		t.Fatalf("expected a new open standard after the prior one closed, got %v", err)
	}
}

func mustStandard(t *testing.T, id shared.StandardId, taskType shared.TaskType, expectedSeconds int64, effectiveFrom time.Time) *standard.LaborStandard {
	t.Helper()
	s, err := standard.New(id, taskType, expectedSeconds, nil, effectiveFrom)
	if err != nil {
		t.Fatalf("unexpected error building standard: %v", err)
	}
	return s
}
