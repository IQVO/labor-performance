package standard_test

import (
	"errors"
	"testing"
	"time"

	"github.com/claudioed/labor-performance/internal/domain/shared"
	"github.com/claudioed/labor-performance/internal/domain/standard"
)

func mustNew(t *testing.T, taskType shared.TaskType, expectedSeconds int64, effectiveFrom time.Time) *standard.LaborStandard {
	t.Helper()
	s, err := standard.New("std-1", taskType, expectedSeconds, nil, effectiveFrom)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return s
}

func TestNew_Success(t *testing.T) {
	from := time.Date(2026, 8, 29, 8, 0, 0, 0, time.UTC)
	s := mustNew(t, shared.Pick, 45, from)

	if s.TaskType() != shared.Pick {
		t.Fatalf("TaskType = %v, want PICK", s.TaskType())
	}
	if s.ExpectedSeconds() != 45 {
		t.Fatalf("ExpectedSeconds = %d, want 45", s.ExpectedSeconds())
	}
	if !s.EffectiveFrom().Equal(from) {
		t.Fatalf("EffectiveFrom = %v, want %v", s.EffectiveFrom(), from)
	}
	if s.EffectiveTo() != nil {
		t.Fatalf("EffectiveTo = %v, want nil (freshly active)", s.EffectiveTo())
	}
}

func TestNew_FailingPath_NonPositiveExpectedSeconds(t *testing.T) {
	from := time.Now()
	for _, seconds := range []int64{0, -1, -100} {
		if _, err := standard.New("std-1", shared.Pick, seconds, nil, from); !errors.Is(err, standard.ErrNonPositiveExpectedSeconds) {
			t.Fatalf("New(%d) error = %v, want ErrNonPositiveExpectedSeconds", seconds, err)
		}
	}
}

func TestClose(t *testing.T) {
	from := time.Date(2026, 8, 29, 8, 0, 0, 0, time.UTC)
	s := mustNew(t, shared.Pick, 45, from)

	closedAt := from.Add(24 * time.Hour)
	s.Close(closedAt)

	if s.EffectiveTo() == nil || !s.EffectiveTo().Equal(closedAt) {
		t.Fatalf("EffectiveTo = %v, want %v", s.EffectiveTo(), closedAt)
	}
	// Closing must not mutate ExpectedSeconds/EffectiveFrom — this is
	// what keeps a frozen historical TaskPerformance row accurate after
	// a later revision.
	if s.ExpectedSeconds() != 45 {
		t.Fatalf("Close must not change ExpectedSeconds, got %d", s.ExpectedSeconds())
	}
	if !s.EffectiveFrom().Equal(from) {
		t.Fatalf("Close must not change EffectiveFrom, got %v", s.EffectiveFrom())
	}
}

func TestIsActiveAt(t *testing.T) {
	from := time.Date(2026, 8, 29, 8, 0, 0, 0, time.UTC)
	to := from.Add(24 * time.Hour)

	t.Run("open-ended standard (never closed)", func(t *testing.T) {
		s := mustNew(t, shared.Pick, 45, from)
		if s.IsActiveAt(from.Add(-time.Second)) {
			t.Fatal("must not be active before EffectiveFrom")
		}
		if !s.IsActiveAt(from) {
			t.Fatal("must be active exactly at EffectiveFrom")
		}
		if !s.IsActiveAt(from.Add(1000 * time.Hour)) {
			t.Fatal("an open-ended standard must remain active arbitrarily far in the future")
		}
	})

	t.Run("closed standard", func(t *testing.T) {
		s := standard.Rehydrate("std-1", shared.Pick, 45, nil, from, &to, 1)
		if !s.IsActiveAt(from) {
			t.Fatal("must be active at EffectiveFrom")
		}
		if !s.IsActiveAt(to.Add(-time.Nanosecond)) {
			t.Fatal("must be active just before EffectiveTo")
		}
		if s.IsActiveAt(to) {
			t.Fatal("must NOT be active exactly at EffectiveTo (half-open interval)")
		}
		if s.IsActiveAt(to.Add(time.Hour)) {
			t.Fatal("must not be active after EffectiveTo")
		}
	})
}

func TestRehydrate_RoundTrip(t *testing.T) {
	from := time.Date(2026, 8, 29, 8, 0, 0, 0, time.UTC)
	s := standard.Rehydrate("std-42", shared.Slam, 60, nil, from, nil, 3)

	if s.ID() != "std-42" || s.TaskType() != shared.Slam || s.ExpectedSeconds() != 60 {
		t.Fatalf("unexpected rehydrated standard: %+v", s)
	}
	if s.Version() != 3 {
		t.Fatalf("Rehydrate must preserve the persisted version, got %d", s.Version())
	}
	if got := standard.Rehydrate("std-0", shared.Slam, 60, nil, from, nil, 0).Version(); got != 1 {
		t.Fatalf("a non-positive version must normalize to 1, got %d", got)
	}
}

func TestNew_TravelComponentSeconds_Nil_IsAlwaysValid(t *testing.T) {
	from := time.Now()
	s, err := standard.New("std-1", shared.Pick, 45, nil, from)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if s.TravelComponentSeconds() != nil {
		t.Fatalf("want nil TravelComponentSeconds by default, got %v", *s.TravelComponentSeconds())
	}
}

func TestNew_TravelComponentSeconds_ValidRange(t *testing.T) {
	from := time.Now()
	for _, seconds := range []int64{0, 20, 45} {
		s, err := standard.New("std-1", shared.Pick, 45, &seconds, from)
		if err != nil {
			t.Fatalf("New(travelComponentSeconds=%d): unexpected error: %v", seconds, err)
		}
		if s.TravelComponentSeconds() == nil || *s.TravelComponentSeconds() != seconds {
			t.Fatalf("want TravelComponentSeconds=%d, got %v", seconds, s.TravelComponentSeconds())
		}
	}
}

func TestNew_RejectsNegativeTravelComponentSeconds(t *testing.T) {
	from := time.Now()
	negative := int64(-1)
	if _, err := standard.New("std-1", shared.Pick, 45, &negative, from); !errors.Is(err, standard.ErrNegativeTravelComponentSeconds) {
		t.Fatalf("want ErrNegativeTravelComponentSeconds, got %v", err)
	}
}

func TestNew_RejectsTravelComponentSecondsExceedingExpectedSeconds(t *testing.T) {
	from := time.Now()
	tooMuch := int64(46)
	if _, err := standard.New("std-1", shared.Pick, 45, &tooMuch, from); !errors.Is(err, standard.ErrTravelComponentExceedsExpectedSeconds) {
		t.Fatalf("want ErrTravelComponentExceedsExpectedSeconds, got %v", err)
	}
}

func TestRehydrate_PreservesTravelComponentSeconds(t *testing.T) {
	from := time.Date(2026, 8, 29, 8, 0, 0, 0, time.UTC)
	travel := int64(15)
	s := standard.Rehydrate("std-1", shared.Pick, 45, &travel, from, nil, 1)
	if s.TravelComponentSeconds() == nil || *s.TravelComponentSeconds() != 15 {
		t.Fatalf("want rehydrated TravelComponentSeconds=15, got %v", s.TravelComponentSeconds())
	}
}
