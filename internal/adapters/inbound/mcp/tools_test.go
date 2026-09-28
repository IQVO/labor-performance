package mcp

import (
	"context"
	"testing"
	"time"

	"github.com/claudioed/labor-performance/internal/adapters/outbound/events"
	"github.com/claudioed/labor-performance/internal/adapters/outbound/memory"
	"github.com/claudioed/labor-performance/internal/application/usecases"
	"github.com/claudioed/labor-performance/internal/domain/shared"
)

// base is the deterministic clock every mcp test runs against.
var base = time.Date(2026, 9, 6, 9, 0, 0, 0, time.UTC)

// harness wires the read use cases the MCP adapter needs over in-memory
// repos, seeding data through the REAL write use cases (DefineStandard,
// RecordTaskPerformance) so the read models under test are built from
// genuinely-recorded aggregates, not hand-constructed fixtures.
type harness struct {
	t *testing.T

	standards    *memory.StandardRepo
	performances *memory.PerformanceRepo
	processed    *memory.ProcessedEventRepo
	idlePeriods  *memory.IdlePeriodRepo
	clock        memory.FixedClock

	defineStandard        *usecases.DefineStandard
	recordTaskPerformance *usecases.RecordTaskPerformance

	deps Deps
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	standards := memory.NewStandardRepo()
	performances := memory.NewPerformanceRepo()
	processed := memory.NewProcessedEventRepo()
	idlePeriods := memory.NewIdlePeriodRepo()
	publisher := events.NewLogPublisher(nil)
	clock := memory.FixedClock{At: base}

	h := &harness{
		t:            t,
		standards:    standards,
		performances: performances,
		processed:    processed,
		idlePeriods:  idlePeriods,
		clock:        clock,

		defineStandard:        &usecases.DefineStandard{Standards: standards, Events: publisher, Clock: clock},
		recordTaskPerformance: &usecases.RecordTaskPerformance{Performances: performances, Standards: standards, Processed: processed, Events: publisher, Clock: clock, IdlePeriods: idlePeriods},
	}
	h.deps = Deps{
		GetAssociateScorecard:  &usecases.GetAssociateScorecard{Performances: performances},
		GetTaskTypePerformance: &usecases.GetTaskTypePerformance{Performances: performances},
		GetStandard:            &usecases.GetStandard{Standards: standards},
		GetUtilization:         &usecases.GetUtilization{Performances: performances, IdlePeriods: idlePeriods, Clock: clock},
	}
	return h
}

func (h *harness) ctx() context.Context { return context.Background() }

func (h *harness) mustDefineStandard(taskType shared.TaskType, expectedSeconds int64) {
	h.t.Helper()
	if _, err := h.defineStandard.Execute(h.ctx(), taskType, expectedSeconds, nil); err != nil {
		h.t.Fatalf("seeding standard %s: %v", taskType, err)
	}
}

func (h *harness) mustRecordTaskPerformance(eventID, taskID string, associateID shared.AssociateId, taskType shared.TaskType, actualSeconds int64, completedAt time.Time) {
	h.t.Helper()
	_, err := h.recordTaskPerformance.Execute(h.ctx(), usecases.RecordTaskPerformanceRequest{
		KafkaEventId:  eventID,
		TaskId:        taskID,
		AssociateId:   associateID,
		TaskType:      taskType,
		ActualSeconds: actualSeconds,
		CompletedAt:   completedAt,
	})
	if err != nil {
		h.t.Fatalf("seeding task performance %s: %v", taskID, err)
	}
}

func TestGetAssociateScorecard(t *testing.T) {
	tests := []struct {
		name        string
		associateID string
		wantErr     bool
		assert      func(t *testing.T, out scorecardDTO)
	}{
		{
			name:        "empty associateId rejected",
			associateID: "",
			wantErr:     true,
		},
		{
			name:        "unknown associate rejected",
			associateID: "assoc-nope",
			wantErr:     true,
		},
		{
			name:        "scorecard for a recorded associate",
			associateID: "assoc-1",
			assert: func(t *testing.T, out scorecardDTO) {
				if out.AssociateId != "assoc-1" {
					t.Fatalf("unexpected associate id %q", out.AssociateId)
				}
				if out.TaskCount != 2 {
					t.Fatalf("expected 2 tasks, got %d", out.TaskCount)
				}
				if out.MeanEfficiencyPct == nil {
					t.Fatal("expected a non-nil mean efficiency (both tasks have an active standard)")
				}
				if _, ok := out.ByTaskType["PICK"]; !ok {
					t.Fatalf("expected a PICK breakdown, got %+v", out.ByTaskType)
				}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			h.mustDefineStandard(shared.Pick, 60)
			h.mustRecordTaskPerformance("evt-1", "task-1", "assoc-1", shared.Pick, 60, base)
			h.mustRecordTaskPerformance("evt-2", "task-2", "assoc-1", shared.Pick, 90, base.Add(time.Hour))

			out, err := h.deps.getAssociateScorecard(h.ctx(), associateScorecardInput{AssociateId: tc.associateID})
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			tc.assert(t, out)
		})
	}
}

func TestGetTaskTypePerformance(t *testing.T) {
	tests := []struct {
		name     string
		taskType string
		wantErr  bool
		assert   func(t *testing.T, out taskTypePerformanceDTO)
	}{
		{"empty taskType rejected", "", true, nil},
		{"unknown taskType rejected", "NOPE", true, nil},
		{
			name:     "never-seen task type still returns successfully",
			taskType: "SLAM",
			assert: func(t *testing.T, out taskTypePerformanceDTO) {
				if out.TaskType != "SLAM" || out.TaskCount != 0 {
					t.Fatalf("expected zero-count SLAM performance, got %+v", out)
				}
			},
		},
		{
			name:     "fleet-wide performance for a recorded task type",
			taskType: "PACK",
			assert: func(t *testing.T, out taskTypePerformanceDTO) {
				if out.TaskType != "PACK" || out.TaskCount != 1 {
					t.Fatalf("unexpected PACK performance %+v", out)
				}
				if out.MeanActualSeconds == nil || *out.MeanActualSeconds != 45 {
					t.Fatalf("expected meanActualSeconds=45, got %+v", out.MeanActualSeconds)
				}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			h.mustDefineStandard(shared.Pack, 40)
			h.mustRecordTaskPerformance("evt-1", "task-1", "assoc-1", shared.Pack, 45, base)

			out, err := h.deps.getTaskTypePerformance(h.ctx(), taskTypePerformanceInput{TaskType: tc.taskType})
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			tc.assert(t, out)
		})
	}
}

func TestGetLaborStandard(t *testing.T) {
	tests := []struct {
		name     string
		taskType string
		wantErr  bool
		assert   func(t *testing.T, out standardDTO)
	}{
		{"empty taskType rejected", "", true, nil},
		{"invalid taskType rejected", "NOPE", true, nil},
		{"no active standard rejected", "SLAM", true, nil},
		{
			name:     "active standard for a defined task type",
			taskType: "PICK",
			assert: func(t *testing.T, out standardDTO) {
				if out.TaskType != "PICK" || out.ExpectedSeconds != 60 {
					t.Fatalf("unexpected standard %+v", out)
				}
				if out.EffectiveTo != nil {
					t.Fatalf("expected a still-active standard (EffectiveTo nil), got %+v", out.EffectiveTo)
				}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			h.mustDefineStandard(shared.Pick, 60)

			out, err := h.deps.getLaborStandard(h.ctx(), laborStandardInput{TaskType: tc.taskType})
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			tc.assert(t, out)
		})
	}
}

// utilizationCase is one row of TestGetTaskTypeUtilization's table.
type utilizationCase struct {
	name     string
	taskType string
	wantErr  bool
	assert   func(t *testing.T, out utilizationDTO)
}

func TestGetTaskTypeUtilization(t *testing.T) {
	tests := []utilizationCase{
		{name: "empty taskType rejected", taskType: "", wantErr: true},
		{name: "invalid taskType rejected", taskType: "NOPE", wantErr: true},
		{
			name:     "never-observed task type still returns successfully",
			taskType: "SLAM",
			assert:   assertUtilizationNeverObserved,
		},
		{
			name:     "utilization computed from real recorded rows",
			taskType: "PICK",
			assert:   assertUtilizationFromRecordedRows,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) { runUtilizationCase(t, tc) })
	}
}

// runUtilizationCase seeds two real PICK rows and exercises
// getTaskTypeUtilization for the case's task type.
func runUtilizationCase(t *testing.T, tc utilizationCase) {
	t.Helper()
	h := newHarness(t)
	h.mustRecordTaskPerformance("evt-1", "task-1", "assoc-1", shared.Pick, 30, base.Add(30*time.Second))
	h.mustRecordTaskPerformance("evt-2", "task-2", "assoc-1", shared.Pick, 30,
		base.Add(30*time.Second).Add(70*time.Second).Add(30*time.Second))

	out, err := h.deps.getTaskTypeUtilization(h.ctx(), taskTypeUtilizationInput{TaskType: tc.taskType, WindowSeconds: 7200})
	if tc.wantErr {
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		return
	}
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	tc.assert(t, out)
}

// assertUtilizationNeverObserved pins the zero-count, nil-mean shape for a
// task type with no rows.
func assertUtilizationNeverObserved(t *testing.T, out utilizationDTO) {
	t.Helper()
	if out.TaskType != "SLAM" || out.TaskSeconds != 0 || out.IdleSeconds != 0 {
		t.Fatalf("expected zero-count SLAM utilization, got %+v", out)
	}
	if out.UtilizationPct != nil {
		t.Fatalf("UtilizationPct = %v, want nil for a never-observed task type", *out.UtilizationPct)
	}
}

// assertUtilizationFromRecordedRows pins utilization computed from two real
// recorded rows.
func assertUtilizationFromRecordedRows(t *testing.T, out utilizationDTO) {
	t.Helper()
	if out.TaskType != "PICK" {
		t.Fatalf("unexpected PICK utilization %+v", out)
	}
	if out.TaskSeconds != 60 || out.IdleSeconds != 70 {
		t.Fatalf("unexpected task/idle seconds %+v", out)
	}
	if out.UtilizationPct == nil {
		t.Fatal("expected a non-nil utilizationPct")
	}
}
