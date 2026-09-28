package report

import (
	"testing"
	"time"
)

func hr(h int) time.Time {
	return time.Date(2026, 9, 5, h, 0, 0, 0, time.UTC)
}

func ptr(v float64) *float64 { return &v }

// eq compares two *float64 for "both nil, or both non-nil and equal
// within a tolerance" — the assertion every mean in this read model
// needs, since nil is a distinct, meaningful outcome from any number.
func eq(got, want *float64) bool {
	switch {
	case got == nil && want == nil:
		return true
	case got == nil || want == nil:
		return false
	default:
		d := *got - *want
		return d < 1e-9 && d > -1e-9
	}
}

func fmtPtr(p *float64) string {
	if p == nil {
		return "nil"
	}
	return time.Duration(*p * float64(time.Second)).String()
}

func TestNormalizeTaskType(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want string
	}{
		{"empty becomes the explicit unclassified label", "", UnclassifiedTaskType},
		{"a known task type passes through", "PICK", "PICK"},
		{"an unknown-but-present value passes through untouched", "AUDIT", "AUDIT"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := NormalizeTaskType(tc.raw); got != tc.want {
				t.Fatalf("NormalizeTaskType(%q) = %q, want %q", tc.raw, got, tc.want)
			}
		})
	}
}

func TestHourBucketTruncatesToUTCHour(t *testing.T) {
	// A non-UTC zone with a sub-hour offset would silently produce a
	// non-hour-aligned bucket if the implementation truncated before
	// converting, so pin that ordering here.
	kathmandu := time.FixedZone("NPT", 5*3600+45*60)
	in := time.Date(2026, 9, 5, 14, 47, 31, 500, kathmandu)

	got := HourBucket(in)

	if got.Location() != time.UTC {
		t.Fatalf("HourBucket location = %v, want UTC", got.Location())
	}
	if got.Minute() != 0 || got.Second() != 0 || got.Nanosecond() != 0 {
		t.Fatalf("HourBucket(%v) = %v, want an hour-aligned instant", in, got)
	}
	if want := in.UTC().Truncate(time.Hour); !got.Equal(want) {
		t.Fatalf("HourBucket(%v) = %v, want %v", in, got, want)
	}
}

func TestRowDerivedMeans(t *testing.T) {
	tests := []struct {
		name              string
		row               Row
		wantUnscored      int
		wantMeanEfficient *float64
		wantMeanActual    *float64
	}{
		{
			name: "a bucket with no rows at all reports nil means, never zero",
			row:  Row{Key: RowKey{TaskType: "PICK", HourBucket: hr(9)}},
			// The empty-bucket case ADR-0004's discipline is about: we
			// have literally no observation, so we report no number.
			wantUnscored:      0,
			wantMeanEfficient: nil,
			wantMeanActual:    nil,
		},
		{
			name: "recorded but entirely unscorable tasks report a nil efficiency mean",
			row: Row{
				Key:           RowKey{TaskType: "PICK", HourBucket: hr(9)},
				TasksRecorded: 4,
				TasksScored:   0,
				// duration_seconds=0 events: recorded, but unmeasurable.
				TasksMeasured:    0,
				ActualSecondsSum: 0,
			},
			wantUnscored:      4,
			wantMeanEfficient: nil,
			wantMeanActual:    nil,
		},
		{
			name: "measured but unscored tasks still report a real mean duration",
			row: Row{
				Key:              RowKey{TaskType: "PACK", HourBucket: hr(9)},
				TasksRecorded:    2,
				TasksScored:      0,
				TasksMeasured:    2,
				ActualSecondsSum: 90,
			},
			// No standard existed, so no efficiency — but the observed
			// pace is real and must survive (the ADR-0006 distinction,
			// applied analytically).
			wantUnscored:      2,
			wantMeanEfficient: nil,
			wantMeanActual:    ptr(45),
		},
		{
			name: "a fully scored bucket averages only the scored subset",
			row: Row{
				Key:              RowKey{TaskType: "PICK", HourBucket: hr(9)},
				TasksRecorded:    5,
				TasksScored:      2,
				EfficiencyPctSum: 174, // 87 + 87
				TasksMeasured:    4,
				ActualSecondsSum: 200,
			},
			wantUnscored:      3,
			wantMeanEfficient: ptr(87),
			wantMeanActual:    ptr(50),
		},
		{
			name: "an impossible scored>recorded row clamps unscored at zero",
			row: Row{
				Key:              RowKey{TaskType: "SLAM", HourBucket: hr(9)},
				TasksRecorded:    1,
				TasksScored:      3,
				EfficiencyPctSum: 300,
			},
			wantUnscored:      0,
			wantMeanEfficient: ptr(100),
			wantMeanActual:    nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.row.TasksUnscored(); got != tc.wantUnscored {
				t.Errorf("TasksUnscored() = %d, want %d", got, tc.wantUnscored)
			}
			if got := tc.row.MeanEfficiencyPct(); !eq(got, tc.wantMeanEfficient) {
				t.Errorf("MeanEfficiencyPct() = %s, want %s", fmtPtr(got), fmtPtr(tc.wantMeanEfficient))
			}
			if got := tc.row.MeanActualSeconds(); !eq(got, tc.wantMeanActual) {
				t.Errorf("MeanActualSeconds() = %s, want %s", fmtPtr(got), fmtPtr(tc.wantMeanActual))
			}
		})
	}
}

func TestBuildEmptyReport(t *testing.T) {
	got := Build(nil)

	if got.Rows == nil {
		t.Error("Build(nil).Rows is nil; want an empty slice so the wire shape is [] not null")
	}
	if len(got.Rows) != 0 {
		t.Errorf("Build(nil).Rows = %v, want empty", got.Rows)
	}
	if len(got.ByTaskType) != 0 {
		t.Errorf("Build(nil).ByTaskType = %v, want empty — an absent TaskType is absent, not zeroed", got.ByTaskType)
	}
	if got.Totals.TasksRecorded != 0 || got.Totals.TasksScored != 0 || got.Totals.TasksUnscored != 0 {
		t.Errorf("Build(nil).Totals = %+v, want zero counts", got.Totals)
	}
	if got.Totals.MeanEfficiencyPct != nil {
		t.Errorf("Build(nil).Totals.MeanEfficiencyPct = %v, want nil", *got.Totals.MeanEfficiencyPct)
	}
	if got.Totals.MeanActualSeconds != nil {
		t.Errorf("Build(nil).Totals.MeanActualSeconds = %v, want nil", *got.Totals.MeanActualSeconds)
	}
}

func TestBuildSortsRowsDeterministically(t *testing.T) {
	// Deliberately out of order on both dimensions.
	rows := []Row{
		{Key: RowKey{TaskType: "SLAM", HourBucket: hr(10)}, TasksRecorded: 1},
		{Key: RowKey{TaskType: "PICK", HourBucket: hr(10)}, TasksRecorded: 1},
		{Key: RowKey{TaskType: "PACK", HourBucket: hr(9)}, TasksRecorded: 1},
	}

	got := Build(rows)

	want := []RowKey{
		{TaskType: "PACK", HourBucket: hr(9)},
		{TaskType: "PICK", HourBucket: hr(10)},
		{TaskType: "SLAM", HourBucket: hr(10)},
	}
	if len(got.Rows) != len(want) {
		t.Fatalf("got %d rows, want %d", len(got.Rows), len(want))
	}
	for i, w := range want {
		if got.Rows[i].Key != w {
			t.Errorf("Rows[%d].Key = %+v, want %+v", i, got.Rows[i].Key, w)
		}
	}

	// Build must not mutate or reorder the caller's slice.
	if rows[0].Key.TaskType != "SLAM" {
		t.Errorf("Build reordered the caller's slice: rows[0] = %+v", rows[0].Key)
	}
}

func TestBuildBreakdownAndTotals(t *testing.T) {
	rows := []Row{
		{
			Key: RowKey{TaskType: "PICK", HourBucket: hr(9)},
			// 2 scored at 80 and 100 -> mean 90; 3 measured summing 150 -> 50s.
			TasksRecorded: 3, TasksScored: 2, EfficiencyPctSum: 180,
			TasksMeasured: 3, ActualSecondsSum: 150,
			StandardsDefined: 1,
		},
		{
			Key: RowKey{TaskType: "PICK", HourBucket: hr(10)},
			// 1 scored at 120; 1 measured at 30s.
			TasksRecorded: 1, TasksScored: 1, EfficiencyPctSum: 120,
			TasksMeasured: 1, ActualSecondsSum: 30,
			StandardsRevised: 1,
		},
		{
			Key: RowKey{TaskType: UnclassifiedTaskType, HourBucket: hr(9)},
			// Recorded, never scorable (no task_type -> no standard),
			// and never measured (duration_seconds=0 on the wire).
			TasksRecorded: 5,
		},
	}

	got := Build(rows)

	if len(got.ByTaskType) != 2 {
		t.Fatalf("ByTaskType has %d entries, want 2", len(got.ByTaskType))
	}
	// Sorted by TaskType: "PICK" < "UNCLASSIFIED".
	pick, unclassified := got.ByTaskType[0], got.ByTaskType[1]
	if pick.TaskType != "PICK" || unclassified.TaskType != UnclassifiedTaskType {
		t.Fatalf("breakdown not sorted by task type: %q then %q", pick.TaskType, unclassified.TaskType)
	}

	assertPickBreakdownBar(t, pick)
	assertUnclassifiedBreakdownBar(t, unclassified)
	assertBreakdownTotals(t, got.Totals)
}

// assertPickBreakdownBar pins the PICK bar: counts merged across both
// buckets, means recomputed from the summed raw counters, and the
// standard-lifecycle events carried through.
func assertPickBreakdownBar(t *testing.T, pick TaskTypeBar) {
	t.Helper()
	if pick.TasksRecorded != 4 || pick.TasksScored != 3 || pick.TasksUnscored != 1 {
		t.Errorf("PICK counts = recorded %d / scored %d / unscored %d, want 4/3/1",
			pick.TasksRecorded, pick.TasksScored, pick.TasksUnscored)
	}
	if !eq(pick.MeanEfficiencyPct, ptr(100)) { // (180 + 120) / 3
		t.Errorf("PICK MeanEfficiencyPct = %s, want 100", fmtPtr(pick.MeanEfficiencyPct))
	}
	if !eq(pick.MeanActualSeconds, ptr(45)) { // (150 + 30) / 4
		t.Errorf("PICK MeanActualSeconds = %s, want 45", fmtPtr(pick.MeanActualSeconds))
	}
	if pick.StandardsDefined != 1 || pick.StandardsRevised != 1 {
		t.Errorf("PICK standards = %d defined / %d revised, want 1/1", pick.StandardsDefined, pick.StandardsRevised)
	}
}

// assertUnclassifiedBreakdownBar pins the unclassified bar: a real bar
// with a real count and NO fabricated means — the case the whole
// nil-not-zero discipline exists for.
func assertUnclassifiedBreakdownBar(t *testing.T, unclassified TaskTypeBar) {
	t.Helper()
	if unclassified.TasksRecorded != 5 || unclassified.TasksScored != 0 || unclassified.TasksUnscored != 5 {
		t.Errorf("UNCLASSIFIED counts = %d/%d/%d, want 5/0/5",
			unclassified.TasksRecorded, unclassified.TasksScored, unclassified.TasksUnscored)
	}
	if unclassified.MeanEfficiencyPct != nil {
		t.Errorf("UNCLASSIFIED MeanEfficiencyPct = %v, want nil", *unclassified.MeanEfficiencyPct)
	}
	if unclassified.MeanActualSeconds != nil {
		t.Errorf("UNCLASSIFIED MeanActualSeconds = %v, want nil", *unclassified.MeanActualSeconds)
	}
}

// assertBreakdownTotals pins the window headline: every task counted, and
// the means averaged over the SCORED subset across every task type.
func assertBreakdownTotals(t *testing.T, totals Totals) {
	t.Helper()
	if totals.TasksRecorded != 9 || totals.TasksScored != 3 || totals.TasksUnscored != 6 {
		t.Errorf("Totals = %+v, want recorded 9 / scored 3 / unscored 6", totals)
	}
	// Totals average the SCORED subset across every task type — the
	// unscorable rows must not drag the mean toward zero.
	if !eq(totals.MeanEfficiencyPct, ptr(100)) {
		t.Errorf("Totals.MeanEfficiencyPct = %s, want 100", fmtPtr(totals.MeanEfficiencyPct))
	}
	if !eq(totals.MeanActualSeconds, ptr(45)) {
		t.Errorf("Totals.MeanActualSeconds = %s, want 45", fmtPtr(totals.MeanActualSeconds))
	}
}

func TestBuildAllUnscorableWindowHasNilTotals(t *testing.T) {
	// Every row in the window is a real recorded task with no score at
	// all: the report must still count them and still refuse to invent
	// a mean.
	rows := []Row{
		{Key: RowKey{TaskType: UnclassifiedTaskType, HourBucket: hr(9)}, TasksRecorded: 2},
		{Key: RowKey{TaskType: UnclassifiedTaskType, HourBucket: hr(10)}, TasksRecorded: 3},
	}

	got := Build(rows)

	if got.Totals.TasksRecorded != 5 {
		t.Errorf("Totals.TasksRecorded = %d, want 5", got.Totals.TasksRecorded)
	}
	if got.Totals.MeanEfficiencyPct != nil {
		t.Errorf("Totals.MeanEfficiencyPct = %v, want nil", *got.Totals.MeanEfficiencyPct)
	}
	if got.Totals.MeanActualSeconds != nil {
		t.Errorf("Totals.MeanActualSeconds = %v, want nil", *got.Totals.MeanActualSeconds)
	}
	if len(got.ByTaskType) != 1 || got.ByTaskType[0].MeanEfficiencyPct != nil {
		t.Errorf("ByTaskType = %+v, want a single UNCLASSIFIED bar with a nil mean", got.ByTaskType)
	}
}
