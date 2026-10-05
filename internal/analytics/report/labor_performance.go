// Package report holds the labor-performance "Labor Performance Report"
// read model: the shapes of the analytical report this service's data
// product serves, the aggregation that derives it, the query that selects
// it, and the outbound ports the writer (projector) and reader (reports)
// adapters implement. It is a read-model region that depends on nothing
// else in this module — the OLTP domain and application layers must not
// import it, and it must not import them (ADR-0007), mirroring
// order-management's own internal/analytics/report isolation.
package report

import (
	"sort"
	"strings"
	"time"
)

// Granularity is the time-bucket resolution a report is rolled up to. Only
// hourly buckets are modelled for this round, matching the sibling data
// products in the fleet.
type Granularity string

// GranularityHour rolls rows up into UTC hour buckets.
const GranularityHour Granularity = "hour"

// UnclassifiedTaskType is the explicit label for rows whose TaskType this
// service could not classify. It is a REAL, expected bucket, not a defect:
// fulfillment-execution's TaskCompleted payload does not yet carry a
// task_type field (a documented wire-contract gap — see the OLTP consumer's
// use of shared.ParseTaskTypeLenient), so every TaskPerformance recorded
// today arrives with an empty task type. The report surfaces that bucket
// honestly under this label rather than dropping the rows, inventing a task
// type for them, or keying a fact table on an empty string.
const UnclassifiedTaskType = "UNCLASSIFIED"

// NormalizeTaskType puts a task type into the canonical form the rollup is
// keyed by: trimmed and upper-cased, so "pick" and "PICK" are one bucket
// rather than two. An empty or whitespace-only value normalises to
// UnclassifiedTaskType, which is a legitimate bucket and never an error.
func NormalizeTaskType(taskType string) string {
	if trimmed := strings.ToUpper(strings.TrimSpace(taskType)); trimmed != "" {
		return trimmed
	}
	return UnclassifiedTaskType
}

// HourBucket truncates t to its UTC hour — the report's single time
// dimension. Both store implementations route through it so they cannot
// disagree about which bucket an event belongs to.
func HourBucket(t time.Time) time.Time {
	return t.UTC().Truncate(time.Hour)
}

// RowKey identifies a single report row: the TaskType the labor was spent
// on, and the UTC hour bucket the row aggregates.
//
// The bucket dimension is derived from the BUSINESS time of the event (a
// TaskPerformance's CompletedAt, a standard's EffectiveFrom), never the
// ingestion time — a replayed or backfilled event belongs in the hour the
// work actually happened, the same "as of the event's own time, not as of
// now" discipline the OLTP side established in ADR-0004.
type RowKey struct {
	TaskType   string
	HourBucket time.Time
}

// Row is one aggregated report row for a (taskType, hourBucket) key.
//
// The counters are stored raw — sums and counts, never a mean — so the
// means can be derived at read time, and so two rows can be merged by
// simple addition (which the per-task-type breakdown relies on). The
// derived means are METHODS, not fields, and are nullable: a bucket with
// tasks but no scored task has no mean, and reporting one would be
// fabricating a number (ADR-0004's discipline, carried onto the analytical
// side).
type Row struct {
	Key RowKey

	// TasksRecorded is every TaskPerformanceRecorded projected into this
	// bucket, including rows with no AssociateId (e.g. a robot station)
	// and rows that are unscorable or unmeasurable — the honest
	// denominator.
	TasksRecorded int
	// TasksScored is the subset of TasksRecorded whose EfficiencyPct was
	// non-nil, i.e. an active standard existed at completion time AND the
	// actual duration was positive.
	TasksScored int
	// EfficiencyPctSum is the sum of EfficiencyPct over exactly the
	// TasksScored rows. Meaningless on its own; the mean is what the
	// report serves.
	EfficiencyPctSum float64
	// TasksMeasured is the subset of TasksRecorded whose ActualSeconds was
	// > 0. Wider than TasksScored: a task can be measured (we know how
	// long it took) without being scored (no standard to compare against).
	TasksMeasured int
	// ActualSecondsSum is the sum of ActualSeconds over exactly the
	// TasksMeasured rows.
	ActualSecondsSum int64

	// StandardsDefined counts LaborStandardDefined events effective in
	// this bucket — a first standard for the TaskType.
	StandardsDefined int
	// StandardsRevised counts LaborStandardRevised events effective in
	// this bucket. A non-zero value is the reader's cue that efficiency
	// numbers on either side of this bucket are measured against
	// DIFFERENT standards and are not directly comparable.
	StandardsRevised int
}

// TasksUnscored is the number of tasks in this bucket that were recorded
// but could not be scored — no active standard at completion time, or a
// non-positive ActualSeconds. Always TasksRecorded - TasksScored, and never
// negative.
func (r Row) TasksUnscored() int {
	if n := r.TasksRecorded - r.TasksScored; n > 0 {
		return n
	}
	return 0
}

// MeanEfficiencyPct is the mean EfficiencyPct across the scored tasks in
// this bucket, or nil when no task in the bucket was scored. Nil is a real
// answer ("we have tasks, but nothing to compare them against"), never
// rendered as a zero.
func (r Row) MeanEfficiencyPct() *float64 {
	return mean(r.EfficiencyPctSum, r.TasksScored)
}

// MeanActualSeconds is the mean ActualSeconds across the measured tasks in
// this bucket, or nil when no task in the bucket had a positive duration.
// Independent of whether any standard exists — the same distinction the
// OLTP read model draws in ADR-0006.
func (r Row) MeanActualSeconds() *float64 {
	return mean(float64(r.ActualSecondsSum), r.TasksMeasured)
}

// TaskTypeBar is one bar of the per-TaskType breakdown: the whole report
// window collapsed onto a single TaskType, which is the shape the console's
// WES Dashboard bar chart consumes.
type TaskTypeBar struct {
	TaskType      string
	TasksRecorded int
	TasksScored   int
	// TasksUnscored is TasksRecorded - TasksScored, never negative.
	TasksUnscored int
	// TasksMeasured is the subset with a positive ActualSeconds — the
	// denominator of MeanActualSeconds. Wider than TasksScored (ADR
	// 0006's measured-without-scored distinction) and surfaced on the
	// wire so a chart can show both denominators honestly.
	TasksMeasured int
	// MeanEfficiencyPct / MeanActualSeconds are recomputed from the
	// summed raw counters, nullable when the respective subset is empty.
	MeanEfficiencyPct *float64
	MeanActualSeconds *float64
	// StandardsDefined and StandardsRevised carry the standard-lifecycle
	// events that landed in the window for this TaskType. A bar whose
	// StandardsRevised is non-zero is one whose MeanEfficiencyPct spans a
	// standard change and is therefore measured against two different
	// yardsticks — the chart can annotate it rather than presenting the
	// mean as if it were a like-for-like comparison.
	StandardsDefined int
	StandardsRevised int
}

// Totals is the entire queried window collapsed to a single headline —
// what a dashboard panel shows above the chart. Its means are computed
// from the summed raw counters across every TaskType, so they are weighted
// by volume, and they are nullable for exactly the same reason a Row's
// are.
type Totals struct {
	TasksRecorded int
	TasksScored   int
	TasksUnscored int
	TasksMeasured int
	// MeanEfficiencyPct / MeanActualSeconds are derived once, here, from
	// the summed raw counters; nullable for exactly the same reason a
	// Row's are.
	MeanEfficiencyPct *float64
	MeanActualSeconds *float64
}

// LaborPerformanceReport is the full result of a report query: the matching
// (taskType, hour) rows, the per-TaskType breakdown derived from them, and
// the window totals. Rows and ByTaskType are always non-nil (possibly
// empty) so a caller — including a JSON encoder feeding a chart — never has
// to special-case a null.
type LaborPerformanceReport struct {
	Rows       []Row
	ByTaskType []TaskTypeBar
	Totals     Totals
}

// Build assembles a report from the rows a store selected: it orders the
// rows deterministically (hour bucket, then task type) and derives the
// per-TaskType breakdown. Both store implementations route through it, so
// the in-memory and Postgres adapters cannot disagree about aggregation —
// only about how rows are fetched.
func Build(rows []Row) LaborPerformanceReport {
	ordered := make([]Row, len(rows))
	copy(ordered, rows)
	sort.Slice(ordered, func(i, j int) bool {
		if !ordered[i].Key.HourBucket.Equal(ordered[j].Key.HourBucket) {
			return ordered[i].Key.HourBucket.Before(ordered[j].Key.HourBucket)
		}
		return ordered[i].Key.TaskType < ordered[j].Key.TaskType
	})
	return LaborPerformanceReport{
		Rows:       ordered,
		ByTaskType: breakdown(ordered),
		Totals:     totalsOf(ordered),
	}
}

// totalsOf collapses every row in the window onto one headline. Like
// breakdown, it sums the RAW counters and derives the means once at the
// end, so a window whose only tasks were unscorable reports a nil mean
// beside a real, non-zero task count — the headline says "we saw 40 tasks
// and can score none of them", never "0% efficiency".
func totalsOf(rows []Row) Totals {
	var acc Row
	for _, row := range rows {
		acc.TasksRecorded += row.TasksRecorded
		acc.TasksScored += row.TasksScored
		acc.EfficiencyPctSum += row.EfficiencyPctSum
		acc.TasksMeasured += row.TasksMeasured
		acc.ActualSecondsSum += row.ActualSecondsSum
	}
	return Totals{
		TasksRecorded:     acc.TasksRecorded,
		TasksScored:       acc.TasksScored,
		TasksUnscored:     acc.TasksUnscored(),
		TasksMeasured:     acc.TasksMeasured,
		MeanEfficiencyPct: acc.MeanEfficiencyPct(),
		MeanActualSeconds: acc.MeanActualSeconds(),
	}
}

// breakdown collapses rows into one bar per TaskType, sorted by TaskType so
// the chart's bar order is stable across refreshes.
//
// The means are recomputed from the SUMMED raw counters rather than
// averaged from the per-bucket means, so a busy hour weighs more than a
// quiet one (a mean of means would silently mis-weight them). A TaskType
// present in the window with tasks but no scored task yields a bar with a
// nil MeanEfficiencyPct — an empty bucket is a legitimate bar, not a reason
// to drop the TaskType from the chart.
func breakdown(rows []Row) []TaskTypeBar {
	totals := map[string]*Row{}
	order := []string{}
	for _, row := range rows {
		t, ok := totals[row.Key.TaskType]
		if !ok {
			t = &Row{Key: RowKey{TaskType: row.Key.TaskType}}
			totals[row.Key.TaskType] = t
			order = append(order, row.Key.TaskType)
		}
		t.TasksRecorded += row.TasksRecorded
		t.TasksScored += row.TasksScored
		t.EfficiencyPctSum += row.EfficiencyPctSum
		t.TasksMeasured += row.TasksMeasured
		t.ActualSecondsSum += row.ActualSecondsSum
		t.StandardsDefined += row.StandardsDefined
		t.StandardsRevised += row.StandardsRevised
	}

	sort.Strings(order)
	bars := make([]TaskTypeBar, 0, len(order))
	for _, taskType := range order {
		t := totals[taskType]
		bars = append(bars, TaskTypeBar{
			TaskType:          taskType,
			TasksRecorded:     t.TasksRecorded,
			TasksScored:       t.TasksScored,
			TasksUnscored:     t.TasksUnscored(),
			TasksMeasured:     t.TasksMeasured,
			MeanEfficiencyPct: t.MeanEfficiencyPct(),
			MeanActualSeconds: t.MeanActualSeconds(),
			StandardsDefined:  t.StandardsDefined,
			StandardsRevised:  t.StandardsRevised,
		})
	}
	return bars
}

// mean divides sum by n, returning nil when n is not positive — the single
// place this read model refuses to fabricate a number out of an empty
// bucket, and the reason no caller ever divides by a count itself.
func mean(sum float64, n int) *float64 {
	if n <= 0 {
		return nil
	}
	m := sum / float64(n)
	return &m
}

// ReportQuery selects and filters the rows a report covers. From is
// inclusive and To is exclusive, both compared against a row's HourBucket.
// TaskType is an optional exact-match filter; empty means "no filter on
// this dimension". Callers normalise it with NormalizeTaskType before
// querying, so "pick" matches the PICK bucket and an explicit
// "UNCLASSIFIED" selects the unclassified one.
type ReportQuery struct {
	From        time.Time
	To          time.Time
	TaskType    string
	Granularity Granularity
}
