package main_test

// Wave-2 step definitions: data-driven behaviour (scorecards, trends, the
// coaching flag, fleet-wide performance, idle gaps and utilization) and the
// events the service publishes.
//
// The facts those read models report come from fulfillment-execution's
// TaskCompleted Kafka events, which are not REST-reachable. These steps seed
// them through the REAL RecordTaskPerformance use case — the same one the
// Kafka consumer calls — and then every assertion goes through the REST API
// (or, for published events, the recording publisher). Nothing is read from
// the repositories directly.

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/cucumber/godog"

	"github.com/claudioed/labor-performance/internal/application/ports"
	"github.com/claudioed/labor-performance/internal/application/usecases"
	"github.com/claudioed/labor-performance/internal/domain/shared"
)

var (
	// baseInstant is where the scenario clock starts: before activityBase, so
	// a standard defined in a Given is already in force when the auto-timed
	// completions that follow it finish, and in the past so the repositories
	// that resolve "currently active" against the wall clock see it as open.
	baseInstant = time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	// activityBase is where an associate's first auto-timed completion is
	// measured from.
	activityBase = time.Date(2026, 10, 1, 8, 0, 0, 0, time.UTC)
)

// defaultIdleGapBetweenTasks is the idle time auto-timed completions leave
// between one task's completion and the next one's claim.
const defaultIdleGapBetweenTasks = 60

// tickClock is a settable ports.Clock.
type tickClock struct {
	mu sync.Mutex
	at time.Time
}

func (c *tickClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.at
}

func (c *tickClock) Set(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.at = t
}

// recordingPublisher forwards to the production-shaped publisher and
// remembers every event it was asked to publish.
type recordingPublisher struct {
	inner  ports.EventPublisher
	mu     sync.Mutex
	events []shared.DomainEvent
}

func (p *recordingPublisher) Publish(ctx context.Context, evs ...shared.DomainEvent) error {
	p.mu.Lock()
	p.events = append(p.events, evs...)
	p.mu.Unlock()
	return p.inner.Publish(ctx, evs...)
}

func (p *recordingPublisher) named(name string) []shared.DomainEvent {
	p.mu.Lock()
	defer p.mu.Unlock()
	var out []shared.DomainEvent
	for _, e := range p.events {
		if e.EventName() == name {
			out = append(out, e)
		}
	}
	return out
}

// wave2 is the per-scenario state of the wave-2 steps.
type wave2 struct {
	w    *world
	last map[string]time.Time
	seq  int
	reqs map[string]usecases.RecordTaskPerformanceRequest
}

func (s *wave2) reset() {
	s.last = map[string]time.Time{}
	s.reqs = map[string]usecases.RecordTaskPerformanceRequest{}
	s.seq = 0
}

func (s *wave2) record(req usecases.RecordTaskPerformanceRequest) error {
	s.reqs[req.TaskId] = req
	if _, err := s.w.h.record.Execute(context.Background(), req); err != nil {
		return fmt.Errorf("record task %s: %w", req.TaskId, err)
	}
	if prev, ok := s.last[string(req.AssociateId)]; !ok || req.CompletedAt.After(prev) {
		s.last[string(req.AssociateId)] = req.CompletedAt
	}
	return nil
}

// completeAuto records a completion timed relative to the associate's
// previous one: it is claimed idleSeconds after the previous completion and
// takes actualSeconds. An associate's first completion is timed from
// activityBase and has no predecessor to be idle after.
func (s *wave2) completeAuto(associate, taskType string, actualSeconds, idleSeconds int) error {
	var at time.Time
	if prev, ok := s.last[associate]; ok {
		at = prev.Add(time.Duration(idleSeconds+actualSeconds) * time.Second)
	} else {
		at = activityBase.Add(time.Duration(actualSeconds) * time.Second)
	}
	s.seq++
	id := fmt.Sprintf("auto-%d", s.seq)
	return s.record(usecases.RecordTaskPerformanceRequest{
		KafkaEventId:  "evt-" + id,
		TaskId:        id,
		AssociateId:   shared.AssociateId(associate),
		TaskType:      shared.TaskType(taskType),
		ActualSeconds: int64(actualSeconds),
		CompletedAt:   at,
	})
}

func (s *wave2) clockAt(instant string) error {
	t, err := time.Parse(time.RFC3339, instant)
	if err != nil {
		return fmt.Errorf("clock instant %q: %w", instant, err)
	}
	s.w.h.clock.Set(t)
	return nil
}

func (s *wave2) completedTask(associate, taskType string, actualSeconds int) error {
	return s.completeAuto(associate, taskType, actualSeconds, defaultIdleGapBetweenTasks)
}

func (s *wave2) completedTaskAfterIdle(associate, taskType string, actualSeconds, idleSeconds int) error {
	return s.completeAuto(associate, taskType, actualSeconds, idleSeconds)
}

func (s *wave2) completedTasks(associate string, count int, taskType string, actualSeconds int) error {
	for i := 0; i < count; i++ {
		if err := s.completeAuto(associate, taskType, actualSeconds, defaultIdleGapBetweenTasks); err != nil {
			return err
		}
	}
	return nil
}

func (s *wave2) completedTaskAt(associate, taskType, taskId string, actualSeconds int, at string) error {
	t, err := time.Parse(time.RFC3339, at)
	if err != nil {
		return fmt.Errorf("completion instant %q: %w", at, err)
	}
	return s.record(usecases.RecordTaskPerformanceRequest{
		KafkaEventId:  "evt-" + taskId,
		TaskId:        taskId,
		AssociateId:   shared.AssociateId(associate),
		TaskType:      shared.TaskType(taskType),
		ActualSeconds: int64(actualSeconds),
		CompletedAt:   t,
	})
}

func (s *wave2) stationCompletedTask(taskType string, actualSeconds int) error {
	return s.completeAuto("", taskType, actualSeconds, defaultIdleGapBetweenTasks)
}

func (s *wave2) completionDeliveredAgain(taskId string) error {
	req, ok := s.reqs[taskId]
	if !ok {
		return fmt.Errorf("no completion was ever delivered for task %q", taskId)
	}
	_, err := s.w.h.record.Execute(context.Background(), req)
	return err
}

// --- Then steps: read models ----------------------------------------------

func approxEqual(got, want float64) bool { return math.Abs(got-want) < 0.01 }

func (s *wave2) optionalNumber(field string) (*float64, error) {
	obj, err := s.w.decodeLast()
	if err != nil {
		return nil, err
	}
	raw, present := obj[field]
	if !present {
		return nil, fmt.Errorf("response has no field %q: %s", field, string(s.w.lastBody))
	}
	if raw == nil {
		return nil, nil
	}
	n, ok := raw.(float64)
	if !ok {
		return nil, fmt.Errorf("field %q is %T, want a number or null: %s", field, raw, string(s.w.lastBody))
	}
	return &n, nil
}

func (s *wave2) expectNumber(field string, want float64) error {
	got, err := s.optionalNumber(field)
	if err != nil {
		return err
	}
	if got == nil {
		return fmt.Errorf("%s is null, want %v: %s", field, want, string(s.w.lastBody))
	}
	if !approxEqual(*got, want) {
		return fmt.Errorf("%s is %v, want %v", field, *got, want)
	}
	return nil
}

func (s *wave2) expectNull(field string) error {
	got, err := s.optionalNumber(field)
	if err != nil {
		return err
	}
	if got != nil {
		return fmt.Errorf("%s is %v, want null", field, *got)
	}
	return nil
}

func parseFloat(raw string) (float64, error) {
	v, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return 0, fmt.Errorf("number %q: %w", raw, err)
	}
	return v, nil
}

func (s *wave2) scorecardReports(tasks int, pct string) error {
	if err := s.expectCount("taskCount", tasks); err != nil {
		return err
	}
	want, err := parseFloat(pct)
	if err != nil {
		return err
	}
	return s.expectNumber("meanEfficiencyPct", want)
}

func (s *wave2) scorecardReportsNoMean(tasks int) error {
	if err := s.expectCount("taskCount", tasks); err != nil {
		return err
	}
	return s.expectNull("meanEfficiencyPct")
}

func (s *wave2) expectCount(field string, want int) error {
	got, err := s.w.numberField(field)
	if err != nil {
		return err
	}
	if int(got) != want {
		return fmt.Errorf("%s is %d, want %d", field, int(got), want)
	}
	return nil
}

func (s *wave2) scorecardTrend(want string) error {
	got, err := s.w.stringField("trend")
	if err != nil {
		return err
	}
	if got != want {
		return fmt.Errorf("trend is %q, want %q", got, want)
	}
	return nil
}

func (s *wave2) scorecardCoachingFlag(want bool) error {
	obj, err := s.w.decodeLast()
	if err != nil {
		return err
	}
	got, ok := obj["coachingFlag"].(bool)
	if !ok {
		return fmt.Errorf("response has no boolean coachingFlag: %s", string(s.w.lastBody))
	}
	if got != want {
		return fmt.Errorf("coachingFlag is %v, want %v", got, want)
	}
	return nil
}

func (s *wave2) scorecardBreakdown(taskType string, tasks int, pct string) error {
	obj, err := s.w.decodeLast()
	if err != nil {
		return err
	}
	by, ok := obj["byTaskType"].(map[string]any)
	if !ok {
		return fmt.Errorf("response has no byTaskType object: %s", string(s.w.lastBody))
	}
	entry, ok := by[taskType].(map[string]any)
	if !ok {
		return fmt.Errorf("byTaskType has no %q entry: %s", taskType, string(s.w.lastBody))
	}
	if got, _ := entry["taskCount"].(float64); int(got) != tasks {
		return fmt.Errorf("byTaskType[%s].taskCount is %v, want %d", taskType, entry["taskCount"], tasks)
	}
	want, err := parseFloat(pct)
	if err != nil {
		return err
	}
	got, ok := entry["meanEfficiencyPct"].(float64)
	if !ok || !approxEqual(got, want) {
		return fmt.Errorf("byTaskType[%s].meanEfficiencyPct is %v, want %v", taskType, entry["meanEfficiencyPct"], want)
	}
	return nil
}

func (s *wave2) performanceMeanEfficiency(pct string) error {
	want, err := parseFloat(pct)
	if err != nil {
		return err
	}
	return s.expectNumber("meanEfficiencyPct", want)
}

func (s *wave2) performanceNoMeanEfficiency() error { return s.expectNull("meanEfficiencyPct") }

func (s *wave2) performanceMeanActual(seconds string) error {
	want, err := parseFloat(seconds)
	if err != nil {
		return err
	}
	return s.expectNumber("meanActualSeconds", want)
}

func (s *wave2) performanceNoMeanActual() error { return s.expectNull("meanActualSeconds") }

func (s *wave2) utilizationPercent(pct string) error {
	want, err := parseFloat(pct)
	if err != nil {
		return err
	}
	return s.expectNumber("utilizationPct", want)
}

func (s *wave2) windowSeconds(want int) error { return s.expectCount("windowSeconds", want) }

func (s *wave2) responseFieldIs(field, want string) error {
	got, err := s.w.stringField(field)
	if err != nil {
		return err
	}
	if got != want {
		return fmt.Errorf("%s is %q, want %q", field, got, want)
	}
	return nil
}

func (s *wave2) responseOmits(field string) error {
	obj, err := s.w.decodeLast()
	if err != nil {
		return err
	}
	if _, present := obj[field]; present {
		return fmt.Errorf("response must omit %q: %s", field, string(s.w.lastBody))
	}
	return nil
}

// --- Then steps: published events ------------------------------------------

func (s *wave2) eventsPublished(count int, name string) error {
	if got := len(s.w.h.published.named(name)); got != count {
		return fmt.Errorf("%d %s event(s) published, want %d", got, name, count)
	}
	return nil
}

func (s *wave2) recordedEventFor(taskId string) (shared.TaskPerformanceRecorded, error) {
	for _, e := range s.w.h.published.named("TaskPerformanceRecorded") {
		if ev, ok := e.(shared.TaskPerformanceRecorded); ok && ev.TaskId == taskId {
			return ev, nil
		}
	}
	return shared.TaskPerformanceRecorded{}, fmt.Errorf("no TaskPerformanceRecorded event for task %q", taskId)
}

func (s *wave2) recordedEventIdle(taskId string, want int) error {
	ev, err := s.recordedEventFor(taskId)
	if err != nil {
		return err
	}
	if ev.IdleSecondsBefore == nil {
		return fmt.Errorf("task %q event carries no idle seconds before, want %d", taskId, want)
	}
	if int(*ev.IdleSecondsBefore) != want {
		return fmt.Errorf("task %q event idle seconds before is %d, want %d", taskId, *ev.IdleSecondsBefore, want)
	}
	return nil
}

func (s *wave2) recordedEventNoIdle(taskId string) error {
	ev, err := s.recordedEventFor(taskId)
	if err != nil {
		return err
	}
	if ev.IdleSecondsBefore != nil {
		return fmt.Errorf("task %q event carries idle seconds before %d, want none", taskId, *ev.IdleSecondsBefore)
	}
	return nil
}

func (s *wave2) recordedEventEfficiency(taskId, pct string) error {
	ev, err := s.recordedEventFor(taskId)
	if err != nil {
		return err
	}
	want, err := parseFloat(pct)
	if err != nil {
		return err
	}
	if ev.EfficiencyPct == nil || !approxEqual(*ev.EfficiencyPct, want) {
		return fmt.Errorf("task %q event efficiency is %v, want %v", taskId, ev.EfficiencyPct, want)
	}
	return nil
}

func (s *wave2) recordedEventUnscored(taskId string) error {
	ev, err := s.recordedEventFor(taskId)
	if err != nil {
		return err
	}
	if ev.EfficiencyPct != nil {
		return fmt.Errorf("task %q event efficiency is %v, want none", taskId, *ev.EfficiencyPct)
	}
	return nil
}

func (s *wave2) revisedEventReports(previous, next int) error {
	events := s.w.h.published.named("LaborStandardRevised")
	if len(events) != 1 {
		return fmt.Errorf("%d LaborStandardRevised events published, want 1", len(events))
	}
	ev, ok := events[0].(shared.LaborStandardRevised)
	if !ok {
		return fmt.Errorf("unexpected event type %T", events[0])
	}
	if int(ev.PreviousExpectedSeconds) != previous || int(ev.NewExpectedSeconds) != next {
		return fmt.Errorf("revision event reports %d -> %d, want %d -> %d", ev.PreviousExpectedSeconds, ev.NewExpectedSeconds, previous, next)
	}
	return nil
}

func (s *wave2) definedEventReports(taskType string, seconds int) error {
	events := s.w.h.published.named("LaborStandardDefined")
	if len(events) != 1 {
		return fmt.Errorf("%d LaborStandardDefined events published, want 1", len(events))
	}
	ev, ok := events[0].(shared.LaborStandardDefined)
	if !ok {
		return fmt.Errorf("unexpected event type %T", events[0])
	}
	if string(ev.TaskType) != taskType || int(ev.ExpectedSeconds) != seconds {
		return fmt.Errorf("definition event reports %s/%d, want %s/%d", ev.TaskType, ev.ExpectedSeconds, taskType, seconds)
	}
	return nil
}

// --- When steps: raw requests ------------------------------------------------

func (s *wave2) associateUtilizationWindowed(associate, window string) error {
	return s.w.do(http.MethodGet, "/associates/"+associate+"/utilization?window="+window, nil)
}

func (s *wave2) malformedStandardBody() error {
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, s.w.server.URL+"/standards", bytes.NewReader([]byte(`{"taskType": "PICK", "expectedSeconds": `)))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.w.server.Client().Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	s.w.lastStatus = resp.StatusCode
	s.w.lastBody = raw
	s.w.lastContentType = resp.Header.Get("Content-Type")
	return nil
}

func registerWave2(sc *godog.ScenarioContext, w *world) {
	s := &wave2{w: w}
	sc.Before(func(ctx context.Context, _ *godog.Scenario) (context.Context, error) {
		s.reset()
		return ctx, nil
	})

	const tt = `(PICK|PACK|SLAM)`

	sc.Step(`^the clock is at "([^"]*)"$`, s.clockAt)
	sc.Step(`^associate "([^"]*)" completed a `+tt+` task in (\d+) seconds$`, s.completedTask)
	sc.Step(`^associate "([^"]*)" completed a `+tt+` task in (\d+) seconds after being idle for (\d+) seconds$`, s.completedTaskAfterIdle)
	sc.Step(`^associate "([^"]*)" completed (\d+) `+tt+` tasks of (\d+) seconds each$`, s.completedTasks)
	sc.Step(`^associate "([^"]*)" completed a `+tt+` task "([^"]*)" in (\d+) seconds at "([^"]*)"$`, s.completedTaskAt)
	sc.Step(`^a station with no checked-in associate completed a `+tt+` task in (\d+) seconds$`, s.stationCompletedTask)
	sc.Step(`^the completion of task "([^"]*)" is delivered again$`, s.completionDeliveredAgain)

	sc.Step(`^the scorecard reports (\d+) tasks? and a mean efficiency of ([0-9.]+) percent$`, s.scorecardReports)
	sc.Step(`^the scorecard reports (\d+) tasks? and no mean efficiency$`, s.scorecardReportsNoMean)
	sc.Step(`^the scorecard trend is "([^"]*)"$`, s.scorecardTrend)
	sc.Step(`^the scorecard coaching flag is (true|false)$`, func(v string) error { return s.scorecardCoachingFlag(v == "true") })
	sc.Step(`^the scorecard breaks down "([^"]*)" as (\d+) tasks? with a mean efficiency of ([0-9.]+) percent$`, s.scorecardBreakdown)

	sc.Step(`^the task type performance response reports a mean efficiency of ([0-9.]+) percent$`, s.performanceMeanEfficiency)
	sc.Step(`^the task type performance response reports no mean efficiency$`, s.performanceNoMeanEfficiency)
	sc.Step(`^the task type performance response reports a mean actual duration of ([0-9.]+) seconds$`, s.performanceMeanActual)
	sc.Step(`^the task type performance response reports no mean actual duration$`, s.performanceNoMeanActual)

	sc.Step(`^the utilization response reports a utilization percent of ([0-9.]+)$`, s.utilizationPercent)
	sc.Step(`^the utilization response reports a (\d+) second window$`, s.windowSeconds)

	sc.Step(`^the response field "([^"]*)" is "([^"]*)"$`, s.responseFieldIs)
	sc.Step(`^the response omits the field "([^"]*)"$`, s.responseOmits)

	sc.Step(`^(\d+) "([^"]*)" events? (?:was|were) published$`, s.eventsPublished)
	sc.Step(`^the "TaskPerformanceRecorded" event for task "([^"]*)" carries (\d+) idle seconds before$`, s.recordedEventIdle)
	sc.Step(`^the "TaskPerformanceRecorded" event for task "([^"]*)" carries no idle seconds before$`, s.recordedEventNoIdle)
	sc.Step(`^the "TaskPerformanceRecorded" event for task "([^"]*)" reports an efficiency of ([0-9.]+) percent$`, s.recordedEventEfficiency)
	sc.Step(`^the "TaskPerformanceRecorded" event for task "([^"]*)" reports no efficiency$`, s.recordedEventUnscored)
	sc.Step(`^the "LaborStandardRevised" event reports previous expected seconds (\d+) and new expected seconds (\d+)$`, s.revisedEventReports)
	sc.Step(`^the "LaborStandardDefined" event reports task type "([^"]*)" and expected seconds (\d+)$`, s.definedEventReports)

	sc.Step(`^the utilization for associate "([^"]*)" is requested with window "([^"]*)"$`, s.associateUtilizationWindowed)
	sc.Step(`^a standard definition with a malformed JSON body is sent$`, s.malformedStandardBody)
}
