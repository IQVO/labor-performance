package main_test

// Documentation-derived rule steps (BDD wave 3). The scenarios in
// features/*_rules.feature pin rules that the warehouse-docs pack for this
// context states (business-context, use-cases, aggregate-design-canvas,
// domain-events, troubleshooting) and that no earlier scenario asserted.
//
// Like the earlier steps these never bypass the service's own contracts:
//   - completions are seeded through the real RecordTaskPerformance use case
//     (what the Kafka consumer calls);
//   - published events are encoded by the REAL analytics and integration
//     encoders (outbound/kafka) and the analytics ones are consumed by the
//     REAL AnalyticsConsumer into the in-memory report store, which the REAL
//     reports router serves (steps_wire_rules_test.go);
//   - REST assertions go through the OLTP router.
//
// Out of reach by design and therefore NOT pinned here: the Idempotency-Key
// middleware (needs a pgx pool: integration tests), the transactional outbox
// relay, and the Kafka consumer's wire decoding and DLQ (unexported in
// internal/adapters/inbound/kafka, which has its own unit and integration
// tests).

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/cucumber/godog"

	inboundhttp "github.com/claudioed/labor-performance/internal/adapters/inbound/http"
	"github.com/claudioed/labor-performance/internal/application/ports"
	"github.com/claudioed/labor-performance/internal/application/usecases"
	"github.com/claudioed/labor-performance/internal/domain/shared"
)

const problemBase = "https://errors.labor-performance.warehouse-systems.dev/"

// probeState is the controllable Postgres and Kafka the readiness gate asks.
type probeState struct {
	pgUp, kafkaUp atomic.Bool
	// rounds counts the dependency-check rounds the gate actually ran (one
	// Postgres ping per round), as opposed to the probes it answered from
	// its cached verdict.
	rounds atomic.Int64
}

func newProbeState() *probeState {
	p := &probeState{}
	p.pgUp.Store(true)
	p.kafkaUp.Store(true)
	return p
}

func (p *probeState) readiness(ttl time.Duration) *inboundhttp.Readiness {
	return inboundhttp.NewReadiness(ttl, 0,
		inboundhttp.Dependency{Name: "postgres", Check: func(context.Context) error {
			p.rounds.Add(1)
			if !p.pgUp.Load() {
				return errors.New("connection refused")
			}
			return nil
		}},
		inboundhttp.Dependency{Name: "kafka", Check: func(context.Context) error {
			if !p.kafkaUp.Load() {
				return errors.New("no broker answered a metadata request")
			}
			return nil
		}},
	)
}

// snapshot copies every event published so far, in publication order.
func (p *recordingPublisher) snapshot() []shared.DomainEvent {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]shared.DomainEvent(nil), p.events...)
}

// rules is the per-scenario state of the wave-3 steps.
type rules struct {
	w       *world
	s       *wave2
	wire    *wireState
	reports *reportsState
	// allowOrigin is the Access-Control-Allow-Origin of the last preflight.
	allowOrigin string
}

func (r *rules) reset() {
	r.wire = newWireState()
	r.closeReports()
}

// --- raw HTTP ---------------------------------------------------------------

// doURL performs one request against an absolute URL with a verbatim body and
// records it as the "last" response.
func (w *world) doURL(method, url string, body []byte) error {
	req, err := http.NewRequestWithContext(context.Background(), method, url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build %s %s: %w", method, url, err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := w.server.Client().Do(req)
	if err != nil {
		return fmt.Errorf("%s %s: %w", method, url, err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read %s %s response: %w", method, url, err)
	}
	w.lastStatus, w.lastBody, w.lastContentType = resp.StatusCode, raw, resp.Header.Get("Content-Type")
	return nil
}

func (r *rules) pathRequested(path string) error {
	return r.w.doURL(http.MethodGet, r.w.server.URL+path, nil)
}

func (r *rules) definitionWithBody(body string) error {
	body = strings.ReplaceAll(body, "{NUL}", "\x00")
	return r.w.doURL(http.MethodPost, r.w.server.URL+"/standards", []byte(body))
}

func (r *rules) probe(path string) error { return r.pathRequested(path) }

// --- problem details, generic JSON assertions -------------------------------

func (r *rules) problemTypeIs(slug string) error {
	got, err := r.w.stringField("type")
	if err != nil {
		return err
	}
	if want := problemBase + slug; got != want {
		return fmt.Errorf("problem type is %q, want %q (body: %s)", got, want, string(r.w.lastBody))
	}
	return nil
}

func (r *rules) problemDetailMentions(text string) error {
	got, err := r.w.stringField("detail")
	if err != nil {
		return err
	}
	if !strings.Contains(got, text) {
		return fmt.Errorf("problem detail %q does not mention %q", got, text)
	}
	return nil
}

func (r *rules) statusFieldIs(want string) error { return r.w.expectString("status", want) }

// lookup walks a dotted path ("rows.0.taskType") through decoded JSON.
func lookup(root any, path string) (any, bool) {
	if path == "" {
		return root, true
	}
	cur := root
	for _, part := range strings.Split(path, ".") {
		switch node := cur.(type) {
		case map[string]any:
			next, ok := node[part]
			if !ok {
				return nil, false
			}
			cur = next
		case []any:
			i, err := strconv.Atoi(part)
			if err != nil || i < 0 || i >= len(node) {
				return nil, false
			}
			cur = node[i]
		default:
			return nil, false
		}
	}
	return cur, true
}

func (r *rules) decodeAny(body []byte) (any, error) {
	var root any
	if err := json.Unmarshal(body, &root); err != nil {
		return nil, fmt.Errorf("decode response %q: %w", string(body), err)
	}
	return root, nil
}

// jsonEqual compares decoded JSON values, numbers within 0.01.
func jsonEqual(got, want any) bool {
	gf, gok := got.(float64)
	wf, wok := want.(float64)
	if gok && wok {
		return math.Abs(gf-wf) < 0.01
	}
	return reflect.DeepEqual(got, want)
}

func (r *rules) pathIs(path, literal string) error {
	root, err := r.decodeAny(r.w.lastBody)
	if err != nil {
		return err
	}
	got, ok := lookup(root, path)
	if !ok {
		return fmt.Errorf("response has no %q: %s", path, string(r.w.lastBody))
	}
	var want any
	if err := json.Unmarshal([]byte(literal), &want); err != nil {
		return fmt.Errorf("expected value %q is not a JSON literal: %w", literal, err)
	}
	if !jsonEqual(got, want) {
		return fmt.Errorf("%q is %v, want %v", path, got, want)
	}
	return nil
}

func (r *rules) pathAbsent(path string) error {
	root, err := r.decodeAny(r.w.lastBody)
	if err != nil {
		return err
	}
	if got, ok := lookup(root, path); ok {
		return fmt.Errorf("response must not carry %q, got %v", path, got)
	}
	return nil
}

func (r *rules) listHas(path string, count int) error {
	root, err := r.decodeAny(r.w.lastBody)
	if err != nil {
		return err
	}
	got, ok := lookup(root, path)
	if !ok {
		return fmt.Errorf("response has no %q: %s", path, string(r.w.lastBody))
	}
	list, ok := got.([]any)
	if !ok {
		return fmt.Errorf("%q is %T, want a list (a null list breaks chart bindings): %s", path, got, string(r.w.lastBody))
	}
	if len(list) != count {
		return fmt.Errorf("%q has %d items, want %d", path, len(list), count)
	}
	return nil
}

// --- CORS -------------------------------------------------------------------

func (r *rules) preflight(origin, method, path, api string) error {
	base := r.w.server.URL
	if api == "reports" {
		base = r.ensureReports().server.URL
	}
	req, err := http.NewRequestWithContext(context.Background(), http.MethodOptions, base+path, http.NoBody)
	if err != nil {
		return err
	}
	req.Header.Set("Origin", origin)
	req.Header.Set("Access-Control-Request-Method", method)
	resp, err := r.w.server.Client().Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	r.allowOrigin = resp.Header.Get("Access-Control-Allow-Origin")
	return nil
}

func (r *rules) preflightStep(origin, method, path, api string) error {
	return r.preflight(origin, method, path, strings.ToLower(api))
}

func (r *rules) browserVerdict(origin, verdict string) error {
	allowed := r.allowOrigin == origin
	if allowed != (verdict == "allowed") {
		return fmt.Errorf("preflight answered Access-Control-Allow-Origin %q for origin %q, want the call %s", r.allowOrigin, origin, verdict)
	}
	return nil
}

// --- readiness --------------------------------------------------------------

func (r *rules) dependencyState(name, change string) error {
	flag := &r.w.h.deps.pgUp
	if name == "Kafka" {
		flag = &r.w.h.deps.kafkaUp
	}
	flag.Store(change == "answers again")
	return nil
}

func (r *rules) verdictNotCached() error {
	r.w.h.srv.Readiness = r.w.h.deps.readiness(time.Nanosecond)
	return nil
}

func (r *rules) startDraining() error {
	r.w.h.srv.Readiness.SetNotReady()
	return nil
}

func (r *rules) dependencyRounds(want int) error {
	if got := int(r.w.h.deps.rounds.Load()); got != want {
		return fmt.Errorf("dependency checks ran %d time(s), want %d", got, want)
	}
	return nil
}

// --- standards: scripted write failures -------------------------------------

func (r *rules) nextWriteFails(kind string) error {
	switch kind {
	case "loses an optimistic-concurrency race":
		r.w.h.standards.FailNextSaveWith = ports.ErrConcurrentModification
	case "hits the one-open-standard unique index":
		r.w.h.standards.FailNextSaveWith = ports.ErrOpenStandardConflict
	default:
		r.w.h.standards.FailNextSaveWith = errors.New("storage: disk full")
	}
	return nil
}

// --- completions ------------------------------------------------------------

func parseInstant(raw string) (time.Time, error) {
	t, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return time.Time{}, fmt.Errorf("instant %q: %w", raw, err)
	}
	return t, nil
}

func (r *rules) completedWithDuration(associate, taskType, taskId string, seconds int, at string) error {
	t, err := parseInstant(at)
	if err != nil {
		return err
	}
	return r.s.record(usecases.RecordTaskPerformanceRequest{
		KafkaEventId: "evt-" + taskId, TaskId: taskId,
		AssociateId: shared.AssociateId(associate), TaskType: shared.TaskType(taskType),
		ActualSeconds: int64(seconds), CompletedAt: t,
	})
}

// completedUnrecognised seeds a completion whose raw task_type goes through
// shared.ParseTaskTypeLenient, exactly as the Kafka consumer does.
func (r *rules) completedUnrecognised(associate, taskId, rawType string, seconds int, at string) error {
	t, err := parseInstant(at)
	if err != nil {
		return err
	}
	return r.s.record(usecases.RecordTaskPerformanceRequest{
		KafkaEventId: "evt-" + taskId, TaskId: taskId,
		AssociateId: shared.AssociateId(associate), TaskType: shared.ParseTaskTypeLenient(rawType),
		ActualSeconds: int64(seconds), CompletedAt: t,
	})
}

func (r *rules) completedHandledAt(associate, taskType, taskId, unit string, seconds int, at string) error {
	t, err := parseInstant(at)
	if err != nil {
		return err
	}
	return r.s.record(usecases.RecordTaskPerformanceRequest{
		KafkaEventId: "evt-" + taskId, TaskId: taskId,
		AssociateId: shared.AssociateId(associate), TaskType: shared.TaskType(taskType),
		HandlingUnit: shared.HandlingUnit(unit), ActualSeconds: int64(seconds), CompletedAt: t,
	})
}

func (r *rules) deliveredAgainUnderNewEventId(taskId string) error {
	req, ok := r.s.reqs[taskId]
	if !ok {
		return fmt.Errorf("no completion was ever delivered for task %q", taskId)
	}
	req.KafkaEventId += "-redelivered-with-a-new-id"
	return r.s.record(req)
}

// redeliveredCarrying delivers the event of task `original` again under the
// SAME event id but with another task id and duration.
func (r *rules) redeliveredCarrying(original, taskId string, seconds int) error {
	req, ok := r.s.reqs[original]
	if !ok {
		return fmt.Errorf("no completion was ever delivered for task %q", original)
	}
	req.TaskId, req.ActualSeconds = taskId, int64(seconds)
	_, err := r.w.h.record.Execute(context.Background(), req)
	return err
}

func (r *rules) efficiencyRecorded(taskId, want string) error {
	if want == "none" {
		return r.s.recordedEventUnscored(taskId)
	}
	return r.s.recordedEventEfficiency(taskId, want)
}

func (r *rules) latestRecorded() (shared.TaskPerformanceRecorded, error) {
	evs := r.w.h.published.named("TaskPerformanceRecorded")
	if len(evs) == 0 {
		return shared.TaskPerformanceRecorded{}, errors.New("no TaskPerformanceRecorded event was published")
	}
	ev, ok := evs[len(evs)-1].(shared.TaskPerformanceRecorded)
	if !ok {
		return shared.TaskPerformanceRecorded{}, fmt.Errorf("unexpected event type %T", evs[len(evs)-1])
	}
	return ev, nil
}

func (r *rules) latestIdle(want int) error {
	ev, err := r.latestRecorded()
	if err != nil {
		return err
	}
	if ev.IdleSecondsBefore == nil || int(*ev.IdleSecondsBefore) != want {
		return fmt.Errorf("latest event idle seconds before is %v, want %d", ev.IdleSecondsBefore, want)
	}
	return nil
}

func (r *rules) latestNoIdle() error {
	ev, err := r.latestRecorded()
	if err != nil {
		return err
	}
	if ev.IdleSecondsBefore != nil {
		return fmt.Errorf("latest event idle seconds before is %d, want none (a zero gap is not a gap)", *ev.IdleSecondsBefore)
	}
	return nil
}

func (r *rules) recordedEventCarries(taskId, associate, taskType string, seconds int, at string) error {
	ev, err := r.s.recordedEventFor(taskId)
	if err != nil {
		return err
	}
	want, err := parseInstant(at)
	if err != nil {
		return err
	}
	if string(ev.AssociateId) != associate || string(ev.TaskType) != taskType || int(ev.ActualSeconds) != seconds || !ev.CompletedAt.Equal(want) {
		return fmt.Errorf("event for %s carries %q/%q/%ds/%s, want %q/%q/%ds/%s",
			taskId, ev.AssociateId, ev.TaskType, ev.ActualSeconds, ev.CompletedAt.Format(time.RFC3339), associate, taskType, seconds, at)
	}
	return nil
}

func (r *rules) scorecardBreakdownUnscored(taskType string, tasks int) error {
	obj, err := r.w.decodeLast()
	if err != nil {
		return err
	}
	by, _ := obj["byTaskType"].(map[string]any)
	entry, ok := by[taskType].(map[string]any)
	if !ok {
		return fmt.Errorf("byTaskType has no %q entry: %s", taskType, string(r.w.lastBody))
	}
	if got, _ := entry["taskCount"].(float64); int(got) != tasks {
		return fmt.Errorf("byTaskType[%s].taskCount is %v, want %d", taskType, entry["taskCount"], tasks)
	}
	if v, present := entry["meanEfficiencyPct"]; !present || v != nil {
		return fmt.Errorf("byTaskType[%s].meanEfficiencyPct must be present and null, got %v", taskType, v)
	}
	return nil
}

// registerRules wires every wave-3 step (this file plus the wire and report
// steps) into the scenario context.
func registerRules(sc *godog.ScenarioContext, w *world, s *wave2) {
	r := &rules{w: w, s: s}
	sc.Before(func(ctx context.Context, _ *godog.Scenario) (context.Context, error) {
		r.reset()
		return ctx, nil
	})
	sc.After(func(ctx context.Context, _ *godog.Scenario, _ error) (context.Context, error) {
		r.closeReports()
		return ctx, nil
	})

	const tt = `(PICK|PACK|SLAM)`

	sc.Step(`^the problem type is "([^"]*)"$`, r.problemTypeIs)
	sc.Step(`^the problem detail mentions "([^"]*)"$`, r.problemDetailMentions)
	sc.Step(`^the response status field is "([^"]*)"$`, r.statusFieldIs)
	sc.Step(`^the response path "([^"]*)" is (.+)$`, r.pathIs)
	sc.Step(`^the response has no path "([^"]*)"$`, r.pathAbsent)
	sc.Step(`^the response list "([^"]*)" has (\d+) items?$`, r.listHas)
	sc.Step(`^the path "([^"]*)" is requested$`, r.pathRequested)
	sc.Step(`^a standard definition with the body '(.*)' is sent$`, r.definitionWithBody)

	sc.Step(`^a browser preflight from origin "([^"]*)" asks to (GET|POST|PUT|DELETE) "([^"]*)" on the (OLTP|reports) API$`, r.preflightStep)
	sc.Step(`^the browser is (allowed|refused) to call from origin "([^"]*)"$`, func(verdict, origin string) error { return r.browserVerdict(origin, verdict) })

	sc.Step(`^the readiness probe is requested$`, func() error { return r.probe("/readyz") })
	sc.Step(`^the liveness probe is requested$`, func() error { return r.probe("/healthz") })
	sc.Step(`^(Postgres|Kafka) (stops answering|answers again)$`, r.dependencyState)
	sc.Step(`^the readiness verdict is not cached$`, r.verdictNotCached)
	sc.Step(`^the pod receives SIGTERM and starts draining$`, r.startDraining)
	sc.Step(`^the dependency checks ran (\d+) times?$`, r.dependencyRounds)

	sc.Step(`^the next standard write (loses an optimistic-concurrency race|hits the one-open-standard unique index|fails with an unexpected storage error)$`, r.nextWriteFails)

	sc.Step(`^associate "([^"]*)" completed a `+tt+` task "([^"]*)" with a duration of (-?\d+) seconds at "([^"]*)"$`, r.completedWithDuration)
	sc.Step(`^associate "([^"]*)" completed a task "([^"]*)" of unrecognised type "([^"]*)" in (\d+) seconds at "([^"]*)"$`, r.completedUnrecognised)
	sc.Step(`^associate "([^"]*)" completed a `+tt+` task "([^"]*)" for handling unit "([^"]*)" in (\d+) seconds at "([^"]*)"$`, r.completedHandledAt)
	sc.Step(`^the completion of task "([^"]*)" is delivered again under a new event id$`, r.deliveredAgainUnderNewEventId)
	sc.Step(`^the event of task "([^"]*)" is redelivered carrying task "([^"]*)" and a duration of (\d+) seconds$`, r.redeliveredCarrying)
	sc.Step(`^the efficiency recorded for task "([^"]*)" is "(none|[0-9.]+)"$`, r.efficiencyRecorded)
	sc.Step(`^the latest "TaskPerformanceRecorded" event carries (\d+) idle seconds before$`, r.latestIdle)
	sc.Step(`^the latest "TaskPerformanceRecorded" event carries no idle seconds before$`, r.latestNoIdle)
	sc.Step(`^the "TaskPerformanceRecorded" event for task "([^"]*)" carries associate "([^"]*)", task type "([^"]*)", (\d+) actual seconds and completed at "([^"]*)"$`, r.recordedEventCarries)
	sc.Step(`^the scorecard breaks down "([^"]*)" as (\d+) tasks? with no mean efficiency$`, r.scorecardBreakdownUnscored)

	registerWireSteps(sc, r)   // steps_wire_rules_test.go
	registerReportSteps(sc, r) // steps_reports_rules_test.go
}
