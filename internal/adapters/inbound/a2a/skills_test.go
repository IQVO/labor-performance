package a2a

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2aclient"

	"github.com/claudioed/labor-performance/internal/adapters/outbound/events"
	"github.com/claudioed/labor-performance/internal/adapters/outbound/memory"
	"github.com/claudioed/labor-performance/internal/application/usecases"
	"github.com/claudioed/labor-performance/internal/domain/shared"
)

// base is the deterministic clock every test runs against.
var base = time.Date(2026, 9, 6, 9, 0, 0, 0, time.UTC)

// harness wires the read use cases over in-memory repos, seeding data through
// the REAL write use cases (DefineStandard, RecordTaskPerformance) so the read
// models under test are built from genuinely-recorded aggregates. The write
// use cases exist only in the test: the A2A Deps carry reads alone.
type harness struct {
	t      *testing.T
	define *usecases.DefineStandard
	record *usecases.RecordTaskPerformance
	deps   Deps
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	standards := memory.NewStandardRepo()
	performances := memory.NewPerformanceRepo()
	processed := memory.NewProcessedEventRepo()
	idlePeriods := memory.NewIdlePeriodRepo()
	publisher := events.NewLogPublisher(nil)
	clock := memory.FixedClock{At: base}
	return &harness{
		t:      t,
		define: &usecases.DefineStandard{Standards: standards, Events: publisher, Clock: clock},
		record: &usecases.RecordTaskPerformance{Performances: performances, Standards: standards, Processed: processed, Events: publisher, Clock: clock, IdlePeriods: idlePeriods},
		deps: Deps{
			GetAssociateScorecard:  &usecases.GetAssociateScorecard{Performances: performances},
			GetTaskTypePerformance: &usecases.GetTaskTypePerformance{Performances: performances},
			GetStandard:            &usecases.GetStandard{Standards: standards},
			GetUtilization:         &usecases.GetUtilization{Performances: performances, IdlePeriods: idlePeriods, Clock: clock},
		},
	}
}

func (h *harness) standard(taskType shared.TaskType, expectedSeconds int64) {
	h.t.Helper()
	if _, err := h.define.Execute(context.Background(), taskType, expectedSeconds, nil); err != nil {
		h.t.Fatalf("seeding standard %s: %v", taskType, err)
	}
}

func (h *harness) performance(eventID, taskID string, associate shared.AssociateId, taskType shared.TaskType, actualSeconds int64, completedAt time.Time) {
	h.t.Helper()
	_, err := h.record.Execute(context.Background(), usecases.RecordTaskPerformanceRequest{
		KafkaEventId: eventID, TaskId: taskID, AssociateId: associate, TaskType: taskType,
		ActualSeconds: actualSeconds, CompletedAt: completedAt,
	})
	if err != nil {
		h.t.Fatalf("seeding task performance %s: %v", taskID, err)
	}
}

// seededHarness: PICK has a 60s standard and two 30s rows by assoc-1 with a
// 70s idle gap between them; PACK has one 45s row by assoc-2 but NO standard;
// SLAM was never observed.
func seededHarness(t *testing.T) *harness {
	t.Helper()
	h := newHarness(t)
	h.standard(shared.Pick, 60)
	h.performance("evt-1", "task-1", "assoc-1", shared.Pick, 30, base.Add(30*time.Second))
	h.performance("evt-2", "task-2", "assoc-1", shared.Pick, 30, base.Add(130*time.Second))
	h.performance("evt-3", "task-3", "assoc-2", shared.Pack, 45, base.Add(time.Minute))
	return h
}

// ---- the wire: JSON-RPC over HTTP, driven by the SDK's own client ----------

// wireEnvelope is a TEST-LOCAL copy of the answer contract. If the adapter's
// envelope drifts, this suite fails instead of silently following it.
type wireEnvelope struct {
	Skill string `json:"skill"`
	Data  struct {
		AssociateScorecard *struct {
			AssociateId       string   `json:"associateId"`
			TaskCount         int      `json:"taskCount"`
			MeanEfficiencyPct *float64 `json:"meanEfficiencyPct"`
			ByTaskType        map[string]struct {
				TaskCount         int      `json:"taskCount"`
				MeanEfficiencyPct *float64 `json:"meanEfficiencyPct"`
			} `json:"byTaskType"`
			Trend        string `json:"trend"`
			CoachingFlag bool   `json:"coachingFlag"`
		} `json:"associateScorecard"`
		TaskTypeStatus *struct {
			TaskType    string `json:"taskType"`
			Performance struct {
				TaskCount         int      `json:"taskCount"`
				MeanEfficiencyPct *float64 `json:"meanEfficiencyPct"`
				MeanActualSeconds *float64 `json:"meanActualSeconds"`
			} `json:"performance"`
			Standard *struct {
				ExpectedSeconds int64   `json:"expectedSeconds"`
				EffectiveFrom   string  `json:"effectiveFrom"`
				EffectiveTo     *string `json:"effectiveTo"`
			} `json:"standard"`
			Utilization struct {
				Associates     int      `json:"associates"`
				WindowSeconds  int64    `json:"windowSeconds"`
				TaskSeconds    int64    `json:"taskSeconds"`
				IdleSeconds    int64    `json:"idleSeconds"`
				OpenGapSeconds int64    `json:"openGapSeconds"`
				UtilizationPct *float64 `json:"utilizationPct"`
			} `json:"utilization"`
		} `json:"taskTypeStatus"`
	} `json:"data"`
	Problem *struct {
		Type   string `json:"type"`
		Title  string `json:"title"`
		Detail string `json:"detail"`
		Status int    `json:"status"`
	} `json:"problem"`
}

type wire struct{ client *a2aclient.Client }

func newWire(t *testing.T, deps Deps) *wire {
	t.Helper()
	srv := httptest.NewServer(Handler(NewExecutor(deps)))
	t.Cleanup(srv.Close)
	card := Card(Config{AgentURL: srv.URL + "/", Version: "test"})
	client, err := a2aclient.NewFromCard(context.Background(), card)
	if err != nil {
		t.Fatalf("NewFromCard: %v", err)
	}
	return &wire{client: client}
}

// call sends one message part and returns the decoded envelope, the one-line
// text summary and the raw data part JSON of the agent's reply.
func (w *wire) call(t *testing.T, part *a2a.Part) (wireEnvelope, string, string) {
	t.Helper()
	resp, err := w.client.SendMessage(context.Background(), &a2a.SendMessageRequest{Message: a2a.NewMessage(a2a.MessageRoleUser, part)})
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	msg, ok := resp.(*a2a.Message)
	if !ok || len(msg.Parts) < 2 {
		t.Fatalf("reply = %T, want a message with a data and a text part", resp)
	}
	data, ok := msg.Parts[0].Content.(a2a.Data)
	if !ok {
		t.Fatalf("first reply part is %T, want data", msg.Parts[0].Content)
	}
	raw, _ := json.Marshal(data.Value)
	var env wireEnvelope
	if err := json.Unmarshal(raw, &env); err != nil {
		t.Fatalf("reply is not the skill envelope: %v (%s)", err, raw)
	}
	text, _ := msg.Parts[1].Content.(a2a.Text)
	return env, string(text), string(raw)
}

func (w *wire) skill(t *testing.T, skill string, args map[string]any) (wireEnvelope, string, string) {
	t.Helper()
	return w.call(t, a2a.NewDataPart(map[string]any{"skill": skill, "arguments": args}))
}

func wantProblem(t *testing.T, env wireEnvelope, status int, slug string) {
	t.Helper()
	if env.Problem == nil {
		t.Fatalf("expected a %d problem, got data: %+v", status, env.Data)
	}
	if env.Problem.Status != status || env.Problem.Type != problemTypeBase+"/"+slug {
		t.Fatalf("problem = %+v, want %d %s", env.Problem, status, slug)
	}
}

func TestWireAssociateScorecard(t *testing.T) {
	w := newWire(t, seededHarness(t).deps)

	env, text, _ := w.skill(t, skillAssociateScorecard, map[string]any{"associateId": "assoc-1"})
	sc := env.Data.AssociateScorecard
	if env.Skill != skillAssociateScorecard || sc == nil || env.Problem != nil {
		t.Fatalf("envelope = %+v, want an associate scorecard", env)
	}
	if sc.AssociateId != "assoc-1" || sc.TaskCount != 2 {
		t.Fatalf("scorecard = %+v", sc)
	}
	// 60s standard, 30s actual: a real, non-nil efficiency.
	if sc.MeanEfficiencyPct == nil || sc.ByTaskType["PICK"].TaskCount != 2 {
		t.Fatalf("scorecard breakdown = %+v", sc)
	}
	if sc.Trend == "" {
		t.Error("trend is always present")
	}
	if !strings.Contains(text, "assoc-1") || !strings.Contains(text, "2 tasks") {
		t.Errorf("text summary = %q", text)
	}
}

func TestWireAssociateScorecardWithoutAStandardKeepsEfficiencyNull(t *testing.T) {
	w := newWire(t, seededHarness(t).deps)

	// assoc-2 only did a PACK, and PACK has no engineered standard: nothing is
	// scorable, so the efficiency must be null on the wire, never 0.
	env, text, raw := w.skill(t, skillAssociateScorecard, map[string]any{"associateId": "assoc-2"})
	sc := env.Data.AssociateScorecard
	if sc == nil || sc.TaskCount != 1 {
		t.Fatalf("envelope = %+v", env)
	}
	if sc.MeanEfficiencyPct != nil || !strings.Contains(raw, `"meanEfficiencyPct":null`) {
		t.Errorf("meanEfficiencyPct must be null when nothing was scorable: %s", raw)
	}
	if !strings.Contains(text, "n/a") || strings.Contains(text, "0.0%") {
		t.Errorf("summary must say n/a, never 0%%: %q", text)
	}
}

func TestWireTaskTypeStatusComposesPerformanceStandardAndUtilization(t *testing.T) {
	w := newWire(t, seededHarness(t).deps)

	env, text, _ := w.skill(t, skillTaskTypeStatus, map[string]any{"taskType": "PICK", "windowSeconds": 7200})
	st := env.Data.TaskTypeStatus
	if env.Skill != skillTaskTypeStatus || st == nil {
		t.Fatalf("envelope = %+v, want a task type status", env)
	}
	if st.TaskType != "PICK" || st.Performance.TaskCount != 2 {
		t.Fatalf("performance = %+v", st)
	}
	if st.Performance.MeanActualSeconds == nil || *st.Performance.MeanActualSeconds != 30 {
		t.Fatalf("meanActualSeconds = %v, want 30", st.Performance.MeanActualSeconds)
	}
	if st.Standard == nil || st.Standard.ExpectedSeconds != 60 || st.Standard.EffectiveTo != nil {
		t.Fatalf("standard = %+v, want the active 60s standard", st.Standard)
	}
	assertPickUtilization(t, st.Utilization.WindowSeconds, st.Utilization.TaskSeconds, st.Utilization.IdleSeconds, st.Utilization.OpenGapSeconds, st.Utilization.UtilizationPct)
	if !strings.Contains(text, "PICK") || !strings.Contains(text, "standard 60s") {
		t.Errorf("text summary = %q", text)
	}
}

// assertPickUtilization pins the seeded PICK utilization: two 30s tasks with a
// 70s recorded idle gap between them over a 2h window, and no open gap (a
// task type never reports one).
func assertPickUtilization(t *testing.T, window, task, idle, openGap int64, pct *float64) {
	t.Helper()
	if window != 7200 || task != 60 || idle != 70 || openGap != 0 || pct == nil {
		t.Fatalf("utilization window=%d task=%d idle=%d openGap=%d pct=%v", window, task, idle, openGap, pct)
	}
}

func TestWireTaskTypeStatusDefaultsTheWindow(t *testing.T) {
	w := newWire(t, seededHarness(t).deps)
	env, _, _ := w.skill(t, skillTaskTypeStatus, map[string]any{"taskType": "PICK"})
	if st := env.Data.TaskTypeStatus; st == nil || st.Utilization.WindowSeconds != 3600 {
		t.Fatalf("an omitted windowSeconds must apply the 1h default, got %+v", st)
	}
}

// A task type with rows but no active standard is a valid answer: standard is
// null (efficiency cannot be scored), not a 404 for the whole read.
func TestWireTaskTypeStatusWithoutAnActiveStandardAnswersNullStandard(t *testing.T) {
	w := newWire(t, seededHarness(t).deps)

	env, text, raw := w.skill(t, skillTaskTypeStatus, map[string]any{"taskType": "PACK"})
	st := env.Data.TaskTypeStatus
	if st == nil || env.Problem != nil {
		t.Fatalf("envelope = %+v, want a status (not a problem)", env)
	}
	if st.Standard != nil || !strings.Contains(raw, `"standard":null`) {
		t.Errorf("standard must be an explicit null: %s", raw)
	}
	if st.Performance.MeanEfficiencyPct != nil || !strings.Contains(raw, `"meanEfficiencyPct":null`) {
		t.Errorf("efficiency must be null without a standard: %s", raw)
	}
	if st.Performance.MeanActualSeconds == nil || *st.Performance.MeanActualSeconds != 45 {
		t.Errorf("the real measured duration is independent of the standard: %v", st.Performance.MeanActualSeconds)
	}
	if !strings.Contains(text, "no active standard") {
		t.Errorf("summary = %q", text)
	}
}

func TestWireTaskTypeStatusNeverObservedKeepsEverythingNull(t *testing.T) {
	w := newWire(t, seededHarness(t).deps)
	env, _, raw := w.skill(t, skillTaskTypeStatus, map[string]any{"taskType": "SLAM"})
	st := env.Data.TaskTypeStatus
	if st == nil || st.Performance.TaskCount != 0 || st.Utilization.UtilizationPct != nil {
		t.Fatalf("envelope = %+v", env)
	}
	for _, want := range []string{`"utilizationPct":null`, `"meanActualSeconds":null`, `"meanEfficiencyPct":null`} {
		if !strings.Contains(raw, want) {
			t.Errorf("never-observed task type must carry %s: %s", want, raw)
		}
	}
}

// In-band problems keep the REST adapter's status classification.
func TestWireDomainRejectionsAreAnsweredInBand(t *testing.T) {
	w := newWire(t, seededHarness(t).deps)

	env, text, _ := w.skill(t, skillAssociateScorecard, map[string]any{"associateId": "nobody"})
	wantProblem(t, env, 404, "associate-not-found")
	if !strings.Contains(text, "(404)") {
		t.Errorf("problem summary = %q", text)
	}

	env, _, _ = w.skill(t, skillTaskTypeStatus, map[string]any{"taskType": "JUGGLE"})
	wantProblem(t, env, 400, "unknown-task-type")
}

func TestWireArgumentValidation(t *testing.T) {
	w := newWire(t, seededHarness(t).deps)
	cases := []struct {
		name  string
		skill string
		args  map[string]any
		slug  string
	}{
		{"scorecard: no arguments", skillAssociateScorecard, nil, "invalid-argument"},
		{"scorecard: blank associateId", skillAssociateScorecard, map[string]any{"associateId": "  "}, "invalid-argument"},
		{"scorecard: unknown argument", skillAssociateScorecard, map[string]any{"associateId": "assoc-1", "associateID2": "x"}, "invalid-argument"},
		{"status: no arguments", skillTaskTypeStatus, nil, "unknown-task-type"},
		{"status: lower-case task type", skillTaskTypeStatus, map[string]any{"taskType": "pick"}, "unknown-task-type"},
		{"status: misspelt window", skillTaskTypeStatus, map[string]any{"taskType": "PICK", "windowSecond": 60}, "invalid-argument"},
		{"status: negative window", skillTaskTypeStatus, map[string]any{"taskType": "PICK", "windowSeconds": -1}, "invalid-argument"},
		{"status: fractional window", skillTaskTypeStatus, map[string]any{"taskType": "PICK", "windowSeconds": 1.5}, "invalid-argument"},
		{"status: string window", skillTaskTypeStatus, map[string]any{"taskType": "PICK", "windowSeconds": "60"}, "invalid-argument"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			env, _, _ := w.skill(t, tc.skill, tc.args)
			wantProblem(t, env, 400, tc.slug)
		})
	}
}

func TestWireRequestShapeProblems(t *testing.T) {
	w := newWire(t, seededHarness(t).deps)
	cases := []struct {
		name string
		part *a2a.Part
		slug string
	}{
		{"unknown skill", a2a.NewDataPart(map[string]any{"skill": "launch_rocket", "arguments": map[string]any{}}), "unknown-skill"},
		{"no skill field", a2a.NewDataPart(map[string]any{"arguments": map[string]any{}}), "malformed-skill-request"},
		{"unknown envelope field", a2a.NewDataPart(map[string]any{"skill": skillTaskTypeStatus, "extra": 1}), "malformed-skill-request"},
		{"text that is not JSON", a2a.NewTextPart("how is assoc-1 doing?"), "malformed-skill-request"},
		{"text with trailing data", a2a.NewTextPart(`{"skill":"task_type_status","arguments":{"taskType":"PICK"}} {"x":1}`), "malformed-skill-request"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			env, _, _ := w.call(t, tc.part)
			wantProblem(t, env, 400, tc.slug)
		})
	}
}

// The same JSON as a text part is accepted (delegating LLM agents often send
// text), with the same strictness.
func TestWireTextPartCarryingTheSkillJSON(t *testing.T) {
	w := newWire(t, seededHarness(t).deps)
	env, _, _ := w.call(t, a2a.NewTextPart(`{"skill":"associate_scorecard","arguments":{"associateId":"assoc-1"}}`))
	if env.Data.AssociateScorecard == nil {
		t.Fatalf("envelope = %+v", env)
	}
}

// Unrecognised failures are answered as 500 problems that do not leak the
// underlying error text.
func TestProblemFromUseCaseUnknownErrorIs500AndOpaque(t *testing.T) {
	p := problemFromUseCase(errTest("pq: connection refused to 10.0.0.7"))
	if p.Status != 500 || strings.Contains(p.Detail, "10.0.0.7") {
		t.Fatalf("problem = %+v, want an opaque 500", p)
	}
}

type errTest string

func (e errTest) Error() string { return string(e) }

// The agent is read-only by construction: Deps carries no write use case.
func TestDepsCarryReadUseCasesOnly(t *testing.T) {
	// A reflection guard so adding a write use case to Deps fails loudly here
	// and forces a conscious ADR + MCP-charter decision first.
	if n := depsFieldCount(); n != 4 {
		t.Fatalf("Deps has %d fields, want exactly the 4 read use cases; a new field needs an ADR and the MCP write gate", n)
	}
}

func depsFieldCount() int { return reflect.TypeOf(Deps{}).NumField() }
