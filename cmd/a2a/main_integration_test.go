//go:build integration

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2aclient"

	inbounda2a "github.com/claudioed/labor-performance/internal/adapters/inbound/a2a"
	"github.com/claudioed/labor-performance/internal/adapters/outbound/memory"
	"github.com/claudioed/labor-performance/internal/adapters/outbound/postgres"
	"github.com/claudioed/labor-performance/internal/adapters/outbound/system"
	"github.com/claudioed/labor-performance/internal/application/usecases"
	"github.com/claudioed/labor-performance/internal/domain/shared"
	"github.com/claudioed/labor-performance/internal/testing/pgtest"
)

func TestMain(m *testing.M) {
	code := m.Run()
	pgtest.Shutdown()
	os.Exit(code)
}

// The binary migrates a database itself (idempotently: pgtest's databases
// are already migrated, a second run must be a no-op), then answers, over
// the real JSON-RPC router on the cmd's real buildAdapters stack, exactly
// what the seeded facts hold — and its reads add no outbox row: the agent is
// read-only (ADR 0036), so there is nothing for a relay to publish from
// this binary. The client discovers the agent the way a fleet agent does:
// it fetches the Agent Card from the well-known path and dials the URL the
// card advertises.
func TestA2AAgainstPostgres_ReadsTheRecordedFactsAndNeverWrites(t *testing.T) {
	url := pgtest.URL(t) // a fresh, already migrated database
	ctx := context.Background()

	pool, err := postgres.NewPool(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	countOutbox := func() int {
		t.Helper()
		var n int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM outbox_events`).Scan(&n); err != nil {
			t.Fatalf("count outbox_events: %v", err)
		}
		return n
	}

	defineAt := seedLaborFacts(t, url)
	before := countOutbox()

	adapters, closeAdapters, err := buildAdapters(ctx, url, url, migrationsDirForTest(t), quietLogger())
	if err != nil {
		t.Fatalf("buildAdapters: %v", err)
	}
	t.Cleanup(closeAdapters)

	// The same four read use cases cmd/a2a's run() wires; no write use case
	// exists on this surface (ADR 0036).
	deps := inbounda2a.Deps{
		GetAssociateScorecard:  &usecases.GetAssociateScorecard{Performances: adapters.performances},
		GetTaskTypePerformance: &usecases.GetTaskTypePerformance{Performances: adapters.performances},
		GetStandard:            &usecases.GetStandard{Standards: adapters.standards},
		GetUtilization:         &usecases.GetUtilization{Performances: adapters.performances, IdlePeriods: adapters.idlePeriods, Clock: system.Clock{}},
	}

	// Bind the listener first so the served Agent Card can advertise a URL
	// the client can actually dial — exactly the contract A2A_AGENT_URL
	// carries in a deployment.
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	agentURL := "http://" + listener.Addr().String() + "/"
	handler := newRouter(
		inbounda2a.Handler(inbounda2a.NewExecutor(deps)),
		inbounda2a.Card(inbounda2a.Config{AgentURL: agentURL, Version: "it"}),
		"labor-performance-a2a-it",
	)
	srv := &httptest.Server{Listener: listener, Config: &http.Server{Handler: handler, ReadHeaderTimeout: 5 * time.Second}}
	srv.Start()
	t.Cleanup(srv.Close)

	client := discoverAgent(t, agentURL)

	assertSeededReads(t, client, defineAt)

	if after := countOutbox(); after != before {
		t.Fatalf("outbox rows %d -> %d: the A2A binary wrote an event", before, after)
	}
}

// The seeded facts, the documented way in: a 60s PICK standard defined by an
// operator (DefineStandard, the REST-driven write) and three 60s completions
// recorded from fulfillment-execution's TaskCompleted events
// (RecordTaskPerformance, the Kafka-driven write), two minutes apart over the
// last half hour so the real clock's 1h utilization window sees 180s of task
// time and two measured 60s idle gaps.
const (
	seedAssociate       = "assoc-itcov"
	seedTaskCount       = 3
	seedStandardSeconds = 60
)

// discardPublisher stands in for the composition root's publisher on the
// seed path: the events are the OLTP binary's concern, not this read surface's.
type discardPublisher struct{}

// Publish accepts and drops every domain event.
func (discardPublisher) Publish(context.Context, ...shared.DomainEvent) error { return nil }

// seedLaborFacts writes the standard and the three completions through the
// REAL write use cases over the real Postgres repos (UnitOfWork included) and
// returns the instant the standard was defined at, for effectiveFrom.
func seedLaborFacts(t *testing.T, url string) time.Time {
	t.Helper()
	ctx := context.Background()
	pool, err := postgres.NewPool(ctx, url)
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	defer pool.Close()
	now := time.Now()
	defineAt := now.Add(-2 * time.Hour)

	standards := postgres.NewStandardRepo(pool)
	performances := postgres.NewPerformanceRepo(pool)
	define := &usecases.DefineStandard{
		Standards: standards, Events: discardPublisher{},
		Clock: memory.FixedClock{At: defineAt}, UnitOfWork: postgres.NewUnitOfWork(pool),
	}
	record := &usecases.RecordTaskPerformance{
		Performances: performances, Standards: standards, Processed: postgres.NewProcessedEventRepo(pool),
		Events: discardPublisher{}, Clock: memory.FixedClock{At: now},
		UnitOfWork: postgres.NewUnitOfWork(pool), IdlePeriods: postgres.NewIdlePeriodRepo(pool),
	}

	if _, err := define.Execute(ctx, shared.Pick, seedStandardSeconds, nil); err != nil {
		t.Fatalf("seed define standard: %v", err)
	}
	for i := range seedTaskCount {
		completedAt := now.Add(-30*time.Minute + time.Duration(i)*2*time.Minute)
		if _, err := record.Execute(ctx, usecases.RecordTaskPerformanceRequest{
			KafkaEventId:  fmt.Sprintf("itcov-a2a-evt-%d", i+1),
			TaskId:        fmt.Sprintf("ITCOV-A2A-%d", i+1),
			AssociateId:   seedAssociate,
			TaskType:      shared.Pick,
			ActualSeconds: seedStandardSeconds,
			CompletedAt:   completedAt,
		}); err != nil {
			t.Fatalf("seed record %d: %v", i+1, err)
		}
	}
	return defineAt
}

// discoverAgent fetches the Agent Card from the well-known path and builds
// the SDK client from it, so the test dials exactly what the card advertises
// (the discovery contract of ADR 0036).
func discoverAgent(t *testing.T, agentURL string) *a2aclient.Client {
	t.Helper()
	res, err := http.Get(agentURL + ".well-known/agent-card.json")
	if err != nil {
		t.Fatalf("fetch agent card: %v", err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("agent card status = %d, want 200", res.StatusCode)
	}
	var card a2a.AgentCard
	if err := json.NewDecoder(res.Body).Decode(&card); err != nil {
		t.Fatalf("decode agent card: %v", err)
	}
	if card.Name != "labor-performance" || len(card.Skills) != 2 {
		t.Fatalf("agent card = %s/%d skills, want labor-performance with the two curated skills", card.Name, len(card.Skills))
	}
	client, err := a2aclient.NewFromCard(context.Background(), &card)
	if err != nil {
		t.Fatalf("client from card: %v", err)
	}
	return client
}

// envelopeOf sends one skill call and returns the decoded envelope (a
// test-local shape) and everything the agent put on the wire.
func envelopeOf(t *testing.T, client *a2aclient.Client, skill string, args map[string]any) (map[string]any, string) {
	t.Helper()
	msg := a2a.NewMessage(a2a.MessageRoleUser, a2a.NewDataPart(map[string]any{"skill": skill, "arguments": args}))
	resp, err := client.SendMessage(context.Background(), &a2a.SendMessageRequest{Message: msg})
	if err != nil {
		t.Fatalf("%s: %v", skill, err)
	}
	reply, ok := resp.(*a2a.Message)
	if !ok || len(reply.Parts) < 2 {
		t.Fatalf("%s: reply = %T", skill, resp)
	}
	data, ok := reply.Parts[0].Content.(a2a.Data)
	if !ok {
		t.Fatalf("%s: first part is %T", skill, reply.Parts[0].Content)
	}
	raw, _ := json.Marshal(data.Value)
	var env map[string]any
	if err := json.Unmarshal(raw, &env); err != nil {
		t.Fatal(err)
	}
	return env, string(raw)
}

// num reads one numeric field out of a decoded JSON map.
func num(t *testing.T, m map[string]any, field string) float64 {
	t.Helper()
	v, ok := m[field].(float64)
	if !ok {
		t.Fatalf("field %q = %v, want a number", field, m[field])
	}
	return v
}

// assertSeededReads reads back, through both curated skills (ADR 0036), what
// seedLaborFacts persisted.
func assertSeededReads(t *testing.T, client *a2aclient.Client, defineAt time.Time) {
	t.Helper()

	env, raw := envelopeOf(t, client, "associate_scorecard", map[string]any{"associateId": seedAssociate})
	sc, _ := env["data"].(map[string]any)["associateScorecard"].(map[string]any)
	if env["problem"] != nil || sc["associateId"] != seedAssociate || num(t, sc, "taskCount") != seedTaskCount {
		t.Fatalf("associate_scorecard = %s", raw)
	}
	if num(t, sc, "meanEfficiencyPct") != 100 {
		t.Fatalf("meanEfficiencyPct = %v, want 100 (three 60s tasks against the 60s standard)", sc["meanEfficiencyPct"])
	}
	if sc["trend"] != "STABLE" || sc["coachingFlag"] != false {
		t.Fatalf("trend/coachingFlag = %v/%v, want STABLE/false", sc["trend"], sc["coachingFlag"])
	}
	byTaskType, ok := sc["byTaskType"].(map[string]any)
	if !ok {
		t.Fatalf("byTaskType = %v, want a map", sc["byTaskType"])
	}
	if pick, ok := byTaskType["PICK"].(map[string]any); !ok || num(t, pick, "taskCount") != seedTaskCount {
		t.Fatalf("byTaskType[PICK] = %v, want %d tasks", byTaskType["PICK"], seedTaskCount)
	}

	env, raw = envelopeOf(t, client, "task_type_status", map[string]any{"taskType": "PICK"})
	st, _ := env["data"].(map[string]any)["taskTypeStatus"].(map[string]any)
	if env["problem"] != nil || st["taskType"] != "PICK" {
		t.Fatalf("task_type_status = %s", raw)
	}
	perf, _ := st["performance"].(map[string]any)
	if num(t, perf, "taskCount") != seedTaskCount || num(t, perf, "meanEfficiencyPct") != 100 || num(t, perf, "meanActualSeconds") != seedStandardSeconds {
		t.Fatalf("task_type_status performance = %v, want %d tasks, 100%%, 60s", perf, seedTaskCount)
	}
	std, ok := st["standard"].(map[string]any)
	if !ok || num(t, std, "expectedSeconds") != seedStandardSeconds {
		t.Fatalf("task_type_status standard = %v, want the active 60s standard", st["standard"])
	}
	if want := defineAt.UTC().Format(time.RFC3339); std["effectiveFrom"] != want {
		t.Fatalf("effectiveFrom = %v, want the define clock's instant %s", std["effectiveFrom"], want)
	}
	util, _ := st["utilization"].(map[string]any)
	if num(t, util, "windowSeconds") != 3600 || num(t, util, "taskSeconds") != 180 || num(t, util, "idleSeconds") != 120 {
		t.Fatalf("task_type_status utilization = %v, want 3600s window, 180 task, 120 idle", util)
	}
	if num(t, util, "associates") != 1 || num(t, util, "openGapSeconds") != 0 {
		t.Fatalf("associates/openGap = %v/%v, want 1/0", util["associates"], util["openGapSeconds"])
	}
	if got := num(t, util, "utilizationPct"); got != 60 {
		t.Fatalf("utilizationPct = %v, want 60 (180 of 180+120)", got)
	}

	env, raw = envelopeOf(t, client, "associate_scorecard", map[string]any{"associateId": "assoc-never-seen"})
	problem, _ := env["problem"].(map[string]any)
	if data, _ := env["data"].(map[string]any); len(data) != 0 || problem == nil || num(t, problem, "status") != 404 {
		t.Fatalf("an unknown associate must be a 404 problem answer, got %s", raw)
	}
	if ptype, _ := problem["type"].(string); !strings.HasSuffix(ptype, "/associate-not-found") {
		t.Fatalf("problem type = %v, want the associate-not-found type", problem["type"])
	}
}
