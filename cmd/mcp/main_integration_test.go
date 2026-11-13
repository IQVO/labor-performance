//go:build integration

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	sdk "github.com/modelcontextprotocol/go-sdk/mcp"

	inboundmcp "github.com/claudioed/labor-performance/internal/adapters/inbound/mcp"
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
// in-memory MCP transports on the cmd's real buildAdapters stack, exactly
// what the seeded facts hold — and its reads add no outbox row: every tool
// is read-only by charter, so there is nothing for a relay to publish from
// this binary.
func TestMCPAgainstPostgres_ReadsTheRecordedFactsAndNeverWrites(t *testing.T) {
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

	adapters, closeAdapters, err := buildAdapters(ctx, url, url, migrationsDirForTestMCP(t), quietLoggerMCP())
	if err != nil {
		t.Fatalf("buildAdapters: %v", err)
	}
	t.Cleanup(closeAdapters)

	// The same four read use cases cmd/mcp's run() wires; no write use case
	// exists on this surface (MCP Governance Charter, ADR 0009).
	deps := inboundmcp.Deps{
		GetAssociateScorecard:  &usecases.GetAssociateScorecard{Performances: adapters.performances},
		GetTaskTypePerformance: &usecases.GetTaskTypePerformance{Performances: adapters.performances},
		GetStandard:            &usecases.GetStandard{Standards: adapters.standards},
		GetUtilization:         &usecases.GetUtilization{Performances: adapters.performances, IdlePeriods: adapters.idlePeriods, Clock: system.Clock{}},
	}

	ct, st := sdk.NewInMemoryTransports()
	ss, err := inboundmcp.NewServer(deps).Connect(ctx, st, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ss.Close()
	session, err := sdk.NewClient(&sdk.Implementation{Name: "t", Version: "0"}, nil).Connect(ctx, ct, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()

	assertSeededReads(t, session, defineAt)
	assertDocumentedRejections(t, session)

	if after := countOutbox(); after != before {
		t.Fatalf("outbox rows %d -> %d: the MCP binary wrote an event", before, after)
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
			KafkaEventId:  fmt.Sprintf("itcov-mcp-evt-%d", i+1),
			TaskId:        fmt.Sprintf("ITCOV-MCP-%d", i+1),
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

// num reads one numeric field out of a structured-content map.
func num(t *testing.T, m map[string]any, field string) float64 {
	t.Helper()
	v, ok := m[field].(float64)
	if !ok {
		t.Fatalf("field %q = %v, want a number", field, m[field])
	}
	return v
}

// assertSeededReads reads back, through every documented read tool
// (docs/docs/mcp/tools.md), what seedLaborFacts persisted.
func assertSeededReads(t *testing.T, session *sdk.ClientSession, defineAt time.Time) {
	t.Helper()
	ctx := context.Background()
	call := func(name string, args map[string]any) map[string]any {
		t.Helper()
		res, err := session.CallTool(ctx, &sdk.CallToolParams{Name: name, Arguments: args})
		if err != nil || res.IsError {
			t.Fatalf("%s: err=%v res=%+v", name, err, res)
		}
		return res.StructuredContent.(map[string]any)
	}

	sc := call("get_associate_scorecard", map[string]any{"associateId": seedAssociate})
	if sc["associateId"] != seedAssociate || num(t, sc, "taskCount") != seedTaskCount {
		t.Fatalf("get_associate_scorecard = %v, want %d tasks for %s", sc, seedTaskCount, seedAssociate)
	}
	if got := num(t, sc, "meanEfficiencyPct"); got != 100 {
		t.Fatalf("meanEfficiencyPct = %v, want 100 (three 60s tasks against the 60s standard)", got)
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

	ttp := call("get_task_type_performance", map[string]any{"taskType": "PICK"})
	if ttp["taskType"] != "PICK" || num(t, ttp, "taskCount") != seedTaskCount {
		t.Fatalf("get_task_type_performance = %v, want %d PICK tasks", ttp, seedTaskCount)
	}
	if num(t, ttp, "meanEfficiencyPct") != 100 || num(t, ttp, "meanActualSeconds") != seedStandardSeconds {
		t.Fatalf("get_task_type_performance means = %v/%v, want 100%%/60s",
			ttp["meanEfficiencyPct"], ttp["meanActualSeconds"])
	}

	std := call("get_labor_standard", map[string]any{"taskType": "PICK"})
	if num(t, std, "expectedSeconds") != seedStandardSeconds {
		t.Fatalf("expectedSeconds = %v, want %d", std["expectedSeconds"], seedStandardSeconds)
	}
	if want := defineAt.UTC().Format(time.RFC3339); std["effectiveFrom"] != want {
		t.Fatalf("effectiveFrom = %v, want the define clock's instant %s", std["effectiveFrom"], want)
	}
	if _, exists := std["effectiveTo"]; exists {
		t.Fatalf("the active standard must leave effectiveTo omitted, got %v", std["effectiveTo"])
	}

	util := call("get_task_type_utilization", map[string]any{"taskType": "PICK"})
	if num(t, util, "windowSeconds") != 3600 {
		t.Fatalf("windowSeconds = %v, want the 1h default", util["windowSeconds"])
	}
	if num(t, util, "taskSeconds") != 180 || num(t, util, "idleSeconds") != 120 {
		t.Fatalf("task/idle seconds = %v/%v, want 180/120", util["taskSeconds"], util["idleSeconds"])
	}
	if num(t, util, "associates") != 1 || num(t, util, "openGapSeconds") != 0 {
		t.Fatalf("associates/openGap = %v/%v, want 1/0", util["associates"], util["openGapSeconds"])
	}
	if got := num(t, util, "utilizationPct"); got != 60 {
		t.Fatalf("utilizationPct = %v, want 60 (180 of 180+120)", got)
	}
}

// assertDocumentedRejections proves the documented error semantics
// (docs/docs/mcp/tools.md, "Errors"): domain rejections cross the wire as
// TOOL errors the model can read, never JSON-RPC transport errors.
func assertDocumentedRejections(t *testing.T, session *sdk.ClientSession) {
	t.Helper()
	ctx := context.Background()
	rejected := func(name string, args map[string]any, wantText string) {
		t.Helper()
		res, err := session.CallTool(ctx, &sdk.CallToolParams{Name: name, Arguments: args})
		if err != nil {
			t.Fatalf("%s: transport error (must be a tool error instead): %v", name, err)
		}
		if !res.IsError {
			t.Fatalf("%s with args %v must surface a tool error, got %+v", name, args, res)
		}
		raw, err := json.Marshal(res.Content)
		if err != nil {
			t.Fatalf("%s: marshal error content: %v", name, err)
		}
		if !strings.Contains(string(raw), wantText) {
			t.Fatalf("%s error content = %s, want it to contain %q", name, raw, wantText)
		}
	}
	rejected("get_associate_scorecard", map[string]any{"associateId": "assoc-never-seen"},
		"no task performance recorded for this associate")
	rejected("get_labor_standard", map[string]any{"taskType": "PACK"},
		"no active labor standard for this task type")
}
