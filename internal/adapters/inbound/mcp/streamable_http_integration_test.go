//go:build integration

// Integration tests for the MCP inbound adapter over the REAL Streamable
// HTTP transport: inboundmcp.Handler(server) mounted on an httptest.Server,
// driven by the SDK's own client (mcp.NewClient + StreamableClientTransport),
// with the REAL Postgres-backed read use cases behind it — exactly the
// deployment shape cmd/mcp serves (ADR-0009). labor-performance deliberately
// exposes NO write use case over MCP (see Deps' doc comment in tools.go), so
// every registered tool is a read; each one is driven here against rows
// seeded through the real write use cases (DefineStandard,
// RecordTaskPerformance) over the real Postgres repos, UnitOfWork included.
// Domain rejections (unknown associate, task type with no active standard)
// and invalid input (unknown/empty task type, missing associate id) must come
// back as TOOL errors (res.IsError), never transport errors.
//
// Postgres comes from testcontainers: one container for the whole package
// (TestMain below — this package's first integration suite), migrated once
// into a template database, one private database per test (CREATE DATABASE
// ... WITH TEMPLATE, a file-level copy: milliseconds). Never an external
// DATABASE_URL, never t.Skip.
package mcp_test

import (
	"context"
	"fmt"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	sdkmcp "github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	inboundmcp "github.com/claudioed/labor-performance/internal/adapters/inbound/mcp"
	"github.com/claudioed/labor-performance/internal/adapters/outbound/memory"
	"github.com/claudioed/labor-performance/internal/adapters/outbound/postgres"
	"github.com/claudioed/labor-performance/internal/application/usecases"
	"github.com/claudioed/labor-performance/internal/domain/shared"
)

// One Postgres container serves the whole package (containers are slow to
// boot). TestMain starts it, applies every migration ONCE into a template
// database, and each test then gets its own database cloned from that
// template (CREATE DATABASE ... TEMPLATE, a file-level copy: milliseconds) —
// the same harness wes-work-planning's MCP suite pioneered. Isolation is
// total: no TRUNCATE bookkeeping, no dependence on test order.
//
// Never an external DATABASE_URL, never t.Skip.
const templateDB = "mcp_migrated_template"

var (
	sharedBaseURL string // connection URL of the container's default database
	dbSeq         atomic.Uint64
)

// TestMain owns the package-wide container lifecycle.
func TestMain(m *testing.M) {
	os.Exit(runTests(m))
}

func runTests(m *testing.M) int {
	ctx := context.Background()
	container, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("labor_mcp_it"),
		tcpostgres.WithUsername("labor"),
		tcpostgres.WithPassword("labor"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(90*time.Second),
		),
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "start postgres container: %v\n", err)
		return 1
	}
	defer func() {
		if err := testcontainers.TerminateContainer(container); err != nil {
			fmt.Fprintf(os.Stderr, "terminate postgres container: %v\n", err)
		}
	}()

	var err2 error
	sharedBaseURL, err2 = container.ConnectionString(ctx, "sslmode=disable")
	if err2 != nil {
		fmt.Fprintf(os.Stderr, "postgres connection string: %v\n", err2)
		return 1
	}

	// Migrate a template database once; every test clones it.
	if err := createDatabase(ctx, templateDB); err != nil {
		fmt.Fprintf(os.Stderr, "create template database: %v\n", err)
		return 1
	}
	if err := postgres.RunMigrations(withDB(sharedBaseURL, templateDB), migrationsDir()); err != nil {
		fmt.Fprintf(os.Stderr, "migrate template: %v\n", err)
		return 1
	}
	return m.Run()
}

// migrationsDir resolves the repo's migrations directory relative to this
// test file, so the suite does not depend on the working directory go test
// was invoked from.
func migrationsDir() string {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		panic("unable to resolve test file path")
	}
	return filepath.Join(filepath.Dir(thisFile), "..", "..", "..", "..", "migrations")
}

// withDB rewrites the path of a connection URL to the named database.
func withDB(baseURL, name string) string {
	u, err := url.Parse(baseURL)
	if err != nil {
		panic(err)
	}
	u.Path = "/" + name
	return u.String()
}

// createDatabase creates an empty database inside the shared container.
func createDatabase(ctx context.Context, name string) error {
	conn, err := pgx.Connect(ctx, sharedBaseURL)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer conn.Close(ctx)
	if _, err := conn.Exec(ctx, fmt.Sprintf("CREATE DATABASE %q", name)); err != nil {
		return fmt.Errorf("create database %s: %w", name, err)
	}
	return nil
}

// migratedDB hands the test a connection URL to its own private database
// cloned from the migrated template. Cloning is a file-level copy, so it
// costs milliseconds and the test's writes never leak into another test.
func migratedDB(t *testing.T) string {
	t.Helper()
	name := fmt.Sprintf("mcp_it_%d", dbSeq.Add(1))
	conn, err := pgx.Connect(context.Background(), sharedBaseURL)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close(context.Background())
	if _, err := conn.Exec(context.Background(), fmt.Sprintf(
		"CREATE DATABASE %q WITH TEMPLATE %q", name, templateDB)); err != nil {
		t.Fatalf("clone database: %v", err)
	}
	return withDB(sharedBaseURL, name)
}

// mcpEpoch is the fixed instant the read-side clock freezes at: the seeded
// standard predates it by 2h, the three seeded completions by 26-30 minutes.
var mcpEpoch = time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)

// seedAssociate is the associate whose completions the harness seeds.
const seedAssociate = "assoc-itcov"

// bufferingPublisher accumulates published domain events for assertions,
// standing in for the composition root's publisher on the seed path.
type bufferingPublisher struct {
	mu     sync.Mutex
	events []shared.DomainEvent
}

// Publish records every event, in order.
func (p *bufferingPublisher) Publish(_ context.Context, evts ...shared.DomainEvent) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.events = append(p.events, evts...)
	return nil
}

// Published returns a copy of everything published so far, in order.
func (p *bufferingPublisher) Published() []shared.DomainEvent {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]shared.DomainEvent(nil), p.events...)
}

// countOf reports how many published events carry the given name.
func (p *bufferingPublisher) countOf(name string) int {
	n := 0
	for _, e := range p.Published() {
		if e.EventName() == name {
			n++
		}
	}
	return n
}

// mcpHarness is the real production stack served over Streamable HTTP: the
// Postgres repos + UnitOfWork, the four read use cases cmd/mcp wires,
// inboundmcp.NewServer + Handler, and a connected SDK client session the
// tests drive exactly like a model host would.
type mcpHarness struct {
	session   *sdkmcp.ClientSession
	publisher *bufferingPublisher
}

// newMCPHarness seeds one associate's three PICK completions on a fresh
// private database — each 60s actual against a 60s standard (100%
// efficiency), two minutes apart, so every claim after the first is
// preceded by a measured 60s idle gap — then serves the real MCP stack over
// Streamable HTTP. The seed runs through the REAL write use cases over the
// real repos, exactly the paths cmd/labor drives (operator-defined standard,
// Kafka-consumer-recorded completions).
func newMCPHarness(t *testing.T) *mcpHarness {
	t.Helper()
	ctx := context.Background()
	pool, err := postgres.NewPool(ctx, migratedDB(t))
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	t.Cleanup(pool.Close)

	standards := postgres.NewStandardRepo(pool)
	performances := postgres.NewPerformanceRepo(pool)
	processed := postgres.NewProcessedEventRepo(pool)
	idlePeriods := postgres.NewIdlePeriodRepo(pool)
	uow := postgres.NewUnitOfWork(pool)

	publisher := &bufferingPublisher{}
	defineClock := memory.FixedClock{At: mcpEpoch.Add(-2 * time.Hour)}
	nowClock := memory.FixedClock{At: mcpEpoch}

	define := &usecases.DefineStandard{
		Standards: standards, Events: publisher,
		Clock: defineClock, UnitOfWork: uow,
	}
	record := &usecases.RecordTaskPerformance{
		Performances: performances, Standards: standards, Processed: processed,
		Events: publisher, Clock: nowClock, UnitOfWork: uow, IdlePeriods: idlePeriods,
	}

	if _, err := define.Execute(ctx, shared.Pick, 60, nil); err != nil {
		t.Fatalf("seed define standard: %v", err)
	}
	for i := 0; i < 3; i++ {
		completedAt := mcpEpoch.Add(-30*time.Minute + time.Duration(i)*2*time.Minute)
		if _, err := record.Execute(ctx, usecases.RecordTaskPerformanceRequest{
			KafkaEventId:  fmt.Sprintf("itcov-mcp-evt-%d", i+1),
			TaskId:        fmt.Sprintf("ITCOV-MCP-%d", i+1),
			AssociateId:   seedAssociate,
			TaskType:      shared.Pick,
			ActualSeconds: 60,
			CompletedAt:   completedAt,
		}); err != nil {
			t.Fatalf("seed record %d: %v", i+1, err)
		}
	}

	server := inboundmcp.NewServer(inboundmcp.Deps{
		GetAssociateScorecard:  &usecases.GetAssociateScorecard{Performances: performances},
		GetTaskTypePerformance: &usecases.GetTaskTypePerformance{Performances: performances},
		GetStandard:            &usecases.GetStandard{Standards: standards},
		GetUtilization: &usecases.GetUtilization{
			Performances: performances, IdlePeriods: idlePeriods, Clock: nowClock,
		},
	})

	hs := httptest.NewServer(inboundmcp.Handler(server))
	t.Cleanup(hs.Close)

	client := sdkmcp.NewClient(&sdkmcp.Implementation{Name: "itcov-test-host", Version: "1.0.0"}, nil)
	session, err := client.Connect(ctx, &sdkmcp.StreamableClientTransport{
		Endpoint: hs.URL, DisableStandaloneSSE: true,
	}, nil)
	if err != nil {
		t.Fatalf("mcp connect: %v", err)
	}
	t.Cleanup(func() { _ = session.Close() })

	return &mcpHarness{session: session, publisher: publisher}
}

// mustCallTool drives tools/call and fails the test on a transport-level
// error, so a test only ever asserts on res.IsError (tool errors) — the
// distinction the wire contract is about.
func (h *mcpHarness) mustCallTool(t *testing.T, name string, args map[string]any) *sdkmcp.CallToolResult {
	t.Helper()
	res, err := h.session.CallTool(context.Background(), &sdkmcp.CallToolParams{
		Name: name, Arguments: args,
	})
	if err != nil {
		t.Fatalf("tools/call %s: transport error (must be a tool error instead): %v", name, err)
	}
	return res
}

// structured returns a successful call's structured content as a map.
func (h *mcpHarness) structured(t *testing.T, res *sdkmcp.CallToolResult) map[string]any {
	t.Helper()
	m, ok := res.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("no structured content: %+v", res.StructuredContent)
	}
	return m
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

// TestMCPIntegration_ListToolsExposesTheContract proves the wire contract:
// initialize + tools/list expose every registered tool, and — the inversion
// of the fleet's write-tool assertion that fits this read-only context —
// EVERY tool must be annotated read-only, because labor-performance exposes
// no write use case over MCP at all.
func TestMCPIntegration_ListToolsExposesTheContract(t *testing.T) {
	h := newMCPHarness(t)

	list, err := h.session.ListTools(context.Background(), nil)
	if err != nil {
		t.Fatalf("tools/list: %v", err)
	}
	if len(list.Tools) == 0 {
		t.Fatal("tools/list advertised no tools")
	}
	names := map[string]bool{}
	for _, tool := range list.Tools {
		if tool.Annotations == nil || !tool.Annotations.ReadOnlyHint {
			t.Fatalf("tool %q must carry ReadOnlyHint=true — labor-performance exposes no write use case over MCP", tool.Name)
		}
		names[tool.Name] = true
	}
	for _, want := range []string{
		"get_associate_scorecard", "get_task_type_performance",
		"get_labor_standard", "get_task_type_utilization",
	} {
		if !names[want] {
			t.Fatalf("tools/list must expose %q, got %v", want, names)
		}
	}
}

// TestMCPIntegration_ReadToolsRoundTripThroughPostgres drives EVERY
// registered tool over the real Postgres-backed stack and asserts the seeded
// rows come back through the DTO mapping: the scorecard (3 tasks, 100% mean,
// STABLE trend, no coaching flag), the fleet-wide task-type read model, the
// active standard, and the windowed utilization whose inputs (180s task
// time, two 60s idle gaps) were derived and persisted by the real
// RecordTaskPerformance on those same rows.
func TestMCPIntegration_ReadToolsRoundTripThroughPostgres(t *testing.T) {
	h := newMCPHarness(t)

	sc := h.structured(t, h.mustCallTool(t, "get_associate_scorecard",
		map[string]any{"associateId": seedAssociate}))
	if sc["associateId"] != seedAssociate || num(t, sc, "taskCount") != 3 {
		t.Fatalf("scorecard = %v, want 3 tasks for %s", sc, seedAssociate)
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
	pick, ok := byTaskType["PICK"].(map[string]any)
	if !ok || num(t, pick, "taskCount") != 3 {
		t.Fatalf("byTaskType[PICK] = %v, want 3 tasks", byTaskType["PICK"])
	}

	ttp := h.structured(t, h.mustCallTool(t, "get_task_type_performance",
		map[string]any{"taskType": "PICK"}))
	if ttp["taskType"] != "PICK" || num(t, ttp, "taskCount") != 3 {
		t.Fatalf("task-type performance = %v, want 3 PICK tasks", ttp)
	}
	if num(t, ttp, "meanEfficiencyPct") != 100 || num(t, ttp, "meanActualSeconds") != 60 {
		t.Fatalf("task-type performance means = %v/%v, want 100%%/60s",
			ttp["meanEfficiencyPct"], ttp["meanActualSeconds"])
	}

	std := h.structured(t, h.mustCallTool(t, "get_labor_standard",
		map[string]any{"taskType": "PICK"}))
	if num(t, std, "expectedSeconds") != 60 {
		t.Fatalf("expectedSeconds = %v, want 60", std["expectedSeconds"])
	}
	if std["effectiveFrom"] != "2026-10-09T10:00:00Z" {
		t.Fatalf("effectiveFrom = %v, want the define clock's instant", std["effectiveFrom"])
	}
	if _, exists := std["effectiveTo"]; exists {
		t.Fatalf("the active standard must leave effectiveTo omitted, got %v", std["effectiveTo"])
	}

	util := h.structured(t, h.mustCallTool(t, "get_task_type_utilization",
		map[string]any{"taskType": "PICK"}))
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

	// The seed really went through the real write path on the real
	// database: the buffering publisher saw the standard definition and
	// all three recordings.
	if got := h.publisher.countOf("LaborStandardDefined"); got != 1 {
		t.Fatalf("expected 1 LaborStandardDefined, got %d", got)
	}
	if got := h.publisher.countOf("TaskPerformanceRecorded"); got != 3 {
		t.Fatalf("expected 3 TaskPerformanceRecorded, got %d", got)
	}
}

// TestMCPIntegration_DomainRejectionsAreToolErrors proves the not-found
// domain rejections cross the wire as TOOL errors the model can read, never
// transport-level failures: an associate this service has never recorded,
// and a task type with no active standard.
func TestMCPIntegration_DomainRejectionsAreToolErrors(t *testing.T) {
	h := newMCPHarness(t)

	ghost := h.mustCallTool(t, "get_associate_scorecard",
		map[string]any{"associateId": "assoc-never-seen"})
	if !ghost.IsError {
		t.Fatal("unknown associate must surface a tool error, not success")
	}

	noStandard := h.mustCallTool(t, "get_labor_standard",
		map[string]any{"taskType": "PACK"})
	if !noStandard.IsError {
		t.Fatal("task type with no active standard must surface a tool error, not success")
	}
}

// TestMCPIntegration_InvalidInputAreToolErrors proves schema-level input
// rejections surface as tool errors too: an unknown task type (this context
// models only PICK, PACK, SLAM), an empty task type, and a missing
// associate id.
func TestMCPIntegration_InvalidInputAreToolErrors(t *testing.T) {
	h := newMCPHarness(t)

	for _, call := range []struct {
		tool string
		args map[string]any
	}{
		{"get_task_type_performance", map[string]any{"taskType": "REBIN"}},
		{"get_task_type_performance", map[string]any{"taskType": ""}},
		{"get_labor_standard", map[string]any{"taskType": "rebin"}},
		{"get_task_type_utilization", map[string]any{"taskType": "REBIN"}},
		{"get_associate_scorecard", map[string]any{}},
	} {
		if res := h.mustCallTool(t, call.tool, call.args); !res.IsError {
			t.Fatalf("%s with args %v must surface a tool error", call.tool, call.args)
		}
	}
}
