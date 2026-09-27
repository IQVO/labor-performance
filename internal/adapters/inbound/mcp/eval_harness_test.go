// Shared harness for the MCP eval suites (E1-E3): one place that builds a
// real Streamable HTTP server over in-memory adapters and connects a real
// SDK client to it, so schema evals, wire conformance evals, and the
// Gherkin behavioral evals all exercise exactly the surface a model host
// would.
package mcp_test

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	sdk "github.com/modelcontextprotocol/go-sdk/mcp"

	inboundmcp "github.com/claudioed/labor-performance/internal/adapters/inbound/mcp"
	"github.com/claudioed/labor-performance/internal/adapters/outbound/events"
	"github.com/claudioed/labor-performance/internal/adapters/outbound/memory"
	"github.com/claudioed/labor-performance/internal/application/usecases"
	"github.com/claudioed/labor-performance/internal/domain/shared"
)

// evalBase is the deterministic instant every eval runs against — the same
// 2026-09-06 09:00:00 UTC the package's own test harness uses, so eval
// pins and unit-test pins read against the same clock.
var evalBase = time.Date(2026, 9, 6, 9, 0, 0, 0, time.UTC)

// evalHarness is a fully wired MCP server over in-memory repos plus the
// client session talking to it. labor-performance exposes no write tool
// over MCP, so the harness carries no publisher/event knobs — only the
// read-side repos and the fixed clock the time-windowed reads are pinned
// against.
type evalHarness struct {
	session *sdk.ClientSession
	server  *httptest.Server
	clock   memory.FixedClock

	lastCallResult  *sdk.CallToolResult
	lastCallErr     error
	lastCallContent string
}

// newEvalDeps builds the full read-only tool surface (all four use cases —
// registration is unconditional in this context) over empty in-memory
// repos — enough for schema and conformance evals that do not seed state.
func newEvalDeps() inboundmcp.Deps {
	performances := memory.NewPerformanceRepo()
	idlePeriods := memory.NewIdlePeriodRepo()
	clock := memory.FixedClock{At: evalBase}
	return inboundmcp.Deps{
		GetAssociateScorecard:  &usecases.GetAssociateScorecard{Performances: performances},
		GetTaskTypePerformance: &usecases.GetTaskTypePerformance{Performances: performances},
		GetStandard:            &usecases.GetStandard{Standards: memory.NewStandardRepo()},
		GetUtilization:         &usecases.GetUtilization{Performances: performances, IdlePeriods: idlePeriods, Clock: clock},
	}
}

// newEvalHarness seeds the canonical eval state over a real Streamable
// HTTP server and connects a client session to it. The canonical state is
// seeded through the REAL write use cases (DefineStandard,
// RecordTaskPerformance — never hand-built fixtures), with every completed
// instant placed at a fixed offset from evalBase:
//
//   - PICK standard: 60 expected seconds, effective from evalBase.
//   - assoc-1, PICK task-1: 60 actual seconds, completed evalBase+60s
//     (efficiency 100%), and PICK task-2: 30 actual seconds, completed
//     evalBase+120s (efficiency 200%; the 30s between task-1's completion
//     and task-2's claim instant is the one derived idle gap).
//   - assoc-2, PACK task-3: 50 actual seconds, completed
//     evalBase-7100s — outside the default 1h window, inside a 2h window,
//     and never scorable (no PACK standard exists).
//
// From this state every number the tools report is exact: assoc-1's mean
// efficiency is 150%, PICK utilization over the default window is 75%
// (90 task-seconds vs 30 idle-seconds), and PACK over a 2h window is 100%
// task-time with a null-free denominator of one row.
func newEvalHarness(t *testing.T) *evalHarness {
	t.Helper()

	h := &evalHarness{clock: memory.FixedClock{At: evalBase}}

	standards := memory.NewStandardRepo()
	performances := memory.NewPerformanceRepo()
	processed := memory.NewProcessedEventRepo()
	idlePeriods := memory.NewIdlePeriodRepo()
	publisher := events.NewLogPublisher(nil)
	ctx := context.Background()

	defineStandard := &usecases.DefineStandard{Standards: standards, Events: publisher, Clock: h.clock}
	recordTaskPerformance := &usecases.RecordTaskPerformance{
		Performances: performances, Standards: standards, Processed: processed,
		Events: publisher, Clock: h.clock, IdlePeriods: idlePeriods,
	}

	if _, err := defineStandard.Execute(ctx, shared.Pick, 60, nil); err != nil {
		t.Fatalf("seed PICK standard: %v", err)
	}
	seeds := []usecases.RecordTaskPerformanceRequest{
		{KafkaEventId: "eval-evt-1", TaskId: "task-1", AssociateId: "assoc-1", TaskType: shared.Pick, ActualSeconds: 60, CompletedAt: evalBase.Add(60 * time.Second)},
		{KafkaEventId: "eval-evt-2", TaskId: "task-2", AssociateId: "assoc-1", TaskType: shared.Pick, ActualSeconds: 30, CompletedAt: evalBase.Add(120 * time.Second)},
		{KafkaEventId: "eval-evt-3", TaskId: "task-3", AssociateId: "assoc-2", TaskType: shared.Pack, ActualSeconds: 50, CompletedAt: evalBase.Add(-7100 * time.Second)},
	}
	for _, s := range seeds {
		if _, err := recordTaskPerformance.Execute(ctx, s); err != nil {
			t.Fatalf("seed task performance %s: %v", s.TaskId, err)
		}
	}

	server := inboundmcp.NewServer(inboundmcp.Deps{
		GetAssociateScorecard:  &usecases.GetAssociateScorecard{Performances: performances},
		GetTaskTypePerformance: &usecases.GetTaskTypePerformance{Performances: performances},
		GetStandard:            &usecases.GetStandard{Standards: standards},
		GetUtilization:         &usecases.GetUtilization{Performances: performances, IdlePeriods: idlePeriods, Clock: h.clock},
	})
	h.server = httptest.NewServer(inboundmcp.Handler(server))
	t.Cleanup(h.server.Close)

	client := sdk.NewClient(&sdk.Implementation{Name: "eval-client", Version: "0.0.1"}, nil)
	transport := &sdk.StreamableClientTransport{Endpoint: h.server.URL}
	session, err := client.Connect(context.Background(), transport, nil)
	if err != nil {
		t.Fatalf("eval harness connect: %v", err)
	}
	h.session = session
	t.Cleanup(func() { _ = session.Close() })
	return h
}

// wireSession builds a real Streamable HTTP server over the given deps and
// connects a client session to it, for evals that do not need seeded
// state.
func wireSession(t *testing.T, deps inboundmcp.Deps) *sdk.ClientSession {
	t.Helper()
	server := inboundmcp.NewServer(deps)
	httpSrv := httptest.NewServer(inboundmcp.Handler(server))
	t.Cleanup(httpSrv.Close)

	client := sdk.NewClient(&sdk.Implementation{Name: "eval-client", Version: "0.0.1"}, nil)
	transport := &sdk.StreamableClientTransport{Endpoint: httpSrv.URL}
	session, err := client.Connect(context.Background(), transport, nil)
	if err != nil {
		t.Fatalf("wire session connect: %v", err)
	}
	t.Cleanup(func() { _ = session.Close() })
	return session
}

// callTool invokes a tool and records the result for the Then steps.
func (h *evalHarness) callTool(ctx context.Context, name string, args map[string]any) error {
	res, err := h.session.CallTool(ctx, &sdk.CallToolParams{Name: name, Arguments: args})
	h.lastCallResult, h.lastCallErr = res, err
	h.lastCallContent = ""
	if res != nil {
		for _, c := range res.Content {
			if text, ok := c.(*sdk.TextContent); ok {
				h.lastCallContent += text.Text
			}
		}
	}
	return err
}
