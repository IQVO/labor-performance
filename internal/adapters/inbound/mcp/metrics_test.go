package mcp

import (
	"context"
	"testing"

	sdk "github.com/modelcontextprotocol/go-sdk/mcp"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/claudioed/labor-performance/internal/domain/shared"
)

// metricsSession boots the real server (every tool, the charter counters)
// over an in-process transport, with a manual metric reader standing in for
// the OTLP exporter.
func metricsSession(t *testing.T, seed func(h *harness)) (*sdk.ClientSession, *sdkmetric.ManualReader) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })

	h := newHarness(t)
	if seed != nil {
		seed(h)
	}
	deps := h.deps
	deps.MeterProvider = provider
	server := NewServer(deps)

	ctx := context.Background()
	ct, st := sdk.NewInMemoryTransports()
	ss, err := server.Connect(ctx, st, nil)
	if err != nil {
		t.Fatalf("server connect: %v", err)
	}
	t.Cleanup(func() { _ = ss.Close() })
	cs, err := sdk.NewClient(&sdk.Implementation{Name: "metrics", Version: "0"}, nil).Connect(ctx, ct, nil)
	if err != nil {
		t.Fatalf("client connect: %v", err)
	}
	t.Cleanup(func() { _ = cs.Close() })
	return cs, reader
}

// counterPoints returns the data points of one Int64 sum, keyed by its
// attribute set rendered as "k=v,k=v" (sorted by key).
func counterPoints(t *testing.T, reader *sdkmetric.ManualReader, name string) map[string]int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collect: %v", err)
	}
	out := map[string]int64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("%s is %T, want an Int64 counter", name, m.Data)
			}
			if !sum.IsMonotonic {
				t.Fatalf("%s must be a monotonic counter", name)
			}
			for _, dp := range sum.DataPoints {
				out[attrKey(dp.Attributes)] += dp.Value
			}
		}
	}
	return out
}

func attrKey(set attribute.Set) string {
	key := ""
	for _, kv := range set.ToSlice() {
		if key != "" {
			key += ","
		}
		key += string(kv.Key) + "=" + kv.Value.AsString()
	}
	return key
}

func assertPoints(t *testing.T, name string, got, want map[string]int64) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s: got %v, want %v", name, got, want)
	}
	for k, v := range want {
		if got[k] != v {
			t.Fatalf("%s[%s] = %d, want %d (all points: %v)", name, k, got[k], v, got)
		}
	}
}

// TestToolMetrics_CountInvocationsByOutcomeAndDenialsByReason pins gap fix
// D28 (ADR 0033): the charter's invocation and denial counters. A handled
// call counts as ok or error; a call the SDK refuses before the handler runs
// (wrong-typed or unknown arguments, an unregistered tool) counts as denied
// AND as one denial with its reason. An unregistered tool name is never a
// label value.
func TestToolMetrics_CountInvocationsByOutcomeAndDenialsByReason(t *testing.T) {
	cs, reader := metricsSession(t, func(h *harness) { h.mustDefineStandard(shared.Pick, 45) })
	ctx := context.Background()

	calls := []struct {
		name      string
		args      map[string]any
		wantRPC   bool
		wantError bool
	}{
		{"get_labor_standard", map[string]any{"taskType": "PICK"}, false, false},
		{"get_labor_standard", map[string]any{"taskType": "PACK"}, false, true},                // no active standard: handler error
		{"get_labor_standard", map[string]any{"taskType": "FLY"}, false, true},                 // handler-level validation: error
		{"get_task_type_performance", map[string]any{"taskType": 42}, false, true},             // schema: wrong type
		{"get_task_type_performance", map[string]any{"taskType": "PICK", "x": 1}, false, true}, // schema: extra argument
		{"drop_all_standards", map[string]any{}, true, false},                                  // unregistered tool
	}
	for _, c := range calls {
		res, err := cs.CallTool(ctx, &sdk.CallToolParams{Name: c.name, Arguments: c.args})
		if c.wantRPC {
			if err == nil {
				t.Fatalf("%s: want a JSON-RPC error", c.name)
			}
			continue
		}
		if err != nil {
			t.Fatalf("%s %v: unexpected JSON-RPC error %v", c.name, c.args, err)
		}
		if res.IsError != c.wantError {
			t.Fatalf("%s %v: IsError = %v, want %v", c.name, c.args, res.IsError, c.wantError)
		}
	}

	assertPoints(t, invocationsCounterName, counterPoints(t, reader, invocationsCounterName), map[string]int64{
		"mcp.tool.name=get_labor_standard,outcome=ok":            1,
		"mcp.tool.name=get_labor_standard,outcome=error":         2,
		"mcp.tool.name=get_task_type_performance,outcome=denied": 2,
		"mcp.tool.name=unknown,outcome=denied":                   1,
	})
	assertPoints(t, denialsCounterName, counterPoints(t, reader, denialsCounterName), map[string]int64{
		"mcp.tool.name=get_task_type_performance,reason=invalid_arguments": 2,
		"mcp.tool.name=unknown,reason=unknown_tool":                        1,
	})
}

// TestToolMetrics_OtherMethodsAreNotCounted: listing tools, reading a
// resource or getting a prompt is not a tool invocation.
func TestToolMetrics_OtherMethodsAreNotCounted(t *testing.T) {
	cs, reader := metricsSession(t, nil)
	ctx := context.Background()
	if _, err := cs.ListTools(ctx, nil); err != nil {
		t.Fatalf("list tools: %v", err)
	}
	if _, err := cs.GetPrompt(ctx, &sdk.GetPromptParams{Name: "review_associate_performance"}); err != nil {
		t.Fatalf("get prompt: %v", err)
	}
	if got := counterPoints(t, reader, invocationsCounterName); len(got) != 0 {
		t.Fatalf("invocations recorded for non-tool methods: %v", got)
	}
}

// TestClassify covers the outcome table directly, including a handler that
// answered with a JSON-RPC error (counted as error, never as a denial).
func TestClassify(t *testing.T) {
	errRes := &sdk.CallToolResult{IsError: true}
	okRes := &sdk.CallToolResult{}
	tests := []struct {
		name        string
		tool        string
		ran         bool
		res         sdk.Result
		err         error
		wantOutcome string
		wantReason  string
	}{
		{"handled ok", "get_labor_standard", true, okRes, nil, outcomeOK, ""},
		{"handled tool error", "get_labor_standard", true, errRes, nil, outcomeError, ""},
		{"handled rpc error", "get_labor_standard", true, nil, context.Canceled, outcomeError, ""},
		{"schema refused", "get_labor_standard", false, errRes, nil, outcomeDenied, reasonInvalidArguments},
		{"unknown tool", unknownToolLabel, false, nil, context.Canceled, outcomeDenied, reasonUnknownTool},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			outcome, reason := classify(tt.tool, tt.ran, tt.res, tt.err)
			if outcome != tt.wantOutcome || reason != tt.wantReason {
				t.Fatalf("classify = (%q, %q), want (%q, %q)", outcome, reason, tt.wantOutcome, tt.wantReason)
			}
		})
	}
}
