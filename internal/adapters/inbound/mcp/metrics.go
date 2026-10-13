package mcp

import (
	"context"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
)

// The MCP Governance Charter (§9, docs/docs/mcp/governance-charter.md)
// requires "invocation and denial counters" next to the per-call span.
// These are those two instruments (ADR 0033). Both are pushed over OTLP like
// every other instrument of this service; the Collector exposes them as
// labor_performance_mcp_tool_invocations_total and
// labor_performance_mcp_tool_denials_total.
const (
	// invocationsCounterName counts every tools/call request the server
	// receives, by tool and outcome (ok, error, denied).
	invocationsCounterName = "labor_performance.mcp.tool.invocations"
	// denialsCounterName counts the subset of calls the server refused
	// before the tool handler ran, by tool and reason.
	denialsCounterName = "labor_performance.mcp.tool.denials"
)

// Attribute keys. mcp.tool.name is the same key the per-call span carries,
// outcome follows the fleet metrics convention (one counter with an outcome
// attribute, ADR 0008).
const (
	toolNameKey = attribute.Key("mcp.tool.name")
	outcomeKey  = attribute.Key("outcome")
	reasonKey   = attribute.Key("reason")
)

// Outcome values.
const (
	// outcomeOK: the handler ran and returned a result.
	outcomeOK = "ok"
	// outcomeError: the handler ran and returned an error (a domain
	// rejection such as "no active labor standard", or a handler-level input
	// check such as an empty associateId or an unknown task type).
	outcomeError = "error"
	// outcomeDenied: the server refused the call before the handler ran.
	outcomeDenied = "denied"
)

// Denial reasons.
const (
	// reasonUnknownTool: the requested tool is not registered (JSON-RPC
	// invalid params from the SDK).
	reasonUnknownTool = "unknown_tool"
	// reasonInvalidArguments: the arguments failed the tool's input schema
	// (wrong type, unknown extra argument, missing required field), so the
	// SDK answered with a tool error without calling the handler.
	reasonInvalidArguments = "invalid_arguments"
)

// unknownToolLabel replaces a tool name the server does not register, so a
// caller cannot create unbounded metric series by inventing names.
const unknownToolLabel = "unknown"

// toolMetrics records the charter's invocation and denial counters from a
// receiving middleware on tools/call, the only place that sees every call:
// the SDK rejects unknown tools and schema-invalid arguments before the tool
// handler (and its span) ever runs.
type toolMetrics struct {
	invocations metric.Int64Counter
	denials     metric.Int64Counter
	known       map[string]bool
}

// newToolMetrics builds both counters from mp (nil means the global
// MeterProvider that telemetry.Setup installs). An instrument that cannot be
// created falls back to a no-op one: MCP calls must never fail because
// metrics are unavailable.
func newToolMetrics(mp metric.MeterProvider, toolNames []string) *toolMetrics {
	if mp == nil {
		mp = otel.GetMeterProvider()
	}
	meter := mp.Meter(tracerName)
	fallback := noop.NewMeterProvider().Meter(tracerName)

	invocations, err := meter.Int64Counter(invocationsCounterName,
		metric.WithDescription("MCP tools/call requests received, by tool (mcp.tool.name) and outcome (ok, error, denied)."),
		metric.WithUnit("{call}"))
	if err != nil {
		invocations, _ = fallback.Int64Counter(invocationsCounterName)
	}
	denials, err := meter.Int64Counter(denialsCounterName,
		metric.WithDescription("MCP tools/call requests refused before the tool handler ran, by tool (mcp.tool.name) and reason (unknown_tool, invalid_arguments)."),
		metric.WithUnit("{call}"))
	if err != nil {
		denials, _ = fallback.Int64Counter(denialsCounterName)
	}

	known := make(map[string]bool, len(toolNames))
	for _, n := range toolNames {
		known[n] = true
	}
	return &toolMetrics{invocations: invocations, denials: denials, known: known}
}

// middleware counts every tools/call and passes every other method through
// untouched.
func (m *toolMetrics) middleware(next mcp.MethodHandler) mcp.MethodHandler {
	return func(ctx context.Context, method string, req mcp.Request) (mcp.Result, error) {
		if method != "tools/call" {
			return next(ctx, method, req)
		}
		probe := &handlerProbe{}
		res, err := next(context.WithValue(ctx, handlerProbeKey{}, probe), method, req)
		m.record(ctx, m.toolLabel(req), probe.ran, res, err)
		return res, err
	}
}

// record adds one invocation and, for a refused call, one denial.
func (m *toolMetrics) record(ctx context.Context, tool string, handlerRan bool, res mcp.Result, err error) {
	outcome, reason := classify(tool, handlerRan, res, err)
	m.invocations.Add(ctx, 1, metric.WithAttributes(toolNameKey.String(tool), outcomeKey.String(outcome)))
	if outcome == outcomeDenied {
		m.denials.Add(ctx, 1, metric.WithAttributes(toolNameKey.String(tool), reasonKey.String(reason)))
	}
}

// classify maps one finished tools/call onto an outcome and, for a denial,
// its reason. Whether the handler ran is what separates a denial (refused by
// the SDK's guardrails) from an error (the handler's own answer).
func classify(tool string, handlerRan bool, res mcp.Result, err error) (outcome, reason string) {
	if handlerRan {
		if err != nil || isToolError(res) {
			return outcomeError, ""
		}
		return outcomeOK, ""
	}
	if tool == unknownToolLabel {
		return outcomeDenied, reasonUnknownTool
	}
	return outcomeDenied, reasonInvalidArguments
}

func isToolError(res mcp.Result) bool {
	r, ok := res.(*mcp.CallToolResult)
	return ok && r != nil && r.IsError
}

// toolLabel is the requested tool name when the server registers it, and
// unknownToolLabel otherwise.
func (m *toolMetrics) toolLabel(req mcp.Request) string {
	call, ok := req.(*mcp.CallToolRequest)
	if !ok || call.Params == nil || !m.known[call.Params.Name] {
		return unknownToolLabel
	}
	return call.Params.Name
}

// handlerProbe is set by the tool wrapper (addTool) when the handler runs,
// so the middleware can tell a refused call from a handled one.
type handlerProbe struct{ ran bool }

type handlerProbeKey struct{}

// markHandlerRan records, for the enclosing tools/call, that the SDK's
// argument validation passed and the tool handler was invoked.
func markHandlerRan(ctx context.Context) {
	if p, ok := ctx.Value(handlerProbeKey{}).(*handlerProbe); ok {
		p.ran = true
	}
}
