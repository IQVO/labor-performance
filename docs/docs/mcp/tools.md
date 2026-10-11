---
id: tools
title: MCP tools
sidebar_label: MCP tools
description: Every tool, resource template and prompt the labor-performance MCP server (cmd/mcp) exposes, with input fields, output shape and read-only annotation.
---

# MCP tools

`cmd/mcp` serves the Model Context Protocol over **Streamable HTTP**
only, at `/` on `MCP_ADDR` (default `:8090`), plus `GET /healthz`. It is
unauthenticated ([ADR 0012](../adr/0012-remove-rest-auth-layer.md)).
Server info is `labor-performance-mcp` `1.0.0`
([ADR 0009](../adr/0009-mcp-inbound-adapter.md)). The rules it follows
are in the [MCP Governance Charter](./governance-charter.md).

**Every tool is read-only.** No write use case is wired:
`DefineStandard` stays on REST (an operator decision) and
`RecordTaskPerformance` stays on Kafka. The tools call the same use
cases as the REST adapter (`internal/adapters/inbound/mcp/tools.go`),
over `cmd/mcp`'s own OLTP pool.

The schemas below come from a live `tools/list` against `cmd/mcp` built
from this branch. They match `testdata/tool_registry.golden`.

## Tools

| Tool | Input fields | Annotations | Use case | Output |
| --- | --- | --- | --- | --- |
| `get_associate_scorecard` | `associateId` (string, **required**): the associate whose scorecard to return. An empty string returns the tool error `associateId is required`. | `readOnlyHint: true`, `idempotentHint: false` | `GetAssociateScorecard` | `{associateId, taskCount, meanEfficiencyPct, byTaskType: {<TYPE>: {taskCount, meanEfficiencyPct}}, trend, coachingFlag}` |
| `get_task_type_performance` | `taskType` (string, **required**): `PICK`, `PACK` or `SLAM` | `readOnlyHint: true`, `idempotentHint: false` | `GetTaskTypePerformance` | `{taskType, taskCount, meanEfficiencyPct, meanActualSeconds}` |
| `get_labor_standard` | `taskType` (string, **required**): `PICK`, `PACK` or `SLAM` | `readOnlyHint: true`, `idempotentHint: false` | `GetStandard` | `{taskType, expectedSeconds, travelComponentSeconds?, effectiveFrom, effectiveTo?}` |
| `get_task_type_utilization` | `taskType` (string, **required**); `windowSeconds` (integer, optional; omitted or ≤ 0 means 3600) | `readOnlyHint: true`, `idempotentHint: false` | `GetUtilization.ForTaskType` | `{taskType, associates, windowSeconds, taskSeconds, idleSeconds, openGapSeconds, utilizationPct}` |

Every input schema has `additionalProperties: false`, so unknown
arguments are rejected (eval E2). Each call runs inside an OTel span
`mcp.tool <name>` with `mcp.tool.name` and `mcp.tool.outcome`
(`ok` or `error`).

Interpreting the output:

- `meanEfficiencyPct`, `meanActualSeconds` and `utilizationPct` are
  `null` when nothing was scorable, measured or observed. `null` never
  means 0.
- `trend` is `IMPROVING`, `DECLINING`, `STABLE` or `INSUFFICIENT_DATA`.
  `coachingFlag` is true when the last 3 scored tasks are all below 85%.
  It is a visibility signal for a human, never a trigger for action.
- `openGapSeconds` is always 0 on `get_task_type_utilization`. The
  still-running idle gap is computed only per associate, by the REST
  endpoint `GET /associates/{associateId}/utilization`.

### Errors

Domain errors come back as tool results with `isError: true` and the
error text. They are not JSON-RPC errors. For example
`get_labor_standard {"taskType":"PICK"}` with no standard defined
returns:

```json
{"content":[{"type":"text","text":"no active labor standard for this task type"}],"isError":true}
```

An unknown task type returns `unknown task type: must be PICK, PACK, or SLAM`.
An associate the service has never seen returns
`no task performance recorded for this associate`.

## Resource template

| URI template | Name | MIME type | Backed by |
| --- | --- | --- | --- |
| `scorecard://labor/{associateId}` | `associate scorecard` | `application/json` | `GetAssociateScorecard`. The body has the same shape as the `get_associate_scorecard` output. |

## Prompt

| Name | Arguments | Content |
| --- | --- | --- |
| `review_associate_performance` | none | A standard operating procedure: call `get_associate_scorecard`, then `get_labor_standard` and `get_task_type_performance` for context. Treat `null` as "no data". Frame a `coachingFlag` as a signal for human follow-up. |

## Consumers

`warehouse-ops-agent` calls these tools through its `mcpclient` adapter
(`LABOR_PERFORMANCE_MCP_ENDPOINT` on its side). Its flow-balance advisory
uses `get_task_type_utilization`. See [Integration](../ecosystem/integration.md).

## Calling it by hand

```bash
curl -si localhost:8090/ \
  -H 'Content-Type: application/json' -H 'Accept: application/json, text/event-stream' \
  -d '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"curl","version":"0"}}}'
# note the Mcp-Session-Id response header, then:
curl -s localhost:8090/ -H "Mcp-Session-Id: $SID" -H 'Mcp-Protocol-Version: 2025-06-18' \
  -H 'Content-Type: application/json' -H 'Accept: application/json, text/event-stream' \
  -d '{"jsonrpc":"2.0","method":"notifications/initialized"}'
curl -s localhost:8090/ -H "Mcp-Session-Id: $SID" -H 'Mcp-Protocol-Version: 2025-06-18' \
  -H 'Content-Type: application/json' -H 'Accept: application/json, text/event-stream' \
  -d '{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"get_task_type_performance","arguments":{"taskType":"PICK"}}}'
# event: message
# data: {"jsonrpc":"2.0","id":2,"result":{"content":[...],"structuredContent":{"meanActualSeconds":null,"meanEfficiencyPct":null,"taskCount":0,"taskType":"PICK"}}}
```

Sessions live in process memory. Run a single replica
([Runbook](../operations/runbook.md#scaling)).
