# MCP behavioral evals for the labor-performance tool surface.
#
# Each scenario drives tools/call over the REAL Streamable HTTP handler
# with a connected SDK client — exactly the call a model host makes — and
# pins the structured result. Arguments are deliberately model-realistic:
# extra keys, wrong types, unknown associates and task types.
#
# Derived from the tool contracts documented in:
#   - internal/adapters/inbound/mcp/tools.go (tool descriptions and
#     semantics: nullable metrics are never fabricated numbers; trend
#     INSUFFICIENT_DATA is a real value; the coaching flag is a visibility
#     signal only)
#   - docs/docs/mcp/governance-charter.md (§2 tool curation: intent-level
#     read-only tools; this context exposes no write tool)
#   - apis/openapi.yaml GET /performance/associates/{id}/scorecard and
#     GET /performance/task-types/{taskType} — the same read models the
#     tools serve.
#
# Canonical eval state (clock fixed at 2026-09-06T09:00:00Z):
#   PICK standard 60s from evalBase; assoc-1 PICK task-1 60s completed
#   +60s (efficiency 100%) and task-2 30s completed +120s (efficiency
#   200%, preceded by a 30s idle gap); assoc-2 PACK task-3 50s completed
#   -7100s (out of the default 1h window, unscorable: no PACK standard).

Feature: MCP tool behavioral evals
  The labor-performance MCP tools expose this bounded context to AI
  agents: per-associate scorecards, fleet-wide per-task-type performance,
  the active engineered labor standard, and trailing-window
  idleness/utilization. An agent relying on them must get the same
  semantics the REST API guarantees — nullable metrics stay null, never
  zero — through the schema-decoded argument path a model host actually
  uses.

  Background:
    Given the MCP server is running with the canonical eval state (PICK standard 60s; assoc-1 scored 100% then 200% with a 30s idle gap; assoc-2 one out-of-window unscorable PACK task)

  Scenario: An associate's scorecard pins counts, means, and breakdown
    Two scored tasks are too few for a trend (ClassifyTrend needs 3), so
    trend is the real INSUFFICIENT_DATA value — and the coaching flag
    needs 3 consecutive sub-floor tasks, so it is false.
    When I call the tool "get_associate_scorecard" with argument "associateId" = "assoc-1"
    Then the tool call succeeds
    And the structured result field "associateId" is "assoc-1"
    And the structured result field "taskCount" is 2
    And the structured result field "meanEfficiencyPct" is 150
    And the structured result field "byTaskType.PICK.taskCount" is 2
    And the structured result field "byTaskType.PICK.meanEfficiencyPct" is 150
    And the structured result field "trend" is "INSUFFICIENT_DATA"
    And the structured result field "coachingFlag" is false

  Scenario: An unknown associate is a clean tool error
    When I call the tool "get_associate_scorecard" with argument "associateId" = "ghost"
    Then the tool call reports a problem mentioning "no task performance recorded"

  Scenario: An empty associateId is a clean tool error
    When I call the tool "get_associate_scorecard" with argument "associateId" = ""
    Then the tool call reports a problem mentioning "associateId"

  Scenario: Fleet-wide task-type performance pins the measured mean
    When I call the tool "get_task_type_performance" with argument "taskType" = "PICK"
    Then the tool call succeeds
    And the structured result field "taskType" is "PICK"
    And the structured result field "taskCount" is 2
    And the structured result field "meanEfficiencyPct" is 150
    And the structured result field "meanActualSeconds" is 45

  Scenario: Unscorable work reports null efficiency, never zero
    assoc-2's PACK task was completed while no PACK standard existed, so
    nothing was scorable: meanEfficiencyPct is null (the "never fabricate
    a number" invariant) while the measured meanActualSeconds is real.
    When I call the tool "get_task_type_performance" with argument "taskType" = "PACK"
    Then the tool call succeeds
    And the structured result field "taskType" is "PACK"
    And the structured result field "taskCount" is 1
    And the structured result field "meanEfficiencyPct" is null
    And the structured result field "meanActualSeconds" is 50

  Scenario: A never-seen task type reports zeros and nulls, not an error
    When I call the tool "get_task_type_performance" with argument "taskType" = "SLAM"
    Then the tool call succeeds
    And the structured result field "taskType" is "SLAM"
    And the structured result field "taskCount" is 0
    And the structured result field "meanEfficiencyPct" is null

  Scenario: An invalid task type is a clean tool error
    When I call the tool "get_task_type_performance" with argument "taskType" = "NOPE"
    Then the tool call reports a problem mentioning "unknown task type"

  Scenario: The active labor standard pins its target and effective-from
    When I call the tool "get_labor_standard" with argument "taskType" = "PICK"
    Then the tool call succeeds
    And the structured result field "taskType" is "PICK"
    And the structured result field "expectedSeconds" is 60
    And the structured result field "effectiveFrom" is "2026-09-06T09:00:00Z"

  Scenario: A task type with no active standard is a clean tool error
    When I call the tool "get_labor_standard" with argument "taskType" = "SLAM"
    Then the tool call reports a problem mentioning "no active labor standard"

  Scenario: Utilization over the default window pins the exact split
    With no windowSeconds the tool applies the 1h default: 90 measured
    task-seconds vs the one derived 30s idle gap = 75% utilization, and
    the associates count comes from the idle-period read (assoc-1 only).
    When I call the tool "get_task_type_utilization" with argument "taskType" = "PICK"
    Then the tool call succeeds
    And the structured result field "taskType" is "PICK"
    And the structured result field "windowSeconds" is 3600
    And the structured result field "associates" is 1
    And the structured result field "taskSeconds" is 90
    And the structured result field "idleSeconds" is 30
    And the structured result field "utilizationPct" is 75

  Scenario: A wider window admits the out-of-window PACK task
    windowSeconds 7200 moves the window start past assoc-2's task, so
    its 50 task-seconds count — all task time, no idle gaps, 100%
    utilization. associates stays 0: the count derives from recorded
    idle periods, and a lone task records none.
    When I call the tool "get_task_type_utilization" with arguments
      | taskType      | PACK |
      | windowSeconds | 7200 |
    Then the tool call succeeds
    And the structured result field "taskSeconds" is 50
    And the structured result field "idleSeconds" is 0
    And the structured result field "associates" is 0
    And the structured result field "utilizationPct" is 100

  Scenario: Nothing observed in the window yields null utilization, never zero
    Over the default 1h window PACK has no rows at all (its one task is
    ~2h old): utilizationPct is null — "no data", not "0% busy".
    When I call the tool "get_task_type_utilization" with argument "taskType" = "PACK"
    Then the tool call succeeds
    And the structured result field "taskSeconds" is 0
    And the structured result field "utilizationPct" is null

  Scenario: An invalid task type is rejected by the utilization tool too
    When I call the tool "get_task_type_utilization" with argument "taskType" = "NOPE"
    Then the tool call reports a problem mentioning "unknown task type"

  Scenario: Model chatter in the arguments is rejected, not ignored
    The typed tool schemas are strict (additionalProperties: false, the
    SDK default): a host forwarding stray model-generated keys gets a
    clean schema validation error rather than a silent ignore.
    When I call the tool "get_associate_scorecard" with arguments
      | associateId   | assoc-1                          |
      | model_chatter | maybe they are struggling?       |
      | step          | 2                                |
    Then the tool call reports a problem mentioning "model_chatter"

  Scenario: A wrong-typed argument is rejected without coercion
    When I call the tool "get_associate_scorecard" with argument "associateId" = 42
    Then the tool call does not succeed silently
