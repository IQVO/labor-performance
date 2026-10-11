# Scorecard scenarios over RECORDED task performance.
#
# Derived from:
#   - apis/openapi.yaml, GET /associates/{associateId}/scorecard and
#     components.schemas.Scorecard / TaskTypeBreakdown (taskCount,
#     meanEfficiencyPct "nullable", byTaskType, trend, coachingFlag; 404 for
#     an associate this service has never recorded a TaskPerformance for)
#   - .claude/rules/domain-model.md and the TaskPerformance aggregate:
#     EfficiencyPct = 100 * standard / actual, NEVER a division by zero --
#     nil (unscored) when the actual duration is not positive or no standard
#     was active when the task completed; scored against the standard in
#     force AS OF the completion instant, not "active right now"
#   - RecordTaskPerformance: idempotent on the Kafka event id (a
#     redelivery is a no-op, never a double count)
#
# The facts are seeded through the real RecordTaskPerformance use case (what
# the Kafka consumer calls); every assertion goes through the REST API.

Feature: Scoring completed tasks and reading the scorecard
  As the fleet's labor performance service
  I want each completed task scored against the engineered standard in force
  So that a scorecard reports efficiency that is honest about what it could not measure

  Background:
    Given the Labor Performance service is running

  @bdd
  Scenario: A completed task is scored against the active standard
    Given a standard of 60 expected seconds is already defined for task type "PICK"
    And associate "A-1" completed a PICK task in 60 seconds
    When the scorecard for associate "A-1" is requested
    Then the request is accepted with status 200
    And the scorecard reports 1 task and a mean efficiency of 100 percent
    And the scorecard trend is "INSUFFICIENT_DATA"
    And the scorecard coaching flag is false

  @bdd
  Scenario Outline: Efficiency is the standard divided by the actual duration
    Given a standard of 60 expected seconds is already defined for task type "PICK"
    And associate "A-1" completed a PICK task in <actual> seconds
    When the scorecard for associate "A-1" is requested
    Then the scorecard reports 1 task and a mean efficiency of <efficiency> percent

    Examples:
      | actual | efficiency |
      | 30     | 200        |
      | 60     | 100        |
      | 90     | 66.67      |
      | 120    | 50         |
      | 600    | 10         |

  @bdd
  Scenario: The mean efficiency averages every scored task
    Given a standard of 60 expected seconds is already defined for task type "PICK"
    And associate "A-1" completed a PICK task in 60 seconds
    And associate "A-1" completed a PICK task in 120 seconds
    When the scorecard for associate "A-1" is requested
    Then the scorecard reports 2 tasks and a mean efficiency of 75 percent

  @bdd
  Scenario: A task completed with no active standard is counted but left unscored
    Given associate "A-1" completed a PICK task in 60 seconds
    When the scorecard for associate "A-1" is requested
    Then the request is accepted with status 200
    And the scorecard reports 1 task and no mean efficiency
    And the scorecard trend is "INSUFFICIENT_DATA"

  @bdd
  Scenario: A task with a zero duration is counted but never divided by
    Given a standard of 60 expected seconds is already defined for task type "PICK"
    And associate "A-1" completed a PICK task in 0 seconds
    When the scorecard for associate "A-1" is requested
    Then the scorecard reports 1 task and no mean efficiency

  @bdd
  Scenario: Unscored tasks do not drag the mean efficiency down
    Given a standard of 60 expected seconds is already defined for task type "PICK"
    And associate "A-1" completed a PICK task in 60 seconds
    And associate "A-1" completed a PICK task in 0 seconds
    When the scorecard for associate "A-1" is requested
    Then the scorecard reports 2 tasks and a mean efficiency of 100 percent

  @bdd
  Scenario: The scorecard breaks efficiency down per task type
    Given a standard of 60 expected seconds is already defined for task type "PICK"
    And a standard of 30 expected seconds is already defined for task type "PACK"
    And associate "A-1" completed a PICK task in 60 seconds
    And associate "A-1" completed a PACK task in 60 seconds
    When the scorecard for associate "A-1" is requested
    Then the scorecard reports 2 tasks and a mean efficiency of 75 percent
    And the scorecard breaks down "PICK" as 1 task with a mean efficiency of 100 percent
    And the scorecard breaks down "PACK" as 1 task with a mean efficiency of 50 percent

  @bdd
  Scenario: Each associate's scorecard counts only their own tasks
    Given a standard of 60 expected seconds is already defined for task type "PICK"
    And associate "A-1" completed a PICK task in 60 seconds
    And associate "A-2" completed a PICK task in 120 seconds
    And associate "A-2" completed a PICK task in 120 seconds
    When the scorecard for associate "A-1" is requested
    Then the scorecard reports 1 task and a mean efficiency of 100 percent
    When the scorecard for associate "A-2" is requested
    Then the scorecard reports 2 tasks and a mean efficiency of 50 percent

  @bdd
  Scenario: An associate who only appears in someone else's data is still unknown
    Given a standard of 60 expected seconds is already defined for task type "PICK"
    And associate "A-1" completed a PICK task in 60 seconds
    When the scorecard for associate "A-9" is requested
    Then the request is rejected with status 404
    And the response is an RFC 7807 problem

  @bdd
  Scenario: A task completed under a superseded standard keeps the score it earned then
    Given the clock is at "2026-10-01T12:00:00Z"
    And a standard of 60 expected seconds is already defined for task type "PICK"
    And the clock is at "2026-10-01T13:00:00Z"
    And a standard of 30 expected seconds is already defined for task type "PICK"
    And associate "A-1" completed a PICK task "T-1" in 60 seconds at "2026-10-01T12:30:00Z"
    And associate "A-1" completed a PICK task "T-2" in 60 seconds at "2026-10-01T13:30:00Z"
    When the scorecard for associate "A-1" is requested
    Then the scorecard reports 2 tasks and a mean efficiency of 75 percent
    And the "TaskPerformanceRecorded" event for task "T-1" reports an efficiency of 100 percent
    And the "TaskPerformanceRecorded" event for task "T-2" reports an efficiency of 50 percent

  @bdd
  Scenario: A redelivered completion is counted once
    Given a standard of 60 expected seconds is already defined for task type "PICK"
    And associate "A-1" completed a PICK task "T-1" in 60 seconds at "2026-10-01T09:00:00Z"
    When the completion of task "T-1" is delivered again
    And the scorecard for associate "A-1" is requested
    Then the scorecard reports 1 task and a mean efficiency of 100 percent
    And 1 "TaskPerformanceRecorded" event was published

  @bdd
  Scenario: Recording a completion publishes what was measured
    Given a standard of 60 expected seconds is already defined for task type "PICK"
    And associate "A-1" completed a PICK task "T-1" in 120 seconds at "2026-10-01T09:00:00Z"
    Then 1 "TaskPerformanceRecorded" event was published
    And the "TaskPerformanceRecorded" event for task "T-1" reports an efficiency of 50 percent

  @bdd
  Scenario: A completion without a standard publishes an unscored event
    Given associate "A-1" completed a PICK task "T-1" in 120 seconds at "2026-10-01T09:00:00Z"
    Then the "TaskPerformanceRecorded" event for task "T-1" reports no efficiency
