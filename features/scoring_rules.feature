# Scoring a completed task: what is scored, what is only recorded, and how the idle gap is derived.
#
# Derived from the documentation pack (warehouse-docs, labor-performance):
#   - ubiquitous-language.md "Unclassified": a completion whose task type is
#     absent or not one of the three is "Recorded, never scored"
#   - business-context.md: an unmeasurable completion (ActualSeconds <= 0)
#     "yield[s] a clean null, a real business fact, not a fabricated number"
#   - use-cases.md RecordTaskPerformance step 5: "claimedAt = CompletedAt -
#     ActualSeconds. The gap runs from the previous completion to claimedAt,
#     capped at IDLE_GAP_CAP_SECONDS (default 3600) ... A gap <= 0 (out-of-order
#     delivery) is logged and skipped, never an error"
#   - aggregate-design-canvas.md IdlePeriod: "Gap is strictly positive"
#   - domain-events.md TaskPerformanceRecorded: the fields carried on the fact
#
# The raw task_type goes through shared.ParseTaskTypeLenient, the function the
# Kafka consumer uses; the consumer's own wire decoding has its own suite.

Feature: Scoring completions and deriving idle gaps
  As a labor planner
  I want every completion recorded honestly and only scored when it can be
  So that no number is ever invented and idle time is measured from facts

  Background:
    Given the Labor Performance service is running

  @bdd
  Scenario Outline: A completion with no recognised task type is recorded but never scored
    Given a standard of 60 expected seconds is already defined for task type "PICK"
    And a standard of 30 expected seconds is already defined for task type "PACK"
    And a standard of 20 expected seconds is already defined for task type "SLAM"
    And associate "A-1" completed a task "T-1" of unrecognised type "<rawType>" in 60 seconds at "2026-10-01T09:00:00Z"
    When the scorecard for associate "A-1" is requested
    Then the scorecard reports 1 task and no mean efficiency
    And the "TaskPerformanceRecorded" event for task "T-1" reports no efficiency
    And the "TaskPerformanceRecorded" event for task "T-1" carries associate "A-1", task type "", 60 actual seconds and completed at "2026-10-01T09:00:00Z"
    When the fleet-wide performance for task type "PICK" is requested
    Then the task type performance response reports task type "PICK" and task count 0

    Examples: REBIN is a real fourth task type this context does not model; matching is case-sensitive
      | rawType |
      | REBIN   |
      | pick    |
      |         |

  @bdd
  Scenario Outline: A completion without a positive duration is recorded, never scored and never measured
    Given a standard of 60 expected seconds is already defined for task type "PICK"
    And associate "A-1" completed a PICK task "T-1" with a duration of <seconds> seconds at "2026-10-01T09:00:00Z"
    When the scorecard for associate "A-1" is requested
    Then the scorecard reports 1 task and no mean efficiency
    When the fleet-wide performance for task type "PICK" is requested
    Then the task type performance response reports task type "PICK" and task count 1
    And the task type performance response reports no mean efficiency
    And the task type performance response reports no mean actual duration

    Examples:
      | seconds |
      | 0       |
      | -1      |
      | -60     |

  @bdd
  Scenario: One second is the shortest measurable duration
    Given a standard of 60 expected seconds is already defined for task type "PICK"
    And associate "A-1" completed a PICK task "T-1" with a duration of 1 seconds at "2026-10-01T09:00:00Z"
    Then the efficiency recorded for task "T-1" is "6000"
    When the fleet-wide performance for task type "PICK" is requested
    Then the task type performance response reports a mean actual duration of 1 seconds

  @bdd
  Scenario: The recorded fact carries what was measured
    Given a standard of 20 expected seconds is already defined for task type "SLAM"
    And associate "A-7" completed a SLAM task "T-7" in 40 seconds at "2026-10-01T09:15:30Z"
    Then the "TaskPerformanceRecorded" event for task "T-7" carries associate "A-7", task type "SLAM", 40 actual seconds and completed at "2026-10-01T09:15:30Z"
    And the efficiency recorded for task "T-7" is "50"

  @bdd
  Scenario: A case-level completion still counts in its task type's fleet-wide numbers
    Given a standard of 45 expected seconds is already defined for task type "PICK"
    And a standard of 30 expected seconds is already defined for task type "PICK" and handling unit "CASE"
    And associate "A-1" completed a PICK task "T-1" for handling unit "CASE" in 60 seconds
    And associate "A-2" completed a PICK task "T-2" in 60 seconds
    When the fleet-wide performance for task type "PICK" is requested
    Then the task type performance response reports task type "PICK" and task count 2
    And the task type performance response reports a mean efficiency of 62.5 percent
    And the task type performance response reports a mean actual duration of 60 seconds

  @bdd
  Scenario Outline: The idle gap is the wait up to the claim instant, clipped at the one-hour cap
    Given the clock is at "2026-10-01T12:00:00Z"
    And associate "A-1" completed a PICK task in 60 seconds
    And associate "A-1" completed a PICK task in 60 seconds after being idle for <idle> seconds
    Then the latest "TaskPerformanceRecorded" event carries <reported> idle seconds before

    Examples: one second, just under the cap, at the cap, just over it, a whole day
      | idle  | reported |
      | 1     | 1        |
      | 3599  | 3599     |
      | 3600  | 3600     |
      | 3601  | 3600     |
      | 86400 | 3600     |

  @bdd
  Scenario: A task claimed at the very instant the previous one completed leaves no gap
    Given the clock is at "2026-10-01T08:02:00Z"
    And associate "A-1" completed a PICK task in 60 seconds
    And associate "A-1" completed a PICK task in 60 seconds after being idle for 0 seconds
    Then the latest "TaskPerformanceRecorded" event carries no idle seconds before
    When the utilization for associate "A-1" is requested
    Then the utilization response reports 120 task seconds, 0 idle seconds and 0 open gap seconds
    And the utilization response reports a utilization percent of 100

  @bdd
  Scenario: The previous completion is the latest by completion time, whatever order they arrived in
    Given associate "A-1" completed a PICK task "T-NEW" in 60 seconds at "2026-10-01T08:10:00Z"
    And associate "A-1" completed a PICK task "T-OLD" in 60 seconds at "2026-10-01T08:05:00Z"
    And associate "A-1" completed a PICK task "T-3" in 60 seconds at "2026-10-01T08:12:00Z"
    Then the "TaskPerformanceRecorded" event for task "T-OLD" carries no idle seconds before
    And the "TaskPerformanceRecorded" event for task "T-3" carries 60 idle seconds before
