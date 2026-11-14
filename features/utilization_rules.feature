# Utilization read model at its window boundaries.
#
# Derived from the documentation pack (warehouse-docs, labor-performance):
#   - use-cases.md GetUtilization: "ForTaskType: sums ActualSeconds and recorded
#     (closed) idle gaps for the task type since now - window ... ForAssociate:
#     ... plus the open gap: the time from the associate's last activity ... to
#     now, clamped to the window. It is computed at read time and never
#     persisted"; "utilizationPct = 100 x task / (task + idle + openGap)"
#   - aggregate-design-canvas.md IdlePeriod: "taskType is the type of the task
#     that ended the gap"
#
# Window and clock are exact, so each row sits ON a boundary.

Feature: Utilization windows and the open gap at their boundaries
  As a floor supervisor
  I want work counted from the exact instant the window opens
  So that a utilization figure never silently includes or drops a task

  Background:
    Given the Labor Performance service is running

  @bdd
  Scenario Outline: Work is inside the window from the instant the window opens
    Given the clock is at "2026-10-01T10:00:00Z"
    And associate "A-1" completed a PICK task "T-1" in 60 seconds at "<completedAt>"
    When the utilization for associate "A-1" is requested
    Then the utilization response reports <task> task seconds, 0 idle seconds and <open> open gap seconds

    Examples: the window is [09:00:00, 10:00:00]
      | completedAt          | task | open |
      | 2026-10-01T08:59:59Z | 0    | 3600 |
      | 2026-10-01T09:00:00Z | 60   | 3600 |
      | 2026-10-01T09:00:01Z | 60   | 3599 |

  @bdd
  Scenario Outline: The open gap starts the moment the last task completes and is never negative
    Given the clock is at "2026-10-01T10:00:00Z"
    And associate "A-1" completed a PICK task "T-1" in 60 seconds at "<completedAt>"
    When the utilization for associate "A-1" is requested
    Then the utilization response reports 60 task seconds, 0 idle seconds and <open> open gap seconds
    And the utilization response reports a utilization percent of <pct>

    Examples: completed right now, one second ago, and (clock skew) half a minute from now
      | completedAt          | open | pct   |
      | 2026-10-01T10:00:00Z | 0    | 100   |
      | 2026-10-01T09:59:59Z | 1    | 98.36 |
      | 2026-10-01T10:00:30Z | 0    | 100   |

  @bdd
  Scenario Outline: A recorded idle gap counts in the window from the instant it ended
    Given the clock is at "2026-10-01T10:00:00Z"
    And associate "A-1" completed a PICK task "T-1" in 60 seconds at "2026-10-01T08:00:00Z"
    And associate "A-1" completed a PICK task "T-2" in 60 seconds at "<completedAt>"
    When the fleet-wide utilization for task type "PICK" is requested
    Then the utilization response reports 60 task seconds, <idle> idle seconds and 0 open gap seconds

    Examples: T-2 claims the next task at completedAt minus 60 seconds; the window opens at 09:00:00
      | completedAt          | idle |
      | 2026-10-01T09:00:59Z | 0    |
      | 2026-10-01T09:01:00Z | 3600 |
