# Utilization read-model scenarios over RECORDED idle gaps.
#
# Derived from:
#   - docs/docs/adr/0014-labor-utilization-idleness.md: an IDLE GAP is the
#     wait between completing one task and claiming the next (the claim
#     instant is completedAt - duration); it is attributed to the TaskType
#     of the task that ENDED the gap; a gap is capped at 3600s by default
#     (cross-shift gaps); a first observation has no gap; an empty
#     AssociateId (robot station) never records one; Kafka delivery is
#     unordered, so a non-positive gap is skipped, never an error
#   - apis/openapi.yaml, GET /task-types/{taskType}/utilization ("The
#     still-running open gap ... is NOT included at this scope:
#     openGapSeconds is always 0 here") and GET
#     /associates/{associateId}/utilization ("plus the still-running
#     open-gap contribution when that associate is idle right now"),
#     components.parameters.WindowQueryParam, components.schemas.Utilization
#     (utilizationPct = 100 * task / (task + idle + open gap), null when
#     nothing was observed)
#
# Facts are seeded through RecordTaskPerformance; "the clock is at" is the
# instant the read model measures "now" from.

Feature: Measuring utilization from recorded idle gaps
  As a floor supervisor
  I want to see how much of a trailing window an associate or a task type spent working
  So that I can tell a starved station from a busy one

  Background:
    Given the Labor Performance service is running

  @bdd
  Scenario: The wait between two tasks is idle time
    Given the clock is at "2026-10-01T08:04:00Z"
    And associate "A-1" completed a PICK task in 60 seconds
    And associate "A-1" completed a PICK task in 60 seconds after being idle for 120 seconds
    When the utilization for associate "A-1" is requested
    Then the request is accepted with status 200
    And the utilization response reports associate id "A-1"
    And the utilization response reports 1 associates over a 3600 second window
    And the utilization response reports 120 task seconds, 120 idle seconds and 0 open gap seconds
    And the utilization response reports a utilization percent of 50

  @bdd
  Scenario: A first observation has no idle gap to measure
    Given the clock is at "2026-10-01T08:01:00Z"
    And associate "A-1" completed a PICK task in 60 seconds
    When the utilization for associate "A-1" is requested
    Then the utilization response reports 60 task seconds, 0 idle seconds and 0 open gap seconds
    And the utilization response reports a utilization percent of 100

  @bdd
  Scenario: An associate who is idle right now has an open gap that is never persisted
    Given the clock is at "2026-10-01T08:11:00Z"
    And associate "A-1" completed a PICK task in 60 seconds
    When the utilization for associate "A-1" is requested
    Then the utilization response reports 60 task seconds, 0 idle seconds and 600 open gap seconds
    And the utilization response reports a utilization percent of 9.09

  @bdd
  Scenario: The open gap grows with the clock, because it is computed when read
    Given the clock is at "2026-10-01T08:11:00Z"
    And associate "A-1" completed a PICK task in 60 seconds
    When the utilization for associate "A-1" is requested
    Then the utilization response reports 60 task seconds, 0 idle seconds and 600 open gap seconds
    When the clock is at "2026-10-01T08:21:00Z"
    And the utilization for associate "A-1" is requested
    Then the utilization response reports 60 task seconds, 0 idle seconds and 1200 open gap seconds

  @bdd
  Scenario: The open gap is clamped to the requested window
    Given the clock is at "2026-10-01T10:00:00Z"
    And associate "A-1" completed a PICK task in 60 seconds
    When the utilization for associate "A-1" is requested with window "30m"
    Then the utilization response reports a 1800 second window
    And the utilization response reports 0 task seconds, 0 idle seconds and 1800 open gap seconds
    And the utilization response reports a utilization percent of 0

  @bdd
  Scenario: Work that finished before the window is left out of it
    Given the clock is at "2026-10-01T12:00:00Z"
    And associate "A-1" completed a PICK task in 60 seconds
    And associate "A-1" completed a PICK task in 60 seconds after being idle for 120 seconds
    When the utilization for associate "A-1" is requested
    Then the utilization response reports 0 task seconds, 0 idle seconds and 3600 open gap seconds

  @bdd
  Scenario: A gap longer than the cap is recorded at the cap
    Given the clock is at "2026-10-01T10:02:00Z"
    And associate "A-1" completed a PICK task in 60 seconds
    And associate "A-1" completed a PICK task in 60 seconds after being idle for 7200 seconds
    When the utilization for associate "A-1" is requested with window "3h"
    Then the utilization response reports 120 task seconds, 3600 idle seconds and 0 open gap seconds
    And the utilization response reports a utilization percent of 3.23

  @bdd
  Scenario: Completions delivered out of order record no negative gap
    Given the clock is at "2026-10-01T08:10:00Z"
    And associate "A-1" completed a PICK task "T-NEW" in 60 seconds at "2026-10-01T08:10:00Z"
    And associate "A-1" completed a PICK task "T-OLD" in 60 seconds at "2026-10-01T08:05:00Z"
    When the utilization for associate "A-1" is requested
    Then the request is accepted with status 200
    And the utilization response reports 120 task seconds, 0 idle seconds and 0 open gap seconds
    And the "TaskPerformanceRecorded" event for task "T-OLD" carries no idle seconds before

  @bdd
  Scenario: The recorded event carries the idle gap that preceded the task
    Given associate "A-1" completed a PICK task "T-1" in 60 seconds at "2026-10-01T08:01:00Z"
    And associate "A-1" completed a PICK task "T-2" in 60 seconds at "2026-10-01T08:04:00Z"
    Then the "TaskPerformanceRecorded" event for task "T-1" carries no idle seconds before
    And the "TaskPerformanceRecorded" event for task "T-2" carries 120 idle seconds before

  @bdd
  Scenario: The event reports a capped gap at the cap
    Given associate "A-1" completed a PICK task "T-1" in 60 seconds at "2026-10-01T08:01:00Z"
    And associate "A-1" completed a PICK task "T-2" in 60 seconds at "2026-10-01T11:00:00Z"
    Then the "TaskPerformanceRecorded" event for task "T-2" carries 3600 idle seconds before

  @bdd
  Scenario: Fleet-wide utilization sums every associate's work and idle time
    Given the clock is at "2026-10-01T08:30:00Z"
    And associate "A-1" completed a PICK task in 60 seconds
    And associate "A-1" completed a PICK task in 60 seconds after being idle for 120 seconds
    And associate "A-2" completed a PICK task in 60 seconds
    And associate "A-2" completed a PICK task in 60 seconds after being idle for 60 seconds
    When the fleet-wide utilization for task type "PICK" is requested
    Then the request is accepted with status 200
    And the utilization response reports task type "PICK"
    And the utilization response reports 2 associates over a 3600 second window
    And the utilization response reports 240 task seconds, 180 idle seconds and 0 open gap seconds
    And the utilization response reports a utilization percent of 57.14

  @bdd
  Scenario: Idle time is attributed to the task type that ended the wait
    Given the clock is at "2026-10-01T08:10:00Z"
    And associate "A-1" completed a PACK task in 60 seconds
    And associate "A-1" completed a PICK task in 60 seconds after being idle for 120 seconds
    When the fleet-wide utilization for task type "PICK" is requested
    Then the utilization response reports 60 task seconds, 120 idle seconds and 0 open gap seconds
    And the utilization response reports a utilization percent of 33.33
    When the fleet-wide utilization for task type "PACK" is requested
    Then the utilization response reports 60 task seconds, 0 idle seconds and 0 open gap seconds
    And the utilization response reports a utilization percent of 100

  @bdd
  Scenario: The fleet-wide view never includes the open gap of someone idle right now
    Given the clock is at "2026-10-01T09:00:00Z"
    And associate "A-1" completed a PICK task in 60 seconds
    When the fleet-wide utilization for task type "PICK" is requested
    Then the utilization response reports 60 task seconds, 0 idle seconds and 0 open gap seconds
    When the utilization for associate "A-1" is requested
    Then the utilization response reports 60 task seconds, 0 idle seconds and 3540 open gap seconds

  @bdd
  Scenario: A station with no checked-in associate records work but never an idle gap
    Given the clock is at "2026-10-01T08:10:00Z"
    And a station with no checked-in associate completed a PICK task in 60 seconds
    And a station with no checked-in associate completed a PICK task in 60 seconds
    When the fleet-wide utilization for task type "PICK" is requested
    Then the utilization response reports 0 associates over a 3600 second window
    And the utilization response reports 120 task seconds, 0 idle seconds and 0 open gap seconds

  @bdd
  Scenario: One associate's utilization ignores everyone else's
    Given the clock is at "2026-10-01T08:30:00Z"
    And associate "A-1" completed a PICK task in 60 seconds
    And associate "A-1" completed a PICK task in 60 seconds after being idle for 120 seconds
    When the utilization for associate "A-2" is requested
    Then the utilization response reports associate id "A-2"
    And the utilization response reports 0 associates over a 3600 second window
    And the utilization response reports 0 task seconds, 0 idle seconds and 0 open gap seconds
    And the utilization response reports a null utilization percent

  @bdd
  Scenario Outline: The window query parameter accepts any positive Go duration
    When the fleet-wide utilization for task type "PICK" is requested with window "<window>"
    Then the request is accepted with status 200
    And the utilization response reports a <seconds> second window

    Examples:
      | window | seconds |
      | 15m    | 900     |
      | 2h     | 7200    |
      | 90s    | 90      |
      | 1h30m  | 5400    |

  @bdd
  Scenario Outline: A window that is not a positive duration falls back to one hour
    When the fleet-wide utilization for task type "PICK" is requested with window "<window>"
    Then the request is accepted with status 200
    And the utilization response reports a 3600 second window

    Examples:
      | window |
      | 0s     |
      | -5m    |
      | soon   |
