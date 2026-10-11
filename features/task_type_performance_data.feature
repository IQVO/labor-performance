# Fleet-wide TaskType performance over RECORDED task performance.
#
# Derived from:
#   - apis/openapi.yaml, GET /task-types/{taskType}/performance and
#     components.schemas.TaskTypePerformance:
#       taskCount; meanEfficiencyPct (nullable: needs an active standard);
#       meanActualSeconds "The real measured mean duration (seconds) for
#       this TaskType, across every recorded row with a positive
#       ActualSeconds -- independent of whether an engineered labor
#       standard exists to compare it against. Null iff no measurable row
#       has ever been recorded for this TaskType."
#   - .claude/rules/domain-model.md: a completion with no checked-in
#     occupant (empty AssociateId) is a real, recorded fact for the
#     fleet-wide TaskType view, though it never appears on a scorecard.

Feature: Reading fleet-wide performance for a task type
  As a labor planner
  I want the fleet-wide numbers per task type to reflect every recorded completion
  So that I can compare real durations with the engineered standard

  Background:
    Given the Labor Performance service is running

  @bdd
  Scenario: Performance spans every associate who completed the task type
    Given a standard of 60 expected seconds is already defined for task type "PICK"
    And associate "A-1" completed a PICK task in 60 seconds
    And associate "A-2" completed a PICK task in 120 seconds
    When the fleet-wide performance for task type "PICK" is requested
    Then the request is accepted with status 200
    And the task type performance response reports task type "PICK" and task count 2
    And the task type performance response reports a mean efficiency of 75 percent
    And the task type performance response reports a mean actual duration of 90 seconds

  @bdd
  Scenario: Other task types do not leak into the result
    Given a standard of 60 expected seconds is already defined for task type "PICK"
    And associate "A-1" completed a PICK task in 60 seconds
    And associate "A-1" completed a PACK task in 300 seconds
    When the fleet-wide performance for task type "PICK" is requested
    Then the task type performance response reports task type "PICK" and task count 1
    And the task type performance response reports a mean actual duration of 60 seconds

  @bdd
  Scenario: Without a standard the real duration is still reported but no efficiency
    Given associate "A-1" completed a PICK task in 60 seconds
    And associate "A-2" completed a PICK task in 120 seconds
    When the fleet-wide performance for task type "PICK" is requested
    Then the task type performance response reports task type "PICK" and task count 2
    And the task type performance response reports no mean efficiency
    And the task type performance response reports a mean actual duration of 90 seconds

  @bdd
  Scenario: A zero-duration completion counts as a task but not towards the mean duration
    Given a standard of 60 expected seconds is already defined for task type "PICK"
    And associate "A-1" completed a PICK task in 60 seconds
    And associate "A-1" completed a PICK task in 0 seconds
    When the fleet-wide performance for task type "PICK" is requested
    Then the task type performance response reports task type "PICK" and task count 2
    And the task type performance response reports a mean efficiency of 100 percent
    And the task type performance response reports a mean actual duration of 60 seconds

  @bdd
  Scenario: A task type that only has unmeasurable completions has no mean duration
    Given associate "A-1" completed a SLAM task in 0 seconds
    When the fleet-wide performance for task type "SLAM" is requested
    Then the task type performance response reports task type "SLAM" and task count 1
    And the task type performance response reports no mean efficiency
    And the task type performance response reports no mean actual duration

  @bdd
  Scenario: A completion at a station with no checked-in associate still counts fleet-wide
    Given a standard of 60 expected seconds is already defined for task type "PACK"
    And a station with no checked-in associate completed a PACK task in 30 seconds
    When the fleet-wide performance for task type "PACK" is requested
    Then the task type performance response reports task type "PACK" and task count 1
    And the task type performance response reports a mean efficiency of 200 percent

  @bdd
  Scenario: A completion at a station with no checked-in associate never creates a scorecard
    Given a standard of 60 expected seconds is already defined for task type "PACK"
    And a station with no checked-in associate completed a PACK task in 30 seconds
    When the scorecard for associate "A-1" is requested
    Then the request is rejected with status 404

  @bdd
  Scenario: A task type that was never recorded reports no means at all
    When the fleet-wide performance for task type "SLAM" is requested
    Then the task type performance response reports task type "SLAM" and task count 0
    And the task type performance response reports no mean efficiency
    And the task type performance response reports no mean actual duration

  @bdd
  Scenario: A redelivered completion does not change the fleet-wide numbers
    Given a standard of 60 expected seconds is already defined for task type "PICK"
    And associate "A-1" completed a PICK task "T-1" in 60 seconds at "2026-10-01T09:00:00Z"
    When the completion of task "T-1" is delivered again
    And the fleet-wide performance for task type "PICK" is requested
    Then the task type performance response reports task type "PICK" and task count 1
    And the task type performance response reports a mean actual duration of 60 seconds
