# Engineered standards per handling unit and the declared heavy-case team lift.
#
# Every scenario below derives from:
#   - docs/docs/adr/0035-handling-unit-standards-and-team-lift.md
#     (contract P11-5: a standard may be declared per (taskType, handlingUnit
#     in EACH|CASE|PALLET); the most specific standard wins and a completion
#     with no handling unit keeps today's behaviour; a CASE standard may
#     declare a team-lift weight threshold and standard; no sibling call)
#   - apis/openapi.yaml, DefineStandardRequest.handlingUnit / teamLift and
#     GET /standards/{taskType}?handlingUnit=
#   - apis/asyncapi.yaml, the additive handling_unit / team_lift fields
Feature: Engineered standards per handling unit
  As the fleet's labor performance service
  I want a case handle to be scored against its own engineered standard
  So that a case pick is not judged by an each-pick yardstick, and a heavy case that needs two operators is judged by the team-lift time

  Background:
    Given the Labor Performance service is running

  @bdd
  Scenario: A CASE standard is declared next to the task-type-only standard without closing it
    Given a standard of 45 expected seconds is already defined for task type "PICK"
    When a standard of 30 expected seconds is defined for task type "PICK" and handling unit "CASE"
    Then the request is accepted with status 201
    And the standard response reports handling unit "CASE"
    When the currently-active standard for task type "PICK" is requested
    Then the request is accepted with status 200
    And the standard response reports task type "PICK" and expected seconds 45
    And the standard response omits the handling unit

  @bdd
  Scenario: The most specific standard wins when resolving what applies to a handle
    Given a standard of 45 expected seconds is already defined for task type "PICK"
    And a standard of 30 expected seconds is already defined for task type "PICK" and handling unit "CASE"
    When the standard for task type "PICK" and handling unit "CASE" is requested
    Then the standard response reports task type "PICK" and expected seconds 30
    And the standard response reports handling unit "CASE"
    When the standard for task type "PICK" and handling unit "EACH" is requested
    Then the standard response reports task type "PICK" and expected seconds 45
    And the standard response omits the handling unit

  @bdd
  Scenario: A case completion is scored against the CASE standard, an each completion against the task-type-only one
    Given a standard of 45 expected seconds is already defined for task type "PICK"
    And a standard of 30 expected seconds is already defined for task type "PICK" and handling unit "CASE"
    And associate "assoc-case" completed a PICK task "t-case" for handling unit "CASE" in 60 seconds
    And associate "assoc-each" completed a PICK task "t-each" in 60 seconds
    When the scorecard for associate "assoc-case" is requested
    Then the scorecard reports 1 task and a mean efficiency of 50.00 percent
    When the scorecard for associate "assoc-each" is requested
    Then the scorecard reports 1 task and a mean efficiency of 75.00 percent
    And the "TaskPerformanceRecorded" event for task "t-case" carries handling unit "CASE" and team lift false
    And the "TaskPerformanceRecorded" event for task "t-each" carries no handling unit

  @bdd
  Scenario: A unit with no standard of its own falls back to the task-type-only standard
    Given a standard of 45 expected seconds is already defined for task type "PICK"
    And associate "assoc-pal" completed a PICK task "t-pal" for handling unit "PALLET" in 90 seconds
    When the scorecard for associate "assoc-pal" is requested
    Then the scorecard reports 1 task and a mean efficiency of 50.00 percent

  @bdd
  Scenario: A unit with no applicable standard at all stays unscored, never a fabricated number
    Given a standard of 90 expected seconds is already defined for task type "PICK" and handling unit "PALLET"
    And associate "assoc-x" completed a PICK task "t-x" for handling unit "CASE" in 60 seconds
    When the scorecard for associate "assoc-x" is requested
    Then the scorecard reports 1 task and no mean efficiency
    And the "TaskPerformanceRecorded" event for task "t-x" reports no efficiency

  @bdd
  Scenario: A case heavier than the declared threshold is scored against the team-lift standard
    Given a standard of 30 expected seconds with a team lift above 18000 grams of 75 seconds is already defined for task type "PICK" and handling unit "CASE"
    And associate "assoc-heavy" completed a PICK task "t-heavy" for handling unit "CASE" weighing 18001 grams in 75 seconds
    When the scorecard for associate "assoc-heavy" is requested
    Then the scorecard reports 1 task and a mean efficiency of 100.00 percent
    And the "TaskPerformanceRecorded" event for task "t-heavy" carries handling unit "CASE" and team lift true

  @bdd
  Scenario: A case weighing exactly the threshold is still a one-operator lift
    Given a standard of 30 expected seconds with a team lift above 18000 grams of 75 seconds is already defined for task type "PICK" and handling unit "CASE"
    And associate "assoc-edge" completed a PICK task "t-edge" for handling unit "CASE" weighing 18000 grams in 60 seconds
    When the scorecard for associate "assoc-edge" is requested
    Then the scorecard reports 1 task and a mean efficiency of 50.00 percent
    And the "TaskPerformanceRecorded" event for task "t-edge" carries handling unit "CASE" and team lift false

  @bdd
  Scenario: A case of unknown weight is never treated as a team lift
    Given a standard of 30 expected seconds with a team lift above 18000 grams of 75 seconds is already defined for task type "PICK" and handling unit "CASE"
    And associate "assoc-unk" completed a PICK task "t-unk" for handling unit "CASE" in 60 seconds
    Then the "TaskPerformanceRecorded" event for task "t-unk" carries handling unit "CASE" and team lift false

  @bdd
  Scenario: The declared team lift is echoed with its two operators
    When a standard of 30 expected seconds with a team lift above 18000 grams of 75 seconds is defined for task type "PICK" and handling unit "CASE"
    Then the request is accepted with status 201
    And the standard response reports a team lift above 18000 grams of 75 seconds with 2 operators
    And the "LaborStandardDefined" event reports task type "PICK" and expected seconds 30

  @bdd
  Scenario: Revising a CASE standard closes only the CASE standard
    Given a standard of 45 expected seconds is already defined for task type "PICK"
    And a standard of 30 expected seconds is already defined for task type "PICK" and handling unit "CASE"
    When a standard of 28 expected seconds is defined for task type "PICK" and handling unit "CASE"
    Then the request is accepted with status 201
    And 1 "LaborStandardRevised" event was published
    When the standard for task type "PICK" and handling unit "CASE" is requested
    Then the standard response reports task type "PICK" and expected seconds 28
    When the currently-active standard for task type "PICK" is requested
    Then the standard response reports task type "PICK" and expected seconds 45

  @bdd
  Scenario: A team lift on a standard that is not a CASE standard is rejected
    When a standard of 30 expected seconds with a team lift above 18000 grams of 75 seconds is defined for task type "PICK" and handling unit "EACH"
    Then the request is rejected with status 422
    And the response is an RFC 7807 problem

  @bdd
  Scenario: A non-positive team-lift threshold is rejected
    When a standard of 30 expected seconds with a team lift above 0 grams of 75 seconds is defined for task type "PICK" and handling unit "CASE"
    Then the request is rejected with status 422
    And the response is an RFC 7807 problem

  @bdd
  Scenario: An unknown handling unit is rejected
    When a standard of 30 expected seconds is defined for task type "PICK" and handling unit "CARTON"
    Then the request is rejected with status 400
    And the response is an RFC 7807 problem
