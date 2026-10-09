# Standards: published events, effective instants and request validation.
#
# Derived from:
#   - apis/openapi.yaml, POST /standards and GET /standards/{taskType}:
#       201 Standard defined (or the revision recorded); 400 "Unknown task
#       type, malformed body"; 422 "Expected seconds must be greater than
#       zero, travelComponentSeconds is negative or exceeds
#       expectedSeconds"; components.schemas.Standard (effectiveFrom, and
#       effectiveTo "Present only on a superseded (closed) standard")
#   - DefineStandard: an append-only history -- the prior standard is
#     closed and a new one starts at the same instant; the first definition
#     publishes LaborStandardDefined, a revision publishes
#     LaborStandardRevised; a rejected request changes and publishes nothing
#   - components.schemas.TaskType: the closed enum PICK | PACK | SLAM

Feature: Defining standards and the events that announce them
  As the fleet's labor performance service
  I want every accepted definition to be announced and every rejected one to change nothing
  So that downstream contexts only ever hear about standards that really exist

  Background:
    Given the Labor Performance service is running

  @bdd
  Scenario: The first definition for a task type is announced as defined
    When a standard of 45 expected seconds is defined for task type "PICK"
    Then the request is accepted with status 201
    And 1 "LaborStandardDefined" event was published
    And the "LaborStandardDefined" event reports task type "PICK" and expected seconds 45
    And 0 "LaborStandardRevised" events were published

  @bdd
  Scenario: A revision is announced with the standard it replaced
    Given a standard of 45 expected seconds is already defined for task type "PICK"
    When a standard of 40 expected seconds is defined for task type "PICK"
    Then the request is accepted with status 201
    And 1 "LaborStandardRevised" event was published
    And the "LaborStandardRevised" event reports previous expected seconds 45 and new expected seconds 40

  @bdd
  Scenario: A new standard is effective from the instant it was defined
    Given the clock is at "2026-10-01T09:30:00Z"
    When a standard of 45 expected seconds is defined for task type "PICK"
    Then the request is accepted with status 201
    And the response field "effectiveFrom" is "2026-10-01T09:30:00Z"
    And the response omits the field "effectiveTo"

  @bdd
  Scenario: The active standard after a revision is the open one
    Given a standard of 45 expected seconds is already defined for task type "PICK"
    And a standard of 40 expected seconds is already defined for task type "PICK"
    When the currently-active standard for task type "PICK" is requested
    Then the request is accepted with status 200
    And the standard response reports task type "PICK" and expected seconds 40
    And the response omits the field "effectiveTo"

  @bdd
  Scenario: Each task type keeps its own standard
    Given a standard of 45 expected seconds is already defined for task type "PICK"
    And a standard of 30 expected seconds is already defined for task type "PACK"
    When a standard of 20 expected seconds is defined for task type "SLAM"
    Then the request is accepted with status 201
    When the currently-active standard for task type "PICK" is requested
    Then the standard response reports task type "PICK" and expected seconds 45
    When the currently-active standard for task type "PACK" is requested
    Then the standard response reports task type "PACK" and expected seconds 30
    And 3 "LaborStandardDefined" events were published
    And 0 "LaborStandardRevised" events were published

  @bdd
  Scenario: One expected second is the smallest valid standard
    When a standard of 1 expected seconds is defined for task type "PICK"
    Then the request is accepted with status 201
    And the standard response reports task type "PICK" and expected seconds 1

  @bdd
  Scenario: A rejected definition leaves the active standard and the event stream untouched
    Given a standard of 60 expected seconds is already defined for task type "PICK"
    When a standard of 0 expected seconds is defined for task type "PICK"
    Then the request is rejected with status 422
    And the response is an RFC 7807 problem
    When the currently-active standard for task type "PICK" is requested
    Then the standard response reports task type "PICK" and expected seconds 60
    And 1 "LaborStandardDefined" event was published
    And 0 "LaborStandardRevised" events were published

  @bdd
  Scenario Outline: A travel component may be anything from zero up to the expected seconds
    When a standard of 60 expected seconds and a travel component of <travel> seconds is defined for task type "PICK"
    Then the request is accepted with status 201
    And the standard response reports travel component seconds <travel>

    Examples:
      | travel |
      | 0      |
      | 30     |
      | 60     |

  @bdd
  Scenario: A rejected travel component publishes nothing
    When a standard of 60 expected seconds and a travel component of 61 seconds is defined for task type "PICK"
    Then the request is rejected with status 422
    And the response is an RFC 7807 problem
    And 0 "LaborStandardDefined" events were published

  @bdd
  Scenario Outline: Only the three known task types can be given a standard
    When a standard of 45 expected seconds is defined for task type "<taskType>"
    Then the request is rejected with status 400
    And the response is an RFC 7807 problem

    Examples:
      | taskType |
      | UNLOAD   |
      | pick     |
      | Pick     |
      |          |

  @bdd
  Scenario: A malformed request body is rejected as a bad request
    When a standard definition with a malformed JSON body is sent
    Then the request is rejected with status 400
    And the response is an RFC 7807 problem

  @bdd
  Scenario Outline: Task types are matched case-sensitively when reading a standard
    When the currently-active standard for task type "<taskType>" is requested
    Then the request is rejected with status 400
    And the response is an RFC 7807 problem

    Examples:
      | taskType |
      | pick     |
      | Pack     |
