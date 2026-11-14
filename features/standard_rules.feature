# Engineered standards: effective range, history and the documented error catalogue.
#
# Derived from the documentation pack (warehouse-docs, labor-performance):
#   - business-context.md: "resolve the active LaborStandard as of the event's
#     CompletedAt ... Neither that field nor the derived EfficiencyPct is ever
#     recomputed later"; "Define the standard BEFORE the work happens"
#   - aggregate-design-canvas.md, LaborStandard: effective range
#     [effectiveFrom, effectiveTo); a revision closes the prior record and the
#     new one starts "at the same instant"; Enforced Invariants table
#   - use-cases.md, RecordTaskPerformance: step 1 "MarkProcessed(KafkaEventId)";
#     step 2 most specific wins, as of CompletedAt (ADR 0035)
#   - troubleshooting.md "Problem types" and "REST callers": every slug, the
#     409 business conflicts, the NUL (U+0000) rules, 404 resource-not-found
#
# Completions are seeded through RecordTaskPerformance (what the Kafka consumer
# calls); the 409 rows script the repository exactly where the Postgres adapter
# fails (version guard, one-open-standard unique index).

Feature: Standard history, as-of scoring and the problem catalogue
  As a labor planner
  I want a completion judged by the standard in force when it happened
  So that revising a standard never rewrites history, and every refusal names its cause

  Background:
    Given the Labor Performance service is running

  @bdd
  Scenario Outline: A standard is in force from the instant it is defined, and not before
    Given the clock is at "2026-10-01T12:00:00Z"
    And a standard of 60 expected seconds is already defined for task type "PICK"
    And associate "A-1" completed a PICK task "T-1" with a duration of 60 seconds at "<completedAt>"
    Then the efficiency recorded for task "T-1" is "<efficiency>"

    Examples:
      | completedAt          | efficiency |
      | 2026-10-01T11:59:59Z | none       |
      | 2026-10-01T12:00:00Z | 100        |

  @bdd
  Scenario Outline: A revision takes over at the very instant it is defined and the prior standard ends there
    Given the clock is at "2026-10-01T12:00:00Z"
    And a standard of 60 expected seconds is already defined for task type "PICK"
    And the clock is at "2026-10-01T13:00:00Z"
    And a standard of 30 expected seconds is already defined for task type "PICK"
    And associate "A-1" completed a PICK task "T-1" with a duration of 60 seconds at "<completedAt>"
    Then the efficiency recorded for task "T-1" is "<efficiency>"

    Examples:
      | completedAt          | efficiency |
      | 2026-10-01T12:59:59Z | 100        |
      | 2026-10-01T13:00:00Z | 50         |

  @bdd
  Scenario: A completion redelivered after its standard was revised keeps the score it was given
    Given the clock is at "2026-10-01T12:00:00Z"
    And a standard of 60 expected seconds is already defined for task type "PICK"
    And associate "A-1" completed a PICK task "T-1" in 60 seconds at "2026-10-01T12:30:00Z"
    And the clock is at "2026-10-01T13:00:00Z"
    And a standard of 30 expected seconds is already defined for task type "PICK"
    When the completion of task "T-1" is delivered again
    And the scorecard for associate "A-1" is requested
    Then the scorecard reports 1 task and a mean efficiency of 100 percent
    And the efficiency recorded for task "T-1" is "100"
    And 1 "TaskPerformanceRecorded" event was published

  @bdd
  Scenario: Defining a standard after the work happened never scores that work retroactively
    Given the clock is at "2026-10-01T09:00:00Z"
    And associate "A-1" completed a PICK task "T-1" in 60 seconds at "2026-10-01T08:30:00Z"
    And the clock is at "2026-10-01T10:00:00Z"
    And a standard of 60 expected seconds is already defined for task type "PICK"
    When the completion of task "T-1" is delivered again
    And the scorecard for associate "A-1" is requested
    Then the scorecard reports 1 task and no mean efficiency
    And the efficiency recorded for task "T-1" is "none"

  @bdd
  Scenario: The CloudEvents id is the dedupe key, so the same task under a new event id counts again
    Given a standard of 60 expected seconds is already defined for task type "PICK"
    And associate "A-1" completed a PICK task "T-1" in 60 seconds at "2026-10-01T09:00:00Z"
    When the completion of task "T-1" is delivered again under a new event id
    And the scorecard for associate "A-1" is requested
    Then the scorecard reports 2 tasks and a mean efficiency of 100 percent
    And 2 "TaskPerformanceRecorded" events were published

  @bdd
  Scenario: A redelivered event id is ignored even when its payload now says something else
    Given a standard of 60 expected seconds is already defined for task type "PICK"
    And associate "A-1" completed a PICK task "T-1" in 60 seconds at "2026-10-01T09:00:00Z"
    When the event of task "T-1" is redelivered carrying task "T-9" and a duration of 120 seconds
    And the scorecard for associate "A-1" is requested
    Then the scorecard reports 1 task and a mean efficiency of 100 percent
    And 1 "TaskPerformanceRecorded" event was published

  @bdd
  Scenario: A revision that loses the optimistic-concurrency race is refused and announces nothing
    Given a standard of 45 expected seconds is already defined for task type "PICK"
    And the next standard write loses an optimistic-concurrency race
    When a standard of 40 expected seconds is defined for task type "PICK"
    Then the request is rejected with status 409
    And the problem type is "concurrent-modification"
    And the response is an RFC 7807 problem
    And 0 "LaborStandardRevised" events were published
    When the currently-active standard for task type "PICK" is requested
    Then the standard response reports task type "PICK" and expected seconds 45

  @bdd
  Scenario: A second open standard for one task type is refused as a standard-conflict
    Given the next standard write hits the one-open-standard unique index
    When a standard of 45 expected seconds is defined for task type "PICK"
    Then the request is rejected with status 409
    And the problem type is "standard-conflict"
    And 0 "LaborStandardDefined" events were published
    When the currently-active standard for task type "PICK" is requested
    Then the request is rejected with status 404

  @bdd
  Scenario: An unexpected storage failure is an internal-error problem and stores nothing
    Given the next standard write fails with an unexpected storage error
    When a standard of 45 expected seconds is defined for task type "PICK"
    Then the request is rejected with status 500
    And the problem type is "internal-error"
    And 0 "LaborStandardDefined" events were published
    When the currently-active standard for task type "PICK" is requested
    Then the request is rejected with status 404

  @bdd
  Scenario Outline: Every refused definition names its cause in the problem type and changes nothing
    When a standard definition with the body '<body>' is sent
    Then the request is rejected with status <status>
    And the problem type is "<slug>"
    And the response is an RFC 7807 problem
    And 0 "LaborStandardDefined" events were published

    Examples: the LaborStandard invariants
      | body                                                                                                                  | status | slug                                      |
      | {"taskType":"PICK","expectedSeconds":0}                                                                               | 422    | non-positive-expected-seconds             |
      | {"taskType":"PICK","expectedSeconds":-1}                                                                              | 422    | non-positive-expected-seconds             |
      | {"taskType":"PICK","expectedSeconds":60,"travelComponentSeconds":-1}                                                  | 422    | negative-travel-component-seconds         |
      | {"taskType":"PICK","expectedSeconds":60,"travelComponentSeconds":61}                                                  | 422    | travel-component-exceeds-expected-seconds |
      | {"taskType":"PICK","handlingUnit":"EACH","expectedSeconds":30,"teamLift":{"thresholdG":18000,"expectedSeconds":75}}   | 422    | team-lift-requires-case-unit              |
      | {"taskType":"PICK","handlingUnit":"PALLET","expectedSeconds":30,"teamLift":{"thresholdG":18000,"expectedSeconds":75}} | 422    | team-lift-requires-case-unit              |
      | {"taskType":"PICK","expectedSeconds":30,"teamLift":{"thresholdG":18000,"expectedSeconds":75}}                         | 422    | team-lift-requires-case-unit              |
      | {"taskType":"PICK","handlingUnit":"CASE","expectedSeconds":30,"teamLift":{"thresholdG":0,"expectedSeconds":75}}       | 422    | non-positive-team-lift-threshold          |
      | {"taskType":"PICK","handlingUnit":"CASE","expectedSeconds":30,"teamLift":{"thresholdG":18000,"expectedSeconds":0}}    | 422    | non-positive-team-lift-expected-seconds   |

    Examples: the request itself is wrong
      | body                                                                | status | slug                   |
      | {"taskType":"UNLOAD","expectedSeconds":45}                          | 400    | unknown-task-type      |
      | {"taskType":"PICK","handlingUnit":"CARTON","expectedSeconds":45}    | 400    | unknown-handling-unit  |
      | {"taskType":"PICK","handlingUnit":"case","expectedSeconds":45}      | 400    | unknown-handling-unit  |
      |                                                                     | 400    | malformed-request-body |
      | not json                                                            | 400    | malformed-request-body |

  @bdd
  Scenario Outline: A NUL character in a body is a malformed request, but only a real one
    When a standard definition with the body '<body>' is sent
    Then the request is rejected with status 400
    And the problem type is "<slug>"
    And 0 "LaborStandardDefined" events were published

    Examples: raw and escaped U+0000 are refused; an escaped backslash followed by u0000 is only text
      | body                                              | slug                   |
      | {"taskType":"PI{NUL}CK","expectedSeconds":45}     | malformed-request-body |
      | {"taskType":"PI\u0000CK","expectedSeconds":45}    | malformed-request-body |
      | {"taskType":"PICK\\\\u0000","expectedSeconds":45} | unknown-task-type      |

  @bdd
  Scenario Outline: Every refused read names its cause in the problem type
    When the path "<path>" is requested
    Then the request is rejected with status <status>
    And the problem type is "<slug>"
    And the response is an RFC 7807 problem

    Examples:
      | path                                 | status | slug                  |
      | /standards/SLAM                      | 404    | standard-not-found    |
      | /associates/A-404/scorecard          | 404    | associate-not-found   |
      | /standards/UNLOAD                    | 400    | unknown-task-type     |
      | /standards/PICK?handlingUnit=CARTON  | 400    | unknown-handling-unit |
      | /task-types/UNLOAD/performance       | 400    | unknown-task-type     |
      | /task-types/UNLOAD/utilization       | 400    | unknown-task-type     |
      | /standards/PICK%00                   | 404    | resource-not-found    |
      | /associates/A%001/scorecard          | 404    | resource-not-found    |

  @bdd
  Scenario: Revising the task-type-only standard leaves a CASE standard open
    Given a standard of 45 expected seconds is already defined for task type "PICK"
    And a standard of 30 expected seconds is already defined for task type "PICK" and handling unit "CASE"
    When a standard of 40 expected seconds is defined for task type "PICK"
    Then the request is accepted with status 201
    And 1 "LaborStandardRevised" event was published
    When the standard for task type "PICK" and handling unit "CASE" is requested
    Then the standard response reports task type "PICK" and expected seconds 30
    And the standard response reports handling unit "CASE"
    When the currently-active standard for task type "PICK" is requested
    Then the standard response reports task type "PICK" and expected seconds 40

  @bdd
  Scenario: A CASE standard declared after the work leaves earlier completions on the standard that applied then
    Given the clock is at "2026-10-01T12:00:00Z"
    And a standard of 45 expected seconds is already defined for task type "PICK"
    And the clock is at "2026-10-01T13:00:00Z"
    And a standard of 30 expected seconds is already defined for task type "PICK" and handling unit "CASE"
    And associate "A-1" completed a PICK task "T-1" for handling unit "CASE" in 60 seconds at "2026-10-01T12:30:00Z"
    And associate "A-1" completed a PICK task "T-2" for handling unit "CASE" in 60 seconds at "2026-10-01T13:30:00Z"
    Then the efficiency recorded for task "T-1" is "75"
    And the efficiency recorded for task "T-2" is "50"

  @bdd
  Scenario: A heavy case is never a team lift when its CASE standard declares none
    Given a standard of 30 expected seconds is already defined for task type "PICK" and handling unit "CASE"
    And associate "A-1" completed a PICK task "T-1" for handling unit "CASE" weighing 40000 grams in 60 seconds
    Then the "TaskPerformanceRecorded" event for task "T-1" carries handling unit "CASE" and team lift false
    And the efficiency recorded for task "T-1" is "50"
