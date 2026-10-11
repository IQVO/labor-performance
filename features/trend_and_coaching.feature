# Trend and coaching-flag scenarios.
#
# Derived from:
#   - apis/openapi.yaml, components.schemas.Scorecard:
#       trend: "How this associate's most recent scored tasks (up to the
#       last 10) compare to their all-time mean EfficiencyPct.
#       INSUFFICIENT_DATA when fewer than 3 recent tasks were scored --
#       always present, never omitted"
#       coachingFlag: "True iff this associate's 3 most recent SCORED tasks
#       were all below the coaching floor (85% efficiency). Visibility
#       only"
#   - performance.ClassifyTrend: a recent mean at least 5 points above (or
#     below) the all-time mean is IMPROVING (or DECLINING), otherwise STABLE
#   - performance.DetectCoachingFlag: unscored rows are skipped, never
#     counted as "below standard"; exactly 85% is NOT below the floor
#
# Tasks are seeded oldest-first through RecordTaskPerformance; assertions go
# through GET /associates/{associateId}/scorecard.

Feature: Reading performance trends and coaching signals
  As a floor supervisor
  I want the scorecard to tell me which way an associate is heading
  So that I can offer help early without reacting to a single hard task

  @bdd
  Scenario Outline: The trend compares the last ten scored tasks with the all-time mean
    Given the Labor Performance service is running
    And a standard of 60 expected seconds is already defined for task type "PICK"
    And associate "A-1" completed <older> PICK tasks of <older_seconds> seconds each
    And associate "A-1" completed <newer> PICK tasks of <newer_seconds> seconds each
    When the scorecard for associate "A-1" is requested
    Then the request is accepted with status 200
    And the scorecard trend is "<trend>"

    Examples: improving, declining and steady histories
      | older | older_seconds | newer | newer_seconds | trend             |
      | 10    | 120           | 10    | 60            | IMPROVING         |
      | 10    | 60            | 10    | 120           | DECLINING         |
      | 12    | 60            | 0     | 60            | STABLE            |
      | 10    | 60            | 10    | 62            | STABLE            |

    Examples: too little scored history to call a direction
      | older | older_seconds | newer | newer_seconds | trend             |
      | 1     | 60            | 1     | 60            | INSUFFICIENT_DATA |
      | 2     | 60            | 0     | 60            | INSUFFICIENT_DATA |
      | 2     | 60            | 1     | 60            | STABLE            |

  @bdd
  Scenario: Unscored tasks do not count towards the three needed for a trend
    Given the Labor Performance service is running
    And a standard of 60 expected seconds is already defined for task type "PICK"
    And associate "A-1" completed 2 PICK tasks of 60 seconds each
    And associate "A-1" completed 5 PICK tasks of 0 seconds each
    When the scorecard for associate "A-1" is requested
    Then the scorecard reports 7 tasks and a mean efficiency of 100 percent
    And the scorecard trend is "INSUFFICIENT_DATA"

  @bdd
  Scenario Outline: The coaching flag needs the latest three scored tasks all below 85 percent
    Given the Labor Performance service is running
    And a standard of 60 expected seconds is already defined for task type "PICK"
    And associate "A-1" completed <first> PICK tasks of <first_seconds> seconds each
    And associate "A-1" completed <second> PICK tasks of <second_seconds> seconds each
    When the scorecard for associate "A-1" is requested
    Then the scorecard coaching flag is <flag>

    Examples:
      | first | first_seconds | second | second_seconds | flag  |
      | 3     | 100           | 0      | 60             | true  |
      | 2     | 100           | 0      | 60             | false |
      | 3     | 100           | 1      | 60             | false |
      | 3     | 60            | 3      | 100            | true  |
      | 3     | 100           | 3      | 60             | false |

  @bdd
  Scenario Outline: The coaching floor is exclusive at 85 percent
    Given the Labor Performance service is running
    And a standard of <standard> expected seconds is already defined for task type "PICK"
    And associate "A-1" completed 3 PICK tasks of 100 seconds each
    When the scorecard for associate "A-1" is requested
    Then the scorecard coaching flag is <flag>

    Examples:
      | standard | flag  |
      | 85       | false |
      | 84       | true  |
      | 100      | false |

  @bdd
  Scenario: Unscored tasks are skipped when looking for a below-standard streak
    Given the Labor Performance service is running
    And a standard of 60 expected seconds is already defined for task type "PICK"
    And associate "A-1" completed 2 PICK tasks of 100 seconds each
    And associate "A-1" completed 1 PICK tasks of 0 seconds each
    And associate "A-1" completed 1 PICK tasks of 100 seconds each
    When the scorecard for associate "A-1" is requested
    Then the scorecard reports 4 tasks and a mean efficiency of 60 percent
    And the scorecard coaching flag is true

  @bdd
  Scenario: A declining associate is flagged for coaching while the trend shows the direction
    Given the Labor Performance service is running
    And a standard of 60 expected seconds is already defined for task type "PICK"
    And associate "A-1" completed 10 PICK tasks of 60 seconds each
    And associate "A-1" completed 10 PICK tasks of 120 seconds each
    When the scorecard for associate "A-1" is requested
    Then the scorecard trend is "DECLINING"
    And the scorecard coaching flag is true
