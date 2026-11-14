# Scorecard rules at their documented boundaries.
#
# Derived from the documentation pack (warehouse-docs, labor-performance):
#   - use-cases.md GetAssociateScorecard: "from the 10 most recent rows, trend
#     (ClassifyTrend: the recent mean against the all-time mean, +/-5 percentage
#     points, needs at least 3 scored tasks, otherwise INSUFFICIENT_DATA) and
#     coachingFlag (DetectCoachingFlag: the last 3 scored tasks are all below 85%)"
#   - ubiquitous-language.md Trend / Coaching flag
#   - business-context.md: a scorecard is visibility only; the number is null,
#     never 0, when nothing was scorable
#
# Standard: 60 seconds. 120 s is 50 percent, 100 s is 60 percent.

Feature: Scorecard trend, coaching flag and per-task-type breakdown at their boundaries
  As a floor supervisor
  I want the scorecard thresholds to behave exactly as documented
  So that I coach on real shortfalls and never on noise or missing data

  Background:
    Given the Labor Performance service is running
    And a standard of 60 expected seconds is already defined for task type "PICK"

  @bdd
  Scenario Outline: A recent mean exactly five points away from the all-time mean is already a trend
    Given associate "A-1" completed <older> PICK tasks of <older_seconds> seconds each
    And associate "A-1" completed <newer> PICK tasks of <newer_seconds> seconds each
    When the scorecard for associate "A-1" is requested
    Then the scorecard trend is "<trend>"

    Examples: ten tasks at 50 then ten at 60 percent (mean 55) is +5; the mirror image is -5
      | older | older_seconds | newer | newer_seconds | trend     |
      | 10    | 120           | 10    | 100           | IMPROVING |
      | 10    | 100           | 10    | 120           | DECLINING |

  @bdd
  Scenario Outline: The coaching flag and the trend look only at the ten most recent tasks, scored or not
    Given associate "A-1" completed 3 PICK tasks of 100 seconds each
    And associate "A-1" completed <unscored> PICK tasks of 0 seconds each
    When the scorecard for associate "A-1" is requested
    Then the scorecard coaching flag is <flag>
    And the scorecard trend is "<trend>"

    Examples: three scored tasks at 60 percent followed by unscored ones
      | unscored | flag  | trend             |
      | 0        | true  | STABLE            |
      | 7        | true  | STABLE            |
      | 8        | false | INSUFFICIENT_DATA |

  @bdd
  Scenario: A task type with only unscorable completions shows its count and no mean, never zero
    Given associate "A-1" completed a PICK task in 60 seconds
    And associate "A-1" completed 2 PACK tasks of 60 seconds each
    When the scorecard for associate "A-1" is requested
    Then the scorecard reports 3 tasks and a mean efficiency of 100 percent
    And the scorecard breaks down "PICK" as 1 task with a mean efficiency of 100 percent
    And the scorecard breaks down "PACK" as 2 tasks with no mean efficiency

  @bdd
  Scenario: The empty associate id is no associate, even though station completions were recorded
    Given a station with no checked-in associate completed a PICK task in 60 seconds
    When the path "/associates//scorecard" is requested
    Then the request is rejected with status 404
    And the problem type is "associate-not-found"
