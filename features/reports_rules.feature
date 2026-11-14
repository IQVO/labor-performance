# The Labor Performance Report: projection of the analytics events and the
# reports API (cmd/labor-reports).
#
# Derived from the documentation pack and the repo's own docs (apis/openapi-
# reports.yaml, docs/docs/operations/runbook.md "Reports API"), troubleshooting.md
# "Publishing, outbox and analytics" and domain-events.md "Own analytics events":
#   - rows are bucketed by the BUSINESS time (completedAt / effectiveFrom), never
#     ingestion time; an empty task_type becomes UNCLASSIFIED
#   - meanEfficiencyPct and meanActualSeconds are null, never 0, "when nothing in
#     the bucket was scored or measured"; "Report shows meanEfficiencyPct: null
#     beside tasksRecorded > 0 ... Expected. Null is never shown as 0%"
#   - window [from, to) compared against the hour bucket; 400 invalid-report-query
#     for a missing/malformed from or to, to <= from, or granularity other than
#     hour; 500 report-store-error; rows and byTaskType are [] for an empty window
#   - "Report with taskType=pick returns empty rows: ... Send the task type in
#     upper case"
#   - freshness: lagSeconds is now minus the newest applied event, 0 when empty
#   - the projector dedupes on the CloudEvents id and skips what it cannot or
#     need not project
#
# Facts flow through the real encoders, the real AnalyticsConsumer and the real
# reports router over the in-memory report store.

Feature: Labor Performance Report
  As a console user
  I want the hourly report to reflect exactly what was recorded and never invent a mean
  So that I can chart efficiency by task type and see when it stops being comparable

  Background:
    Given the Labor Performance service is running

  @bdd
  Scenario: A task is reported in the hour it completed, not the hour it was ingested
    Given the clock is at "2026-10-01T07:00:00Z"
    And a standard of 60 expected seconds is already defined for task type "PICK"
    And associate "A-1" completed a PICK task "T-1" in 60 seconds at "2026-10-01T08:59:59Z"
    And associate "A-1" completed a PICK task "T-2" in 60 seconds at "2026-10-01T09:00:00Z"
    When the analytics projector consumes the published events
    And the performance report is requested with query "from=2026-10-01T00:00:00Z&to=2026-10-02T00:00:00Z"
    Then the request is accepted with status 200
    And the response list "rows" has 3 items
    And the response path "rows.0.hourBucket" is "2026-10-01T07:00:00Z"
    And the response path "rows.0.tasksRecorded" is 0
    And the response path "rows.0.standardsDefined" is 1
    And the response path "rows.1.hourBucket" is "2026-10-01T08:00:00Z"
    And the response path "rows.1.tasksRecorded" is 1
    And the response path "rows.2.hourBucket" is "2026-10-01T09:00:00Z"
    And the response path "rows.2.tasksRecorded" is 1

  @bdd
  Scenario: A completion with no recognised task type is reported under UNCLASSIFIED, never dropped
    Given associate "A-1" completed a task "T-1" of unrecognised type "REBIN" in 60 seconds at "2026-10-01T09:10:00Z"
    When the analytics projector consumes the published events
    And the performance report is requested with query "from=2026-10-01T00:00:00Z&to=2026-10-02T00:00:00Z"
    Then the response list "rows" has 1 item
    And the response path "rows.0.taskType" is "UNCLASSIFIED"
    And the response path "rows.0.tasksRecorded" is 1
    And the response path "byTaskType.0.taskType" is "UNCLASSIFIED"

  @bdd
  Scenario: Tasks that cannot be scored or measured are counted but never given a mean
    Given associate "A-1" completed a PICK task "T-1" in 60 seconds at "2026-10-01T09:10:00Z"
    And associate "A-1" completed a PICK task "T-2" with a duration of 0 seconds at "2026-10-01T09:20:00Z"
    When the analytics projector consumes the published events
    And the performance report is requested with query "from=2026-10-01T00:00:00Z&to=2026-10-02T00:00:00Z"
    Then the response path "rows.0.tasksRecorded" is 2
    And the response path "rows.0.tasksScored" is 0
    And the response path "rows.0.tasksUnscored" is 2
    And the response path "rows.0.tasksMeasured" is 1
    And the response path "rows.0.meanEfficiencyPct" is null
    And the response path "rows.0.meanActualSeconds" is 60
    And the response path "totals.tasksUnscored" is 2
    And the response path "totals.meanEfficiencyPct" is null

  @bdd
  Scenario: Rows are ordered by hour and then by task type, and the breakdown is sorted by task type
    Given associate "A-1" completed a PICK task "T-1" in 60 seconds at "2026-10-01T09:10:00Z"
    And associate "A-1" completed a PACK task "T-2" in 60 seconds at "2026-10-01T09:20:00Z"
    And associate "A-1" completed a SLAM task "T-3" in 60 seconds at "2026-10-01T08:50:00Z"
    When the analytics projector consumes the published events
    And the performance report is requested with query "from=2026-10-01T00:00:00Z&to=2026-10-02T00:00:00Z"
    Then the response path "rows.0.taskType" is "SLAM"
    And the response path "rows.1.taskType" is "PACK"
    And the response path "rows.2.taskType" is "PICK"
    And the response path "byTaskType.0.taskType" is "PACK"
    And the response path "byTaskType.1.taskType" is "PICK"
    And the response path "byTaskType.2.taskType" is "SLAM"

  @bdd
  Scenario: The breakdown and the totals are weighted by volume, not a mean of hourly means
    Given the clock is at "2026-10-01T08:00:00Z"
    And a standard of 60 expected seconds is already defined for task type "PICK"
    And associate "A-1" completed a PICK task "T-1" in 60 seconds at "2026-10-01T08:10:00Z"
    And associate "A-1" completed a PICK task "T-2" in 120 seconds at "2026-10-01T09:10:00Z"
    And associate "A-1" completed a PICK task "T-3" in 120 seconds at "2026-10-01T09:20:00Z"
    And associate "A-1" completed a PICK task "T-4" in 120 seconds at "2026-10-01T09:30:00Z"
    When the analytics projector consumes the published events
    And the performance report is requested with query "from=2026-10-01T08:00:00Z&to=2026-10-01T10:00:00Z"
    Then the response path "rows.0.meanEfficiencyPct" is 100
    And the response path "rows.1.meanEfficiencyPct" is 50
    And the response path "byTaskType.0.tasksScored" is 4
    And the response path "byTaskType.0.meanEfficiencyPct" is 62.5
    And the response path "totals.meanEfficiencyPct" is 62.5

  @bdd
  Scenario: Definitions and revisions are counted in the hour they take effect
    Given the clock is at "2026-10-01T08:15:00Z"
    And a standard of 60 expected seconds is already defined for task type "PICK"
    And the clock is at "2026-10-01T09:20:00Z"
    And a standard of 30 expected seconds is already defined for task type "PICK"
    When the analytics projector consumes the published events
    And the performance report is requested with query "from=2026-10-01T00:00:00Z&to=2026-10-02T00:00:00Z"
    Then the response path "rows.0.hourBucket" is "2026-10-01T08:00:00Z"
    And the response path "rows.0.standardsDefined" is 1
    And the response path "rows.0.standardsRevised" is 0
    And the response path "rows.1.hourBucket" is "2026-10-01T09:00:00Z"
    And the response path "rows.1.standardsDefined" is 0
    And the response path "rows.1.standardsRevised" is 1
    And the response path "byTaskType.0.standardsDefined" is 1
    And the response path "byTaskType.0.standardsRevised" is 1

  @bdd
  Scenario Outline: The window is half-open over the hour buckets: from is inclusive, to is exclusive
    Given associate "A-1" completed a PICK task "T-1" in 60 seconds at "2026-10-01T08:30:00Z"
    And associate "A-1" completed a PICK task "T-2" in 60 seconds at "2026-10-01T09:30:00Z"
    When the analytics projector consumes the published events
    And the performance report is requested with query "from=<from>&to=<to>"
    Then the request is accepted with status 200
    And the response path "totals.tasksRecorded" is <recorded>

    Examples: tasks sit in the 08:00 and 09:00 buckets
      | from                 | to                   | recorded |
      | 2026-10-01T08:00:00Z | 2026-10-01T09:00:00Z | 1        |
      | 2026-10-01T09:00:00Z | 2026-10-01T10:00:00Z | 1        |
      | 2026-10-01T08:00:00Z | 2026-10-01T10:00:00Z | 2        |
      | 2026-10-01T08:01:00Z | 2026-10-01T10:00:00Z | 1        |
      | 2026-10-01T07:00:00Z | 2026-10-01T08:00:00Z | 0        |

  @bdd
  Scenario: The window is echoed back in UTC whatever offset the caller used
    When the performance report is requested with query "from=2026-10-01T05:00:00-03:00&to=2026-10-01T09:00:00-03:00"
    Then the request is accepted with status 200
    And the response path "from" is "2026-10-01T08:00:00Z"
    And the response path "to" is "2026-10-01T12:00:00Z"

  @bdd
  Scenario Outline: A malformed report query is a 400 invalid-report-query
    When the performance report is requested with query "<query>"
    Then the request is rejected with status 400
    And the problem type is "invalid-report-query"
    And the response is an RFC 7807 problem

    Examples: missing, malformed, empty or reversed windows and an unknown granularity
      | query                                                                  |
      |                                                                        |
      | to=2026-10-01T10:00:00Z                                                |
      | from=2026-10-01T09:00:00Z                                              |
      | from=yesterday&to=2026-10-01T10:00:00Z                                 |
      | from=2026-10-01T09:00:00Z&to=today                                     |
      | from=2026-10-01T10:00:00Z&to=2026-10-01T09:00:00Z                      |
      | from=2026-10-01T09:00:00Z&to=2026-10-01T09:00:00Z                      |
      | from=2026-10-01T09:00:00Z&to=2026-10-01T10:00:00Z&granularity=day      |

  @bdd
  Scenario: Hour is the one accepted granularity
    When the performance report is requested with query "from=2026-10-01T09:00:00Z&to=2026-10-01T10:00:00Z&granularity=hour"
    Then the request is accepted with status 200

  @bdd
  Scenario: A window with no data is a 200 with empty lists and null means, not a 404
    When the performance report is requested with query "from=2026-10-01T00:00:00Z&to=2026-10-01T01:00:00Z"
    Then the request is accepted with status 200
    And the response list "rows" has 0 items
    And the response list "byTaskType" has 0 items
    And the response path "totals.tasksRecorded" is 0
    And the response path "totals.meanEfficiencyPct" is null
    And the response path "totals.meanActualSeconds" is null

  @bdd
  Scenario Outline: The task type filter is an exact match on the upper-case bucket name
    Given associate "A-1" completed a PICK task "T-1" in 60 seconds at "2026-10-01T09:10:00Z"
    And associate "A-1" completed a PACK task "T-2" in 60 seconds at "2026-10-01T09:20:00Z"
    And associate "A-1" completed a task "T-3" of unrecognised type "REBIN" in 60 seconds at "2026-10-01T09:30:00Z"
    When the analytics projector consumes the published events
    And the performance report is requested with query "from=2026-10-01T00:00:00Z&to=2026-10-02T00:00:00Z<filter>"
    Then the response list "rows" has <rows> items
    And the response list "byTaskType" has <rows> items

    Examples: a lower-case value matches nothing, UNCLASSIFIED selects the unclassified bucket
      | filter                  | rows |
      |                         | 3    |
      | &taskType=PICK          | 1    |
      | &taskType=PACK          | 1    |
      | &taskType=UNCLASSIFIED  | 1    |
      | &taskType=pick          | 0    |

  @bdd
  Scenario: A redelivered analytics message is projected once
    Given associate "A-1" completed a PICK task "T-1" in 60 seconds at "2026-10-01T09:10:00Z"
    When the analytics projector consumes the published events
    And the analytics projector consumes the last analytics "TaskPerformanceRecorded" message again
    And the performance report is requested with query "from=2026-10-01T00:00:00Z&to=2026-10-02T00:00:00Z"
    Then the response path "totals.tasksRecorded" is 1

  @bdd
  Scenario: Messages the projector cannot or need not project leave the report untouched
    Given a legacy flat-envelope message arrives on the analytics topic
    And an analytics message of the unrelated type "SomethingElse" arrives
    And an analytics "TaskPerformanceRecorded" message with undecodable data arrives
    When the performance report is requested with query "from=2026-10-01T00:00:00Z&to=2026-10-02T00:00:00Z"
    Then the request is accepted with status 200
    And the response list "rows" has 0 items

  @bdd
  Scenario: A read model that has applied nothing reports zero lag
    When the freshness lag is requested
    Then the request is accepted with status 200
    And the response path "lagSeconds" is 0

  @bdd
  Scenario: Freshness lag is how long ago the newest applied event occurred
    Given the clock is at "2026-10-01T08:00:00Z"
    And a standard of 60 expected seconds is already defined for task type "PICK"
    When the analytics projector consumes the published events
    And the clock is at "2026-10-01T08:01:30Z"
    And the freshness lag is requested
    Then the response path "lagSeconds" is 90

  @bdd
  Scenario Outline: A report store failure is a 500 report-store-error
    Given the analytical database becomes unreachable
    When the reports path "<path>" is requested
    Then the request is rejected with status 500
    And the problem type is "report-store-error"
    And the response is an RFC 7807 problem

    Examples:
      | path                                                                |
      | /reports/performance?from=2026-10-01T00:00:00Z&to=2026-10-02T00:00:00Z |
      | /reports/performance/freshness                                         |
