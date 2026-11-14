# The published wire contract: topic, partition key, subject, CloudEvents
# attributes and payload of every event this context publishes.
#
# Derived from the documentation pack (warehouse-docs, labor-performance),
# domain-events.md ("Published"): CloudEvents 1.0 structured mode with a fresh
# `id` per message, `source` /warehouse/labor-performance, `time` = the domain
# event's OccurredAt in UTC, `dataschema` urn:warehouse:labor-performance:
# <stream>:<EventName>:v1; the standard events go to the analytics topic ONLY
# (partition key task_type, subject the new standard_id); TaskPerformanceRecorded
# goes to BOTH topics with the same data but separate ids (analytics key
# task_type, integration key associate_id); efficiency_pct and
# idle_seconds_before are null, never 0; handling_unit/team_lift and the travel
# component are OMITTED when there is none.
#
# Every domain event is encoded by the real analytics and integration encoders
# (the ones the transactional outbox stores); nothing here needs a broker.

Feature: The event contract other contexts and the projector rely on
  As a consumer of this context's events
  I want every message routed, keyed and shaped exactly as documented
  So that workforce-management and the projector never have to guess

  Background:
    Given the Labor Performance service is running

  @bdd
  Scenario: A first definition is announced on the analytics topic only, keyed by task type
    Given the clock is at "2026-10-01T09:30:00Z"
    When a standard of 45 expected seconds and a travel component of 15 seconds is defined for task type "PICK"
    Then the request is accepted with status 201
    And the analytics "LaborStandardDefined" message is on topic "warehouse.labor-performance.analytics" with key "PICK" and subject "std-1"
    And the analytics "LaborStandardDefined" message has type "com.warehouse.wes.labor-performance.standard.LaborStandardDefined" and data schema "urn:warehouse:labor-performance:analytics:LaborStandardDefined:v1"
    And the analytics "LaborStandardDefined" message is a CloudEvents 1.0 event from "/warehouse/labor-performance" occurring at "2026-10-01T09:30:00Z"
    And the analytics "LaborStandardDefined" message data field "standard_id" is "std-1"
    And the analytics "LaborStandardDefined" message data field "task_type" is "PICK"
    And the analytics "LaborStandardDefined" message data field "expected_seconds" is 45
    And the analytics "LaborStandardDefined" message data field "travel_component_seconds" is 15
    And the analytics "LaborStandardDefined" message data field "effective_from" is "2026-10-01T09:30:00Z"
    And 0 integration "LaborStandardDefined" messages were encoded

  @bdd
  Scenario: What a standard does not declare is omitted from its event, not sent as null or zero
    When a standard of 45 expected seconds is defined for task type "PICK"
    Then the analytics "LaborStandardDefined" message data has no field "travel_component_seconds"
    And the analytics "LaborStandardDefined" message data has no field "handling_unit"
    And the analytics "LaborStandardDefined" message data has no field "team_lift_threshold_g"
    And the analytics "LaborStandardDefined" message data has no field "team_lift_expected_seconds"

  @bdd
  Scenario: A revision is announced with the standard it replaced and starts at the instant the prior one closed
    Given the clock is at "2026-10-01T09:00:00Z"
    And a standard of 45 expected seconds and a travel component of 15 seconds is already defined for task type "PICK"
    And the clock is at "2026-10-01T10:00:00Z"
    When a standard of 40 expected seconds is defined for task type "PICK"
    Then 1 analytics "LaborStandardDefined" message was encoded
    And the analytics "LaborStandardRevised" message is on topic "warehouse.labor-performance.analytics" with key "PICK" and subject "std-2"
    And the analytics "LaborStandardRevised" message has type "com.warehouse.wes.labor-performance.standard.LaborStandardRevised" and data schema "urn:warehouse:labor-performance:analytics:LaborStandardRevised:v1"
    And the analytics "LaborStandardRevised" message data field "previous_expected_seconds" is 45
    And the analytics "LaborStandardRevised" message data field "expected_seconds" is 40
    And the analytics "LaborStandardRevised" message data field "effective_from" is "2026-10-01T10:00:00Z"
    And the analytics "LaborStandardRevised" message data has no field "travel_component_seconds"
    And 0 integration "LaborStandardRevised" messages were encoded

  @bdd
  Scenario: A CASE standard's event carries its handling unit and its team-lift declaration
    When a standard of 30 expected seconds with a team lift above 18000 grams of 75 seconds is defined for task type "PICK" and handling unit "CASE"
    Then the analytics "LaborStandardDefined" message data field "handling_unit" is "CASE"
    And the analytics "LaborStandardDefined" message data field "team_lift_threshold_g" is 18000
    And the analytics "LaborStandardDefined" message data field "team_lift_expected_seconds" is 75

  @bdd
  Scenario: A rejected definition encodes no event at all
    When a standard of 0 expected seconds is defined for task type "PICK"
    Then the request is rejected with status 422
    And 0 analytics "LaborStandardDefined" messages were encoded
    And 0 analytics "LaborStandardRevised" messages were encoded

  @bdd
  Scenario: A recorded task goes out twice, keyed by task type for analytics and by associate for integration
    Given the clock is at "2026-10-01T08:00:00Z"
    And a standard of 60 expected seconds is already defined for task type "PICK"
    And the clock is at "2026-10-01T09:05:00Z"
    And associate "A-1" completed a PICK task "T-1" in 120 seconds at "2026-10-01T09:00:00Z"
    Then the analytics "TaskPerformanceRecorded" message is on topic "warehouse.labor-performance.analytics" with key "PICK" and subject "A-1"
    And the integration "TaskPerformanceRecorded" message is on topic "warehouse.labor-performance.events" with key "A-1" and subject "A-1"
    And the analytics "TaskPerformanceRecorded" message has type "com.warehouse.wes.labor-performance.performance.TaskPerformanceRecorded" and data schema "urn:warehouse:labor-performance:analytics:TaskPerformanceRecorded:v1"
    And the integration "TaskPerformanceRecorded" message has type "com.warehouse.wes.labor-performance.performance.TaskPerformanceRecorded" and data schema "urn:warehouse:labor-performance:events:TaskPerformanceRecorded:v1"
    And the analytics and integration "TaskPerformanceRecorded" messages carry the same data under different ids
    And the integration "TaskPerformanceRecorded" message is a CloudEvents 1.0 event from "/warehouse/labor-performance" occurring at "2026-10-01T09:05:00Z"
    And the integration "TaskPerformanceRecorded" message data field "task_id" is "T-1"
    And the integration "TaskPerformanceRecorded" message data field "associate_id" is "A-1"
    And the integration "TaskPerformanceRecorded" message data field "task_type" is "PICK"
    And the integration "TaskPerformanceRecorded" message data field "efficiency_pct" is 50
    And the integration "TaskPerformanceRecorded" message data field "actual_seconds" is 120
    And the integration "TaskPerformanceRecorded" message data field "completed_at" is "2026-10-01T09:00:00Z"
    And the integration "TaskPerformanceRecorded" message data field "idle_seconds_before" is null
    And the integration "TaskPerformanceRecorded" message data has no field "handling_unit"
    And the integration "TaskPerformanceRecorded" message data has no field "team_lift"

  @bdd
  Scenario: The wait before a task travels on its event once there is a previous completion
    Given associate "A-1" completed a PICK task "T-1" in 60 seconds at "2026-10-01T08:01:00Z"
    And associate "A-1" completed a PICK task "T-2" in 60 seconds at "2026-10-01T08:04:00Z"
    Then the integration "TaskPerformanceRecorded" message data field "idle_seconds_before" is 120

  @bdd
  Scenario: A robot-station completion is keyed by the empty associate and subjected by its task id
    Given a station with no checked-in associate completed a PICK task in 60 seconds
    Then the integration "TaskPerformanceRecorded" message is on topic "warehouse.labor-performance.events" with key "" and subject "auto-1"
    And the analytics "TaskPerformanceRecorded" message is on topic "warehouse.labor-performance.analytics" with key "PICK" and subject "auto-1"
    And the analytics "TaskPerformanceRecorded" message data field "associate_id" is ""

  @bdd
  Scenario: An unclassified, unscorable completion travels with an empty type and a null efficiency, never zero
    Given associate "A-1" completed a task "T-1" of unrecognised type "REBIN" in 60 seconds at "2026-10-01T09:00:00Z"
    Then the analytics "TaskPerformanceRecorded" message is on topic "warehouse.labor-performance.analytics" with key "" and subject "A-1"
    And the analytics "TaskPerformanceRecorded" message data field "task_type" is ""
    And the analytics "TaskPerformanceRecorded" message data field "efficiency_pct" is null

  @bdd
  Scenario: A completion that needed a team lift says so, and only completions with a unit say anything
    Given a standard of 30 expected seconds with a team lift above 18000 grams of 75 seconds is already defined for task type "PICK" and handling unit "CASE"
    And associate "A-1" completed a PICK task "T-1" for handling unit "CASE" weighing 18001 grams in 75 seconds
    Then the analytics "TaskPerformanceRecorded" message data field "handling_unit" is "CASE"
    And the analytics "TaskPerformanceRecorded" message data field "team_lift" is true

  @bdd
  Scenario: A redelivered completion encodes nothing a second time
    Given a standard of 60 expected seconds is already defined for task type "PICK"
    And associate "A-1" completed a PICK task "T-1" in 60 seconds at "2026-10-01T09:00:00Z"
    When the completion of task "T-1" is delivered again
    Then 1 analytics "TaskPerformanceRecorded" message was encoded
    And 1 integration "TaskPerformanceRecorded" message was encoded
