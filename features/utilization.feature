# Utilization (idle-gap) read-model scenarios.
#
# Every scenario below derives from:
#   - docs/docs/adr/0014-labor-utilization-idleness.md ("Decision" ->
#     "Inbound surfaces": GET /task-types/{taskType}/utilization and
#     GET /associates/{associateId}/utilization; "Utilization" /
#     "Open Gap" definitions; open gap computed at read time, never
#     persisted)
#   - apis/openapi.yaml, paths /task-types/{taskType}/utilization and
#     /associates/{associateId}/utilization ("Always returns 200,
#     including a zero-count result for a TaskType / an associate this
#     service has never observed"; 400 for an unknown task type),
#     components.parameters.WindowQueryParam ("A Go duration string
#     (e.g. "1h", "30m") ... Defaults to 1 hour when omitted or
#     malformed") and components.schemas.Utilization (utilizationPct
#     "Null when nothing was observed in the window — never a fabricated
#     number")
#
# Both endpoints read only recorded TaskPerformance/IdlePeriod rows,
# which are written exclusively by the Kafka consumer (ADR 0014); over
# REST the only reachable state is "nothing observed yet", which is
# exactly the documented zero-count behavior asserted here.
Feature: Reading idle-gap utilization over a trailing window
  As the fleet's labor performance service
  I want to expose windowed task-vs-idle utilization read models
  So that a never-observed subject is a real zero-count result with a null utilization percent, never a fabricated number or a 404

  Background:
    Given the Labor Performance service is running

  @bdd
  # apis/openapi.yaml GET /task-types/{taskType}/utilization description
  # + components.schemas.Utilization (utilizationPct nullable).
  Scenario: A TaskType never observed still returns a 200 zero-count utilization result
    When the fleet-wide utilization for task type "SLAM" is requested
    Then the request is accepted with status 200
    And the utilization response reports task type "SLAM"
    And the utilization response reports 0 associates over a 3600 second window
    And the utilization response reports 0 task seconds, 0 idle seconds and 0 open gap seconds
    And the utilization response reports a null utilization percent

  @bdd
  # apis/openapi.yaml GET /associates/{associateId}/utilization
  # description + components.schemas.Utilization ("Always 1 for a
  # per-associate result" only when observed; 0 for a never-observed
  # subject).
  Scenario: An associate never observed still returns a 200 zero-count utilization result
    When the utilization for associate "assoc-never-seen" is requested
    Then the request is accepted with status 200
    And the utilization response reports associate id "assoc-never-seen"
    And the utilization response reports 0 associates over a 3600 second window
    And the utilization response reports 0 task seconds, 0 idle seconds and 0 open gap seconds
    And the utilization response reports a null utilization percent

  @bdd
  # apis/openapi.yaml GET /task-types/{taskType}/utilization 400
  # response ("Unknown task type").
  Scenario: Requesting task-type utilization for an unrecognized TaskType is rejected
    When the fleet-wide utilization for task type "UNLOAD" is requested
    Then the request is rejected with status 400
    And the response is an RFC 7807 problem

  @bdd
  # apis/openapi.yaml components.parameters.WindowQueryParam ("Defaults
  # to 1 hour when omitted or malformed") — a malformed window degrades
  # gracefully to the default instead of failing the request.
  Scenario: A malformed window query parameter degrades to the default one-hour window
    When the fleet-wide utilization for task type "SLAM" is requested with window "not-a-duration"
    Then the request is accepted with status 200
    And the utilization response reports task type "SLAM"
    And the utilization response reports 0 associates over a 3600 second window
