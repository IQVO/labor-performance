# Travel-component scenarios for the LaborStandard aggregate.
#
# Every scenario below derives from:
#   - docs/docs/adr/0015-optional-travel-component-on-labor-standard.md
#     ("Decision": optional caller-supplied TravelComponentSeconds; the
#     single local invariant 0 <= TravelComponentSeconds <=
#     ExpectedSeconds; nil means "not broken out", omitted entirely on
#     the wire, never defaulted to 0; a revision declares its own value,
#     never inheriting the closed standard's)
#   - apis/openapi.yaml, components.schemas.TravelComponentSeconds and
#     components.schemas.DefineStandardRequest ("When present, must be
#     between 0 and expectedSeconds inclusive"), and the POST /standards
#     422 response ("travelComponentSeconds is negative or exceeds
#     expectedSeconds")
#   - apis/openapi.yaml, components.schemas.Standard
#     (travelComponentSeconds nullable; omitted when never declared)
Feature: Declaring an optional travel component on a labor standard
  As the fleet's labor performance service
  I want to persist a caller-declared travel breakdown of a standard's expected seconds
  So that a reviewer can see travel vs. execution time without this service ever calling a sibling context to compute it

  Background:
    Given the Labor Performance service is running

  @bdd
  # ADR 0015, "Decision" point 1 (declarative input) + point 2 (invariant
  # upper bound respected); apis/openapi.yaml TravelComponentSeconds.
  Scenario: Defining a standard with a declared travel component reports the split
    When a standard of 45 expected seconds and a travel component of 15 seconds is defined for task type "PICK"
    Then the request is accepted with status 201
    And the standard response reports task type "PICK" and expected seconds 45
    And the standard response reports travel component seconds 15

  @bdd
  # ADR 0015, "Decision" point 2 (ErrTravelComponentExceedsExpectedSeconds);
  # apis/openapi.yaml POST /standards 422 response description.
  Scenario: A travel component larger than the standard's own expected seconds is rejected
    When a standard of 40 expected seconds and a travel component of 41 seconds is defined for task type "PICK"
    Then the request is rejected with status 422
    And the response is an RFC 7807 problem

  @bdd
  # ADR 0015, "Decision" point 2 (ErrNegativeTravelComponentSeconds);
  # apis/openapi.yaml POST /standards 422 response description.
  Scenario: A negative travel component is rejected
    When a standard of 45 expected seconds and a travel component of -5 seconds is defined for task type "PICK"
    Then the request is rejected with status 422
    And the response is an RFC 7807 problem

  @bdd
  # ADR 0015, "Decision" point 3 (nil means "not broken out" — omitted
  # entirely, not defaulted to 0, so "never declared" stays distinct from
  # "genuinely zero").
  Scenario: A standard that never declared a travel component omits it entirely rather than reporting zero
    When a standard of 45 expected seconds is defined for task type "SLAM"
    Then the request is accepted with status 201
    And the standard response omits the travel component entirely
    When the currently-active standard for task type "SLAM" is requested
    Then the request is accepted with status 200
    And the standard response omits the travel component entirely

  @bdd
  # ADR 0015, "Decision" point 4 (a revision's travel component is its
  # own value, never inherited from the closed standard).
  Scenario: A revision declares its own travel component, never inheriting the closed one
    Given a standard of 45 expected seconds and a travel component of 15 seconds is already defined for task type "PACK"
    When a standard of 40 expected seconds is defined for task type "PACK"
    Then the request is accepted with status 201
    When the currently-active standard for task type "PACK" is requested
    Then the request is accepted with status 200
    And the standard response reports task type "PACK" and expected seconds 40
    And the standard response omits the travel component entirely
