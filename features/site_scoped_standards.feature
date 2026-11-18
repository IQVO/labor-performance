# Labor standards on the integration topic, and the optional site scope.
#
# Every scenario below derives from:
#   - docs/docs/adr/0038-labor-standards-on-the-integration-topic-and-site-scope.md
#     (decision 1: LaborStandardDefined / LaborStandardRevised are also
#     published on warehouse.labor-performance.events with the analytics
#     data, key task_type, subject standard_id; decision 2: an optional
#     siteCode, "" = every site, at most one open standard per (task type,
#     handling unit, site) and a revision closes only the same triple;
#     decision 3: scoring still resolves site-less standards only)
#   - apis/openapi.yaml, DefineStandardRequest.siteCode, Standard.siteCode
#     and GET /standards/{taskType}?siteCode=
#   - apis/asyncapi.yaml, the integration-channel standard messages and the
#     additive site_code field
Feature: Labor standards for the planning context, optionally per site
  As the warehouse-planning context keeping a local copy of the standards
  I want every standard change announced on the integration topic, with the site it applies to
  So that I can plan per site without ever calling this service, while scores stay exactly as they are

  Background:
    Given the Labor Performance service is running

  @bdd
  Scenario: A standard definition is published on the integration topic
    Given the clock is at "2026-11-18T09:00:00Z"
    When a standard of 45 expected seconds is defined for task type "PACK"
    Then the request is accepted with status 201
    And 1 integration "LaborStandardDefined" message was encoded
    And the integration "LaborStandardDefined" message is on topic "warehouse.labor-performance.events" with key "PACK" and subject "std-1"
    And the integration "LaborStandardDefined" message has type "com.warehouse.wes.labor-performance.standard.LaborStandardDefined" and data schema "urn:warehouse:labor-performance:events:LaborStandardDefined:v1"
    And the integration "LaborStandardDefined" message data field "expected_seconds" is 45
    And the analytics and integration "LaborStandardDefined" messages carry the same data under different ids

  @bdd
  Scenario: A site-scoped standard is defined next to the site-less one without closing it
    Given a standard of 45 expected seconds is already defined for task type "PICK"
    When a standard definition with the body '{"taskType":"PICK","expectedSeconds":40,"siteCode":"SITE-GRU-01"}' is sent
    Then the request is accepted with status 201
    And the response path "siteCode" is "SITE-GRU-01"
    And the response path "expectedSeconds" is 40
    And 2 integration "LaborStandardDefined" messages were encoded
    And 0 integration "LaborStandardRevised" messages were encoded
    And the integration "LaborStandardDefined" message is on topic "warehouse.labor-performance.events" with key "PICK" and subject "std-2"
    And the integration "LaborStandardDefined" message data field "site_code" is "SITE-GRU-01"
    And the analytics and integration "LaborStandardDefined" messages carry the same data under different ids
    When the currently-active standard for task type "PICK" is requested
    Then the standard response reports task type "PICK" and expected seconds 45
    And the response has no path "siteCode"

  @bdd
  Scenario: A site's standard is read exactly, never falling back to the site-less one
    Given a standard of 45 expected seconds is already defined for task type "PICK"
    And a standard definition with the body '{"taskType":"PICK","expectedSeconds":40,"siteCode":"SITE-GRU-01"}' is sent
    When the path "/standards/PICK?siteCode=SITE-GRU-01" is requested
    Then the request is accepted with status 200
    And the response path "expectedSeconds" is 40
    And the response path "siteCode" is "SITE-GRU-01"
    When the path "/standards/PICK?siteCode=SITE-POA-02" is requested
    Then the request is rejected with status 404
    And the response is an RFC 7807 problem

  @bdd
  Scenario: Revising a site's standard closes only that site's standard
    Given a standard of 45 expected seconds is already defined for task type "PICK"
    And a standard definition with the body '{"taskType":"PICK","expectedSeconds":40,"siteCode":"SITE-GRU-01"}' is sent
    And a standard definition with the body '{"taskType":"PICK","expectedSeconds":42,"siteCode":"SITE-POA-02"}' is sent
    When a standard definition with the body '{"taskType":"PICK","expectedSeconds":35,"siteCode":"SITE-GRU-01"}' is sent
    Then the request is accepted with status 201
    And 1 integration "LaborStandardRevised" message was encoded
    And the integration "LaborStandardRevised" message is on topic "warehouse.labor-performance.events" with key "PICK" and subject "std-4"
    And the integration "LaborStandardRevised" message data field "previous_expected_seconds" is 40
    And the integration "LaborStandardRevised" message data field "expected_seconds" is 35
    And the integration "LaborStandardRevised" message data field "site_code" is "SITE-GRU-01"
    And the analytics and integration "LaborStandardRevised" messages carry the same data under different ids
    When the path "/standards/PICK?siteCode=SITE-POA-02" is requested
    Then the response path "expectedSeconds" is 42
    When the currently-active standard for task type "PICK" is requested
    Then the standard response reports task type "PICK" and expected seconds 45

  @bdd
  Scenario: A site-scoped standard for the same task type does not change a score
    Given a standard of 45 expected seconds is already defined for task type "PICK"
    And a standard definition with the body '{"taskType":"PICK","expectedSeconds":10,"siteCode":"SITE-GRU-01"}' is sent
    And associate "assoc-site" completed a PICK task "t-site" in 60 seconds
    When the scorecard for associate "assoc-site" is requested
    Then the scorecard reports 1 task and a mean efficiency of 75.00 percent

  @bdd
  Scenario Outline: A site code that is too long or contains whitespace is rejected
    When a standard definition with the body '{"taskType":"PICK","expectedSeconds":40,"siteCode":"<siteCode>"}' is sent
    Then the request is rejected with status 400
    And the problem type is "invalid-site-code"
    And 0 integration "LaborStandardDefined" messages were encoded

    Examples:
      | siteCode                          |
      | SITE GRU                          |
      | SITE-GRU-01-SITE-GRU-01-SITE-GRU1 |

  @bdd
  Scenario: Legacy unchanged: a site-less standard behaves and serialises exactly as today
    When a standard of 45 expected seconds is defined for task type "PICK"
    Then the request is accepted with status 201
    And the response has no path "siteCode"
    And the analytics "LaborStandardDefined" message data has no field "site_code"
    And the integration "LaborStandardDefined" message data has no field "site_code"
    And the analytics "LaborStandardDefined" message is on topic "warehouse.labor-performance.analytics" with key "PICK" and subject "std-1"
    And the analytics "LaborStandardDefined" message has type "com.warehouse.wes.labor-performance.standard.LaborStandardDefined" and data schema "urn:warehouse:labor-performance:analytics:LaborStandardDefined:v1"
    When the currently-active standard for task type "PICK" is requested
    Then the standard response reports task type "PICK" and expected seconds 45
    And the response has no path "siteCode"
    And associate "assoc-legacy" completed a PICK task "t-legacy" in 60 seconds
    When the scorecard for associate "assoc-legacy" is requested
    Then the scorecard reports 1 task and a mean efficiency of 75.00 percent
