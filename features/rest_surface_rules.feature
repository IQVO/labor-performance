# The REST surface: what a browser may call and how a bad window degrades.
#
# Derived from the documentation pack (warehouse-docs, labor-performance),
# troubleshooting.md "REST callers":
#   - "Browser call blocked by CORS: The origin is not in CORS_ALLOWED_ORIGINS.
#      The OLTP API allows only GET/POST, and the reports API only GET."
#   - "?window=90 returns a 1h window: window is a Go duration (90m, 2h). A bad
#      value silently falls back to the 1h default instead of returning 400."
#
# The built-in origin allow-list (the two local console origins) is asserted;
# the runner clears CORS_ALLOWED_ORIGINS so an operator's setting cannot change it.

Feature: What a browser may call and how a malformed window degrades
  As a console developer
  I want the cross-origin surface to match the documented method and origin lists
  So that a blocked call is explained by the docs and never by a surprise

  Background:
    Given the Labor Performance service is running

  @bdd
  Scenario Outline: The OLTP API answers a browser preflight only for GET and POST from an allowed origin
    When a browser preflight from origin "<origin>" asks to <method> "<path>" on the OLTP API
    Then the browser is <verdict> to call from origin "<origin>"

    Examples:
      | origin                | method | path             | verdict |
      | http://localhost:5173 | POST   | /standards       | allowed |
      | http://localhost:5187 | GET    | /standards/PICK  | allowed |
      | http://localhost:5173 | PUT    | /standards       | refused |
      | http://localhost:5173 | DELETE | /standards/PICK  | refused |
      | http://evil.example   | POST   | /standards       | refused |

  @bdd
  Scenario Outline: The reports API answers a browser preflight only for GET
    When a browser preflight from origin "http://localhost:5173" asks to <method> "/reports/performance" on the reports API
    Then the browser is <verdict> to call from origin "http://localhost:5173"

    Examples:
      | method | verdict |
      | GET    | allowed |
      | POST   | refused |

  @bdd
  Scenario: A window written without a unit falls back to one hour instead of being refused
    When the fleet-wide utilization for task type "PICK" is requested with window "90"
    Then the request is accepted with status 200
    And the utilization response reports a 3600 second window
