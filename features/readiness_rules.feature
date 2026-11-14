# Readiness is not liveness: the drain gate, the dependency checks and the cache.
#
# Derived from the documentation pack (warehouse-docs, labor-performance),
# troubleshooting.md "Boot and readiness":
#   - "/readyz returns 503 dependency-unavailable while /healthz is 200:
#      Postgres did not answer a ping or no Kafka broker answered a metadata
#      request. The problem detail names the dependency. The verdict is cached 3s
#      ... Liveness is unaffected, so nothing restarts"
#   - "/readyz returns 503 {"status":"not_ready"}: The pod received SIGTERM and
#      is draining ... This is the only way readiness flips"
#   - Problem types: dependency-unavailable, 503, raised by /readyz
#
# The Postgres and Kafka probes are controllable doubles behind the real
# Readiness gate and router.

Feature: Readiness, liveness and graceful shutdown
  As a platform operator
  I want a dependency outage to take the pod out of rotation without restarting it
  So that accepted writes wait in the outbox instead of being lost to a restart

  Background:
    Given the Labor Performance service is running

  @bdd
  Scenario: A pod whose dependencies answer is ready
    When the readiness probe is requested
    Then the request is accepted with status 200
    And the response status field is "ready"

  @bdd
  Scenario Outline: A dependency that stops answering makes the pod not ready, while liveness stays green
    Given <dependency> stops answering
    When the readiness probe is requested
    Then the request is rejected with status 503
    And the problem type is "dependency-unavailable"
    And the response is an RFC 7807 problem
    And the problem detail mentions "<name>"
    When the liveness probe is requested
    Then the request is accepted with status 200

    Examples:
      | dependency | name     |
      | Postgres   | postgres |
      | Kafka      | kafka    |

  @bdd
  Scenario: Every failing dependency is named in the problem detail
    Given Postgres stops answering
    And Kafka stops answering
    When the readiness probe is requested
    Then the problem type is "dependency-unavailable"
    And the problem detail mentions "postgres"
    And the problem detail mentions "kafka"

  @bdd
  Scenario: A pod that is draining is not ready even though its dependencies are healthy, and stays alive
    Given the pod receives SIGTERM and starts draining
    When the readiness probe is requested
    Then the request is rejected with status 503
    And the response status field is "not_ready"
    When the liveness probe is requested
    Then the request is accepted with status 200

  @bdd
  Scenario: Draining is answered before any dependency is consulted
    Given Postgres stops answering
    And the pod receives SIGTERM and starts draining
    When the readiness probe is requested
    Then the request is rejected with status 503
    And the response status field is "not_ready"
    And the dependency checks ran 0 times

  @bdd
  Scenario: The dependency verdict is cached so probes cannot hammer Postgres or Kafka
    Given Postgres stops answering
    When the readiness probe is requested
    Then the request is rejected with status 503
    Given Postgres answers again
    When the readiness probe is requested
    Then the request is rejected with status 503
    And the dependency checks ran 1 time

  @bdd
  Scenario: Once the verdict has expired a recovered dependency makes the pod ready again
    Given the readiness verdict is not cached
    And Postgres stops answering
    When the readiness probe is requested
    Then the request is rejected with status 503
    Given Postgres answers again
    When the readiness probe is requested
    Then the request is accepted with status 200
    And the response status field is "ready"
    And the dependency checks ran 2 times
