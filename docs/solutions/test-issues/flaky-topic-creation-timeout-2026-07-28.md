---
title: Flaky integration tests from a duplicated topic-creation timeout
date: 2026-07-28
category: test-issues
module: parallel-consumer-core
problem_type: flaky_test
component: integration_tests
severity: high
root_cause: duplicated_logic_drift
resolution_type: consolidation
applies_when:
  - Integration tests fail intermittently on topic/broker setup
  - Seeing TimeoutException from BrokerIntegrationTest.ensureTopic
  - Adding a new test helper that creates topics, produces, or builds clients
  - A CI check is flaky and blocks otherwise-green PRs
symptoms:
  - "java.util.concurrent.TimeoutException at BrokerIntegrationTest.ensureTopic"
  - "Integration Tests check fails on PRs that change no production code"
  - "Re-running the job makes it pass (classic flake)"
tags:
  - flaky-test
  - integration-tests
  - testcontainers
  - kafka
  - dry
  - topic-creation
---

# Flaky integration tests from a duplicated topic-creation timeout

## Context

The required `Integration Tests` CI check failed intermittently on PRs that changed **no production code** (e.g. docs-only PRs astubbs#56 and astubbs#61). The failure was always the same:

```
java.lang.RuntimeException: java.util.concurrent.TimeoutException
    at ...integrationTests.BrokerIntegrationTest.ensureTopic(BrokerIntegrationTest.java:153)
```

Because it's a *required* gate, every flake blocked an otherwise-green PR and forced a manual re-run.

## Root cause

There were **two** implementations of "create a Kafka topic and wait for the broker to confirm it":

- `KafkaClientUtils.createTopics(int)` — waited with an **unbounded** `admin.createTopics(...).all().get()`. Robust.
- `BrokerIntegrationTest.ensureTopic(String, int)` — a near-duplicate that waited with `...all().get(1, TimeUnit.SECONDS)`. On a cold or loaded CI broker, topic creation regularly took **longer than 1 second**, so the `TimeoutException` fell through to a generic `catch (Exception)` and was rethrown as a hard failure.

The two had drifted: same operation, one robust, one with a too-tight timeout. The duplicate also swallowed **all** `ExecutionException`s with a `// fine` comment, so it couldn't distinguish "topic already exists" from a real broker error.

## Resolution

Consolidate onto a single blocking helper in the canonical util and delegate:

- `KafkaClientUtils.createTopic(String name, int numPartitions)` + a shared private `createTopicsBlocking(List<NewTopic>)` that waits with a generous 60s bound (vs the flaky 1s) and throws a clear timeout message if the broker is genuinely unresponsive — bounded so a dead broker fails fast rather than hanging until the CI timeout. Tolerates only `TopicExistsException` (idempotent "ensure"); any other failure propagates instead of being silently swallowed.
- `BrokerIntegrationTest.ensureTopic(...)` now just calls `kcu.createTopic(...)`.
- `KafkaClientUtils.createTopics(int)` routes through the same shared method too — one implementation, no drift.

## Why this matters

The flake was not a broker/infra problem to paper over with retries — it was **duplicated logic that drifted**. Fixing the cause (one helper, robust wait) removes the whole class of failure. Reaching for `rerunFailingTestsCount` would have hidden it instead.

## When to apply

- Before writing a new topic-creation / producer / consumer helper in a test, search `KafkaClientUtils` and `BrokerIntegrationTest` and extend them — do not copy. See AGENTS.md "Testing".
- When a test flakes on setup, check whether the setup path duplicates a robust shared helper before adding waits/retries.

## Related

- `parallel-consumer-core/src/test-integration/java/io/confluent/parallelconsumer/integrationTests/utils/KafkaClientUtils.java` — `createTopic`, `createTopicsBlocking`, `createTopics`
- `parallel-consumer-core/src/test-integration/java/io/confluent/parallelconsumer/integrationTests/BrokerIntegrationTest.java` — `ensureTopic` (now delegates)
- `docs/inflight.md` — "CI reliability / gate issues" (this was the top-priority flaky *required* gate)
