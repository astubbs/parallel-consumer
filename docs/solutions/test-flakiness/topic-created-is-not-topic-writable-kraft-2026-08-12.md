---
title: A created topic is not a writable topic - the leaderless window that expires an idempotent producer
date: 2026-08-12
category: test-flakiness
module: parallel-consumer-core
problem_type: flaky_test
component: integration_tests
severity: high
root_cause: missing_readiness_wait
resolution_type: strengthen_shared_helper
applies_when:
  - A test creates a topic and immediately produces to it
  - Producer futures fail with "Expiring N record(s) ... has passed since batch creation"
  - Broker logs show NOT_LEADER_OR_FOLLOWER shortly after a topic is created
  - Changing the broker image, broker version, or ZooKeeper-to-KRaft mode
symptoms:
  - "org.apache.kafka.common.errors.TimeoutException: Expiring 14 record(s) for <topic>-1:120001 ms has passed since batch creation"
  - "Got error produce response ... Error: NOT_LEADER_OR_FOLLOWER immediately after createTopics returned"
  - "A long run of Error: OUT_OF_ORDER_SEQUENCE_NUMBER that never recovers"
  - "Test fails after exactly delivery.timeout.ms, not at the assertion it was written to check"
tags:
  - flaky-test
  - integration-tests
  - testcontainers
  - kafka
  - kraft
  - topic-creation
  - idempotent-producer
---

# A created topic is not a writable topic

## Context

Moving the integration broker from `confluentinc/cp-kafka` to `apache/kafka` (astubbs/parallel-consumer#290)
turned three previously-green integration tests red - `PartitionOrderProcessingTest` (both methods) and
`DrainingMemberRebalanceIT` - all with the same signature and none of them touched by the change:

```
java.util.concurrent.ExecutionException: org.apache.kafka.common.errors.TimeoutException:
  Expiring 14 record(s) for LoadTest-1470224979-1:120001 ms has passed since batch creation
    at ...KafkaClientUtils.produceMessages(KafkaClientUtils.java:382)
```

The test failed after exactly `delivery.timeout.ms`, in a producer call, nowhere near the behaviour it
was written to assert. That shape - a two-minute expiry at a `produceMessages` line - is the tell.

## What it actually was

Reading upward from the expiry, past a thousand lines of retry noise, the first thing the broker ever
said about that topic was:

```
Got error produce response ... on topic-partition LoadTest-1470224979-1 ...
  Error: NOT_LEADER_OR_FOLLOWER
Received invalid metadata error ... Going to request metadata update now
```

then, from that point until the expiry, only:

```
Error: OUT_OF_ORDER_SEQUENCE_NUMBER
```

Two mechanisms chained:

1. **`createTopics(..).all()` completing does not mean the topic is writable.** It resolves when the
   controller has accepted the topic. Electing a leader for each partition, and making that leadership
   known to the broker serving produce requests, happens *afterwards*. Produce inside that window and
   the broker answers `NOT_LEADER_OR_FOLLOWER` - a *retriable* error, which is exactly why it looks
   harmless and why nobody had noticed the helper was missing a wait.

2. **It is not harmless for an idempotent producer**, which is the default for these tests. The
   rejected in-flight batches come back out of order, the producer's sequence numbers desynchronise
   from the broker's, and it spins on `OUT_OF_ORDER_SEQUENCE_NUMBER` until `delivery.timeout.ms`
   expires. The retriable error never surfaces; the timeout two minutes later does.

The window exists under both ZooKeeper and KRaft, but is far wider under KRaft - which is why a change
of broker image, touching no test, exposed it.

## The control arm

One term changed, everything else identical: the same full integration lane, same machine, same
commit except for the broker image.

| Arm | Broker | `NOT_LEADER_OR_FOLLOWER` | `OUT_OF_ORDER_SEQUENCE_NUMBER` | Result |
|---|---|---|---|---|
| Control | `confluentinc/cp-kafka:7.9.0` (ZooKeeper) | 0 | 0 | green |
| Before fix | `apache/kafka:3.9.2` (KRaft) | 25 | 1111 | 3 errors |

The counts are the useful part, not the pass/fail: zero occurrences of the trigger in the control arm
is what rules out "a pre-existing flake that happened to show up".

## The fix, and the weaker version of it that was not enough

`KafkaClientUtils#createTopicsBlocking` now waits, before returning, for every partition of every
created topic to **answer a request only its leader can serve** (`listOffsets`), so the helper delivers
what its own javadoc already promised - a topic that is ready, not merely accepted.

The first attempt asked `describeTopics` whether each partition had a leader assigned. That is the
obvious check, and it is not sufficient: the broker learns the assignment from the metadata log and
will report it before it has finished transitioning the local replica into the leader role. Measured
rather than reasoned about - it cut the trigger from **25 occurrences per suite run to 2**, not to
zero. Had the suite gone green on that run, the weaker check would have shipped and the flake would
have come back under load.

**Fixed in the shared helper, not in the tests that lost the race.** Any test that creates a topic and
produces to it can lose it; which ones actually do is a matter of machine load, so patching the three
that failed would have moved the flake rather than removed it.

## Traps

- **The stack trace names the wrong thing.** It points at `produceMessages`, so the instinct is to look
  at production rate, buffer sizes or the assertion's timeout. None of those are involved. Search the
  log for the *first* broker error on that topic-partition instead of the last.
- **A retriable error is not a harmless error.** `NOT_LEADER_OR_FOLLOWER` is designed to be retried,
  and the client does retry it - the damage is what the retry does to idempotent sequencing, which
  shows up under a completely different error code.
- **Do not raise `delivery.timeout.ms` to make this pass.** The producer is not slow, it is stuck; a
  larger bound buys a longer wait for the same failure.
