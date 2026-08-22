---
title: "Benchmark a rival on the semantics it actually offers, and measure what the win costs somewhere else"
date: 2026-08-22
category: best-practices
problem_type: best_practice
component: testing_framework
severity: high
applies_when:
  - "A benchmark compares this project against a system that makes weaker guarantees"
  - "A rival wins on throughput and the instinct is to find a configuration where it does not"
  - "The comparison's headline is a single ratio taken at one operating point"
  - "Two systems move the same bookkeeping to different places - client versus broker, memory versus disk"
tags:
  - benchmarking
  - kafka-share-groups
  - methodology
  - measurement-honesty
---

# Benchmark a rival on the semantics it actually offers

Measuring Kafka's KIP-932 Share Groups against Parallel Consumer on 2026-08-22 produced a result that
went against this project - a bare `KafkaShareConsumer` ran **2.5x** PC's best arm. Four method
decisions made that number trustworthy, and each was a decision that could easily have gone the other
way.

## 1. Compare on matched semantics, or the table is rigged

Share Groups have **no ordering guarantee at all**. Parallel Consumer's `KEY` mode does.

A table putting them side by side compares a system that orders against one that does not, and
flatters the weaker guarantee for doing strictly less work. **The comparison was restricted to
`UNORDERED`** - the mode where both deliver records to whichever worker is free - and the ordering
mode was reported as **a capability the rival lacks, not a row it wins**.

**The general rule: pick the arm where your guarantees match theirs, and describe the rest as
capability rather than as performance.** A benchmark that quietly compares different contracts is not
a benchmark.

## 2. The winner's operating point is not the only one

The 2.5x held at a 2ms handler. At 100ms **the result inverted** and PC won by 14.5%.

The mechanism was predictable in advance and was predicted: neither acknowledgement mode may poll
while records are unacknowledged, so a share consumer's outstanding work is capped at one batch
(measured: 2,606) while PC held 5,000. Throughput at a long delay is in-flight over delay, so the
smaller ceiling loses as soon as work takes real time.

**A single-point ratio is a claim about that point.** Both rows were published together with an
explicit instruction that **neither may be quoted alone**.

## 3. Measure what the win costs somewhere the consumer cannot see

Share Groups acknowledge per record into the broker's state topic; PC batches the same information
into one encoded commit. **A consumer-side msg/s figure cannot see that at all.**

Differencing the broker container's cgroup CPU across a run, against a **fetch-only control arm that
commits nothing**, isolated acknowledgement cost from fetch cost: **~48x per record**, ~5x total
broker CPU.

**When two designs move the same bookkeeping to different places, a measurement taken in only one
place is not a comparison.** The control arm is what makes the difference attributable - without an
arm that commits nothing, both numbers include fetch and neither means anything.

## 4. Trust nothing measured on shared infrastructure without checking

An early conclusion - "the broker upgrade made PC 20% faster" - was **repeatable across five rounds
and wrong**. `ps -Ao pcpu,etime,args -r | grep '[B]ench '` showed another session benchmarking
against the shared broker while the new one was private. Re-run with a private broker on both sides:
every arm within 2.6%, sign inconsistent.

**Repeatability is not validity.** A confound that is present for every repeat reproduces perfectly.

Two related traps from the same evening, both silent:

- **An arm scored 17,221 msg/s on failed requests.** It issued its own HTTP call, and the run had no
  server. Only `peak_in_flight = 0` gave it away.
- **`LOCAL` names a Maven coordinate, not a build.** Another session's `mvn install` replaced the jar
  under a running sweep twice. Guarded now by a per-cell `cksum`.

## The outcome worth copying

The result was published **losing half first**, and the project's strategy document was corrected in
the same change: its claim that per-record overhead "should be lower" was split into **false
consumer-side, true broker-side**.

**A benchmark that can only confirm what you already believe is not an instrument.** The one that
changed a strategy claim is worth more than the twenty that agreed with it.
