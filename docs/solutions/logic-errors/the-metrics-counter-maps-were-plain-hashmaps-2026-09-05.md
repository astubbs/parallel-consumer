---
title: "The metrics counter maps were plain HashMaps - three fixed by a sweep, and the fourth it could not see"
date: 2026-09-05
category: logic-errors
module: parallel-consumer-core
problem_type: race_condition
component: metrics
root_cause: check_then_act_on_unsynchronised_collection
resolution_type: code_fix
severity: low
symptoms:
  - "A Lincheck harness aimed at commit-path torn reads threw ArrayIndexOutOfBoundsException out of a metrics collection, with no stack recorded"
  - "A counter map populated with get-then-put registers the same meter twice when two callers interleave, and one of the two puts is discarded"
  - "The losing thread's entry never appears, so every later use of that key pays a fresh meter registration under a contended monitor"
applies_when:
  - Sweeping a defect class across a codebase and one instance sits on a different call path from the rest
  - A lazily populated cache's miss handler has a side effect, so the check and the act must be one step
  - Deciding whether to fix a race that today's scheduler makes unreachable
  - An inflight note records a defect that a later PR silently fixed, and nothing closed the note
tags:
  - thread-safety
  - check-then-act
  - concurrent-hashmap
  - metrics
  - micrometer
  - sweep-completeness
  - latent-defect
  - lincheck
related_components:
  - OffsetMapCodecManager
  - WorkManager
  - PartitionStateManager
  - PCMetrics
---

# The metrics counter maps were plain HashMaps - three fixed by a sweep, and the fourth it could not see

## Context

The torn-read hunt of 2026-08-24 and the Lincheck proof of concept
([`docs/plans/2026-08-25-001-test-lincheck-poc-plan.md`](../../plans/2026-08-25-001-test-lincheck-poc-plan.md))
between them produced a finding nobody had pointed either at: the metrics collections reached from
PC's two threads were unsynchronised. astubbs#57 closed `PCMetrics.registeredMeters` by making it a
`LinkedHashSet` behind a private `metersLock`. What it explicitly did not close became two inflight
notes, and this document is where their evidence now lives.

The Lincheck sighting deserves stating precisely, because its value is not what it caught. The
harness was aimed at commit-path torn reads and reported, unprompted, an
`ArrayIndexOutOfBoundsException` out of a metrics collection during two concurrent rebalance
callbacks. **That scenario is not reachable in production** - `ConsumerRebalanceListener` callbacks
are invoked by the consumer from inside one `poll`, so two rebalances never overlap - and **the
report carried no stack, so which collection tore was never established**. It is recorded as an
independent demonstration that these collections were unguarded, not as a reproduction of a live
defect. The sibling sighting from the same harness, two concurrent `createOffsetAndMetadata` calls
tearing `registeredMeters`, is the one on a path that genuinely runs on two threads.

The command that enumerates the family, so no list has to be trusted:

```bash
grep -rnE 'private (final )?(List|Map|Set|HashMap|ArrayList|HashSet)<' \
  parallel-consumer-core/src/main/java --include='*.java' | grep -iE 'meter|metric|counter|gauge'
```

## Guidance

### A sweep by path misses the instance on a different path

The four fields the grep names split two-and-two by *where they are mutated*, and that is what
decided which of them got fixed and when.

Three - `PartitionStateManager.slowWorkCounters`, and `WorkManager.succeededRecordsCounters` and
`failedRecordsCounters` - are registered and deregistered from the rebalance callbacks. astubbs#267
made all three `ConcurrentHashMap` and replaced their `containsKey`-then-`put` pairs with
`computeIfAbsent`, and it did so as part of a broader shared-collection sweep across the poll
boundary, so it found them together.

The fourth - `OffsetMapCodecManager.encodingCounters` - is on the **encode path**, not the rebalance
callbacks, and survived that sweep by a year. It was found later by grepping the class of field
rather than the class of caller, which is the transferable part: **a sweep organised around a call
path terminates at the boundary of that path, and the instance one step outside it looks like a
different problem.** The grep above is organised around the *shape of the field* and does not have
that blind spot.

The second-order cost is worth naming too. Two inflight notes tracked those three fields, and
astubbs#267 fixed them without touching either note - so for a week the repository asserted a defect
its own code no longer had, in two places, and the working-tree grep that would have shown it was
never re-run. **A note that names specific fields is falsifiable by one command; run it before
believing the note.**

### A cache whose miss handler has a side effect is a check-then-act, not a cache

`encodingCounters` reads as an ordinary memo:

```java
Counter counter = encodingCounters.get(encoding);
if (counter == null) {
    counter = pcMetrics.getCounterFromMetricDef(PCMetricsDef.OFFSETS_ENCODING_USAGE,
            Tag.of("encoding", encoding.name()));
    encodingCounters.put(encoding, counter);
}
return counter;
```

The miss handler *registers a meter*, so this is not a cache lookup with a slow path; it is a
check-then-act whose act is externally visible. Two callers inside the window both miss, both
register, and one of the two `put`s is discarded. The discarded entry never appears, so every later
encode of that encoding pays a fresh registration - which re-enters `PCMetrics.track` under
`metersLock`, the monitor `close()` and every rebalance's meter registration also contend for. A hot
path acquiring a contended global monitor per commit is the cost, not a wrong counter value:
micrometer's registry dedupes by meter id, so the *reported* number is right either way.

`computeIfAbsent` on a `ConcurrentHashMap` is the whole fix, and it is what astubbs#267 used on the
other three:

```java
return encodingCounters.computeIfAbsent(encoding, enc ->
        pcMetrics.getCounterFromMetricDef(PCMetricsDef.OFFSETS_ENCODING_USAGE,
                Tag.of("encoding", enc.name())));
```

**Two arguments that look right here and are not.** The first is table corruption on resize: there
are twelve `OffsetEncoding` constants and a default `HashMap` resizes only above twelve entries, so
this map never resized and never could. The second is `ConcurrentModificationException`: nothing
iterates any of the four. Reaching for either would have made the write-up wrong in a way no test
would catch, and both are recorded on the field so nobody re-derives them.

### Fixing a race the scheduler currently makes unreachable

Nothing interleaves at `encodingCounters` today, and establishing that took reading callers rather
than javadoc - the method
[`a query must never mutate`](../architecture-patterns/a-query-must-never-mutate-derive-thread-safety-from-callers.md)
owns. Two facts decide it, and neither is stated where the suspicion is formed:

- `AbstractParallelEoSStreamProcessor.tryCommitOffsetsOnRevoke` takes `commitLock` with `tryLock` and
  **declines** rather than blocking, so the broker-poll thread's revoke commit and the control
  thread's commit are mutually exclusive (that decline is confluentinc#857's fix, astubbs#29).
- `ConsumerOffsetCommitter.commit` routes a non-owner caller through the request queue instead of
  encoding on the calling thread, so in consumer commit modes the encode always happens on the owner
  thread.

One encoder at a time, per instance, in every commit mode. `docs/refactoring.md` had recorded that
conclusion already - as "not a bug in the current single-threaded design" - while naming the wrong
thread for it, which is its own small lesson about conclusions recorded without the discriminator
that produced them.

It was fixed anyway, and the reason is the rule the architecture-patterns document states: **whether
two threads overlap is a property of the scheduler, which is free to change and will not tell you;
whether your method is atomic is a property of your own code. Prefer the guarantee you own.**
confluentinc#233 (split encode from decode) and confluentinc#200 (parallelise encoding) both make
this reachable, and no gate in the repository reasons about which thread arrives at a field.

### The red control for a defect the scheduler will not reproduce

A fix for a latent race cannot be validated by the scenario that found the class, and starting two
threads and hoping is worse than nothing: on this path the second thread loses the race almost
always, so such a test is green on the broken code and reads as a pass.

`EncodingCounterRegistrationIsAtomicTest` drives the interleaving instead, through the one seam the
miss handler crosses. A `PCMetrics` subclass counts registrations of `OFFSETS_ENCODING_USAGE` and
parks the *first* caller inside that window until a second arrives in it:

- against the pre-fix tree, the second encoder reaches registration in milliseconds and the count is
  **2** - `expected: 1 but was: 2`, in under a second, on every run;
- against the fix, the second encoder blocks inside `computeIfAbsent` until the first has published
  its entry, so the rendezvous times out **by design**, the mapping function is never called twice,
  and the count is **1**.

The rendezvous timeout is a liveness bound on the harness, not a loosened deadline on the assertion,
which stays exact in both arms. **Both arms were run: the fix was stashed to produce the red one,
because a green test against a fixed tree establishes nothing about what it would have caught.**

## Why This Matters

The interesting failure here is not the race; on today's scheduler it cannot fire. It is that a
defect class was swept, three of its four instances were fixed, and the fourth stayed open for a
year with two notes describing it - while the notes for the three that *were* fixed also stayed open,
describing code that no longer existed. Both halves of that are the same mistake seen from opposite
sides: **the record and the code were never re-derived from each other**, and each command that would
have done it takes seconds.

The other durable piece is the argument for fixing an unreachable race. It is easy to read
"unreachable today" as "not a bug" and close it - `docs/refactoring.md` very nearly did - and easy to
read the Lincheck sighting as a live reproduction and overstate it, which the note it came from was
careful not to do. Neither is right. The honest position is that the atomicity of a compound
operation is a property you own and can assert on, the thread schedule is not, and a one-line change
buys you the first in exchange for nothing.

## When to Apply

- When a fix sweeps a defect class: enumerate by the **shape of the code**, not by the call path you
  arrived on, and re-run that enumeration at merge prep rather than trusting the list you started
  with.
- When a lazily populated map's miss handler does anything besides construct a value - register,
  publish, allocate an id, write a file. That is a check-then-act, and no choice of thread-safe map
  fixes it; only making the check and the act one operation does.
- Before writing "not a bug in the current design" about a race. Say which fact makes it unreachable
  and where that fact lives, or the next reader inherits a conclusion with no discriminator behind
  it - and cannot tell whether the code has since moved out from under it.
- When a test for a race would depend on winning one. Find the seam that opens the window
  deterministically, and run both arms.
- When your PR resolves what an inflight note tracks. The note stops describing reality at that
  moment; a PR that fixes the code and leaves the note is how the repository ends up asserting
  defects it does not have.

## Related

- [A query must never mutate - derive a thread-safety contract from callers, not javadoc](../architecture-patterns/a-query-must-never-mutate-derive-thread-safety-from-callers.md) -
  owns the caller-enumeration method used to establish that nothing interleaves here, and the
  "prefer the guarantee you own" rule that is why it was fixed regardless.
- [PCMetrics leaked a Meter.Id per registration, and closing it during a failing shutdown orphaned one more](../performance-issues/pcmetrics-meter-registration-leak-2026-08-07.md) -
  the sibling field, `registeredMeters`, and the `metersLock` idiom astubbs#57 introduced for it.
- [A throwing meter registry kills the poll thread and strands close](../runtime-errors/a-throwing-meter-registry-kills-the-poll-thread-and-strands-close.md) -
  why the metrics subsystem is held to a do-not-break-consuming contract at all.
- [`docs/plans/2026-08-25-001-test-lincheck-poc-plan.md`](../../plans/2026-08-25-001-test-lincheck-poc-plan.md) -
  the proof of concept whose harness produced the unprompted sighting, and what its stress and
  model-checking arms each cost.
- [`docs/inflight/bug-torn-read-family.md`](../../inflight/bug-torn-read-family.md) - the hunt this
  finding came out of, as an out-of-family straggler.
- astubbs#267 fixed the three rebalance-path fields; astubbs#57 fixed `registeredMeters`.
