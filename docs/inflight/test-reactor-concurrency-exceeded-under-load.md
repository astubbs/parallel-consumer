# `ReactorPCTest.concurrencyTest` exceeds max concurrency on a saturated machine

Sighting recorded 2026-08-22, on `feats/polyglot-demos`, running locally. **Diagnosed as contention,
with a control arm - not quarantined, and not a known flake before this.** Recorded because it had
no entry anywhere, and the next person to hit it would repeat the whole investigation.

## The failure

```
[ERROR] bz.stub.parallelconsumer.reactor.ReactorPCTest.concurrencyTest
Max concurrency should never be exceeded
expected to be less than: 1200
but was                 : 1266
```

The threshold is `maxConcurrency * MAX_CONCURRENCY_OVERFLOW_ALLOWANCE`, i.e. an allowance that
already exists to absorb scheduling slop. The failing run overshot what the allowance absorbs.

**Conditions:** the full Maven reactor was running while **two Docker image builds** (C++ and Swift
demo containers) compiled on the same 12-core box. That is well past the load anything here is meant
to be measured under.

## The control arm, and why it was not skipped

Both arms uncontended, **run sequentially** - running them together would reintroduce the variable
under test - five runs each, same host, same JDK 17:

| arm | result | observed max concurrency |
|---|---|---|
| `feats/polyglot-demos` | **5 pass / 0 fail** | 1000, 1073, 1000, 1062, 1000 |
| `master` (control) | **5 pass / 0 fail** | 1000, 1000, 1076, 1000, 1000 |

Indistinguishable. The branch does not run hotter than master, and neither approaches 1200 without
load. **Contention, not a regression** - and 0/5 uncontended against 1/1 under saturation is the
distinction, not a verdict from a single run either way.

## Why a control was warranted rather than an assumption

This branch's stack **does** modify the classes that bound concurrency - `ShardManager`,
`WorkManager`, `ProcessingShard`, `WorkContainer` - so "it was just load" was a hypothesis, not an
observation. The change adds the **abandonment path** the language proxy needs: work returned with
no verdict becomes selectable again without earning a retry delay it never earned.

There is even a plausible mechanism from that change to this symptom, which is why it had to be ruled
out rather than waved away: `WorkManager.onAbandonedResult` decrements
`numberRecordsOutForProcessing`, and a **double** decrement would under-count in-flight work, let
extra records out, and surface as observed concurrency exceeding the maximum. The author guarded
exactly that - an early return for `isReturnForSupersededDelivery()`, placed before a branch that
decrements unconditionally, with a comment saying why.

**It is unreachable from this test**, which is what closes the question: nothing in
`parallel-consumer-core` or `parallel-consumer-reactor` calls the abandon path, and
`isReturnForSupersededDelivery()` requires `abandonedAtDelivery >= 0`, which only a proxy return
sets. The new branches are inert here. The matching master baseline is the empirical half of the
same answer.

## Prior art: none, and that is the point of this file

Checked before forming a hypothesis, and all five returned nothing for this test:
`docs/inflight/`, `docs/quarantined-tests.md`, `docs/solutions/`, `docs/plans/`,
`docs/test-hardening/`. The single grep hit was a Streams benchmarking document that happens to
contain the phrase "max concurrency" - unrelated. `git log` shows the test itself last changed in
the package rename (content-only) and before that in upstream work from 2022.

## What NOT to do

**Do not raise `MAX_CONCURRENCY_OVERFLOW_ALLOWANCE`, and do not quarantine this.** Quarantine
requires a diagnosis and is master-state, not PR-state; this is neither failing on master nor
failing uncontended. Loosening the allowance would remove the only signal that the concurrency bound
is real, in the library whose entire purpose is bounded concurrency.

**If it is seen again, capture the load average with it.** The one thing this sighting cannot
supply is the threshold at which it flips, and that is the number that would decide whether the
suite needs a contention guard rather than the test needing a change.
