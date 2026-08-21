# Test: three opt-in execution paths, and nothing in CI runs any of them

<!-- inflight-type: test -->
<!-- inflight-impact: correctness -->
<!-- inflight-labels: needs-measurement -->

Opened 2026-08-22. **This is one structural gap, not three separate missing test suites**, and naming
it that way is what makes it fixable.

## The pattern

Parallel Consumer is growing execution paths selected by a flag or a runtime, and **the default test
run exercises none of them:**

| Path | Selected by | Exercised by the default suite? | Exercised by CI? |
|---|---|---|---|
| **Direct pull** | `-Dpc.directPull=true` | **no** | **no** |
| **Virtual threads** (astubbs#51, in progress) | `useVirtualThreads` + a JDK 21 runtime | no - and its tests **silently skip** on the JDK 17 CI runs | **no** |
| **Async user function** ([`next-core-async-user-function.md`](next-core-async-user-function.md)) | a different entry point | not yet built | not yet |

**Direct pull today is roughly 500 lines of engine code that no test runs by default and no CI job runs
at all.** It was measured, not covered - and the measurement was a one-off on a laptop. It will rot,
and nothing will say so.

**Virtual threads is about to arrive in the same shape**, and worse: a test that skips on JDK 17 and
*passes* on JDK 17 reports green having verified nothing. That is a failure mode this repository has
already shipped once - see `docs/ci.md` on silent-green defects, and
[`docs/client-static-analysis.md`](../client-static-analysis.md), where a whole conformance suite went
dark behind a skip.

## Why per-path test suites are the wrong answer

The obvious response is "write direct-pull tests". **That scales badly and misses the point:** these
paths are meant to be *behaviourally equivalent* to the default. What needs asserting is not that
direct pull works, but that **direct pull and the shipped engine agree** - on ordering, on commits, on
retries, on rebalance, on shutdown. That is the existing suite, run again with a different selector.

**So the fix is a CI matrix dimension, not new tests.** One axis - execution mode - with the existing
core suite behind it.

## What the direct-pull measurement already tells us to expect

Running the 369-test suite with `-Dpc.directPull=true` gave **three failures, none of them a
correctness violation**: two assertions encode the *pre-loaded-queue* design (pause becomes exact;
dispatch granularity changes), one was load-only.

**Those two are the interesting output.** They are tests that assert an implementation detail of the
current engine rather than a behaviour a user depends on - and they are invisible while only one engine
exists. A matrix run turns them from noise into a list of assertions worth loosening or splitting.

## What to build

1. **A `execution-mode` axis on the unit lane**: default, `-Dpc.directPull=true`, and (once astubbs#51
   lands) virtual threads on a JDK 21 runner.
2. **Make skips loud.** A skipped VT test must be visibly skipped in the job summary, not silently
   absent. The repository already has the rule; it needs applying here.
3. **Triage the two design-coupled assertions** rather than tolerating them - each is either a
   behaviour worth keeping for both engines, or an implementation detail that should not have been
   asserted.

## Sequencing

**The JDK 21 lane is already in scope for the virtual-threads work.** Whatever lane mechanism that
produces should be the one direct pull reuses - building two is how the axis ends up with two
incompatible shapes. **Coordinate rather than duplicate.**
