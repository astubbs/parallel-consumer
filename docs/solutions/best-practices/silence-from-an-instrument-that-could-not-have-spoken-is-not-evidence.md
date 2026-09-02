---
title: "Silence from an instrument that could not have spoken is not evidence"
date: 2026-09-01
category: best-practices
module: parallel-consumer-core
problem_type: best_practice
component: testing_framework
severity: high
applies_when:
  - "A bounded detector's threshold exceeds the enclosing test's own timeout or deadline"
  - "A grep for a diagnostic log line returns zero, and that zero is read as proof the code path never executed"
  - "A logger level pins a module below the level a diagnostic was written at (info/debug lines under a warn-pinned profile)"
  - "A newly added detector has never been demonstrated firing on a tree deliberately built to fail it"
  - "About to write 'probe clean' or 'no evidence' into a plan, PR body, or diagnosis on the strength of a quiet detector"
tags:
  - false-negative
  - instrumentation
  - log-levels
  - detector-calibration
  - silent-failure
  - test-timeouts
  - chaos-testing
---

# Silence from an instrument that could not have spoken is not evidence

## Context

While diagnosing the astubbs#29 / confluentinc#857 paused-consumption stall, three unrelated
mechanisms each produced the same shape of false negative: an instrument reported nothing wrong, in
a way that was structurally indistinguishable from a genuine all-clear. All three surfaced within
days of each other, on the same investigation, and each one steered the diagnosis away from the real
defect for a period measured in weeks, not minutes.

1. **A bound longer than the enclosing timeout.** The chaos suite's `ProgressProbe` calibrates
   `INSTANCE_STALL_BOUND` and `LAG_STAGNATION_BOUND` at 150 seconds each, and `NO_PROGRESS_WINDOW`
   at 30 seconds - all tuned against chaos runs that last minutes
   (`parallel-consumer-core/src/test-integration/java/bz/stub/parallelconsumer/integrationTests/chaostests/ProgressProbe.java`,
   `NO_PROGRESS_WINDOW = Duration.ofSeconds(30)`, `LAG_STAGNATION_BOUND = Duration.ofSeconds(150)`,
   `INSTANCE_STALL_BOUND = Duration.ofSeconds(150)`). Run inside a test whose own `@Timeout` is 30
   seconds, none of the 150s-bound detectors can fire, whatever the consumer group is doing. The
   ambient probe's clean verdict - which then read, in full, `"probe clean - no rebalance dwell, no
   lag stagnation, no frozen partitions observed: the fault is likely in the test itself, not
   consumer-group progress"` - printed for weeks about a consumer that had stopped fetching entirely.
   That is the wording BEFORE the counter-measure below; `AmbientProbeExtension` now appends a caveat
   clause to the same sentence, so a reader grepping it today finds a longer one. That verdict was arithmetic, not observation, and it actively pointed the
   diagnosis at "the test is broken" instead of the product.

2. **A level the log profiles filter.** Both `parallel-consumer-core` logback test profiles pin
   `bz.stub.parallelconsumer` to `warn` by default (`logback-test.xml`,
   `<logger name="bz.stub.parallelconsumer" level="${pc.log.level:-warn}"/>`; the integration
   profile pins it to `info` in `logback-integration-test.xml`, still above `debug`). A
   `log.debug` or `log.info` diagnostic under that logger never reaches the log file, and a grep
   returning zero is indistinguishable from the code path never executing. This fired three times in
   one day on this branch: (a) a seed replay against the two `commitLock` revoke-fork log lines in
   `AbstractParallelEoSStreamProcessor` (`log.info("Acquired commitLock on revoke without
   contention...`, paired with `log.info("Skipping offset commit during partition
   revocation...`) could not tell "the race window never opened" from "logging was
   filtered"; (b) an awaitility `catch (ConditionTimeoutException)` block that logged its diagnostic
   counts at `debug` and the missing keys at `info` left a failed CI run showing only the assertion,
   none of the evidence behind it; (c) `docs/investigating.md` itself once recommended asserting an
   instrumentation change reached the run by grepping PC's own options line - which logs at INFO on
   `bz.stub.parallelconsumer`, the exact logger both profiles filter. A silent false negative inside
   the check written to prevent silent false negatives (`docs/investigating.md`, "PC's own options
   line is NOT that assertion, though it reads like the obvious candidate").

3. **A detector never shown to fire.** Two detectors added during this work - a chaos scenario
   reachable only in a newly-added commit mode, and a state-coherence check - had never been
   demonstrated failing on a tree known to be broken. An unarmed detector's silence is not evidence
   of correctness. Both were hedged in their own commit messages and javadoc from the moment they
   landed, which is the habit worth copying: an instrument records that it is unarmed at birth,
   because nobody remembers later.

All three are the same failure in different clothing: **the instrument's silence was read as a
verdict about the system, when it was actually a fact about the instrument.**

## Guidance

### Ask what the instrument could have produced before reading its output as a verdict

Before treating a clean probe, a zero-hit grep, or an unraised assertion as "nothing happened", ask
what value it *could* have produced given how it is wired. If a detector's bound is at or past the
enclosing timeout, it could only ever report clean - so "clean" carries no information. If a logger
is pinned above the level a diagnostic writes at, a grep for that diagnostic can only return zero -
so zero says nothing about whether the code ran. This is the same check `a-timing-bound-used-as-a-
correctness-gate-manufactures-its-own-evidence.md` applies to a *positive* reading (a bound crossing
that could only ever be `bound + detection latency`); here it is applied to a *negative* one (silence
that could only ever be silence).

### State, don't assume, that a detector or logger reached the run

Verifying reach is cheap and mechanical once you know to do it:

- **For a timing detector**, compare its bound against the enclosing timeout (or the scenario's own
  known runtime). If the bound is greater than or equal to the ceiling, the detector could not have
  fired inside this run, no matter what the system under test did.
- **For a log line**, compare the level it's written at against the logger's configured level for
  the run in question - not what you assume the default is, but what the actual profile file says.
  `docs/logging.md` owns the profiles and their levels; check it, don't guess it.
- **For a new detector**, demonstrate it firing on a tree known to be broken before trusting its
  silence on a tree that might be fixed. An assertion nobody has seen fail is decoration, not
  evidence (`docs/investigating.md`).

### Move load-bearing diagnostics somewhere no log profile can filter them

A diagnostic count that only exists inside a `log.debug` call is one profile change away from never
having existed. Moving it into the assertion message itself (or any output that always gets emitted
on failure) removes the log-level dependency entirely - it cannot be filtered because there is no
level to filter it at.

### When you log one branch of a fork, log both, deliberately

Logging only the contended branch of a lock-acquisition fork (or any either/or code path) makes
"never contended" indistinguishable from "never ran that branch at all" - both produce the same
silence in the log. Raise both branches to the same level, deliberately, so their *relative* presence
or absence is the signal, not just one side's presence.

## Why This Matters

Each of these three mechanisms turned a real defect (a consumer that had stopped fetching entirely)
into an argument for the opposite conclusion ("the fault is likely in the test itself"), and did so
with output that looked authoritative - a probe's structured verdict, a clean grep, a passing
assertion. The cost was not just wasted time; it was that the false negative actively pointed
attention in the wrong direction, which is worse than an instrument that simply says nothing. An
instrument whose silence is sometimes meaningful and sometimes structurally guaranteed is worse than
no instrument, unless it also tells you which case you're in.

## When to Apply

Reach for this whenever a diagnosis leans on an instrument's *absence* of a signal: a clean probe
verdict, a zero-hit grep for a diagnostic line, a detector that never fired, an assertion that never
raised. Before treating that absence as "nothing wrong", check whether the instrument was capable of
producing anything else - a shorter enclosing deadline, a filtered log level, or a detector never
armed are the three concrete traps found here, but the underlying question generalizes to any
instrument with a reach shorter than the claim being read off it.

## Examples

**The counter-measure actually shipped for mechanism 1** - `AmbientProbeExtension.
appendUnfireableDetectors` - now appends a second block to
every clean verdict, resolving the enclosing `@Timeout` and checking each detector's bound against
it:

```java
if (unfireable.isEmpty()) {
    sb.append("  detector reach: every detector could have fired within this test's ")
            .append(limit.getSeconds()).append("s ceiling, so the clean result above means something\n");
} else {
    sb.append("  COULD NOT FIRE within this test's ").append(limit.getSeconds())
            .append("s ceiling, so their silence says NOTHING: ")
            .append(String.join(", ", unfireable)).append('\n');
}
```

Three outcomes, not two: every detector could fire; some could not (named, with their bound); or the
test declares no `@Timeout` at all, in which case nothing is claimed either way. The first run of
this code found the third case in the wild - a test with no `@Timeout`, where the old clean verdict
had been printing as if it meant something.

**The counter-measure for mechanism 2, applied three ways:**

- The two `commitLock` revoke-fork lines were raised to INFO deliberately *and* kept paired
  (the two log lines quoted above, in `AbstractParallelEoSStreamProcessor`) so a run's log shows
  which branch of the fork actually executed, not just whether one branch happened to log.
- Diagnostic counts in the awaitility `catch (ConditionTimeoutException)` block were moved out of
  `log.debug` calls and into the assertion failure message, where no logback profile can filter them.
- `docs/investigating.md`'s own instrumentation-verification advice was corrected: instead of
  grepping for PC's options line (INFO on `bz.stub.parallelconsumer`, filtered to `warn` by
  `logback-test.xml`), it now points at `bz.stub.parallelconsumer.integrationTests` classes like
  `ManagedPCInstance`, which sit at `info` and do reach the file.

**Before believing any zero from a grep**, verify the level actually reaches the run by counting
lines you know should be there at that level - on this branch, that meant counting roughly 720,000
DEBUG lines before treating "zero epoch-stale skips" as a real observation rather than a filtered
one.

## What is NOT claimed

Static analysis is the wrong tool for mechanism 1. Whether a detector's bound exceeds its enclosing
timeout is a temporal relationship between two values resolved at very different times - one is a
`Duration` constant, the other is a JUnit `@Timeout` annotation (or, for the chaos suite, a
scenario's own runtime budget) - and nothing short of resolving both at the point of use, as
`appendUnfireableDetectors` does, can compare them correctly. Inventing a lint rule that pattern-
matches "a Duration constant near a Timeout annotation" would produce more false confidence than it
removes; the gap here is named rather than papered over with a rule that looks like coverage.

## Related

- [`negative-results-need-an-instrument-that-could-have-said-yes.md`](../workflow-issues/negative-results-need-an-instrument-that-could-have-said-yes.md) -
  **owns the root principle this doc extends**: prove the instrument could have said yes before
  trusting a negative result. That doc covers the INVESTIGATOR's tools - searches, diffs, caches, a
  command pointed at the wrong target. This one covers the RUNTIME instrument inside the harness: a
  detector bounded past its own deadline, a diagnostic below the profile's level, a detector never
  shown to fire. Same principle, different instruments; read that one first.
- [`a-timing-bound-used-as-a-correctness-gate-manufactures-its-own-evidence.md`](a-timing-bound-used-as-a-correctness-gate-manufactures-its-own-evidence.md) -
  the positive-reading sibling of this doc: a bound crossing read as a defect signature when it is
  only the instrument meeting its own calibration. This doc is the negative-reading case: silence
  read as an all-clear when the instrument could never have produced anything else.
- [`a-stress-probe-is-an-instrument-you-built-not-a-test.md`](a-stress-probe-is-an-instrument-you-built-not-a-test.md) -
  why a probe's output needs the same scrutiny as any other measuring device before its readings are
  trusted.
- [`docs/solutions/workflow-issues/a-title-grep-is-not-a-search-2026-08-31.md`](../workflow-issues/a-title-grep-is-not-a-search-2026-08-31.md) -
  the search-side sibling: an empty SEARCH result is not proof of absence. This doc is the
  instrument-side version of the same principle, applied to probes, log levels, and detector bounds
  instead of grep queries.
- `docs/investigating.md` - "Verify your instrumentation actually reached the run" owns the general
  rule this doc's mechanism 2 is one instance of.
- `docs/logging.md` - owns the test logback profiles and their per-package levels referenced above.
