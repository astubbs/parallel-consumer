---
title: "Silence from an instrument that could not have spoken is not evidence"
date: 2026-09-01
last_updated: 2026-09-04
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
  - "A diagnostic reports at one granularity (fleet, suite, aggregate) while the detector it is meant to classify accuses one member (an instance, a shard, a partition)"
  - "A non-reproduction is about to be read as though it were the same evidence a reproduction-plus-diagnostic would have given"
tags:
  - false-negative
  - instrumentation
  - log-levels
  - detector-calibration
  - silent-failure
  - test-timeouts
  - chaos-testing
  - diagnostic-scope
---

# Silence from an instrument that could not have spoken is not evidence

## Context

While diagnosing the astubbs#29 / confluentinc#857 paused-consumption stall, four unrelated
mechanisms each produced the same shape of false reading: an instrument reported nothing wrong, in
a way that was structurally indistinguishable from a genuine all-clear. The first three surfaced
within days of each other, on the same investigation, and each one steered the diagnosis away from
the real defect for a period measured in weeks, not minutes. **The fourth, added 2026-09-04, is the
one that shows the trap has a positive-output form** - there the instrument was not silent at all.
It answered loudly, correctly, and about something else.

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

4. **A reach that is wide enough but at the wrong GRANULARITY - and this one is not silence.** The
   chaos suite's recovery diagnostic (`-Dchaos.diagnoseStallRecovery=true`, `ChaosScenarioBase`,
   `DIAGNOSE_STALL_RECOVERY`) drops the fail-fast and keeps sampling after a violation, so a run can
   be read for whether the system recovered. It correctly demoted the fleet-scoped `NO_PROGRESS`
   detector: six firings, replayed with recovery watching, all drained. But
   `ChaosScenarioBase#logDiagnosticProgress` logged only fleet-scoped counters - consumed, started,
   in-flight - while the suite's *gating* liveness detector,
   `INSTANCE_STALL/NO_WORK_COMPLETED`, accuses one named member. A fleet drains to its target around
   a single wedged instance in exactly the way it drains when every instance is healthy, so the
   diagnostic could not produce a different reading for the two worlds. Every instance-level sighting
   on that line sat unclassified for the line's whole life while "the backlog drained" kept being
   offered as though it had settled them. Widening the instrument settled it in one run: a
   per-member token (`ProgressProbe#instanceProgressSnapshot`) showed instance 0 live with its
   completion count frozen for the entire remainder of the run while the fleet finished cleanly -
   a real, non-recovering wedge that the fleet line reported as a clean drain
   (astubbs/parallel-consumer#435, open as of writing).

   Two details from that episode are worth more than the episode. **First, a non-reproduction was
   twice read as though it were a drain.** A separate firing of the same detector was waved off as
   "known load-shaped" on the strength of a replay that came back clean - but a clean replay is
   silent on recover-or-wedge, because the mechanism never ran. That is mechanism 3 wearing
   mechanism 4's clothes: an instrument that could not have spoken, read as though it had.
   (session history) **Second, this same flag had itself already been a silent no-op** on this exact
   scenario: it was implemented in one scenario base class while `ChaosChurnStormIT` extended
   another, so runs that claimed to use the diagnostic were measuring nothing until it was lifted
   into the shared base. (session history)

All four are the same failure in different clothing: **the instrument's output was read as a verdict
about the system, when it was actually a fact about the instrument.** Silence is the commonest form
and the one this doc is named for. The fourth is the reminder that a confident, well-formed,
perfectly correct answer is the same trap when it answers a different question than the one asked.

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

### Check the instrument's GRANULARITY against the claim, not only its reach

Reach asks whether the instrument could have spoken at all. Granularity asks whether it could have
spoken *about the thing being accused*. An instrument can be perfectly armed, perfectly calibrated,
and still structurally unable to settle the question, because it reports one level up from where the
claim lives. The check is mechanical: name the subject of the detector's assertion (this instance,
this shard, this partition), then look at the diagnostic's output and ask whether any field in it
varies with that subject. If none does, the diagnostic cannot classify that detector's firings, and
its clean verdict is a fact about its own scope.

This is the harder half to notice, because nothing looks wrong. A silent instrument at least invites
the question "should it have said something?". An aggregate instrument answers fluently and on time,
and its answer is *correct* - just about a different subject. Aggregates are especially prone to it
because a healthy majority hides one sick member by construction: a fleet total climbing to its
target is consistent both with every member finishing and with all-but-one finishing.

### Give a diagnostic a second, independently-moving counter

A single frozen number is ambiguous by design. "This counter has not moved for 150 seconds" is
equally consistent with the thing being watched having stopped, and with the counter itself having
stopped being updated - a stale or phantom counter, which is a real recorded defect class in this
repo (`docs/inflight/bug-number-records-out-for-processing-is-a-plain-int.md`). Both produce
identical silence, so a lone reading always needs interpretation, and interpretation is where the
confident wrong answer enters.

Pair it with a second counter whose behaviour differs between those explanations. In the episode
above, the completion count froze while the records-out-for-processing count kept climbing - a shape
a phantom counter cannot produce, because a phantom is not replenished. The pairing turned an
ambiguous reading into a classification without any further runs. When adding a counter to a
diagnostic, ask which distinct true states could produce the same value, and add whatever second
signal separates them.

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
armed are three of the four concrete traps found here, but the underlying question generalizes to any
instrument with a reach shorter than the claim being read off it.

Reach for it equally when a diagnosis leans on an instrument's *presence* of a signal that is not
about the accused subject: an aggregate that drained, a suite that passed, a fleet that completed,
offered as evidence about one member. Ask whether the aggregate would look identical whether or not
that member individually succeeded. If it would, the aggregate result is not evidence either way.
And treat a non-reproduction as its own case: a clean replay says the mechanism did not run, which
is silent on what the mechanism does when it does run.

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
- `docs/inflight/test-857-churn-storm-async-stalls.md` - the ledger mechanism 4 came from, including
  the run of prior measurement failures of this same shape that it records against itself.
- `docs/inflight/test-per-shard-liveness-has-no-gate.md` - the same granularity mismatch one level
  finer and still open: `INSTANCE_STALL` is per-instance, so one wedged shard on an instance whose
  other shards keep completing is a claim no detector in the suite can currently classify.
