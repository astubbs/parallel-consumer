# Compound-engineering ideas from the engine-performance work

<!-- inflight-type: next -->
<!-- inflight-impact: process -->

**A running ledger, kept as the work happens, to be ranked and selected at the end.** Sibling to
[`next-compound-engineering-ideas.md`](next-compound-engineering-ideas.md), which does the same for
the language-proxy fan-out - same standing instruction, same rule: **add as you go, do not rank
here.** Ranking is the end-of-work step, with the owner.

These are candidate *practices* - things that would compound if turned into a skill, a check, a
brief clause, or a rule.

## Measurement discipline - the strongest cluster, and all of it paid for

**1. When a test fails and a bisect exonerates the code, question the INSTRUMENT, not the bound.**
`OrderingModeDispatchParityTest` failed for a day. Four fixes were tried - fastest-of-three, the
ratio, per-thread CPU time, `@Isolated` - and **three of them after a bisect had already shown every
merge point was flat.** The test was timing two arms with different sensitivities to contention
inside a suite that runs test methods in parallel at `dynamic.factor=20`. Ratios do not cancel a
machine when the arms degrade at different rates.

**2. Changing a test's method repeatedly to make it pass is tuning until it agrees.** The
countable version of rule 1, and a candidate lint: **N method changes to a failing test without a
change to the code under test** is a smell with a threshold.

**3. Count, do not time, when guarding algorithmic complexity.** `DispatchScanMeter` counts entries
the dispatch scan examines. Deterministic, identical on a loaded machine and an idle one, and it
turned a flaky ratio into an exact constant. **It also corrected the belief it was built to guard**:
the modes were asserted to cost "the same within 4x"; the true figures were 1.0 and 20.5 per record.

**4. Build a control arm that decouples variables production conflates.** In the direct-pull engine
the number of scanning threads and the number of in-flight records are **the same number**
(`maxConcurrency`), so no benchmark cell can separate them. `DirectPullScanCostMeasurementTest`
drives them independently, and the `scanners = 1` arm - where contention cannot exist - settled a
two-hypothesis question that a year of benchmarking could not.

**5. Prove the harness can still see the defect before believing a zero.** A fix reported 0 in
14,400,000. That is only meaningful because reinstating the defect produced 5 in 7,200,000 through
the same harness. Sabotage-then-measure, every time a zero is the result.

**6. Report refuted predictions as prominently as confirmed ones - the refutations are where the
false confidence lives.** Two attempts to reproduce a race returned 0 in 72,000 and 0 in 96,000.
Both would have been filed as "cannot reproduce". The defect appeared at 4 in 14,400,000 - roughly
40x more volume. **A negative result needs its N stated or it is not a result.**

## Harness and tooling

**7. A stage that logs nothing on success is indistinguishable from a stage that hung.** Two sweeps
were abandoned believing the produce step had wedged; it had finished, and its announcement was
simply the last line either way. **Log completion, not just commencement** - and bound every stage.

**8. Make the tool refuse a meaningless measurement rather than documenting it.** Non-blocking
engines given a blocking callee produce a plausible number that measures the stub. The fix was a
hard refusal with an explicit override, not a note in the README. Generalises: **when a
configuration produces a confidently wrong answer, the tool should decline.**

**9. Teach the harness the arithmetic of its own arms.** A serial arm's runtime is `records x delay`
and concurrency does nothing - 100,000 records at 100ms is 2.8 hours. The sweep now projects and
skips, **recording the projection as a row** so a reader sees the floor was not measured and why.

**10. An instrumentation false negative is worse than no instrument.** A repeat-runner grepped for
`Tests run:`, which `-q` never prints on success, and reported 6 of 6 failures - all fabricated. The
agent caught it mid-run and said so. **Verify the instrument fires on a known-positive before
trusting its negatives.**

## Agents and briefs

**11. Brief agents with the dead ends, not just the goal.** Each brief in this session carried what
had already been tried and failed. The close-path agent then found that **the injection I proposed
would have passed against unfixed code** - a false-positive test - because the failure was single-shot
and a supervisor retry hid it. Naming my own failed attempts is what let it start past them.

**12. An agent that refuses an instruction because the repo contradicts it is behaving correctly.**
One was told to add a `CHANGELOG` entry and a `Release-Note:` trailer. `AGENTS.md` forbids the first;
the second existed only on an unmerged branch, so it checked, found nothing, and declined to invent
it. **The gap it exposed became its own PR.** Compounding version: treat an agent's pushback as
evidence about the documentation.

**13. `rm` silently stalls a background agent.** It needs an approval prompt nobody sees. `mvn clean`
expresses the same intent. Belongs in every brief that touches a build.

## Merging and prior art

**14. Both sides of a conflict can be load-bearing, and taking either whole loses a capability.** One
`bench` conflict: one side added stderr capture, the other had the array carrying
`-Dpc.directPull=true` and `-Dpc.virtualThreads=true`. Taking the newer side would have left two
engine arms **silently running as plain core**. Rule: **for each side, ask what capability it is the
only source of.**

**15. A retracted verdict must be retracted where it is cited, not only where it was recorded.** The
2022 "1/3 as fast" figure measured a busy-spin with the blocking wait commented out. That is written
down - and it still had to be actively refused three times in one session, because the number
travels and the retraction does not.

**16. The most valuable prior art is found by the files it touched, not by what it was called.** Every
significant precedent here was a 2022 branch nobody searched for, containing the same three designs
proposed again today.

## Design

**17. State inferred from an object's position is the recurring defect.** A record stays in a shard so
that its *presence* means "this key is blocked"; an in-flight record stays so the scan can step over
it; buffer depth is inferred from an executor's queue. Each is a fact recorded as a location, and each
costs a walk to read. **Look for "we know X because the object is still there".**

**18. A design that ADDS a structure holding the same facts must answer why it is not the bug class
this project keeps paying for.** Asked of every proposal in this session. The ones worth building
answered by subtraction.

