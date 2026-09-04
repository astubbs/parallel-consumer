# Shard the Integration Tests gate across runner jobs

<!-- inflight-type: task -->
<!-- inflight-impact: ci -->

The `Integration Tests` lane is the PR build's critical path - 620s against ~500s for the next
slowest. Sharding it across N runner jobs, the way the Chaos Pain Suite already is, is the lever
that would actually halve it. **Deliberately deferred, not undecided**: the free serial work goes
first, because it makes every future shard cheaper.

## Why this is the remaining lever, and why it was not taken first

Measured 2026-09-03 - full write-up in
[`docs/plans/2026-09-03-001-investigate-integration-gate-wall-time.md`](../plans/2026-09-03-001-investigate-integration-gate-wall-time.md).

**Within-job overlap is exhausted.** 1528s of test time runs in 420s of wall on four forks - about
91% parallel efficiency, which is near-linear and only happens because these tests spend most of
their time waiting on the broker rather than burning CPU. Six forks dropped to 75% efficiency AND
inflated total work 11%, so `forkCount=4` is the ceiling, not a starting point. Near-linear to 4 and
degrading at 6 is also the signature of a 4-core box.

So more overlap has to mean **more jobs**, which is this note. What stops it being the obvious first
move is the shape of the remaining cost:

**Each shard re-pays the serial build.** Of the 604s Maven step, ~136s is not tests at all -
`testCompile` 60s, `compile` 42s, javadoc 14s, delombok 8s, Truth codegen 7s. That part is paid once
per JOB, so two shards pay it twice and four shards four times. At today's numbers a 4-way split
costs roughly 400 extra runner-seconds before it saves anything.

**And test work only converts at 1:3.6.** Because four forks already overlap the waiting, removing
3.6s of test time buys 1s of wall - while removing 1s of *serial build* time buys a full second. The
build reduction is therefore worth ~3.6x per second AND it is the exact cost sharding multiplies.
Doing it first is not a detour from sharding; it is the thing that makes sharding pay.

## What this will need when it is picked up

- **Size the split from measured per-class durations, longest-first over N bins** - the chaos suite's
  design note is the precedent and `.github/workflows/maven.yml`'s chaos matrix comment records how
  it was done. The integration suite does not divide evenly: `PartitionStateCommittedOffsetIT` at
  160s puts a floor under any arrangement, and `Rebalance857CommitSyncDeadlockProbeIT` at ~340s puts
  a higher one until it is split (branch `optimize/ig-exp002-probesplit`, green and unmerged).
- **A single entry point that selects the shard's classes**, so this matrix and any on-demand
  dispatch cannot select tests differently - `bin/chaos-test.sh` and its `CHAOS_SCENARIOS` are the
  model, including its refusal to pass a shard whose requested tests produced no failsafe report.
  A shard that goes quietly idle is the failure mode to design against.
- **Do not raise `forkCount` inside a shard to compensate.** Measured harmful; see the plan document.

## Cost, stated honestly

Sharding buys critical path and spends aggregate runner-minutes. That was an explicitly acceptable
trade when this work was scoped (public repo, minutes are free), but it is a real trade and the
build-overhead multiplication above is the part that makes it worse than it first looks.
