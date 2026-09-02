# Shard the Chaos Pain Suite so its slowest lane stops being the critical path

<!-- inflight-type: task -->
<!-- inflight-impact: ci -->

The chaos suite runs as one job and takes appreciably longer than the integration suite, so it sets
the wall-clock floor for feedback on every PR. Splitting it across parallel lanes would remove that,
and the ceiling is the ordinary one for sharding: **the longest single scenario, not the total
divided by the shard count.** Two shards only halve the time if the work divides evenly, and it does
not - one revoke-under-work cell running long on its own puts a floor under every arrangement. So
size the shards from measured per-scenario durations, not from a guess at the split.

**The measurement is already free.** Every chaos run writes failsafe XML with per-class times, so a
recent run's reports give the real distribution without running anything new. Take the numbers from
a run on the self-hosted lane rather than a laptop - the shard sizing has to reflect the hardware it
will run on.

## Why this is worth more than the speed

**It extends a separation the repo already chose deliberately.** Chaos moved off the shared highcpu
box onto per-PR VMs precisely so co-residency could not confound a result; several sightings in
`bug-857-family.md` were only ruled out as contention because that move made overlap structurally
impossible. Sharding by scenario extends the same reasoning one level down: one cell's load stops
perturbing another cell's timing bounds.

**It removes a named, never-replayed confound.** The family ledger records that local replays ran one
scenario alone where CI runs the whole chaos group in one JVM, and that *"sequencing effects from
sibling chaos tests were not replayed"*. Sharding makes CI match the shape the replays already have.

## The two ways this becomes a regression dressed as a speedup

**A shard that selects zero tests must be a hard failure.** `bin/chaos-test.sh` excludes
`@Quarantined` scenarios and its own header warns that this can select nothing, which it reports
loudly rather than *"impersonating a real GREEN run"*. Sharding multiplies that hazard by the shard
count: quarantine one scenario and its shard silently has nothing to do, and a green lane with no
tests in it is indistinguishable from a passing one. This repo has already paid for exactly this
shape once - the mutation lane exited 0 printing *"nothing to mutate, skipping"*, green forever while
scoring nothing (`bin/ci-mutation-test.sh` now exits 2 for it). Every shard needs that guard, not
just the suite as a whole.

**The per-seed replay commands must survive aggregation.** Today one job produces one summary
carrying the seed and its replay command, and that is the asset when a run goes red - the ledger
repeatedly notes a seed is worth more than the failure it came from, because console logs truncate
and expire. Whatever combines N shard summaries has to preserve every seed, or a red shard becomes
unreproducible.

## Shape to aim for

- Keep `bin/chaos-test.sh` the single entry point and give it a scenario filter, so the PR gate and
  the `chaos-pain.yml` dispatch cannot drift apart. Two callers with two different selection rules is
  how a lane ends up testing something nobody intended.
- Balance shards by measured duration, pinning the longest scenario in a lane of its own.
- Fail a shard that selects nothing, and say which scenario it expected.
- Aggregate to one verdict, preserving every shard's seed and replay command.

## Not yet decided

Whether the shard split is static (a matrix of scenario names in the workflow) or derived. Static is
honest and greppable but drifts as scenarios are added; derived cannot be read off the workflow file.
A static matrix with a check that every chaos scenario appears in exactly one shard gets both, and is
the same shape as the quarantine registry's own gate.
