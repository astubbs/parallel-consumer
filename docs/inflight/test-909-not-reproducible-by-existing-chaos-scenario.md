# confluentinc#909 does not reproduce in `ChaosRevokeUnderWorkIT` - measured, not assumed

Attempted while reviewing astubbs/parallel-consumer#31, so the next person does not repeat it blind.

**Result: the defect arm passed.** `ChaosRevokeUnderWorkIT` (scenario `w4`, cooperative=false) at
`-Dchaos.seed=424242`, run against source with `ProcessingShard.addWorkContainer` reverted to master's
pre-fix behaviour (drop the incoming record whenever the offset is occupied):
`tests="1" errors="0" skipped="0" failures="0"`, 154.8s. Verified from
`target/failsafe-reports/TEST-...ChaosRevokeUnderWorkIT.xml` and the `=== CHAOS w4 ... seed=424242 ===`
banner, **not** from the exit code - a `nohup`-backgrounded maven reports exit 0 while still running,
and reported it twice more for a `BUILD FAILURE` and a `No tests to run`.

This is consistent with what the suite already showed and merely confirms it directly: the Chaos Pain
Suite is **green on master**, and master still carries the defect.

## Why it does not reproduce

The scenario asserts the right invariant - `ChaosScenarioBase.allConsumedCovers` requires
`unique.containsAll(expectedKeys)` over per-record unique keys (`"key-" + i`), which is exactly what a
dropped record violates. What it does not reliably generate is the interleaving: a rebalance landing
**inside** `maybeRegisterNewPollBatchAsWork`'s insert loop, **and** a re-delivered offset colliding
with the stale resident left behind. Both must coincide within one registration batch.

## Do not fix this by rerolling seeds

One 155s sample per seed. Hunting seeds until one goes red would produce a result that cannot be
distinguished from scenario flakiness, and a green run says almost nothing either way. A reproduction
worth having needs the scenario changed so the collision is *driven* rather than waited for - e.g.
churn timed against registration batches rather than against wall clock - which is scenario design
work, not a seed search.

The unit-level evidence for the fix is `ProcessingShardStaleReplacement909Test`: four tests, all RED
with the fix reverted, one of them driving the defect through the real registration path
(`maybeRegisterNewPollBatchAsWork`) rather than around it.

## Replay

```
./mvnw -Pci -pl :parallel-consumer-core -am verify -DskipUTs=true \
  -Dexcluded.groups=performance,quarantined -Dit.test=ChaosRevokeUnderWorkIT \
  -Dfailsafe.failIfNoSpecifiedTests=false -Dchaos.seed=424242
```

Chaos is in `excluded.groups` by default (`bin/ci-integration-test.sh` hardcodes the exclusion), which
is why it must be dropped from that list to select the test at all.
