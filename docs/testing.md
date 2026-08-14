# Testing: suites, lanes, and the diagnostic tooling

How the suites are split, what the quarantine lane and chaos suite are for, and how to read an
ambient probe autopsy. The rule that governs all of it - **never weaken a test to make it pass
until you have established why it fails** - lives in AGENTS.md, because it applies whether or not
you have read this file.

## Suites

- **Unit tests**: surefire, sources in `src/test/java/`. Run with `bin/ci-unit-test.sh` (no Docker
  needed).
- **Integration tests**: failsafe (`mvn verify`), sources in `src/test-integration/java/`. Uses
  TestContainers with the `confluentinc/cp-kafka` Docker image. Run with
  `bin/ci-integration-test.sh`.
- **Exclusion patterns**: `**/integrationTest*/**/*.java` and `**/*IT.java` are excluded from
  surefire and included in failsafe.
- **Kafka version matrix**: CI tests against multiple Kafka versions via `-Dkafka.version=X.Y.Z`.

## The ambient probe: contention artifact, or genuine bug?

Every broker integration test failure log includes an `=== AMBIENT PROBE AUTOPSY ===` block (grep
for it) with rebalance-dwell and lag-stagnation violations plus per-partition frozen-committed
detail. It answers the contention-versus-bug question before you start manual diagnosis. Disable it
with `-Dambient.probe=off` or `@NoAmbientProbe` only when the probe itself is the problem (see
`AmbientProbeExtension`'s javadoc).

**`probe clean` is only informative when the probe's detectors could have fired.** Lag stagnation
needs `LAG_STAGNATION_MIN_LAG` (50) of real lag sustained past `LAG_STAGNATION_BOUND` (150s), and
rebalance dwell needs `REBALANCE_DWELL_BOUND` (15s). A test with a handful of records, or one that
fails inside a window shorter than those bounds, cannot trip either - so its autopsy prints
`probe clean` and the sentence beside it carries no evidence at all. Check the test's record count and failure window against those constants before
treating a clean probe as a finding. This is not hypothetical: the `commitTimeout` autopsy of
2026-08-07 read `probe clean` on a 15-record test that failed in 35s, where the thresholds are 50
records and 150s.

**The sharper case is a group that never formed at all**, where the probe cannot be informative
even in principle. A broker container that fails to start, Docker or network trouble, or anything
throwing before the clients open, leaves the detectors with nothing to sample - and produces a clean
autopsy indistinguishable from a genuine test fault. Seen on astubbs#116: `ContainerLaunchException:
Container startup failed for image confluentinc/cp-kafka:7.9.0`, autopsy `probe clean`, cause
Docker. **Read the autopsy's own `failure:` line first**; it is printed above the verdict precisely
so the exception is seen before the classification. The clean line now says only that nothing in
group progress explains the failure, and names this case, rather than pointing at the test.

## Quarantine lane (`@Quarantined`)

For tests that are red on master's *gating* CI when the fix lives in another, open PR. Do **not**
leave such a test red (ambiguous checks, error-prone merge decisions) and do **not** `@Disabled` it
(that loses the signal - a "known flake" can be a real product bug; see the drain-zombie write-up,
[`docs/solutions/test-flakiness/pc-silent-stall-under-contention-2026-07-29.md`](solutions/test-flakiness/pc-silent-stall-under-contention-2026-07-29.md)).

Instead annotate it `@Quarantined(reason, tracking, fixedBy)` (in core's shared test sources). The
test then leaves the gating suites - green means mergeable - but keeps running on every PR push and
after every merge to master (plus `workflow_dispatch`) in the "Quarantine Lane / tests" CI job,
whose summary carries pass/fail plus an audit of every quarantined test and its owner. The
seconds-fast "Quarantine Audit" job enforces the rules on every PR (registry drift and broken owner
claims fail fast; no tests run there).

**What "non-gating" means precisely.** That job *is* a required status check, but its test-running
step carries `continue-on-error: true`, so a quarantined test going red cannot block a merge. Its
registry and owner-claim steps have no such escape and do fail the job: the lane gates on the
registry staying honest, never on the test outcomes.

The live registry and task list is [`docs/quarantined-tests.md`](quarantined-tests.md), enforced by
`bin/check-quarantine-registry.sh` to match the annotations in both directions so it cannot drift;
`bin/check-quarantine-owners.sh` additionally verifies each entry's owner claim when it names one
(the owning PR exists, is open, and eventually removes the quarantine); an entry without an owner is
legal and flagged as an advisory, not an error.

Rules:

1. **No quarantine without diagnosis** - undiagnosed red stays red and blocks, on purpose. The
   repository owner can grant an explicit, recorded exception - see the registry's rule list.
2. **Quarantine is master-state, not PR-state** - see AGENTS.md, Testing.
3. **The owning fix PR deletes the annotation AND its registry entry in the same commit** after
   merging master, atomically restoring the test to the gating lane. An owning PR is the goal, not
   a precondition - unowned entries stay loud via the lane report and the release guard.

A non-empty lane blocks releases - see [`docs/releasing.md`](releasing.md). Run the lane locally
with `bin/quarantined-test.sh`.

## Chaos Pain Suite (on-demand bug detector - never gates)

A seeded, calibrated chaos suite (`integrationTests.chaostests`: `ChaosConductor`, `ProgressProbe`,
`ChaosScenarioBase`, plus scenarios `ChaosChurnStormIT` W1 and `ChaosRevokeUnderWorkIT` W4) that
hunts the "alive but not progressing" bug class: rebalance-dwell zombies, protocol-invisible
per-partition lag stagnation (Class 2, W4's prey), drain overruns, and record loss or duplication.
Tagged `@Tag("chaos")` and excluded from all default and gating suites via `pom.xml`'s
`excluded.groups` default.

- **Run locally** (requires Docker; ~5-6 min):
  `./mvnw -Pci -pl parallel-consumer-core -am verify -DskipUTs=true -Dincluded.groups=chaos -Dexcluded.groups=`
- **Replay a schedule**: every run logs its seed and the full replay command; add
  `-Dchaos.seed=<seed>`.
- **CI**: per same-repo PR commit via the highcpu fast-feedback lane (check `highcpu / Chaos Pain
  Suite` - not optional: a chaos RED shows red); on-demand seeded hunts via `chaos-pain.yml`, e.g.
  `gh workflow run chaos-pain.yml -R astubbs/parallel-consumer -f seed=42 -f reps=3`. Both call
  `bin/chaos-test.sh`. Unlike the local recipe above, CI runs **exclude** `@Quarantined` chaos
  scenarios (the Quarantine Lane owns those), so they can select zero tests - the job summary flags
  that loudly.
- **Probe a fix PR** (the suite's primary purpose): on the fix PR's branch (merge master in first
  if the branch predates the suite landing there), run the suite at a commit before the fix - expect
  RED, and the violation names the mechanism - and again at the fix, expecting GREEN. The local
  recipe above includes `@Quarantined` scenarios (`-Dexcluded.groups=` is empty), so known-RED
  detectors still fire locally. `ChaosChurnStormIT`'s class javadoc has the full recipe.
- **A RED run is investigation food, not flake noise.** The probes are calibrated against the real
  historical drain-zombie defect (RED on pre-fix compositions, GREEN on fixed; thresholds sit in
  measured gaps). **Never loosen a probe to go green** - tune the workload or conductor instead.

## Reusing test utilities

Shared client and broker helpers live in `KafkaClientUtils` (topic creation, producers, consumers,
PC builders) and `BrokerIntegrationTest` (the base class most integration tests extend). Before
writing a new helper or a raw `admin`/producer/consumer call in a test, search these two first and
extend them. Duplicating an existing helper is how bugs get reintroduced - a copy of topic-creation
logic once drifted to a 1-second timeout and became a flaky-CI source (see
[`docs/solutions/test-issues/`](solutions/test-issues/)). When you must add a helper, put it in the
shared util, not the test.
