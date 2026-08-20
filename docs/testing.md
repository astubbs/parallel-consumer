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

Every broker integration test failure **emits** an `AMBIENT PROBE AUTOPSY` block (grep for
it) with rebalance-dwell and lag-stagnation violations plus per-partition frozen-committed detail.
It answers the contention-versus-bug question before you start manual diagnosis. Disable it with
`-Dambient.probe=off` or `@NoAmbientProbe` only when the probe itself is the problem (see
`AmbientProbeExtension`'s javadoc).

**Emitted reliably; found reliably only off the console.** The block is captured into the failsafe
XML that CI uploads, so it survives a truncated console log - and a fetched CI log here has been
cut mid-job twice, silently, with the autopsy past the cut. Fetch it from a route that cannot
truncate, and check the log you did fetch is complete before diagnosing from it:
[`docs/solutions/workflow-issues/gh-run-view-log-truncation.md`](solutions/workflow-issues/gh-run-view-log-truncation.md)
**owns those routes** and the completeness check.

**A failing chaos test's autopsy carries its own replay.** `chaos seed:` and `chaos replay:` sit
directly under the failure line, the replay command complete - the `chaos` tag is excluded by
default, so the seed alone does not select the test. **First move on a chaos failure is to run that
command, not to reason from the log**: the replay is the deciding experiment, and every sighting in
the confluentinc#857 ledger was settled by one. How the seed reaches the block, and why it lives
there rather than only in the run-start log line: `ChaosSeed` and `AmbientProbeExtension`'s
`captureChaosSeed`.

**`probe clean` is only informative when the probe's detectors could have fired.** Lag stagnation
needs `LAG_STAGNATION_MIN_LAG` (50) of real lag sustained past `LAG_STAGNATION_BOUND` (150s), and
rebalance dwell needs `REBALANCE_DWELL_BOUND` (15s). A test with a handful of records, or one that
fails inside a window shorter than those bounds, cannot trip either - so its autopsy prints
`probe clean` and the accompanying sentence "the fault is likely in the test itself" carries no
evidence at all. Check the test's record count and failure window against those constants before
treating a clean probe as a finding. This is not hypothetical: the `commitTimeout` autopsy of
2026-08-07 read `probe clean` on a 15-record test that failed in 35s, where the thresholds are 50
records and 150s.

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

1. **No quarantine without evidence** - a diagnosed mechanism, or a recorded sighting ledger
   proving the failure is master-state. A hunch stays red and blocks, on purpose.
   [`docs/quarantined-tests.md`](quarantined-tests.md) **owns the full rule** and the reasoning
   behind the 2026-08-19 change from "diagnosis" to "evidence".
2. **Quarantine is master-state, not PR-state** - see AGENTS.md, Testing.
3. **The owning fix PR deletes the annotation AND its registry entry in the same commit** after
   merging master, atomically restoring the test to the gating lane. An owning PR is the goal, not
   a precondition - unowned entries stay loud via the lane report and the release guard.

A non-empty lane blocks releases - see [`docs/releasing.md`](releasing.md). Run the lane locally
with `bin/quarantined-test.sh`.

## Chaos Pain Suite (on-demand bug detector - never gates)

A seeded, calibrated chaos suite (`integrationTests.chaostests`: `ChaosConductor`, `ProgressProbe`,
`ChaosScenarioBase`) that hunts the "alive but not progressing" bug class: rebalance-dwell zombies,
protocol-invisible per-partition lag stagnation (Class 2), drain overruns, and record loss or
duplication. Tagged `@Tag("chaos")` and excluded from all default and gating suites via `pom.xml`'s
`excluded.groups` default.

**What it can assert, so you know whether a question is already answerable.** Reach for an existing
capability before building one - the calibration behind each of these is the expensive part, not the
code:

| Capability | Where | What it catches |
|---|---|---|
| Loss and bounded duplication | `ProgressProbe`'s ledger | a record never arrives, or arrives more often than a disturbance explains |
| **Per-key ORDERING and concurrency** | `KeyOrderLedger` | a key's offsets going backwards, or two deliveries of one key in flight at once |
| **A stalled instance** | `InstanceStallProbeIT`, `ProgressProbe` | a member present and heartbeating while making no progress |
| Lag stagnation (Class 2) | `ProgressProbe` | a committed offset frozen while lag grows, group STABLE |
| **Watching a stall instead of killing it** | `-Dchaos.diagnoseStallRecovery=true` | keeps a stalled run alive so its state can be read |

**Recorded but not yet analysed - reach for this before adding instrumentation.** The ledger is an
event register: it writes down facts and lets the end-of-run assessment decide what they mean. So
some questions need only a new *analysis*, not new *recording*. Every `KeyOrderLedger.Delivery`
already carries `incarnationId`, `partition`, `epoch`, `key`, `offset`, `startSeq` and `endSeq`
(`null` while still running), and the full history is retained - the per-window grouping is a choice
`check()` makes, not a limit on what was captured.

The worked example is cross-epoch comparison. Nothing today compares deliveries across an epoch
boundary, but the data to do it is present: a delivery with `endSeq == null` in one epoch, against a
delivery of the same key and partition in a later epoch whose `startSeq` falls after it. **If a test
needs that, write the comparison - do not add instrumentation for it.** The work is the calibration,
not the capture: a revoked owner finishing its in-flight record is legitimate, so such a check needs a
defensible bound on how long an old-epoch delivery may still run before it counts as a violation.

Scenario cells, each isolating one disturbance shape: `ChaosChurnStormIT` (W1, continuous churn),
`ChaosRevokeUnderWorkIT` (W4, revoke while work is in flight), `ChaosKeyOrderIT` (key-ordered
processing under churn), `ChaosRevokeUnderWorkKeyOrderIT` (key order under revoke), and
`ChaosRevokeUnderWorkDrainIT` / `ChaosRevokeUnderWorkCooperativeDrainIT` - a 2x2 control arm over
assignor and stop-mode, whose weights are shared through `drainOnlyChaosWeights()` so the two cannot
drift apart.

**Two limits worth knowing before you trust a verdict.** `KeyOrderLedger` compares only within one
incarnation, partition, epoch and key - so a **cross-epoch overlap** (an old owner still running past
a revoke while the new owner takes the same key) lands in two windows and is not reported. It is
**not** unanswerable, though: every delivery records its epoch and incarnation and the full history is
kept, so the check is a function nobody has written rather than data nobody has. What it would need is
a calibrated bound on how long a revoked owner may legitimately still be finishing - see the class
javadoc. That shape is a real defect this repo has already fixed once (astubbs#80). And `CLASS2_STALL` gates on a timing bound, so a red proves the bound was crossed, not
that the backlog never drained - see `docs/inflight/test-class2-probe-asserts-timing-not-correctness.md`.

- **Run locally** (requires Docker; ~5-6 min):
  `./mvnw -Pci -pl parallel-consumer-core -am verify -DskipUTs=true -Dincluded.groups=chaos -Dexcluded.groups=`
- **Replay a schedule**: every run logs its seed and the full replay command, and a failure repeats
  both inside its autopsy block (above, where truncation cannot reach them); add
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

## Mutation-check every new assertion, not just the risky-looking ones

Delete the guard an assertion claims to pin, run the test, confirm it fails, restore. An assertion
that cannot fail is worse than none, because everyone after you counts it as coverage - and reading
does not find one: three assertions written on astubbs#296 all read as strong, and all passed
against a deleted guard, the last of them surviving two review rounds. The mechanism (a loop whose
first pass moved the state out of the branch it was counting), the repair, and the
counting-assertion heuristics:
[`docs/solutions/test-flakiness/vacuous-counting-assertion-loop-changed-its-own-precondition-2026-08-18.md`](solutions/test-flakiness/vacuous-counting-assertion-loop-changed-its-own-precondition-2026-08-18.md).

## Reusing test utilities

Shared client and broker helpers live in `KafkaClientUtils` (topic creation, producers, consumers,
PC builders) and `BrokerIntegrationTest` (the base class most integration tests extend). Before
writing a new helper or a raw `admin`/producer/consumer call in a test, search these two first and
extend them. Duplicating an existing helper is how bugs get reintroduced - a copy of topic-creation
logic once drifted to a 1-second timeout and became a flaky-CI source (see
[`docs/solutions/test-issues/`](solutions/test-issues/)). When you must add a helper, put it in the
shared util, not the test.
