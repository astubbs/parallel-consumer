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

## Seeing test output: raise the level with a flag, never by editing the config

**The test harness defaults to `warn`, so a passing-but-silent run is expected, not evidence that
nothing happened.** Each of the four library modules' `src/test/resources/logback-test.xml` reads the
`pc.log.level` system property for both the root logger and `bz.stub.parallelconsumer`:

```bash
./mvnw test -pl :parallel-consumer-core -am -Dpc.log.level=debug -Dtest=TheOneTest
./mvnw test -pl :parallel-consumer-core -am -Dpc.log.level=trace -Dtest=TheOneTest
```

Surefire forwards the property into the forked JVM, so nothing needs editing and there is no
uncommitted local edit to remember to revert. **Editing the default in the file is the thing this
replaces** - that is how core alone drifted to `root=info` while vertx, reactor and mutiny all sat
at `warn`.

Measured over the whole core unit suite (387 tests): `info` produced **5,520** lines of Maven output
and `warn` produces **3,687** - a third less, of which 1,438 were INFO-level lines and **196 were
repeats of the `ParallelConsumerOptions` toString banner**, one per constructed instance. A third off
the total is worth having, but the banner count is the real point: reading a *single* failing class
means scrolling past its own options dump once per test.

- **Narrow `-Dtest=` whenever you raise the level - the volume alone breaks tests.** Measured on
  `ParallelEoSStreamProcessorTest` (58 tests): `warn` emits 869 lines and passes; `-Dpc.log.level=debug`
  emits **469,202** and three tests fail on a 30-second loop-cycle latch. The same three pass at
  `debug` when selected alone (4,586 lines). The console appender is synchronous, so a whole suite
  at `debug` starves the control loop - a self-inflicted instance of the contention-versus-genuine-bug
  question below. Do not read those timeouts as a product defect, and do not "fix" them.
- **The flag is a blunt instrument; the file holds the sharp ones.** For one class, one Kafka
  internal, or the standing kafka-client bootstrap harness, uncomment the relevant `<logger>` line -
  `logback-test.xml` carries a commented, annotated switch for each, including the confluentinc#857
  silent-stall set. Those are deliberate and documented; revert before committing.
- **Test narration keeps a higher floor than the product** (in core's config, the only module with
  integration tests). `bz.stub.parallelconsumer.integrationTests` defaults to `info`, not `warn`: what
  made the old default noisy was product logging, whereas a test that deliberately logs a line is
  saying something worth reading. It matters most for the chaos
  suite's run-start banner (`=== CHAOS W1 churn storm: seed=... (replay: ...) ===`), the only copy of
  the seed on a **passing** run - `buildAutopsy()` reprints it on failure, but a pass you later want to
  replay has nothing else. It reads `${pc.log.level:-info}`, so the flag still raises it; a hard `info`
  would have made `-Dpc.log.level=debug` silently skip the integration tests. Integration tests have no
  `resources` directory of their own, so failsafe reads the **unit** module's `logback-test.xml`.
- **Two levels are pinned on regardless and must stay that way**:
  `org.apache.kafka.clients.consumer.internals.SubscriptionState` at `info` (offset-reset decisions -
  one line each, and the number that settled the `committedOffsetRemoved[latest]` nudge race), and
  `org.apache.kafka.common.config.AbstractConfig` at `error` (otherwise every client dumps its full
  config).

**`bin/check-test-log-config.sh` enforces all of the above** (Repo Hygiene, self-tested by
`bin/test-check-test-log-config.sh`): the four library modules must drive both levels from
`pc.log.level`, and no logger may be committed switched on at `debug`/`trace`. It exists because
every failure here is silent - a `debug` default does not go red, it just floods the log, slows the
run, and can time tests out. `parallel-consumer-examples/*` is deliberately out of scope: those are
demonstration apps with tiny suites that legitimately run verbose (and `example-core` carries a
`logback-temp-test.xml` that logback never loads at all).

The autopsy block below is **not** affected by any of this - `AmbientProbeExtension` builds its
report from state it collects itself and prints it on failure, so a quiet run still gets a full
autopsy.

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
