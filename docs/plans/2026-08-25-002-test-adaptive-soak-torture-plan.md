# Soak and torture testing for adaptive concurrency: does it do what we think, everywhere, for hours

Owner's directive, 2026-08-25, at the close of the demo session that produced
`AdaptiveConcurrencyDemo`'s three plants: *"you need to design some great soak and torture tests
that check this does what we think it should in various different (dimensionally) scenarios."*
This is that plan. Requirements and units; implementation detail stays with the units.

## Prior-art check (per AGENTS.md, all six, results stated)

- `docs/plans/`: no soak or torture plan exists for anything. Nearest siblings:
  `2026-07-30-001-feat-chaos-pain-suite-design-plan.md` (protocol honesty under faults - a
  different question, see Boundaries) and `2026-08-24-003-feat-admission-control-law-design.md`
  (the law this plan exercises; its falsifier discipline binds here).
- `docs/solutions/`: `grep -rl soak` - nothing. The flakiness category carries the relevant
  scar tissue: never fit an assertion band to a run length
  (`AdaptiveConcurrencyClosedLoopIT` asserts only a derived, run-length-independent settled band -
  final-third range within two accelerator steps, median bounded by the constructed knee - never a
  band fitted to a run; that derived-band form is this plan's model).
- `docs/inflight/`: no soak item. `test-opt-in-engine-paths-are-unexercised.md` owns the
  mode-lane axis; `core-adaptive-concurrency-future-modes.md` now carries the parked pacing
  profile whose refuting experiment this plan's torture set inherits as a scenario shape.
- Open PRs: astubbs#333 (this stack), astubbs#354 (chaos lag bound reports rather than gates) -
  354's report-not-gate distinction is adopted wholesale here.
- Merged PRs by file: the falsifier suite, `AdmissionController`, the broker ITs - all this
  stack's own work; no third-party collision.
- Issues `--state all`: astubbs#227 tracks the feature; no soak-shaped issue exists.

## What this plan is NOT (boundaries, so three suites stay three suites)

- **Not the chaos suite.** Chaos asks *is the protocol honest under faults* (ordering, commit
  correctness, rebalance survival). This plan asks *does the CONTROLLER behave as designed over
  time and across workload shapes*. A chaos red is a correctness bug; a soak red here is a law
  or calibration finding.
- **Not the U10 bench arm.** The bench asks *how much does it help*, on real hardware, producing
  the publishable number. This plan asks *does it do what the design says*, and runs happily on
  the self-hosted runner where numbers are honest enough for invariants but never published.
- **Not the falsifier suite.** Falsifiers are deterministic, seconds-long, law-level proofs.
  This plan is their long-duration, dimensional extension - and every torture finding that CAN be
  shrunk to a deterministic falsifier MUST be (that is the pipeline: soak finds, falsifier pins).
- **Long-horizon is NOT the same axis as broker-attached, and the split is load-bearing.** The
  falsifier suite's deterministic plant advances exact simulated one-second windows, so a 12-hour
  trajectory is ~43,000 windows - minutes of compute, exactly reproducible, resonance schedules
  included. Every trajectory invariant that needs HORIZON (no-ratchet over hours, drift tracking,
  resonance behaviour) runs there, per-PR, in the simulated-horizon lane (U7). Only what needs
  WALL CLOCK - heap and thread trends, engine events against a real broker, real window density -
  earns a place in the real-broker soak (U5). A scenario in U5 that U7 could run is a scoping
  error, not extra coverage.

## The dimensions

The space is a cross product; the matrix runner (U3) samples it rather than exhausting it, but
every dimension must appear in at least one running scenario:

| Dimension | Values worth money |
|---|---|
| **Plant shape** | hard knee (semaphore); soft knee (CPU contention); congestion curve (ClosedLoopIT's quadratic); NO knee below ceiling (pure sleep - the law must ride to the cap and sit there); thrash curve (throughput FALLS past the knee - the law must not park on the far side) |
| **Capacity dynamics** | static; single step down/up (the demo's outage); slow drift (minutes-scale ramp - the baseline-contamination shape, now the law must track it); **oscillation at and near the law's own cadences** (settle cadence, probe cadences - the resonance torture: a plant that moves at the controller's rhythm is the adversarial case for any control law) |
| **Ordering x keyspace** | UNORDERED; KEY with uniform keys, zipf skew, and few-keys starvation (the ORDERING_STARVED lane must report, hold, and take only its bounded probe); PARTITION |
| **Arrival** | backlog drain (closed loop); arrival-controlled open loop below / at / above capacity; bursts (the catch-up shape, pre-feature) |
| **Outcome mix** | failure fraction swept around the 0.2 growth-freeze threshold (just below, at, just above); overload-drop bursts (the BACKOFF arm); mixed |
| **Engine events** | rebalance (single, periodic, storm - reuse the chaos conductor); pause/resume mid-probe; all three commit modes; partition counts 1 / 8 / 64 |
| **Scale** | floor-hugging (capacity 1-2 - the escape probe's home); mid (the demos' 8-48); wide ceilings (200+) |
| **Duration** | the torture set runs minutes; the simulated-horizon lane (U7) runs hour-scale WINDOW counts in minutes; the real-broker soak runs wall-clock HOURS (12h on master movement, 48h on demand) |

## What "does what we think" means - the assertion philosophy

Point assertions and fitted bands are how long tests rot. Every scenario asserts **trajectory
invariants**, derived from the plant's own construction, never from a previous run's numbers:

- **Safety (always):** target never exceeds the resolved ceiling; never below the floor; the
  target's rolling maximum STOPS GROWING within a derived settle horizon on any static plant -
  the no-ratchet invariant, which is the falsifier suite's core claim extended to wall-clock
  hours; oscillation amplitude at settle bounded by the accelerator step's own size.
- **Liveness:** a park is left within the recovery re-ask bound after capacity returns; on a
  static plant the settled throughput is within a derived fraction of the plant's constructed
  ceiling; the constraint gauge never reports the same non-terminal constraint unchanged for
  longer than its arm's documented cadence allows.
- **Comparative (soak only, report-not-gate):** settled adaptive throughput vs the plant's best
  static, settled avg request time vs the ceiling static - drift in these across nightly runs is
  a finding to read, per astubbs#354's lag-bound lesson, not a red build.
- **Resource:** heap stable across the soak (no growth trend), thread count flat, both log
  channels' volume bounded (the probe channel earns its keep here).
- **Signal integrity:** movements counter equals observed target changes; the trajectory CSV's
  final state equals the controller's; a window's aggregates internally consistent. Instruments
  that lie are worse than no instruments - `docs/investigating.md` owns why.

## Units

- **U1 - the plant library.** Extract `AdaptiveConcurrencyDemo`'s three plants into reusable
  synthetic-downstream components with a capacity SCHEDULE (static / step / drift / oscillation as
  data, not code). ClosedLoopIT's curve is ALREADY shared
  (`integrationTests/utils/SyntheticCongestionCurve`, mirrored by the falsifier suite's
  `DeterministicPlant`) - the library extends that base rather than re-extracting it. The demo and
  the ITs re-consume the result; one definition per plant, per the perf-matrix track's
  one-definition rule.
- **U2 - the trajectory recorder and invariant kit.** A per-run CSV (window, target, rate,
  avg request, constraint, in-flight) plus the invariant assertions above as a reusable checker.
  The demo's ticker becomes a consumer of this rather than its own arithmetic.
- **U3 - the matrix runner.** Config-driven scenario = plant x schedule x ordering x arrival x
  outcome-mix x events; time-boxed arms; emits one verdict line + CSV artifact per scenario.
  Sampled matrix checked in; exhaustive sweeps are a flag away. Two coverage obligations, both
  enforced by the runner itself: (a) every value in the dimension table is exercised by at least
  one checked-in scenario, asserted FROM the table so dropping a dimension in a refactor goes red
  rather than silently thin; (b) the checked-in sample achieves pairwise coverage across the named
  high-risk dimension pairs - at minimum plant-shape x capacity-dynamics, ordering x engine-events,
  and outcome-mix x arrival - and the runner emits a coverage summary stating which pairs the
  current sample exercises, because marginal coverage of eight dimensions can be satisfied by
  eight scenarios that jointly exercise zero interactions, and interactions are what the matrix
  exists to catch.
- **U4 - the torture set (minutes, CI-attached).** The adversarial scenarios, each falsifier-
  first with a born-red history: cadence-resonant oscillation; thrash curve; knee-below-seed;
  knee-at-floor; failure fraction riding the threshold; rebalance storm mid-probe (chaos
  conductor reused); the parked pacing experiment's shape as a regression pin. The conductor
  reuse is not free, and the three gaps are in scope here (or in U1/U2 where they fit better):
  `ManagedPCInstance.Config` carries no adaptive-concurrency options and no seed; the harness
  uses the single-arg processor constructor, so there is no `PCModule` to reach the
  `AdmissionController` through (the closed-loop IT reaches it only by constructing the module
  itself), and each RESTART incarnation builds a fresh controller - which resets the trajectory
  the recorder tracks and must be recorded as such; and "mid-probe" needs a probe-state
  observable on the controller (a public active-probe accessor beside the decision-reason one),
  or the scenario silently degrades to "storm at some unknown time".
- **U5 - the real-broker soak lane.** 12h runs on the self-hosted highcpu runner, never per-PR -
  and NOT on a cron: `docs/ci.md`'s policy is "no scheduled build, deliberately", with the
  admission test *does time alone change the answer?*, and a soak's answer changes when master
  moves, not when the calendar does. Trigger the 12h arm on push-to-master and the 48h arm by
  `workflow_dispatch`; if a cron is ever genuinely wanted, U6 carries the ci.md policy update that
  argues the exception and names who reads a scheduled red. The workflow must set
  `timeout-minutes` above the soak duration - GitHub's default 360-minute job timeout applies to
  self-hosted runners too and would kill the 12h run at 6h as a mysterious cancellation; the 48h
  arm sits inside the self-hosted per-job execution cap. Artifacts retained; invariant breach =
  red, comparative drift = report. Wiring per `docs/self-hosted-runner.md`.
- **U6 - the records.** `docs/testing.md` gains the three-suite boundary table (chaos / soak /
  bench) - disambiguating the existing `bin/soak-test.sh` (single-test flake resurfacing under
  load; the lane's own script takes a non-colliding name). The findings ledger convention grows
  both output branches: a soak finding that CAN be shrunk becomes an inflight note, then a
  falsifier, then a fix - never a band loosened; a finding that RESISTS deterministic shrinking
  (a heap trend, a real-broker-density effect) gets an inflight note carrying the trajectory CSV
  and the shrink attempts that failed, and stays open rather than silently exiting the pipeline.
  Comparative drift gets a named reader: the run's verdict line lands in the job summary and the
  drift check happens at the next PR's merge prep (the cadence a solo-maintainer repo actually
  has), not on an unowned schedule. Trust-pack deliverable 2 cites the soak invariants as
  standing evidence.
- **U7 - the simulated-horizon lane (per-PR, minutes).** The U3 matrix run against the
  deterministic plant library for hour-scale WINDOW counts (12h = ~43k windows), covering every
  wall-clock-independent trajectory invariant - no-ratchet over hours, drift tracking, resonance -
  deterministically and per-PR. This is the primary vehicle for the plan's core claims; U5
  carries only what U7 cannot (see the boundary above).

## Sequencing and gates

U1+U2 first (they pay for themselves in the demo immediately); U3 before U4 (torture scenarios
are matrix-runner configs); U4 and U7 before U5 (torture scenarios are the soak's content, and
the simulated-horizon lane proves the invariant kit before wall-clock hours are spent on it); U6
lands incrementally alongside whichever unit its records describe, finishing with U5. U5 gates on
nothing in PR astubbs#333 and lands separately. Nothing here gates the astubbs#333 merge; the
ladder in `core-adaptive-concurrency-future-modes.md` (ceiling clamp first) is unchanged - but
the clamp, when built, inherits this plan's harness for its own scenarios, which is half the
reason to build U1-U3 well.

## Deferred / Open Questions

### From 2026-08-25 review

- **What is a construction-derived invariant for a DYNAMIC schedule?** Every settle/liveness
  invariant stated above is scoped to static plants; the torture headliners (resonance, drift,
  thrash) currently have only the safety invariants. Candidate shapes: bounded oscillation
  amplitude, bounded time-averaged regret against the schedule's oracle trajectory, or
  safety-bounds-only with the trajectory reported rather than asserted. Decides whether U2's
  invariant kit needs a schedule-aware oracle. (adversarial review, P1)
- **Which plants are invariant-safe on shared hardware?** The CPU-contention plant's ceiling is a
  property of the host and its co-tenants, so on the shared runner a "derived fraction of
  constructed ceiling" is not derivable from construction at all. Options: certify the CPU plant
  torture-only on an uncontended arm, run it in U7's simulated form only, or establish the
  runner's noise envelope empirically first. (adversarial review, P1)
