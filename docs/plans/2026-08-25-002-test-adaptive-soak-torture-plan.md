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
  (`AdaptiveConcurrencyClosedLoopIT` deliberately asserts no band for exactly that reason).
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
  This plan is their long-duration, broker-attached, dimensional extension - and every torture
  finding that CAN be shrunk to a deterministic falsifier MUST be (that is the pipeline: soak
  finds, falsifier pins).

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
| **Duration** | the torture set runs minutes; the soak runs HOURS (first target: 12h nightly, 48h weekly) |

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

- **U1 - the plant library.** Extract `AdaptiveConcurrencyDemo`'s three plants plus
  ClosedLoopIT's curve into reusable synthetic-downstream components with a capacity SCHEDULE
  (static / step / drift / oscillation as data, not code). The demo and the ITs re-consume them;
  one definition per plant, per the perf-matrix track's one-definition rule.
- **U2 - the trajectory recorder and invariant kit.** A per-run CSV (window, target, rate,
  avg request, constraint, in-flight) plus the invariant assertions above as a reusable checker.
  The demo's ticker becomes a consumer of this rather than its own arithmetic.
- **U3 - the matrix runner.** Config-driven scenario = plant x schedule x ordering x arrival x
  outcome-mix x events; time-boxed arms; emits one verdict line + CSV artifact per scenario.
  Sampled matrix checked in; exhaustive sweeps are a flag away.
- **U4 - the torture set (minutes, CI-attached).** The adversarial scenarios, each falsifier-
  first with a born-red history: cadence-resonant oscillation; thrash curve; knee-below-seed;
  knee-at-floor; failure fraction riding the threshold; rebalance storm mid-probe (chaos
  conductor reused); the parked pacing experiment's shape as a regression pin.
- **U5 - the soak lane.** Scheduled (nightly 12h / weekly 48h) on the self-hosted highcpu
  runner, never per-PR; artifacts retained; invariant breach = red, comparative drift = report.
  Wiring per `docs/self-hosted-runner.md` and `docs/ci.md`'s lane conventions.
- **U6 - the records.** `docs/testing.md` gains the three-suite boundary table (chaos / soak /
  bench); findings ledger convention (a soak finding becomes an inflight note, then a falsifier,
  then a fix - never a band loosened); trust-pack deliverable 2 cites the soak invariants as
  standing evidence.

## Sequencing and gates

U1+U2 first (they pay for themselves in the demo immediately); U4 before U5 (torture scenarios
are the soak's content); U5 gates on nothing in PR astubbs#333 and lands separately. Nothing here
gates the astubbs#333 merge; the ladder in `core-adaptive-concurrency-future-modes.md` (ceiling
clamp first) is unchanged - but the clamp, when built, inherits this plan's harness for its own
scenarios, which is half the reason to build U1-U3 well.
