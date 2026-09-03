---
title: "A red under a lane-shape change is not evidence against the change until a seeded control arm on the gate's own configuration says so"
date: 2026-09-03
category: best-practices
module: parallel-consumer-core
problem_type: best_practice
component: testing_framework
severity: high
applies_when:
  - "A test or lane goes red immediately after a change to how it runs (fork count, sharding, parallelism, runner class) and the change is the obvious suspect"
  - "About to write up a red as caused by a change without having run the identical seed on the gate's own unchanged configuration"
  - "Choosing which machine or runner class to replay a seed on, to settle whether a failure reproduces"
  - "A probe already has a ledger of past firings and one more looks like it confirms the pattern"
  - "Deciding whether a clean replay means the defect is absent, or only that it was not replayed under the load that fired it"
related_components:
  - development_workflow
  - infrastructure
tags:
  - control-arm
  - chaos-testing
  - seeded-replay
  - runner-class
  - flaky-tests
  - investigation-method
  - attribution
---

# A red under a lane-shape change is not evidence against the change until a seeded control arm on the gate's own configuration says so

## Context

astubbs#421 sped up the Chaos Pain Suite, the eight `@Tag("chaos")` failsafe classes in
`parallel-consumer-core` that gate every PR. The first candidate was the integration lane's own
pattern, `-DforkCount=2 -DreuseForks=true`: two JVM forks, one broker each, on one `ubuntu-latest`
VM. Its first sample halved the wall-clock and went RED. `ChaosChurnStormIT` was killed by the
gating liveness probe, `INSTANCE_STALL/NO_WORK_COMPLETED` (one instance holding work and returning
no result for the bound), on seed `1630088991107806597`, and the class ran about twice its one-fork
duration that day.

The tempting reading writes itself: two forks add CPU contention on a 4-vCPU VM, the
`docs/inflight/bug-857-family.md` ledger already calls this detector family load-sensitive, so the
change caused it. The same ledger also recorded the same probe firing on the same class under one
fork on master-state CI before this run, and one of those seeds had replayed clean once on a
32-core box off the runner. Both facts were in hand. Neither settles whether *this* red belongs to
the change or to the rate the gate already had, and a single sample cannot.

`ProgressProbe` defines the two bounds involved as `INSTANCE_STALL_BOUND` (150s) and
`NO_PROGRESS_WINDOW` (30s). Both are timing proxies in the sense `CONCEPTS.md` gives the term: a
busy fleet and a wedged one both stop advancing the watched value, so every crossing needs a second
experiment before it means anything.

## Guidance

When a seeded chaos scenario, or any timing-probed test, goes red right after a change to *how* the
lane runs rather than to the code under test, do not attribute the red to the change until you have
run a **control arm on the gate's own unchanged configuration**: same seed, same runner class,
dispatched in the same hour as the changed-shape arm. `docs/investigating.md`, "A fix that works is
not evidence of the cause", owns the general rule (change one term, state the prediction first,
report refuted predictions as prominently as held ones). What this doc adds is which term to hold
fixed and where to run it:

- **The control is the gate as it runs today, not a bigger or quieter machine.** The ledger's earlier
  off-runner replay on a 32-core box came back clean and had been read as "not a reproducer". It had
  answered a different question. A load-shaped stall needs the load, so the replay that can settle
  attribution runs on the runner class that fired it. `docs/solutions/best-practices/a-stress-probes-calibration-is-a-claim-about-one-machine.md`
  states the same rule for probabilistic bounds.
- **Write the prediction table before either arm finishes**, and let the result that matches no row
  be the finding rather than a reason to re-run. On 2026-09-03 the changed-shape arm passed and the
  unchanged-configuration arm failed, which no row predicted; that is what made the reading
  trustworthy.
- **One sample is not a rate.** Two reds in three runs of one seed is a rate; one red under a new
  shape is a sighting. Ask for the existing rate before reading the new sighting:
  `node bin/inflight.mjs codecov test <class>` prints the recorded outcome per commit from history
  that outlives a CI log, and `codecov flaky` lists every test ever recorded with more than one
  outcome. Two limits of that record matter here: it is page-bounded, so an absence proves nothing,
  and runs dispatched through the measurement route below are deliberately kept out of the Codecov
  uploads, so a red seen on a throwaway ref reaches the sighting ledger or nowhere. (session history:
  a nine-hour torture run of the same family put a background rate on this class weeks earlier, with
  every sibling scenario green across the same cycles, and that rate is the reference a new red is
  read against.)
- **Record the seed, both arms' run ids and the prediction in the ledger before anything is re-run.**
  Artifacts and job logs expire; the ledger section is the seed's durable home whichever way the
  arms come out. `docs/solutions/test-flakiness/collect-more-firings-not-more-seeds-2026-09-01.md`
  is the sibling rule for what to collect next.
- **A green re-run on the same tree is never a clearance.** (session history: the fleet-orchestrator
  session recorded a same-tree pass/fail pair with its seed in the ledger and explicitly declined to
  treat the pass as proof the red was spurious.)

The dispatch route that makes the two arms cheap is `.github/workflows/maven.yml`'s
`workflow_dispatch` block with its `suite` input: one suite of the PR matrix runs against any
throwaway ref with no PR, the other matrix jobs skip their suite step, and dispatched runs stay out
of the Codecov uploads. `bin/chaos-test.sh` stays the single entry point and reads the seed from the
`CHAOS_SEED` env (data, never spliced into a script string); it does not forward arbitrary extra
Maven arguments, so a changed-shape arm is a temporary edit to the matrix `cmd` on the throwaway ref.

## Why This Matters

Acting on the first red alone would have blamed forking, and the write-up would have read as a
finding about contention. The control arm showed the opposite: the two-fork arm with the same seed
passed, and the one-fork arm, the gate exactly as it runs today, failed on the same class on a
different gating probe, `NO_PROGRESS`, with the fleet a few thousand records short of its total. Two
reds in three runs of one seed across both lane shapes and two detectors says the seed reproduces a
real stall on this runner class, and says nothing about forking. The same probe then fired again on
the sharded lane's first PR run with yet another seed, one fork, its own VM. The ledger section dated
2026-09-03 in `docs/inflight/bug-857-family.md` carries every run id and names the next experiment,
the stall-recovery diagnostic, on one of those seeds.

Forking was still set aside, but on the right grounds, recorded in `docs/ci.md` under "Chaos runs
as four shards": sharding across VMs gives each shard the gate's own configuration and so inherits
the pre-existing rate without adding contention to it, and failsafe lacks the per-fork log-silo
wiring surefire has. Had the red been charged to forking, the pre-existing detector instability
would have stayed unrecorded and the next red on the sharded lane would have looked like a
regression.

(session history: the same two-arm shape settled an earlier `NO_PROGRESS` seed on this class in
late August. A first fix-branch-versus-master replay read as branch-caused, a malformed log row
made that untrustworthy, and a corrected replay of two seeds, two repetitions, both arms showed the
seed reproducing on unmodified master, which reclassified it as master-state. A second suspected
cause was refuted the same way, with system load logged per repetition so that a control failing at
least as often under heavier load refuted contention on its own terms rather than outvoting it.)

## When to Apply

- Any CI or harness change that alters concurrency, contention, scheduling or runner class (fork
  count, sharding, thread parallelism, moving a lane between self-hosted and hosted runners) and is
  judged against a suite carrying timing-bounded liveness or progress probes.
- A single sample, pass or fail, immediately after such a change, before any sentence about
  causation is written into a PR body, commit message or ledger.
- Any failure whose message names a bound rather than a hard invariant: those gates manufacture
  their own evidence under load, and a red next to a load-shape change is exactly the ambiguous case
  a control arm resolves (`docs/solutions/best-practices/a-timing-bound-used-as-a-correctness-gate-manufactures-its-own-evidence.md`).
- Reading a clean replay: ask which machine it ran on before reading it as "does not reproduce".

## Examples

The two-arm recipe as it can be run today.

1. Cut two throwaway refs from the same base. Arm A carries the changed shape (for the 2026-09-03
   case, a chaos matrix `cmd` of `bin/chaos-test.sh -DforkCount=2 -DreuseForks=true`, which needs a
   one-line forwarding edit in the script on that ref). Arm B is the unmodified matrix: the gate's
   configuration exactly. Both set the seed for the chaos jobs, e.g. `CHAOS_SEED: 1630088991107806597`
   in the step's `env`, so both replay the identical schedule.
2. Dispatch both in the same hour:

   ```
   gh workflow run maven.yml -R astubbs/parallel-consumer --ref <arm-a-ref> -f suite=chaos
   gh workflow run maven.yml -R astubbs/parallel-consumer --ref <arm-b-ref> -f suite=chaos
   ```

3. Write the prediction table before either finishes:

   | Outcome | Reading |
   |---|---|
   | A fails, B passes | contention from the changed shape manufactured the red |
   | Both fail | the seed reproduces a real stall independent of lane shape |
   | Both pass | the seed is not a deterministic reproducer on this runner class either |

4. Read the result against the table. On 2026-09-03 the result was A passes, B fails, a row the
   table did not have. The honest reading combines the second and third rows: the seed reproduces on
   this runner class (two reds in three runs, on two detectors) but not deterministically, and the
   changed shape is not what fired it. That reading went into the ledger with both run ids, and the
   candidate's fate was decided on its own merits rather than on this red.

A note on the fork flag (session history): surefire ignores a bare `-DforkCount` here because the
pom pins `${surefire.forkCount}`, and only `-Dsurefire.forkCount` reaches it; failsafe declares no
fork count, so `-DforkCount` does reach the chaos run. An arm that silently ran unforked would be a
matched pair and a vacuous result, so confirm from the per-class timings that the forks actually ran
before reading either arm.

## Related

- `docs/inflight/bug-857-family.md`, section "2026-09-03, `INSTANCE_STALL` fires a third time - on a
  chaos lane running two forks, with control arms dispatched the same hour": the incident, both run
  ids, the seed, and the sighting that followed on the sharded lane.
- `docs/ci.md`, "Chaos runs as four shards", paragraph "Two things that did not survive measurement":
  the decision this experiment fed.
- `docs/investigating.md`, "A fix that works is not evidence of the cause": the general control-arm
  rule this doc applies to one shape of question.
- `docs/solutions/best-practices/a-timing-bound-used-as-a-correctness-gate-manufactures-its-own-evidence.md`:
  why these detectors need the discipline at all.
- `docs/solutions/best-practices/a-stress-probes-calibration-is-a-claim-about-one-machine.md`: why
  the replay must run on the runner class that fired it.
- `docs/solutions/best-practices/reverting-half-a-fix-is-not-a-control-2026-09-01.md`: the sibling
  rule that a control arm is the real state checked out, not an approximation of it.
- `docs/solutions/best-practices/an-a-b-whose-arms-run-in-time-order-is-confounded-with-time.md`:
  why both arms are dispatched in the same hour.
- `docs/solutions/test-flakiness/collect-more-firings-not-more-seeds-2026-09-01.md`: the same class,
  two days earlier, on what to collect once a probe has fired.
- `docs/solutions/test-flakiness/parallel-integration-tests-flaky-under-concurrency-2026-07-28.md`:
  the integration lane's version of load-sensitive failures under changed parallelism.
- Two siblings that exist only on the astubbs#271 branch as of this writing, found with
  `node bin/inflight.mjs prior-art 'control arm'`: "A control arm must vary exactly one term, not a
  term and whatever it silently derives from it" (the co-variation trap inside an arm) and "Chase the
  refuted prediction" (what to do in the hour after one arrives). This doc is about which arm to run
  and where; those two are about building it and about acting on its result.
