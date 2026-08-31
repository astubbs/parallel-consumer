# Handoff: the overnight torture harness, and the state of the confluentinc#857 work behind it

<!-- inflight-type: task -->
<!-- inflight-impact: coordination -->

For an agent picking this up with no context. Written 2026-08-28 at the end of a long session; the
detail behind every claim is in the notes cited, not repeated here.

## What you are inheriting

`bin/torture-overnight.sh`. An MVP spike, not finished work. Run it overnight on the highcpu rig:

    bin/torture-overnight.sh                  # 8 hours, 30-minute cycles
    bin/torture-overnight.sh --cycles 1       # one cycle, then stop - the smoke test
    bin/torture-overnight.sh --minutes 30     # a short run
    bin/torture-overnight.sh --list           # the rotation, and the mode each scenario hardcodes

It rotates chaos scenarios - scenarios only, never commit modes, and below is why that distinction
is load-bearing - gives each cycle a hard wall-clock budget, **takes a thread dump before killing
anything that overruns**, and packages every cycle's logs, failsafe reports and siloed log streams
into a tarball with a `SUMMARY.md`. The morning review should need the summary and the `dumps/`
directory, nothing else.

**The dump-before-kill is the point of the whole design.** A hang with no stack is a rumour; the six
thread dumps that identified the revoke deadlock are the only reason it stopped being a signature and
became a mechanism.

## What it is hunting, and why those

The AB-BA revoke deadlock is **fixed and verified** - see below. What remains unaccounted for:

- **The unbounded revoke wait in transactional mode.** Carries astubbs#44 / confluentinc#803, the
  only issue upstream ever labelled a verified bug. **This harness does not hunt it** - no chaos
  scenario is built for `PERIODIC_TRANSACTIONAL_PRODUCER`, so no rotation can reach it; see "Known
  gaps that remain" below.
  See `bug-857-transactional-revoke-wait.md` - its design decision is explicitly unsettled, so **do
  not write a fix before settling it with Antony**.
- **Commit-response timeouts**, reported in the field twice and never reproduced -
  `bug-177-commit-response-timeout-unreproduced.md`.
- **Silent data skip.** confluentinc#875 describes an offset never delivered, lag growing, and a
  restart making it reappear. That is not a liveness failure and no liveness detector will see it.
  Completeness is asserted, but only end-of-cycle - what is still missing is below.

## State of the investigation - what is settled, so you do not redo it

- **The deadlock is verified.** A/B on `Rebalance857CommitSyncDeadlockProbeIT`, one term changed:
  control failed every repetition, the fix failed none and logged the contended decline throughout.
  ~240 repetitions per arm. `test-857-deadlock-ab-soak-harness.md`.
- **The async `NO_PROGRESS` line is a TIMING PROXY, not a defect.** Six firings, six drains.
  `test-857-churn-storm-async-stalls.md`.
- **The `CLASS2_STALL` line was demoted the same way** on 2026-08-25. Roughly half the family ledger
  is superseded; every sighting now carries a STATUS line saying which.
- **Six more family defects landed than astubbs#119's status counts** - astubbs#346, astubbs#345,
  astubbs#373, astubbs#336, astubbs#344 and astubbs#349. The issue's `## Fork status` needs rewriting, not appending to.
- **`largeNumberOfInstances` does not reproduce here** - 19 green across three scales. But that is
  evidence about an M2 desktop, not about the code.

## The one habit that mattered most

**Five times this week a measurement error, not the system, was the answer.** The reproducer was
inverted; a probe's window never opened; a test was reshaped between the claim and the measurement; a
grep was narrower than the question; a classifier labelled drained runs flat. Every one produced a
confident wrong answer and every one cost a single command to catch.

So: **before believing any result, check what actually ran.** Did the test execute? Did the code path
fire? Is the file you grepped the file that holds the answer? A green run is evidence only about the
thing that ran.

## Gaps that were checked, and what checking them found

**`-Dchaos.commitMode` was NOT honoured. No such property has ever existed** - it appeared only in
this script and in this note, so every cycle ran the mode its scenario hardcodes while the directory
names, the tally and the summary all said `PERIODIC_TRANSACTIONAL_PRODUCER`. The labels are gone and
each cycle now reports the mode it OBSERVED. **`bin/torture-overnight.sh`'s header owns the detail** -
which class hardcodes which mode, why plumbing a real property is not a one-liner, and why the
decision belongs with the one `bug-857-transactional-revoke-wait.md` says not to settle alone.

The part worth carrying beyond that script is **the tell**: `-Dchaos.seed` and `-Dpc.log.dir`, on
adjacent lines, are real and used by several other scripts in `bin/` (`grep -rl chaos.seed bin/`).
The pattern was copied and the third member of it was never confirmed. A flag sitting among working
flags looks like one.

**The completeness gap is real but narrower than this note used to claim.** An independent
delivery check already exists - `ChaosScenarioBase.assertScenarioSlos` runs one, and the script
header's `COMPLETENESS` block owns what it does and does not reach. So **the missing thing is a
TIME-BOUNDED claim** (delivered within N seconds of production), not an independent one, and that
is a different and larger piece of work. `kafka-verifiable-producer` is still the cheap way in.

**The cycle-budget worry was wrong**: the watchdog returns as soon as the build exits, so a fast
cycle never burnt its slot. A real defect was hiding next to it and is fixed - liveness was tested
*before* the sleep rather than after, so a clean 1m48s pass was reported `HUNG-NO-DUMP` because it
finished nine seconds inside a two-minute budget.

## The first real run, 2026-08-29

An overnight rotation of the five chaos scenarios, **no hangs and a handful of failures, every one
of them `ChaosChurnStormIT`** - the other four scenarios were clean all night. The correctness
ledger balanced on every completed run: no data loss anywhere. The rates and per-scenario tallies
belong to the notes below and to the run's own `SUMMARY.md`.
[`test-857-churn-storm-async-stalls.md`](test-857-churn-storm-async-stalls.md) owns the four
sightings, their three distinct signatures and their seeds; the two `NO_PROGRESS` ones are also in
[`test-no-progress-window-may-not-transfer-to-w1.md`](test-no-progress-window-may-not-transfer-to-w1.md).

What the run exposed in the harness itself is fixed and covered in `bin/torture-overnight.sh`'s own
header, which owns each rationale: the stall-recovery diagnostic was never passed, so nothing could
be classified; `loss=0` meant "not measured" on exactly the failing runs; the drain verdict read a
wedge as a recovery, twice, by window and then by magnitude; and `jstack` had no deadline, so one
unresponsive JVM could consume a whole night. **The transactional gap is NOT among them** - what was
fixed there is the harness's false CLAIM to coverage, not the coverage. It is still open below.

**Machine-local:** the run's artefacts are `~/pc-soak-runs/torture-20260829T210914Z.tar.gz` on
Antony's desktop, copied out of `/tmp` before it was reaped and checksum-verified. Nothing in the
repo depends on it - the seeds above are the part that had to survive.

## Known gaps that remain

- **No time-bounded delivery assertion** - see above. This is the data-skip hunt, and it is design
  work, not plumbing.
- **No transactional coverage, and it cannot be fixed by widening the rotation.** No chaos scenario
  is built for that mode. The vehicles are `RebalanceEoSDeadlockTest` and `TransactionTimeoutsTest`,
  both outside the chaos group, reachable via `--groups`. Whether repeating either actually hunts the
  unbounded revoke wait is still open.
- Not containerised. A desktop passes everything; the constrained rig is where these defects live.
  `test-pc-soak-harness-architecture.md` has the design, including what to reuse rather than build.
- The run is foreground. For an unattended 8 hours, `nohup ... &` it and poll for the `DONE` marker
  the script writes into its output directory - an agent cannot hold an hour-long foreground call.

## Where things sit

<!-- post-merge: checked-begin - written as it reads once the confluentinc#857 work has merged: the
     harness is on master, the branch that carried it is gone, and the mentions below say what that
     work CONTAINED rather than that it is open -->

The harness was built on top of the confluentinc#857 work rather than on master, and depended on it:
it packages the siloed log streams, and it drives the stall-recovery diagnostic, whose LIFT to
`ChaosScenarioBase` was not on master - the flag itself is older, and on master the churn scenario
accepted it and ignored it. **Cut from master the harness still RUNS, which is the trap:**
`-Dpc.log.dir` is likewise accepted and does nothing there, so the specialised logs would be empty
and a morning review would open a tarball of nothing.

**Both of those dependencies left in the other direction**, to
astubbs/parallel-consumer#381, which was cut fresh from master and carries the log silos and the
diagnostic lift. The harness's real dependency is therefore that work, not the deadlock fix - what
tied it to the 857 branch was the packaging it needed and the order things happened in, not the fix.
The trap above is unchanged in shape: run it against a tree without those pieces and it produces an
empty tarball rather than an error.

`feats/overnight-torture-harness` was then absorbed into the 857 branch whole rather than carried as
its own PR, because it contained that branch in its entirety - a PR from it would have shown the
whole 857 diff to review a harness and its notes. Nothing depends on the ref.

<!-- post-merge: checked-end -->
