# Two overnight instruments, and neither can reach the lane that already exists

<!-- inflight-type: task -->
<!-- inflight-impact: ci -->

`bin/torture-overnight.sh` and `bin/soak-deadlock-probe.sh` are both overnight soak instruments. Both
are run by hand or not at all. **The gap is not that a scheduled soak lane needs building** - one
exists and is the right shape - it is that neither script fits its contract, and both mismatches are
design questions rather than renames.

## The lane that exists

`.github/workflows/experiments.yml` runs on `workflow_dispatch` **and** on a schedule, with a
240-minute timeout, a tally-collection step and an artifact upload. `mutation-full-sweep.yml` shows a
cron lane is established practice here, so nothing new has to be argued for.

Its contract is narrow and deliberate:

- the script must be `bin/exp-<name>.sh`,
- `<name>` must be in the workflow's hardcoded allowlist (an allowlist, not a glob, because the input
  is data and must never become code),
- it is invoked as `bin/"$EXPERIMENT".sh ${ITERATIONS}` - **one positional argument**, an iteration
  count the workflow validates before passing.

## Mismatch 1: the A/B probe needs a second tree, and the lane can pass one number

`soak-deadlock-probe.sh` takes `FIXED_TREE CONTROL_TREE [INVOCATIONS]`. The control arm is the whole
point - it is the arm that must deadlock - and there is nowhere in the contract to name it.

**This is not solved by re-ordering the arguments.** A CI run has one checkout, so a control arm has
to be *manufactured*: revert the fix into a scratch worktree, run both, discard it. That is
mechanical - the local A/B this session ran did exactly that with `git checkout <pre-fix-sha> -- <one
file>` - but it puts "which commit is the control" inside the script or inside the workflow input,
and both are decisions with a blast radius. A workflow input naming a SHA to revert is data that
selects code, which is the shape the allowlist exists to prevent.

## Mismatch 2: the torture harness is an eight-hour design against a four-hour cap

Its whole method is hours of short cycles with a watchdog taking a thread dump before each kill;
`branch-overnight-torture-harness.md` records a 214-cycle run. The lane caps at 240 minutes.

Two ways out, and they are not equivalent. **Raise the cap** on the self-hosted runner, which is
where a long soak belongs anyway and which no hosted runner would tolerate. Or **cut the run to fit**,
which changes what the instrument measures: the value of a soak is in the tail, and a four-hour soak
is not a shorter eight-hour soak, it is a different experiment with a worse chance of catching the
thing.

## What is NOT in question

Neither script needs rewriting to be scheduled. Both already write a machine-readable tally, both
already classify from the failsafe report rather than maven's exit code, and both already treat a run
that executed no test as not-a-data-point - which is the discipline the lane's own tally step assumes.

## The decision this is waiting on

Whether a soak lane is worth the runner time at all. Both instruments were written during one
investigation and neither has run since it ended, which is weak evidence for their value and no
evidence against it - nothing has asked them a question lately. Until somebody decides, the honest
<!-- post-merge: checked - past tense and names the PR, so it reads as a record once that PR has landed -->
state is that astubbs/parallel-consumer#405 shipped two instruments with no trigger, and this note
exists so that is a recorded choice rather than an oversight discovered in six months.
