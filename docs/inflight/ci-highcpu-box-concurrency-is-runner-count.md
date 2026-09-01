# Does the self-hosted highcpu box still earn its keep?

<!-- inflight-type: task -->
<!-- inflight-impact: ci -->

**The question changed on 2026-08-26**, when the Chaos Pain Suite moved to the GitHub-hosted gate
(measured no slower - [`ci.md`](../ci.md), "Chaos does not need the self-hosted box") and the
per-PR lane followed it off the box.

**Nothing runs here per-PR any more.** All three workflows targeting `highcpu` are
`workflow_dispatch` only:

- `chaos-pain.yml` - seeded chaos hunts.
- `mutation-full-sweep.yml` - the whole-tree PIT sweep.

**The sweep is dispatched far too rarely to be a measurement of anything** - its run history is
sparse enough that the whole-tree mutation score is effectively unmeasured. Check it rather than
trust this sentence:

```bash
gh run list -R astubbs/parallel-consumer --workflow mutation-full-sweep.yml
```

## Decided 2026-08-26: the full sweep runs nightly

It is wired **nightly**, after per-merge was tried and measured out. The workflow header had argued
for `push: branches: [master]`, and that argument assumes a quiet master - which this one is not: up
to 32 commits a day, a median gap of 0 minutes between them, and 83% of gaps shorter than the
sweep's own 31m27s job-elapsed runtime. Per-push either piles up dozens of concurrent sweeps on one box or, with a
cancelling group, has most of them killed before they finish - the never-completes failure this lane
was rebuilt to escape. [`ci.md`](../ci.md)'s scheduled-lane rule now records this as a deliberate
exception, with the measurement.

**A second, hosted arm runs alongside it** on `ubuntu-latest` at 4 threads (a RAM ceiling: PIT
minions get `-Xmx2g` each, so 16 threads would need 32g and OOM a hosted runner). Neither arm is
required. Compare their SCORES first, not their wall-clock: a hosted arm reaching the same score is
the argument for dropping the self-hosted one.

What made it affordable: the box now runs nothing per-PR, so it is otherwise idle, and the sweep is
measured rather than hoped-for - see the clock table above before quoting a number.

**Still to confirm:** that a nightly behaves the way the one measured dispatch did. Compare a run
against the baseline in [`ci-mutation-testing.md`](ci-mutation-testing.md) - a SCORE that moves
without a deliberate test change is the signal worth acting on, and the score is the sturdier half of
that baseline because it does not depend on which clock anyone read.

**Three clocks, and the difference between them is bigger than any effect anyone is measuring.**
The single prior sweep, run `30953843898` on 2026-08-04, reports as:

| Clock | Value | What it includes |
|---|---|---|
| Run wall | 40m50s | queueing for a runner (~9m22s of it) |
| **Job elapsed** | **31m27s** | checkout, JDK setup, dependency resolution, build, PIT |
| PIT/maven phase | 21m55s | only what `time ./mvnw` wrapped - the 1315s the coverage figure below is a fraction of |

**Use JOB ELAPSED for any scheduling question** - whether a push cancels a running sweep, whether it
fits a timeout - because that is how long the job occupies a runner. Use the PIT phase only when
comparing PIT work to PIT work. **n=1**: one observation supports "roughly this order of magnitude"
and nothing tighter, so do not read a few minutes either way as a regression.

That table exists because this single observation has already been misread three separate ways -
21m55s, 31m27s and 40m50s are all "the runtime" depending on which clock you pick up, and each was
quoted at some point as though it were comparable to the others. It is the same defect as the
"~1m54 solo" performance figure this branch rejected: an inherited number nobody re-measured, whose
units were never stated.

## The open decision

**Is a box that only serves manual dispatches and master pushes worth keeping?** Nothing per-PR
depends on it, so nothing breaks while it is offline - which also means nothing notices. The sweep
is now its main justification; the on-demand performance benchmark and seeded chaos hunts are the
rest. If the sweep turns out not to earn it either, the honest question is whether the hardware
earns its keep at all.

## Co-residency: mostly resolved, still worth a glance

No workflow caps concurrency on the box; how many jobs run at once is set by how many runner
processes it runs. That mattered most for chaos, which has left. The signature to watch for if the
box is ever overloaded is **not** a test failure but a job whose log stops dead mid-scenario and
fails with no `BUILD FAILURE`, no stack trace and no `##[error]` - recorded with a worked example in
[`ci-disabled-jobs-and-runner-load.md`](ci-disabled-jobs-and-runner-load.md). The lever is to run
fewer runner processes; it needs no change in this repository.

Background on why a `concurrency` group was the wrong tool for that job - it deduplicates rather
than queues, and discarded most of the lane's work - is in [`ci.md`](../ci.md), "Why a
`concurrency` group is not a mutex".

## Delete when

Both decisions above are made and acted on.
