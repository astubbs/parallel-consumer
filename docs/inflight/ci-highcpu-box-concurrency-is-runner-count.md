# Is six concurrent jobs too many for the highcpu box?

<!-- inflight-type: task -->
<!-- inflight-impact: ci -->

**The question is now about runner count, not about a workflow.** No workflow caps concurrency on the
highcpu box any more; how many jobs run at once is decided by how many runner processes the box runs.
Whether that number is right is an operator question, and it is open.

The count and their state are a command, not a fact to record here:

```bash
gh api repos/astubbs/parallel-consumer/actions/runners \
  --jq '.runners[] | {name, status, busy, labels: [.labels[].name]}'
```

## What is actually open

**Watch for the co-residency signature and, if it appears, reduce the runner count on the box** -
that is the whole lever, and it needs no change in this repository.

The signature to watch for is **not** a test failure. It is a job whose log stops dead mid-scenario
and which then fails with no `BUILD FAILURE`, no stack trace and no `##[error]` - the process was
killed rather than reporting anything. It is recorded with a worked example in
[`ci-disabled-jobs-and-runner-load.md`](ci-disabled-jobs-and-runner-load.md), which last confirmed it
still happens on 2026-08-17. A reader grepping such a log for a failing test finds nothing, which is
what makes this class expensive to diagnose twice.

## Why the previous answer to this was withdrawn

A repo-wide `highcpu-box-exclusive` concurrency group served as a box mutex for about a day. It was
the wrong primitive - a concurrency group holds one running plus at most one pending and discards the
rest, so it deduplicates where a mutex must queue - and it discarded the large majority of all box
jobs while runners sat idle. The measurement and the reasoning are in
[`ci.md`](../ci.md) under "Why a `concurrency` group is not a mutex".

Two things that were true when that mutex was argued for and are no longer:

- The reds that motivated it were a **timing bound**, `~154s lagStagnation` against a 150s bound, and
  that detector was demoted to non-gating in the same pull request that added the mutex - so the
  failure mode it was bought to prevent is handled in the instrument. See
  [`a-timing-bound-used-as-a-correctness-gate-manufactures-its-own-evidence.md`](../solutions/best-practices/a-timing-bound-used-as-a-correctness-gate-manufactures-its-own-evidence.md).
- **A dedicated single-slot runner label is no longer the pending better shape.** Reducing the number
  of runner processes achieves the same serialisation with no new label, and so without the trap that
  a job pinned to a label nothing serves does not fail but queues silently
  ([`self-hosted-runner.md`](../self-hosted-runner.md)).

## Delete when

Either the co-residency signature has been watched for over a busy week and not seen, or the runner
count has been changed in response to seeing it.
