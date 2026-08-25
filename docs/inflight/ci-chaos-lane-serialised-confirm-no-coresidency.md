# Confirm the serialised chaos lane actually stopped the co-residency

<!-- inflight-type: task -->
<!-- inflight-impact: test-debt -->

**One open item.** The chaos lane was serialised on 2026-08-25 so that at most one chaos suite runs
on the self-hosted box at a time. **Confirm the co-residency rate is actually zero** after a full
week of chaos runs on that lane, then `git rm` this file.

The check costs a second, and is worth running against any chaos RED before diagnosing it:

```bash
gh api repos/astubbs/parallel-consumer/actions/runs \
  --jq '.workflow_runs[] | select(.status=="in_progress") | "\(.name) \(.head_branch)"'
```

## Why this is worth confirming rather than assuming

The fix changed a concurrency **group key**, and the failure it replaced was itself a group doing its
stated job correctly while its stated job was the wrong one - `highcpu-<suite>-<ref>` deduplicated
*within* a branch and did nothing *across* branches, on a box shared by several runner processes.
A key that is wrong in a new way would look identical from here: green runs, no error, and a
contended box. Only counting concurrent chaos jobs during a busy window distinguishes them.

**What the serialisation is worth, measured before it landed:** with the old key, several agent
sessions each pushing to their own PR put `Chaos Pain Suite` on all six runner slots at once, each
starting twenty-plus PC instances against its own broker - four of six red in each of two windows, on
six different seeds, every one ~154s against a 150s bound. That is the bound meeting the load.

## What this note no longer needs to carry

The diagnosis and the decision behind it are settled and recorded where they belong, so they are not
repeated here:

- **Why a ~154s `CLASS2_STALL` peak discriminates nothing, and why the bound now reports instead of
  gating** - [`bug-857-family.md`](bug-857-family.md)'s 2026-08-25 entry and
  [`test-class2-probe-asserts-timing-not-correctness.md`](test-class2-probe-asserts-timing-not-correctness.md).
- **Why the group is keyed the way it is, and the queueing trade it accepts** -
  [`pr-highcpu-fast-feedback.yml`](../../.github/workflows/pr-highcpu-fast-feedback.yml)'s own
  concurrency comment, and [`ci.md`](../ci.md) under "An ABSENT chaos check is not a passing one".
- **The earlier contended-vs-real-occurrence measurement** that first named this problem lives on
  branch `docs/chaos-class2-contention-finding` (`git show 6ffd71585`).
<!-- file-refs: N/A - the branch commit above is deliberately named as history rather than a live path -->

## Delete when

One full week of chaos runs on the serialised lane has been observed and the concurrent-chaos count
is zero. If it is not zero, the group key is still wrong - reopen with the counts.
