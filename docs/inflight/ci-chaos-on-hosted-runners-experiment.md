# Does the Chaos Pain Suite actually need the self-hosted box?

<!-- inflight-type: task -->
<!-- inflight-impact: ci -->

**Running as an experiment now.** `maven.yml` carries an advisory
`Chaos Pain Suite (hosted, experimental)` lane on `ubuntu-latest` alongside the existing self-hosted
one. Chaos therefore runs **twice per PR on purpose** while the trial lasts.

The claim under test is an assumption nobody has measured: that the suite needs many real cores to
provoke anything, which is why it lives on `highcpu`. If it does not, the whole co-residency problem
goes away - a hosted runner gives every job its **own VM**, so there is no shared box to contend for
and no scheduling to get right.

## Why it is worth testing rather than assuming

Every scheduling contortion the `highcpu` lane has accumulated exists to manage one physical
machine being shared: the per-suite group keys, the repo-wide mutex that discarded most of its jobs
([`ci.md`](../ci.md), "Why a `concurrency` group is not a mutex"), and the co-residency watch in
[`ci-highcpu-box-concurrency-is-runner-count.md`](ci-highcpu-box-concurrency-is-runner-count.md).
None of that is chaos's *purpose*; it is the cost of where chaos runs.

`bin/chaos-test.sh` needed no change to run here - it passes no `forkCount` and no
`-Dparallel-tests`, so the suite was never actually configured to exploit the extra cores it was
placed there for.

## What decides it

Read the hosted job's own `Chaos suite timing` summary and its zero-tests-selected warning - a green
that selected no scenarios is not a pass, and the script says so loudly.

- **Works** - neither flaky nor timing out over a useful number of runs: delete the `Chaos Pain
  Suite` entry from `pr-highcpu-fast-feedback.yml`, drop `optional` from the hosted entry, and
  `git rm` this file. That would leave the self-hosted box carrying only the full mutation sweep,
  which is the one lane that genuinely wants every core it can get.
- **Does not work** - record *which* way before reverting: wall-clock against the 60-minute cap, an
  OOM, or scenarios that never fire without real parallelism. Those are three different findings and
  only the third actually justifies the self-hosted box.

The comparison is cheap while both lanes run: the same commit gets a hosted result and a
self-hosted one, so a disagreement between them is the measurement.

## Delete when

The trial has settled either way and the losing lane has been removed.
