# `dups: clones` times out whenever `MultiInstanceRebalanceTest.java` is in the diff

<!-- inflight-type: bug -->
<!-- inflight-impact: misdirection -->

`dups: clones` fails as **`##[error]The operation was canceled.`**, which reads as infrastructure
noise and is not: `maven.yml` gives that job `timeout-minutes: 5`, so the cancellation *is* the
timeout. The job burns its whole budget and is killed, and because the workflow is still running when
the check goes red, `gh run view --job <id> --log` refuses the log
(*"run is still in progress"*) - so the one command that would explain it is unavailable at exactly
the moment somebody looks.

## The control, which is what makes this a tooling defect rather than a duplication finding

<!-- post-merge: checked -->
The rows below are a dated observation, not a live status: both PRs cited will have merged or closed
by the time anyone reads this, and the point they make - which file the diff touched - survives that.

| PR | touches `MultiInstanceRebalanceTest.java` | `dups: clones` |
|---|---|---|
| astubbs/parallel-consumer#441 | yes - **one line** | fail, 5m17s | <!-- post-merge: checked -->
| astubbs/parallel-consumer#444 | yes | fail, 5m14s | <!-- post-merge: checked -->
| astubbs/parallel-consumer#431 | no | pass, 45s |
| astubbs/parallel-consumer#434 | no | pass, 52s |
| astubbs/parallel-consumer#435 | no | pass, 51s |

**astubbs/parallel-consumer#441 is the decisive row.** It changes a single expression
(`(int) ((size - 1) * Math.random())`). A one-line edit cannot introduce duplication, yet it fails
identically, at the same duration, as a PR that rewrites parts of the same file. The discriminator is
not what the diff contains but *which file it is in* - so this is
`astubbs/duplicate-code-cross-check` hitting a pathological runtime on that file, not a clone report.

Ten to twelve times the runtime of a passing run, on a file of roughly nine hundred lines.

## Why it is worth a note rather than a rerun

A rerun is the reflex and it is wrong here: two runs died at 5m14s and 5m17s, which is a systematic
cut-off, not a random one. Rerunning spends five more minutes to reproduce a timeout.

It also **blocks two open PRs at once**, and neither can fix it from inside its own diff. The lever is
in the workflow or the action - raise `timeout-minutes`, exclude the file, or fix the tool - and which
of those is right is a decision about the tool's budget, not about either PR.

## What is NOT established

Why that file is pathological for this action. Size alone is a guess: the repo has larger files that
do not trigger it, and nobody has profiled the action against it. Whoever picks this up should
measure before assuming length is the cause - the file is also unusually repetitive (several
near-identical `runScenario(Scenario.builder()...)` blocks), which is precisely the shape a clone
detector does the most work on, and that is a different fix from a bigger timeout.

## Related

- [`ci-duplication-report-can-fail-to-post.md`](ci-duplication-report-can-fail-to-post.md) - the
  sibling failure where the finding exists but no comment carries it. Same lesson from the other end:
  a red duplication check cannot be read from the check alone.
- [`ci-untriaged-clone-backlog.md`](ci-untriaged-clone-backlog.md) - the standing clone backlog.
