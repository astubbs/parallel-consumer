---
title: "Fresh work needs an independent reviewer, and the tail is what momentum skips"
date: 2026-08-10
category: best-practices
module: development-workflow
problem_type: best_practice
component: development_workflow
severity: high
applies_when:
  - Finishing a change you wrote in this same session and believe is done
  - The work is self-verified, measured and documented, and only shipping is left
  - Deciding whether the review or simplify pass is worth running on code that already looks finished
  - Choosing the order of the simplify and review passes over one change set
  - Writing a code comment that asserts a framework guarantee you have not read the framework's source for
tags:
  - code-review
  - independent-review
  - self-review
  - simplify-pass
  - review-order
  - vacuous-assertions
  - kafka-streams
related_components:
  - testing_framework
  - documentation
---

# Fresh work needs an independent reviewer, and the tail is what momentum skips

## Context

In astubbs/parallel-consumer#271 (issue astubbs#255), the commit-frontier unit was implemented
red-first, self-verified against its own failing test, measured, documented, and recorded in the plan.
It felt finished. The only thing left was to ship it.

Then the pipeline's simplify and review passes ran over that same-session code and returned four
findings. One was a data-loss defect in the exact class the unit had been written to close. Another
falsified a premise the author had written into a code comment as though it were established fact. Two
more were test assertions that could not fail.

None of those were found by the author re-reading the author's own diff, and the author had re-read it.

## Guidance

**1. Dispatch reviewers that did not write the code.** Freshly written code is the hardest code for its
author to review, because the author reviews their *intent* rather than the text on the screen. They
know what each line is supposed to do, so they read that meaning into it and the line confirms itself.
Independence is a property of **separate contexts**, not of trying harder, taking a break, or adopting a
reviewer persona inside the same context that produced the code. The intent is still loaded either way.

The repo already encodes this at the CI level and for the same reason: the automated review workflow
refuses to run when a PR edits the reviewer workflow itself, so a PR cannot rewrite its own reviewer
(`AGENTS.md:273`). Do not disable that gate to get a green check; get a real review instead. The
principle it enforces mechanically is the one to apply by hand: **whatever produced the artefact does
not get to be the thing that clears it.**

**2. Run simplify BEFORE review.** Simplify changes code. If it runs second, every line it touches is
unreviewed at merge, and the review's conclusions were reached about a version of the file that no
longer exists. This session got the order wrong and had to re-verify the review findings against the
simplified tree. Order the tail: implement, simplify, then review the simplified result.

**3. Treat "this feels done" as the trigger for the tail, not permission to skip it.** The moment the
work feels finished is precisely the moment it is least verified: everything the author knows how to
check has been checked, which is a statement about the author's checklist and not about the code. That
feeling is what makes the tail skippable under momentum, and the tail is where these four findings came
from.

**4. A code comment asserting a framework guarantee is an unverified claim.** "This callback is only
reached after success" reads like documentation and behaves like an assumption. Send a reviewer to the
framework's own source with an instruction to confirm or refute it. Refutation is the valuable outcome
and it is common.

**5. Ask of every new assertion: what makes this fail?** An assertion that passes is evidence only if
some reachable state would make it red. Restart, crash and rebalance tests are the usual home for this
defect, because durable output from an earlier phase is still sitting on the topic where a fresh reader
will find it.

## Why This Matters

The P0 in this session is the argument. The unit existed to close a crash-loss window: never commit an
offset whose output is not yet durable. The author, having just reasoned carefully about that window one
layer down, wrote a commit path that opened the same window one layer up - flush the producer, then
collect the commit data - and read it back as correct, because they knew what it was for.

That is the failure mode in one sentence: **the author's knowledge of the intent is what conceals the
defect, so the more carefully they thought about the problem, the better camouflaged its reopening is.**
No amount of the author re-reading fixes that, because the re-reading is done by the same loaded
context. Two independent reviewers found it separately, which is what independence buys.

The cost asymmetry settles the rest. Running simplify and review over finished-feeling work costs one
more pass. Skipping it, on this change set, would have shipped a silent output-loss window on crash, a
false guarantee baked into a comment where the next reader would trust it, two integration tests that
would go on passing forever without testing anything, and a resource leak.

## When to Apply

- Before shipping **any** change written in the current session, however thoroughly self-verified.
- With extra weight when the change touches a correctness boundary the author has been reasoning about
  for a while, because that is when intent most strongly overwrites text.
- Whenever a comment or a plan asserts what a third-party framework guarantees.
- Whenever a test asserts on data that could have been produced by an earlier phase, another process, or
  a previous run.
- After implementation and before review, for the simplify pass specifically - never after review.

## Examples

### The P0: the same defect class, reopened one layer up

The commit path flushed the producer and *then* collected the commit data. Collection drains completed
work, including completions that landed **during** the flush, whose sends `KafkaProducer.flush()` never
covered. Committing that map and crashing loses output.

The fix inverts the order, and the comment now states why in the imperative, at
`parallel-consumer-streams/src/main/patch/pc-streams.patch:370`:

```
// PC dispatch (astubbs#255, U9 review): COLLECT BEFORE FLUSH, and return PC's map directly.
// Workers complete records DURING flush, and KafkaProducer.flush() does not cover sends
// enqueued after it was invoked - collecting afterwards could commit a record whose
// output is not yet durable, which is the crash-loss window this unit exists to close.
```

The author had written the flush-then-collect ordering while holding a correct model of exactly this
hazard. Knowing the hazard did not prevent writing it; it prevented seeing it.

### The refuted premise: `postCommit` is not success-only

The acknowledgement of a successful commit was originally hung off `postCommit`, justified by a comment
asserting that Kafka only reaches `postCommit` after a commit succeeds. A reviewer sent to Kafka 3.9.2's
own source refuted it. In `org/apache/kafka/streams/processor/internals/TaskManager.java` (from
`kafka-streams-3.9.2-sources.jar`), `tryCloseCleanActiveTasks` catches a commit exception, logs it,
reassigns the tasks to the close-dirty set, and then calls `task.postCommit(true)` over the active tasks
anyway (`TaskManager.java:1621-1644`); `closeDirtyAndRevive` calls `task.postCommit(true)` with no commit
attempted at all (`TaskManager.java:297`).

Acknowledging there would mark work clean that was never durably committed. The acknowledgement moved to
a genuinely success-only callback, `updateCommittedOffsets`
(`parallel-consumer-streams/src/main/patch/pc-streams.patch:642`). That one is reachable only from
success: `TaskExecutor.updateTaskCommitMetadata` is its sole caller (`TaskExecutor.java:253-259`), and
its three call sites each sit on the line immediately after a committed transaction or a returned
`commitSync`, inside the `try` and before every `catch` (`TaskExecutor.java:187`, `:204`, `:228`).

Note what the original comment looked like: a confident, specific statement of framework behaviour. That
is the shape to send a reviewer at, precisely because it does not look like a question.

### The vacuous assertions: passing on pre-crash data

Two integration-test assertions in the crash-restart proof were satisfiable without the restart doing
anything. The restart-phase readers consumed from earliest with a fresh group, so they re-read the
previous phase's durable output and asserted on it.

The fix scopes each phase's reader past the previous phase's end offset. Both helpers now carry the
reasoning, in
`parallel-consumer-streams/src/test/java/io/confluent/parallelconsumer/streams/integrationTests/CommitFrontierCrashRestartTest.java`:

- `outputEndOffset` (`:256-263`) - "Without it, an earliest-reading consumer re-reads the previous
  phase's durable outputs and the restart assertions pass on evidence the restart never produced (U9
  review findings on this class - the vacuous-restart-assert defect)."
- `drainFrom` (`:271-275`) - "assign+seek, never subscribe-from-earliest, so the caller's assertions can
  only be satisfied by records the phase under test itself produced."

Call sites at `:178-182` and `:232-245` capture the boundary between phases and read only past it.

### The leak the review missed, and simplify found

The simplify pass found a resource leak that the review pass did not: `prepareRecycle()` never closes the
dispatcher. Kafka's `StreamTask.prepareRecycle()` (`StreamTask.java:595` in the 3.9.2 sources) does not
route through `close(boolean)`, which is the only place the dispatcher shutdown was wired
(`parallel-consumer-streams/src/main/patch/pc-streams.patch:415`). Active/standby recycling therefore
leaks the static registry entry, the worker pool, and the WorkManager's partition state. It is dormant
only because no test configures standby replicas.

It is recorded as issue 3 of six in
`docs/plans/2026-08-08-001-feat-ks-on-pc-spike-plan.md:946` and
`docs/inflight/pr-streams-task-lifecycle-and-rebalance.md:26`.

The lesson is not about the leak. It is that **the two passes have different eyes** - review looked for
correctness defects and did not see an unclosed resource on a path it was not reasoning about, simplify
looked at lifecycle symmetry and saw it immediately. One pass is not a substitute for the other, and
neither is a substitute for being someone other than the author.

## Related

- [Chase refuted predictions](chase-refuted-predictions.md) - what to do with the finding once a
  reviewer refutes something you believed, as `postCommit` was refuted here.
- `docs/plans/2026-08-08-001-feat-ks-on-pc-spike-plan.md` - U9 (the change reviewed) and U10 (where the
  deferred lifecycle findings live).
- `docs/inflight/pr-streams-task-lifecycle-and-rebalance.md` - the six known lifecycle divergences,
  including the recycle leak.
- `AGENTS.md` (*Continuous integration*, `claude-code-review.yml`) - the CI gate that stops a PR
  rewriting its own reviewer, and why disabling it to get green is the wrong move.
- astubbs/parallel-consumer#271, issue astubbs#255 - the PR and issue this was learned on.
