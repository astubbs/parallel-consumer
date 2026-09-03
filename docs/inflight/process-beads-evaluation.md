# Beads: the desk comparison is done, the probes are not

<!-- inflight-type: task -->
<!-- inflight-impact: process -->

[`process-adopt-external-harness.md`](process-adopt-external-harness.md) **owns the adopt-or-build
decision** and defers it until after v6.

Two dated documents own the findings, and the later one wins where they disagree:
[`docs/plans/2026-09-01-001-investigate-beads-comparison.md`](../plans/2026-09-01-001-investigate-beads-comparison.md)
is the desk survey of the whole field, and
[`docs/plans/2026-09-02-001-investigate-adopt-or-build-re-run.md`](../plans/2026-09-02-001-investigate-adopt-or-build-re-run.md)
re-ran the Backlog.md half against a running binary and names the four conclusions it retracts. Read
them there; this note deliberately does not summarise either, because a summary of a document that
may not be rewritten is a second copy that drifts from the first.

**What this note owns is the part that is still OPEN, and it is now only the Beads half.** Backlog.md
has been driven; `bd` has not, so every probe below is untouched.

## Why run `bd` at all, when the decision is "build"?

Antony asked, and the answer has to survive being asked again. **Not to reconsider adopting it -
the 2026-09-02 re-run settled that - but because Beads is the only surveyed tool whose storage
model might not reconcile.** It appends to a log and merges it with git, rather than resolving to
one row per task, and that is the same bet as
[`ci-inflight-next-commands.md`](ci-inflight-next-commands.md)'s "flow with git, do not suppress
it". If `bd` reports disagreement instead of resolving it, it is the one piece of prior art on the
model this tool is committing to, and worth reading closely. If it reconciles like Backlog.md does,
the class result is confirmed and this whole line is closed with evidence.

Either outcome is worth one probe; neither is worth an evaluation. **It blocks nothing** - not the
in-flight tool, not v6. It is a cheap measurement kept open because it is cheap, and it should be
deleted rather than carried if it stops being either.

## What is left, and it needs `bd` to have run

Deferring a decision is not a reason to defer the measurement it will be made on - by the time v6
ships, whoever picks this up would otherwise start from the same README everyone starts from. So the
evidence does not wait even though the decision does.

The plan's §6 carries the probe list and the install decision, already taken so a later session does
not re-litigate it. **Run it; do not read about it** - that rule attaches to these probes, not to the
comparison already done. [`docs/agent-harness.md`](../agent-harness.md) owns why: its own first
version asserted four things about Claude Code that turned out false, each with a design already
built on top.

**Probe 1 is the one that matters** - does `bd` follow the git branch? Nothing in the documentation
says, and the answer is close to deciding this on its own. The 2026-09-02 pass raises its value
rather than lowering it: driving Backlog.md showed that a tool can travel with the branch and still
resolve every disagreement away silently, so "does it follow the branch" is only half the question.
The other half is what it does when two branches disagree, and that has to be provoked, not read.

## Delete when

The probes have run and the verdict is recorded in
[`process-adopt-external-harness.md`](process-adopt-external-harness.md), which owns it. This note
must not grow a second copy of either the findings or the decision.
