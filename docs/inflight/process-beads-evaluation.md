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
