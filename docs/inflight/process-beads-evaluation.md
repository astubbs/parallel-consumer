# Beads: the desk comparison is done, the probes are not

<!-- inflight-type: task -->
<!-- inflight-impact: process -->

[`process-adopt-external-harness.md`](process-adopt-external-harness.md) **owns the adopt-or-build
decision** and defers it until after v6.
[`docs/plans/2026-09-01-001-investigate-beads-comparison.md`](../plans/2026-09-01-001-investigate-beads-comparison.md)
**owns the findings** - all of them, including which candidate keeps state in the working tree, what
adoption would and would not replace, and why the nearest neighbour is not the one everybody names.
Read it there; this note deliberately does not summarise it, because a summary of a document that
may not be rewritten is a second copy that drifts from the first.

**What this note owns is the part that is still OPEN**: the desk pass was documentary, and the
probes it defers have not been run.

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
says, and the answer is close to deciding this on its own.

## Delete when

The probes have run and the verdict is recorded in
[`process-adopt-external-harness.md`](process-adopt-external-harness.md), which owns it. This note
must not grow a second copy of either the findings or the decision.
