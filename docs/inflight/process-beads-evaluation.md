# Beads: the desk comparison is done, the probes are not

<!-- inflight-type: task -->
<!-- inflight-impact: process -->

[`process-adopt-external-harness.md`](process-adopt-external-harness.md) **owns the adopt-or-build
decision** and defers it until after v6. This note owns the evidence, which does not wait: deferring
a decision is not a reason to defer the measurement it will be made on, and by the time v6 ships
whoever picks this up would otherwise start from the same README everyone starts from.

**The documentary half is written up in
[`docs/plans/2026-09-01-001-investigate-beads-comparison.md`](../plans/2026-09-01-001-investigate-beads-comparison.md),
which owns the findings.** Its three headline results, so a reader knows whether to open it:

- **Beads has had two architectures and the current one moves the tracker's state out of the working
  tree**, into a Dolt store synced through a side ref. That threatens the property this directory is
  built on - a note travelling with the branch that produced it - and it fails *silently* rather than
  as a merge conflict.
- **It replaces the tracker core and nothing else.** Every hook and gate here is about delivery
  rather than storage, and none of it is addressed.
- **Compaction is the one thing worth stealing regardless of the decision.** We have no answer to
  index inflation beyond a person noticing.

## What is left, and it needs `bd` to have run

The document's §6 carries the probe list and the install decision. **Run it; do not read about it** -
that rule attaches to those probes, not to the comparison already done.
[`docs/agent-harness.md`](../agent-harness.md) owns why: its own first version asserted four things
about Claude Code that turned out false, each with a design already built on top.

Probe 1 is the one that matters - does `bd` follow the git branch? Nothing in the documentation says,
and the answer is close to deciding this on its own.

## Delete when

The probes have run and the verdict is recorded in
[`process-adopt-external-harness.md`](process-adopt-external-harness.md), which owns it. This note
must not grow a second copy of either the findings or the decision.
