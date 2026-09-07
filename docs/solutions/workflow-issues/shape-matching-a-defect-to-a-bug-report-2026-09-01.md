---
title: A defect matches a report on direction and on the reporter's build, not on a shared adjective
date: 2026-09-01
category: workflow-issues
module: tooling
problem_type: workflow_issue
component: development_workflow
severity: high
root_cause: incorrect_assumption
resolution_type: workflow_improvement
applies_when:
  - Proposing a known defect as the explanation for an incoming bug report
  - Triaging an upstream mirror whose reporter has stopped answering
  - Deciding whether a report can be closed without the reporter confirming a precondition
  - Reading a defect's preconditions out of a write-up built on the current tree
symptoms:
  - A candidate is nominated because its mechanism shares a word with the report
  - A precondition blocks triage that the reported version could not have had
  - A discriminating test recorded earlier on the same issue is not applied to a later candidate
  - Triage stalls waiting on a reporter who last commented over a year ago
tags:
  - triage
  - investigation
  - bug-report
  - prior-art
  - shape-match
  - elimination
  - versioning
---

# A defect matches a report on direction and on the reporter's build, not on a shared adjective

## Context

astubbs#183 (mirroring confluentinc#875) reported one offset silently skipped while its neighbours
processed, lag climbing, consumption eventually stopping, and the missing record returning after a
restart. Four candidates were proposed for it over four months. Three were wrong. The one that was
right was not the one that resembled the report most.

The instructive failure is astubbs#344. It was nominated as a *"strong shape match"* because its
mechanism **silently completes records**, and the report describes a **silent skip**. Both true, and
the match is still wrong: astubbs#344 corrupts committed offset metadata, so the loss happens **at
restore**. It predicts lag *falling*, no live skip, and a record that never comes back - the exact
inverse of all three reported signals. The reasoning stopped at the word *silently* and never asked
which way the failure pointed.

Worse, the same issue already carried the test that kills it. An earlier comment had singled out
*"it comes back after a restart"* as the decisive signal, on the grounds that *"a permanent data-loss
bug would not behave that way"*. astubbs#344 **is** a permanent data-loss bug. The test was recorded
in August and ignored in September.

## Guidance

**Match on the direction of the failure, not on its adjective.** For each reported signal, write what
the candidate *predicts*. "Silently completes" and "silently skips" share a word and point opposite
ways. A candidate that fails one signal in the wrong direction is refuted, not weakened - and one
row of a two-column table is usually enough to see it.

**Apply the discriminating tests already recorded on the issue.** If an earlier round named a signal
as decisive, every later candidate has to pass it. A thread that accumulates candidates without
re-running its own tests will re-admit what it already excluded.

**Read the candidate's preconditions on the version the reporter ran.** confluentinc#909 was
documented as needing three coincidences, the third being a *saturated* pipeline - and that gated
astubbs#183 on a question its reporter was never going to answer. The third precondition exists to
suppress a lazy stale eviction in `ProcessingShard#getWorkIfAvailable`, and that eviction **was added
after the reported release**. At tag `0.5.3.1` the only stale sweep runs in the rebalance callbacks,
so on the reported build the defect needs a rebalance and nothing more. A precondition derived from
the current tree is not a fact about the reporter's build, and checking that out is cheap:
`git show <tag>:<path>`.

**Prefer elimination over resemblance, and scope it to the reported version.** Enumerating every
silent-drop site in core main source at `0.5.3.1` - the `dropping`/`skipping`/`ignoring` log sites
plus every `containsKey`/`putIfAbsent`/`computeIfAbsent` - left exactly one path that can lose a
single record for a still-assigned partition. That is a different class of claim from "this looks
like it", and it is what let the issue close without the reporter.

**Let the code predict a discriminator, then look for it in the other report.** At `0.5.3.1` the
shard's `entries` is a `ConcurrentSkipListMap` and `couldBeTakenAsWork` returns false for a stale
container, which *breaks* the shard scan - so a stale resident freezes its whole shard. Under
`PARTITION`/`UNORDERED`, where the shard is the topic-partition, the reported `[1,2,3,5,6,7...]` is
impossible; only `KEY` ordering lets the neighbours flow. confluentinc#909's reporter had annotated
their own timeline `...055 (K_A), ...056 (K_B), ...057 (K_C)` - one shard per key - which nobody had
read as a config disclosure. A prediction the code makes and an independent report satisfies is
evidence; a shared adjective is not.

**State the residual in the close.** astubbs#183 was closed with the two things still unproven named
in the comment: a rebalance is required and the report never mentions one, and the `0.5.3.1` analysis
came from reading that tag rather than running a reproduction against it. A close that hides its
residual cannot be reopened intelligently.

## What it looked like when it went wrong

| The report says | astubbs#344 predicts |
|---|---|
| the record is skipped **while the app is running** | **no live skip** - the corruption lands only in committed offset metadata |
| **lag increases yet consumption still continues** | **lag decreases** - falsely-completed offsets let the commit base advance |
| **after restarting, the missing message came back** | **the opposite** - restart is when the corrupt payload is read and the record is dropped for good |

Three rows, all pointing the same way, none of which needed new investigation to fill in - only the
question *"which direction?"* asked once per signal.

## Related

- The close-out reasoning: astubbs#183, and `docs/solutions/logic-errors/` for the
  confluentinc#909 preconditions this corrects the scope of.
- [Prove the instrument could have said yes before trusting a negative result](negative-results-need-an-instrument-that-could-have-said-yes.md) -
  the same asymmetry one step earlier, where the wrong answer comes from the instrument rather than
  the match.
