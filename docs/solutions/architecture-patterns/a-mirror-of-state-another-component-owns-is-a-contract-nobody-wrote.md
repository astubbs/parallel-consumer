---
title: "A mirror of state another component owns is a contract nobody wrote"
date: 2026-08-18
category: architecture-patterns
module: parallel-consumer-core/internal
problem_type: architecture_pattern
component: background_job
severity: high
applies_when:
  - About to add a field that caches, shadows, or summarises state some other component is the authority for
  - A threading constraint makes asking the owner directly awkward, and a local copy looks like the cheap way out
  - Diagnosing a stall or wrong decision where two components disagree about one fact (paused or not, stopping or not, committed or not)
  - Reviewing a fix that adds a flag or a reset hook to keep a copy in step with its original
  - Deciding whether the right fix is repairing a copy's synchronisation or deleting the copy
related_components:
  - ConsumerManager
  - BrokerPollSystem
  - AbstractParallelEoSStreamProcessor
tags:
  - mirrored-state
  - single-source-of-truth
  - state-drift
  - thread-safety
  - defect-class
  - issue-857
---

# A mirror of state another component owns is a contract nobody wrote

## Context

Four incidents in this codebase - three of them in the confluentinc#857 family - share one shape:
**a component kept its own copy of a fact some other component is the authority for, and the copy
drifted.** Each was found separately, months apart, and diagnosed as if it were a new kind of bug.
Naming the class is the point of this document; the individual mechanisms are owned by the documents
cited from the table.

The lesson is *not* "avoid caching". It is that a copy of state you do not own carries a
synchronisation contract - who updates it, on which thread, what invalidates it, how it is
published - and that contract is written nowhere. The compiler does not ask for it, no test observes
it, and the copy answers confidently whether or not it is current. So the failure is always silent:
not an exception, but a component acting on a fact that stopped being true.

## The instances

| Copy | The authority it shadowed | How the contract broke | Outcome |
|---|---|---|---|
| A consumer-manager "shutdown requested" flag | The poll system's own `runState` lifecycle | Second source of truth for one lifecycle, settable out of step with the first. Drain set the flag, polling stopped dead: a ~10 kHz busy-spin and a member that zombie-held its assignment through rebalances | **Deleted** (astubbs#80) - the poll guard now reads the one lifecycle that exists |
| `pausedForThrottling` | Kafka's own per-partition pause state (`consumer.paused()`) | Unmaintainable by construction: the eager rebalance protocol clears pause state on reassignment, cooperative retains it, so no reset hook is correct in both. When flag and truth disagreed, nothing ever resumed - paused consumption after rebalance, the confluentinc#857 symptom itself, introduced by a fix for it | **Deleted** - replaced by asking Kafka, which makes the protocol irrelevant and needs no reset hook |
| `metaCache` | The consumer's group metadata (carries the generation used for zombie fencing on transactional offset send) | Written by the poll thread, read by control, and on `master` published without `volatile` - the publication half of the contract simply missing | Repaired (made `volatile` on the confluentinc#857 fix branch); the copy itself remains, see the counter-pressure below |
| `commitCommand` | *The boundary case - not a copy, but the same unwritten contract.* An `AtomicBoolean` recording that a user asked for a commit | The flag's **monitor** silently became the lock guarding commit *execution*. That obligation was written nowhere, so the revoke callback taking "the flag's lock" was in fact taking the commit path's lock - the AB-BA deadlock at the centre of confluentinc#857 | Execution moved to its own named lock; the flag went back to being a flag |

The first three are literal mirrors. The fourth is included because it sharpens the definition: the
defect class is **state carrying a synchronisation obligation nobody wrote down**. Mirroring is
merely the most reliable way to create one, because a mirror *always* has that obligation and
*never* has it written.

## Why the mirror keeps getting written

Every instance above was the cheap local fix for a threading constraint, and that is the honest
counter-pressure to this whole document. `metaCache` is the clearest: AK 2.7+ blocks concurrent
access to `groupMetadata()`, the control thread cannot safely ask the consumer live, so a mirror
refreshed by the poll thread was the workaround. The alternative - restructuring which thread owns
the consumer - is the shared-nothing refactor (astubbs#142, confluentinc#200), still open.

That is not a refutation; it is the diagnosis. **A mirror is usually a threading constraint wearing
a field's clothes.** The copy is evidence that some component cannot reach the authority from where
it runs - which is itself the argument for fixing the reachability, not for keeping the copy. The
seam that makes asking hard is the same seam the deadlocks live on
([two-threads-one-consumer](two-threads-one-consumer-why-the-commit-seam-keeps-deadlocking.md) owns
that story).

## Guidance

In descending order of preference when you find yourself reaching for a local copy:

1. **Ask the owner.** If the authority can answer directly, the copy has no reason to exist. The
   `pausedForThrottling` deletion is the model: `consumer.paused()` is self-correcting under both
   rebalance protocols, where the flag could not be correct under both no matter how many reset
   hooks were added.
2. **Delete the duplicate source of truth.** If the fact already lives in one lifecycle or state
   machine, route readers to it rather than maintaining a second. Two of the four instances ended
   in deletion, and both deletions were the fix - not a refactor after the fix.
3. **If you must mirror, write the contract at the field.** Who writes it, on which thread; who
   reads it; how it is published (`volatile`, lock, message); what invalidates it; and why asking
   the owner is impossible - so the next reader knows the copy is a workaround with an expiry
   condition, not a design. `metaCache` sat for five years with
   half its contract missing because nothing stated it had a contract at all.
4. **Treat a reset hook as a symptom.** A mirror that needs re-synchronising on some event has
   already demonstrated it can disagree with its authority between events. Ask what happens during
   the disagreement window before writing the hook - for `pausedForThrottling`, the answer was
   "consumption stays paused forever", and the hook made it worse.

## Why This Matters

A drifted mirror does not fail; it *answers*. Every instance above presented as a behaviour bug
somewhere else - a busy-spin, a paused group, a deadlock - and in no case did the first
investigation point at the copy. The copy is invisible in a stack trace, because reading a stale
field is a perfectly legal instruction. That is what makes this a defect *class* worth naming: the
shape is recognisable at review time ("this field restates something another component knows") far
more cheaply than at diagnosis time.

## When to Apply

- Reviewing any new field whose value restates a fact obtainable elsewhere - especially across a
  thread boundary, where the restatement is probably a workaround for the boundary.
- Diagnosing a component acting on a fact that observably is not true: ask "who is the authority
  for this fact, and is this component reading the authority or a copy?"
- Weighing "repair the copy's synchronisation" against "delete the copy": the record here is two
  deletions, one repair, and one lock disentanglement - and both deletions removed the whole
  failure mode, where the repair left the workaround in place awaiting the real fix.

## Related

- [two-threads-one-consumer-why-the-commit-seam-keeps-deadlocking.md](two-threads-one-consumer-why-the-commit-seam-keeps-deadlocking.md) -
  owns the thread-ownership seam that motivates every mirror above, the `metaCache` and
  `pausedForThrottling` mechanisms in full, and the constraint that control must not read
  `groupMetadata()` live.
- [../runtime-errors/revoke-path-commit-deadlock-between-poll-and-control-threads.md](../runtime-errors/revoke-path-commit-deadlock-between-poll-and-control-threads.md) -
  the `commitCommand` monitor's two purposes and the AB-BA cycle they produced.
- astubbs#80 - the shutdown-flag deletion: its PR body carries the full desync mechanism
  (busy-spin plus zombie member) and the single-source-of-truth fix.
- astubbs#142, confluentinc#200 - the shared-nothing refactor tracker: the structural fix that
  would remove the constraint the mirrors work around.
