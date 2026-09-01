# What recovers an un-mailboxed container? Nobody has established it

<!-- inflight-type: bug -->
<!-- inflight-impact: blind-spot -->
<!-- inflight-labels: concurrency -->

A `WorkContainer` reaches the control thread by being put on the mailbox. Every failure path in core
and in the engines ends with `addToMailbox`, and each is now wrapped so a throw earlier in the
handler cannot skip it. **What none of them establish is what happens when `addToMailbox` itself does
not run** - whether the record is recovered by some other mechanism, or stays in flight until the
process restarts.

The question matters in two directions and neither has an answer:

<!-- post-merge: checked-begin -->
- **If something recovers it** - a timeout sweep, a redelivery on the next poll, anything - then the
  per-container guards astubbs#267 added are a **latency** fix, not a stall fix, and every write-up
  that calls them a stall fix is overclaiming.
- **If nothing recovers it** then an un-mailboxed container is a permanent, silent stall for that
  record's shard, and the `catch` blocks that log "it may stay in flight" are accepting it.
<!-- post-merge: checked-end -->

**It was ablated, and the ablation does not settle it.** Removing the guards three ways across two
engines did not strand records: they came back regardless. That refutes the strong form of the
motivating claim, and is exactly why the guards are documented as defence in depth
([`../solutions/best-practices/a-guard-outlives-the-claim-that-motivated-it.md`](../solutions/best-practices/a-guard-outlives-the-claim-that-motivated-it.md)
owns that lesson). It does **not** identify the recovery mechanism, so it cannot tell you whether the
mechanism covers this case too, or whether the ablation simply never produced the shape that fails.

## Where it surfaces concretely

<!-- post-merge: checked-begin -->
The vert.x `send.onFailure` handler, at its `catch (Throwable mailboxingThrew)` - grep
`Failed to return {} to the mailbox`. The question was raised as a review thread on astubbs#267 -
should that catch be fatal - and the options below are the ones weighed there. Whichever way that PR
went, the question this note holds is unchanged, because none of the three can be chosen without it:
<!-- post-merge: checked-end -->

1. **Leave it and state the bound in the comment.** `addToMailbox` is PC's own code, so a throw means
   something is already badly wrong. **The bound got tighter after this note was written, and that
   weakens this option rather than strengthening it.** It used to be more than a queue add:
   `onPostAddToMailBox` released the produce lock in transactional mode and `ProducerManager`'s
   <!-- post-merge: checked -->
   `ensureProduceStarted` threw when the hold count was below one. astubbs#257 fixed that double
   release, deleted the method and made `cleanUpContext` the single release point, so core's
   `addToMailbox` is now a queue add and nothing else. PC therefore no longer has a *named* reachable
   throw here - which means "state the bound in the comment" now has almost nothing to state, and the
   comment cannot point a reader at a real route the way it could when this option was written.
2. **Route the failure to the control thread's own failure path**, so an un-mailboxed record surfaces
   as a PC failure rather than a silent stall. A new escalation path in core and all three engines.
3. **Mark the container so a sweep recovers it** - which presupposes the sweep this note is asking
   about.

**Rethrowing is not among them.** `FutureImpl` iterates its listener array with no per-listener
try/catch, so a throw escaping the handler skips every remaining listener and strands the sibling
containers as well - a bigger blast radius, not an escalation.

## Why it is filed as a blind spot rather than a defect

Nothing is known to be broken. What is missing is the signal: there is no test, no assertion and no
log line anywhere that distinguishes "recovered by X" from "never came back", so both worlds look
identical from outside. Answering it is a reading exercise plus one test - drop a container on the
floor deliberately and see whether it returns - and the answer decides which of the three options
above is right, and whether several existing write-ups need weakening.

Related: [`core-control-thread-contract-debts.md`](core-control-thread-contract-debts.md) owns the
mailbox-versus-interrupt protocol, which is the adjacent question of how the control thread is *told*
about mail, not what happens to mail that was never posted.
