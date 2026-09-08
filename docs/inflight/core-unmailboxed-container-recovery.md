# What recovers an un-mailboxed container? Nobody has established it

<!-- inflight-type: bug -->
<!-- inflight-impact: blind-spot -->
<!-- inflight-labels: concurrency -->
<!-- inflight-vetted: 2026-09-08 - applied: shrunk. The "where it surfaces concretely" half is replaced by a record that its option 2 shipped - the three options are no longer live choices - and the note now holds only the recovery-mechanism blind spot and the one test that would settle it; checked: `failFatallyOnUnmailboxableRecord` is called from core, `ExternalEngine` and the vert.x handler and is pinned by `UnmailboxableRecordIsFatalTest`, and the quoted log line "Failed to return {} to the mailbox" is nowhere in the tree. Still nothing anywhere distinguishes "recovered by X" from "never came back" -->

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
- **If nothing recovers it** then an un-mailboxed container would be a permanent stall for that
  record's shard - no longer a silent one, since the escalation below now stops PC instead, but a
  stall that only a restart clears.
<!-- post-merge: checked-end -->

**It was ablated, and the ablation does not settle it.** Removing the guards three ways across two
engines did not strand records: they came back regardless. That refutes the strong form of the
motivating claim, and is exactly why the guards are documented as defence in depth
([`../solutions/best-practices/a-guard-outlives-the-claim-that-motivated-it.md`](../solutions/best-practices/a-guard-outlives-the-claim-that-motivated-it.md)
owns that lesson). It does **not** identify the recovery mechanism, so it cannot tell you whether the
mechanism covers this case too, or whether the ablation simply never produced the shape that fails.

## The escalation shipped; the recovery question did not move

<!-- post-merge: checked-begin -->
This was raised as a review thread on astubbs#267 - should the vert.x `send.onFailure` handler's
`catch (Throwable mailboxingThrew)` be fatal - and it has since been answered in code.
`AbstractParallelEoSStreamProcessor#failFatallyOnUnmailboxableRecord` is the escalation path, called
from core, from `ExternalEngine` and from the vert.x handler, with
`UnmailboxableRecordException`/`ProduceLockNotHeldException` naming the two shapes and
`UnmailboxableRecordIsFatalTest` holding it to the contract. An un-mailboxed record now terminates PC
instead of disappearing quietly, and the log line this note used to send readers to grep is gone with
the `catch` that wrote it.
<!-- post-merge: checked-end -->

**That closes the "what should the handler do" question and not this one.** Failing fatally is what
you do when you do not know whether anything recovers the record - it converts an unknown into a
loud, bounded outcome. It still does not say whether something *would* have recovered it, which is
what decides whether astubbs#267's per-container guards are a latency fix or a stall fix.

Two details from that decision are worth not re-deriving. Rethrowing was never an option:
`FutureImpl` iterates its listener array with no per-listener try/catch, so a throw escaping the
handler skips every remaining listener and strands the sibling containers too - a bigger blast
radius, not an escalation. And core's `addToMailbox` is now a queue add and nothing else: astubbs#257
deleted `onPostAddToMailBox` and made `cleanUpContext` the single produce-lock release point, so PC
has no *named* reachable throw there any more.

## Why it is filed as a blind spot rather than a defect

Nothing is known to be broken. What is missing is the signal: there is no test, no assertion and no
log line anywhere that distinguishes "recovered by X" from "never came back", so both worlds look
identical from outside. Answering it is a reading exercise plus one test - drop a container on the
floor deliberately and see whether it returns - and the answer decides whether astubbs#267's guards
are described correctly, and whether several existing write-ups need weakening.

Related: [`core-control-thread-contract-debts.md`](core-control-thread-contract-debts.md) owns the
mailbox-versus-interrupt protocol, which is the adjacent question of how the control thread is *told*
about mail, not what happens to mail that was never posted.
