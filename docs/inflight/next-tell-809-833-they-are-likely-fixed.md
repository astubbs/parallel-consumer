# confluentinc#809 and confluentinc#833 are plausibly already fixed here, and nobody has said so

Two open upstream reports, mirrored as astubbs#175 and astubbs#177. Both show the same signature:

```
InternalRuntimeException: Timeout waiting for commit response PT30S to request
  ConsumerOffsetCommitter.CommitRequest(id=..., requestedAtMs=...)
```

Found while checking whether the transactional battle test had covered the reported transactional
hangs. It had not - it planned against confluentinc#803 and never opened these two, even though
astubbs#44's mirror names them.

## Why they are plausibly fixed

`ConsumerOffsetCommitter`'s own javadoc, added with astubbs#100, names this exact symptom and its
cause:

> **Throw** - let it escape. Fatal: this runs on the broker-poll thread, the only producer of commit
> responses, so killing it strands every waiting committer until `offsetCommitTimeout` and then takes
> the whole PC instance down. This is the "Timeout waiting for commit response" symptom, **whose cause
> looks nothing like it**.

astubbs#100 fixed exactly that: a mid-rebalance commit threw `RebalanceInProgressException`, nothing
caught it, and the broker-poll thread died permanently. It has landed on this fork.

confluentinc#833's own forensics fit the mechanism precisely. The reporter shows the last commit
response added at 21:16:54 and a waiter still blocked at 21:33:39 - **about 17 minutes** with no
commit response produced, and `pc_processed_records_total` flat across the window. A 30-second
timeout expiring does not explain a 17-minute gap; the only producer of commit responses being dead
does.

**Not established:** whether both reports are that cause. confluentinc#833 runs a 50%-failure
workload, which also touches the *dirty* asymmetry - only a success marks a partition dirty, so a
workload where nothing succeeds attempts no commits at all. confluentinc#809's stack is inside
`close()` -> `doClose` -> `commitOffsetsThatAreReady`, which is a different entry point. They may be
one defect or two.

## The reported number is wrong, which is why this stayed confusing

`ConsumerOffsetCommitter#commitAndWait` waits on `commitTimeout` (`options.getOffsetCommitTimeout()`)
but interpolates `AbstractParallelEoSStreamProcessor.DEFAULT_TIMEOUT` into the message:

```java
Duration timeout = AbstractParallelEoSStreamProcessor.DEFAULT_TIMEOUT;          // PT30S
CommitResponse take = commitResponseQueue.poll(commitTimeout.toMillis(), ...);  // waits commitTimeout
if (take == null)
    throw InternalRuntimeException.msg("Timeout waiting for commit response {} ...", timeout, ...);
```

So every occurrence of this message reports a duration that is not the one that elapsed, unless the
two happen to be equal. Both reporters quote `PT30S` and reason from it - and anyone triaging from
their logs inherits the same wrong figure. This is a one-line fix (report `commitTimeout`) and it
should land before anyone else diagnoses from this message.

Previously noted in `docs/plans/2026-08-01-001-investigate-chaos-w4-red-report.md` §8 and not acted
on.

## What to do

1. **Fix the message** to report `commitTimeout`. One line, its own small PR. Everything else here
   depends on the logs being truthful.
2. **Establish whether astubbs#100 covers them.** The mechanism matches confluentinc#833 closely;
   confluentinc#809's close-path entry needs its own look. A reproduction would settle it - the shape
   is a broker-poll thread death during rebalance under a failing workload.
3. **If it does, say so upstream and on the mirrors.** Both are open with no response. Per AGENTS.md,
   comment upstream only when there is something to act on - a fix in a published version is exactly
   that. Use plain cross-repo references, never `Fixes`/`Closes`, and check for the hidden marker
   before posting so it cannot double-comment.
4. **Correct the mirrors either way.** astubbs#44's summary asserts these two are "likely the same
   defect" as confluentinc#803, i.e. the `synchronized(commitCommand)` deadlock whose fix is still
   open in astubbs#29. If they are actually astubbs#100's already-landed defect, that summary is
   pointing the next reader at the wrong PR - and AGENTS.md's rule is to correct a mirror when it turns
   out wrong rather than leave the next reader to inherit it.

## Why this is worth doing

These are user-visible hangs, reported by people running this in production, sitting open upstream with
no reply. If the fork already fixes them, that is both the single best advertisement for the fork's
existence and a debt owed to the people who reported them.
