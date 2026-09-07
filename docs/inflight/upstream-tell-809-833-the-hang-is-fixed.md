# confluentinc#809 and confluentinc#833 are fixed here, and the reporters have not been told

<!-- inflight-type: task -->
<!-- inflight-impact: stranded-work -->

Two upstream reports, **both still open upstream with no reply**, mirrored as astubbs#175 and
astubbs#177. Both show the same signature:

```
InternalRuntimeException: Timeout waiting for commit response PT30S to request
  ConsumerOffsetCommitter.CommitRequest(id=..., requestedAtMs=...)
```

Found while checking whether the transactional battle test had covered the reported transactional
hangs. It had not - it planned against confluentinc#803 and never opened these two, even though
astubbs#44's mirror names them.

## The state has changed since this note was written: it is fixed, not "plausibly fixed"

Four commits titled `astubbs#177` landed on master on 2026-08-19, and they close the class this note
was reasoning about:

- **`fe45fddfa` - the poll thread now publishes its own death.** `BrokerPollSystem`'s exceptional exit
  calls `ConsumerOffsetCommitter#notifyPollerDied` before unwinding, so a waiter is released at that
  moment with the poller's own exception as the cause, and later commits fail fast on the recorded
  death instead of each waiting out the timeout. A waiter cannot learn of that death by waiting -
  waiting is precisely what does not work - which is why being told is the version that is always
  right.
- **`c31829fe6` - `commitSync`'s budgets.** Four defects, including a retry budget captured inside the
  retry loop so every attempt reset it, measured at 51 attempts against a 500ms budget with PC neither
  committing nor failing.
- **`e9fc445ac`** and **`2b5d0ec86`** carry the test and a sighting.

Later, **`d9a9a4408`** added `PollThreadStallDiagnosis`: when the timeout *does* fire, it reads the
thread at that moment and states DEADLOCK / BLOCKED / WAITING / RUNNABLE / INCONCLUSIVE. So a
recurrence now arrives with its own diagnosis attached rather than needing this note's reasoning
repeated.

## Correcting this note's own diagnosis: astubbs#100 was not the answer

The section below argues these reports are "plausibly already fixed by astubbs#100". That reading was
**incomplete, and `fe45fddfa` says why in its own words**: astubbs#100 and astubbs#108 removed two
ways to kill the broker-poll thread, but broker-down, offset-encoding and authorization failures still
produced the same symptom, and *fixing exceptions one at a time was never going to close the class*.

The mechanism this note identified was right - the only producer of commit responses dies, so every
later sync commit reports a timeout that describes nothing - and the inference from it was wrong: the
fix is not "one more exception handled" but "the death is published". The reasoning is left below
rather than deleted, because it is what found the mechanism.

## Task 1 is done - the message no longer lies about its own budget

The section below on the reported duration being wrong is **fixed**, in `fe45fddfa`, and the code now
carries a comment saying so at the throw site. `commitAndWait` interpolates `commitTimeout`, the
budget it actually waited on. Grep `report the timeout actually waited` in `ConsumerOffsetCommitter`.

This matters for the reports themselves: both reporters quote `PT30S` and reason from it, and that
figure was the unrelated `DEFAULT_TIMEOUT` constant, overstating the shipped default threefold. Any
reply should say so, because their own analysis is built on it.

## astubbs#177's mirror is closed; confluentinc#833 is not

The mirror was closed on 2026-09-01 as completed, with no closing comment and no linking commit on the
issue itself - but `git log --format='%s' origin/master | grep astubbs#177` resolves it immediately,
and an earlier revision of this note calling the closure unexplained had simply not run that grep.
The fix landed; closing the mirror follows.

**confluentinc#833 is still open upstream, and still has no reply.** So the *fixing* happened and the
*telling* did not - which is the entire remaining content of this note.

## What to do

<!-- post-merge: checked-begin - each item is stated against master and against the upstream issues,
     neither of which is a branch, so none of it turns false on a merge -->
1. **Reply on confluentinc#833.** There is now a named fix rather than a hypothesis: the poll thread
   publishes its death, waiters are released with its exception, and the timeout message reports the
   budget it actually waited on. Say which released version carries it. Per AGENTS.md, comment
   upstream only when there is something to act on - a fix in a published version is exactly that.
   Use plain cross-repo references, never `Fixes`/`Closes`, and check for the hidden marker before
   posting so it cannot double-comment.
2. **Establish whether confluentinc#809 is the same defect, then reply there too.** Its stack is
   inside `close()` -> `doClose` -> `commitOffsetsThatAreReady`, a different entry point from
   confluentinc#833's steady-state commit, and **no commit on master names astubbs#175 or
   confluentinc#809** - so nothing has been claimed about it. `notifyPollerDied` plausibly covers it
   for the same reason it covers the other, but that is the inference this note already got wrong
   once. Read the close path before replying.
3. **Correct astubbs#44's summary.** It asserts these two are "likely the same defect" as
   confluentinc#803, i.e. the `synchronized(commitCommand)` deadlock whose fix is still open as
   astubbs#29. They are not: confluentinc#833's cause is the poll-thread death astubbs#177 fixed. That
   summary currently points the next reader at the wrong PR, and AGENTS.md's rule is to correct a
   mirror when it turns out wrong rather than leave the next reader to inherit it.
<!-- post-merge: checked-end -->

## The original diagnosis, kept because it found the mechanism

`ConsumerOffsetCommitter`'s own javadoc, added with astubbs#100, names this exact symptom and its
cause:

> **Throw** - let it escape. Fatal: this runs on the broker-poll thread, the only producer of commit
> responses, so killing it strands every waiting committer until `offsetCommitTimeout` and then takes
> the whole PC instance down. This is the "Timeout waiting for commit response" symptom, **whose cause
> looks nothing like it**.

astubbs#100 fixed one route to that: a mid-rebalance commit threw `RebalanceInProgressException`,
nothing caught it, and the broker-poll thread died permanently.

confluentinc#833's own forensics fit the mechanism precisely. The reporter shows the last commit
response added at 21:16:54 and a waiter still blocked at 21:33:39 - **about 17 minutes** with no
commit response produced, and `pc_processed_records_total` flat across the window. A 30-second
timeout expiring does not explain a 17-minute gap; the only producer of commit responses being dead
does.

**What was not established at the time:** whether both reports are that cause. confluentinc#833 runs a
50%-failure workload, which also touches the *dirty* asymmetry - only a success marks a partition
dirty, so a workload where nothing succeeds attempts no commits at all. That question is now closed
for confluentinc#833 and still open for confluentinc#809, per task 2 above.

### How the wrong duration got into both reports

`ConsumerOffsetCommitter#commitAndWait` used to wait on `commitTimeout`
(`options.getOffsetCommitTimeout()`) while interpolating `AbstractParallelEoSStreamProcessor.DEFAULT_TIMEOUT`
into the message:

```java
Duration timeout = AbstractParallelEoSStreamProcessor.DEFAULT_TIMEOUT;          // PT30S
CommitResponse take = commitResponseQueue.poll(commitTimeout.toMillis(), ...);  // waits commitTimeout
if (take == null)
    throw InternalRuntimeException.msg("Timeout waiting for commit response {} ...", timeout, ...);
```

So every occurrence reported a duration that was not the one that elapsed, unless the two happened to
be equal. Previously noted in `docs/plans/2026-08-01-001-investigate-chaos-w4-red-report.md` and not
acted on until astubbs#177's fix.

## Why this is worth doing

These are user-visible hangs, reported by people running this in production, sitting open upstream with
no reply - and the fork now fixes them. That is both the single best advertisement for the fork's
existence and a debt owed to the people who reported them.
