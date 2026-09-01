# Two commit signals the PC path's `commitNeeded()` cannot see

<!-- inflight-type: bug -->
<!-- inflight-impact: data-loss -->

<!-- post-merge: checked-begin -->
<!-- astubbs/parallel-consumer#271 is cited as the permanent home of the review threads these two
     findings came from, which is exactly what stays true after it merges - a landed PR is a durable
     link, and the whole reason this note exists is that a thread is not. -->
`parallel-consumer-streams` (astubbs#255). Both come from unresolved review threads on
astubbs/parallel-consumer#271, and **neither is answered by any rung of the reconstructed stack** -
which is why they are written down here rather than left in a thread that closes when the PR does.

They are one shape seen twice. The patched `StreamTask.commitNeeded()` short-circuits to
`pcAwareCommitNeeded()` whenever a dispatcher is present, and that helper answers exactly one
question - does Parallel Consumer hold uncommitted work. Everything stock's version *also* consults
is therefore unreachable on this path, and each of those things is a real signal.

## 1. An explicit `context().commit()` is set and never read

Stock returns `commitNeeded || commitRequested`. `commitRequested` is the flag
`ProcessorContextImpl.commit()` sets, and it is the only field the patch makes `volatile` precisely
because a **worker** thread can now set it - the patch's own javadoc calls that "the one sanctioned
cross-thread commit-state write on the PC path", which only makes sense if something reads it.
Nothing does: `pcAwareCommitNeeded()` returns `pcDispatcher.hasUncommittedWork()` and never looks at
it.

So a processor that asks for an out-of-band commit gets one only when PC's own accounting
independently agrees a commit is due. When it does not, the request is silently dropped. The
volatile field is currently write-only on the PC path, which is the tell.

## 2. A poll batch of nothing but control markers advances the position unseen

Stock does not trust its `commitNeeded` flag alone either: when it is false it compares the consumer
position against `committedOffsets`, with the comment that "there may be non data records such as
control markers bypassed" - records that move the consumer position without ever reaching
`addRecords` or `doProcess`. The PC branch returns before that sweep, and the patch's own comment
concedes why the sweep could not be kept: it reads `consumedOffsets`, which the PC path no longer
maintains.

Under EOS or `read_committed`, a poll batch can contain only aborted-transaction control markers.
Nothing is registered with the `WorkManager`, `hasUncommittedWork()` stays false, and the advance is
never signalled.

## Why neither is a rung's leftover

Both were read against the reconciled tree rather than against the state the threads were written on,
because four rungs have changed this area since: the refusal envelope, the task lifecycle, error
surfacing and stream time. The other nine threads on astubbs/parallel-consumer#271 either close
against a named rung or are answered as a documented contract. These two survive all of it, and
`grep -n 'commitRequested' parallel-consumer-streams/src/main/patch/pc-streams.patch` is the check -
it finds the declaration and the javadoc, and no read.
<!-- post-merge: checked-end -->

## What settling them needs

Not a one-line `|| commitRequested`. That is the same shape as the
`hasUncommittedWork() || commitNeeded` candidate the stream-time rung measured and rejected: it
changes commit cadence for every PC-path caller, and `validateClean()` reads the same answer, so a
clean close after a punctuate-only interval starts throwing `TaskMigratedException`. Whatever is
done has to separate "a commit is *requested*" from "there is *work* outstanding", because
`validateClean` wants only the second.

The control-marker half additionally needs a position source the PC path does maintain -
`consumedOffsets` is gone by design, so reconstructing the sweep means deciding what replaces it.

## Delete when

Both signals reach a commit decision on the PC path, or a measurement shows neither can arise on the
supported envelope and that is recorded where a reader meets the claim.
