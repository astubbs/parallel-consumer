# `PartitionState.stateChangedSinceCommitStart` is written from both threads

<!-- inflight-type: bug -->
<!-- inflight-impact: stall -->
<!-- inflight-labels: concurrency -->

A plain `boolean` that both the control thread and the broker-poll thread write. Recorded because
the sweep found it, **not diagnosed** - the pair below is read off the call graph, and nothing has
measured it.

## The pair

- **Control thread** writes `true` in `setDirty`, when a record completes.
- **Broker-poll thread** clears it to `false` in `getCommitDataIfDirty`, at the start of a commit
  cycle, and reads it in `setClean` - which clears `dirty` only `if (!stateChangedSinceCommitStart)`.

The field exists precisely to catch a completion that lands *during* a commit, so the commit does
not mark the partition clean over state it did not include.

## Why it is not the same fix as `dirty`

`volatile` alone is not obviously right here, because this is a **lost update** risk as well as a
visibility one: two threads write the same field with no read-modify-write discipline between them.
<!-- post-merge: checked-begin -->
A control-thread `true` that is lost - whether to staleness or to being overwritten by the poll
thread's `false` - lets the commit be marked clean over state that changed inside the commit window.
That is the burnt-commit-cycle shape astubbs/parallel-consumer#349 fenced `dirty` against, and on a
partition that then goes idle the committed offset waits for the next rebalance.
<!-- post-merge: checked-end -->

Fencing it the way `dirty` was fenced would publish the writes without settling which write wins, so
the fix needs a decision about the protocol, not just a modifier.

## Before fixing it

- There is no measurement. The honest instrument is a probe pair in `jcstress-poc/`, in the shape
  `CommitPathVisibilityProbes` uses for the `dirty` pair -
  [`test-jcstress-probe-module-open-items.md`](test-jcstress-probe-module-open-items.md) records
  that nothing binds those probes to the real code, which applies to a new one too.
- [`test-jcstress-probe-module-open-items.md`](test-jcstress-probe-module-open-items.md) also
  records that no jcstress arm reports this field, so the burnt-commit-cycle harm attributed to the
  `dirty` pair rests on this field's second stale read being argued rather than measured.
- Read [`core-control-thread-contract-debts.md`](core-control-thread-contract-debts.md) and
  [`docs/refactoring.md`](../refactoring.md)'s caution that these fields may be absorbed by the
  shared-nothing rework (confluentinc#200, mirror astubbs#142) rather than fixed one at a time.

## The analyser already names it

SpotBugs names this field under fb-contrib's `AT_STALE_THREAD_WRITE_OF_PRIMITIVE`. Reproduce with
`./mvnw -o spotbugs:spotbugs -pl :parallel-consumer-core` and read the SpotBugs XML report in that
module's build output. The offender list under that rule in
[`docs/refactoring.md`](../refactoring.md) does not carry the `PartitionState` fields - **that entry
owns the list**, so correcting it there is the fix, not restating fields here. The lane runs with
`-Dspotbugs.failOnError=false`, so the finding annotates and never blocks.

<!-- post-merge: checked-begin -->
Split out of [`bug-allowed-more-records-crosses-threads-unfenced.md`](bug-allowed-more-records-crosses-threads-unfenced.md)
during merge prep for astubbs/parallel-consumer#349: it was recorded inside that note, which is
deleted when *that* field is fenced, and would have taken this finding with it.
<!-- post-merge: checked-end -->
