# The confluentinc#857 branch's red lanes - cause ESTABLISHED and fixed; what remains open

<!-- inflight-type: bug -->
<!-- inflight-impact: throughput -->
<!-- inflight-labels: concurrency -->

**The question this note was opened on is answered.** The filename predates the answer and is kept
so citations keep resolving; read the title, not the name. The full diagnosis, differential
evidence, fix and lessons moved to their durable owner:
[`../solutions/performance-issues/paused-poll-wakeup-lost-to-stale-pause-cache-2026-09-01.md`](../solutions/performance-issues/paused-poll-wakeup-lost-to-stale-pause-cache-2026-09-01.md).

<!-- post-merge: checked - names the PR historically; the sentence reads the same after it lands -->
One line of it: the confluentinc#857 branch's (astubbs/parallel-consumer#29) CME fix moved
`ConsumerManager.updateCache()` to poll EXIT only, so
the control thread's paused-poller wakeup read a pause state one poll stale, never fired, and every
back-pressure pause cost the full 2s long-poll timeout with the pipeline drained. Fixed by restoring
an ENTRY refresh (before `pollingBroker` is set, preserving the CME fix);
`ConsumerManagerPauseCacheTest` holds the contract. The `Integration Tests` lane's
`testTransactionalDefaultMaxPoll` failures were this.

## Still open

- **The `Performance Tests` lane failure is not yet re-measured.** `MultiInstanceHighVolumeTest`
  (`PERIODIC_CONSUMER_SYNC`, KEY) plausibly shares the cause - the pause-cache mechanism is
  commit-mode-independent - but that is an expectation, not a measurement. Verify the lane after
  this fix lands.
- **RESOLVED 2026-09-01: the "residual throughput gap" was a measurement artifact, not a defect.**
  It was measured under `-Dpc.log.level=debug` with five concurrent repetitions, which depresses both
  arms and the branch more. Re-measured in the conditions the original regression was measured in -
  no debug logging, arms alternated, three repetitions each at rising load - master ran 4,507-6,106
  records/second and the fixed branch 4,360-5,181, overlapping, with zero failures on either arm.
  Against the pre-fix branch's 648-1,777, the regression is gone and there is no residual to explain.
  Kept as an entry rather than deleted, because a plausible unexplained residual is exactly the kind
  of open thread that survives for months on nobody's list.
- **The "incoherent counters" observation stands but its reading changed.** The 0-vs-319
  disagreement was sampled by the TEST thread at failure time, while the 319 came from the control
  thread - and `numberRecordsOutForProcessing` is a plain int with no fence
  ([`bug-number-records-out-for-processing-is-a-plain-int.md`](bug-number-records-out-for-processing-is-a-plain-int.md)),
  so a cross-thread stale read is the parsimonious explanation. Nothing yet demonstrates a
  same-thread incoherence; the `describeProgress()` coherence flag has still never fired and remains
  unarmed. Anyone promoting it to a gate must first account for reader-thread staleness, and check
  astubbs/parallel-consumer#336 / astubbs/parallel-consumer#335, which rebuild exactly these
  counters - and astubbs/parallel-consumer#361 (direct pull) removes the executor-queue signal the
  condition reads, under which the check would go permanently quiet rather than fail.
