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
- **A residual throughput gap vs master remains, milder and unexplained.** Two alternated runs per
  arm, debug on: fixed branch 1444-1833 rec/s, master control 2465-3107 rec/s, every repetition on
  both arms green and well inside the deadline (the second branch run sat at higher ambient load
  than its adjacent master run, so the ratio is bounded, not precise). It is a separate, milder
  defect than the one fixed - and cluster 2 per-call overhead is NOT convicted of it (JFR showed no
  contention).
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
