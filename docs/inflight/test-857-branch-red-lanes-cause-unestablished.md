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

<!-- post-merge: checked-begin - every reference below names its PR in full and is written
     in the past tense as a record of runs that happened, so it reads the same once these
     branches have landed and stopped existing -->
- **MEASURED 2026-09-01, and the expectation was WRONG: the `Performance Tests` lane is a
  SEPARATE defect from the pause-cache one.** This bullet used to predict that
  `MultiInstanceHighVolumeTest` (`PERIODIC_CONSUMER_SYNC`, KEY) shared the cause, on the reasoning
  that the pause-cache mechanism is commit-mode-independent. The lane has now run with the fix in
  place and it still fails, so that prediction is refuted rather than confirmed - recorded here
  because a refuted prediction is worth as much as a held one and disappears if only the new number
  is written down.

  The comparison, same test and identical configuration (`order=KEY maxPoll=500`), each figure from
  one CI run of the lane rather than a standing property - `grep PC-THROUGHPUT` in the job log to
  re-derive any of them:

  | Branch | Result |
  |---|---|
  | astubbs/parallel-consumer#381 (test-scope, cut from master, so the closest thing to a master baseline) | `processed=3000000 expected=3000000 elapsedMs=42024 recordsPerSecond=71387` - PASS |
  | astubbs/parallel-consumer#29 | `processed=2191095 expected=3000000 elapsedMs=60554 recordsPerSecond=36184` - FAILED |

  astubbs/parallel-consumer#29 failed the lane twice with consistent numbers against one clean run
  on astubbs/parallel-consumer#381, so this was two samples versus one and not a single-run coin
  flip. It was still three runs on GitHub-hosted runners, so a repeat would firm it up before anyone
  treats the ratio itself as precise.

- **astubbs/parallel-consumer#393 is RULED OUT as the cause, for free.** Its `Performance Tests`
  lane passed - `MultiInstanceHighVolumeTest` ran green there - and its thread-confinement refactor
  was also present in astubbs/parallel-consumer#29's tree. So whatever halved throughput there was
  not the consumer-ownership work. (No `PC-THROUGHPUT` line exists on that branch: it does not carry
  `ThroughputReport`, which arrived with astubbs/parallel-consumer#381. The evidence is the passing
  test, not a rate.)

  This is the ablation the extraction bought without anyone running an experiment - a branch cut
  from a suspect tree is a control arm for whatever stayed behind, and it is worth checking for one
  before designing a bisect.

- **Cheapest next ablation, not yet run: the INFO-level logging raise.** Both branches of the revoke
  fork were lifted to INFO by the astubbs/parallel-consumer#29 work (`grep "Acquired commitLock on
  revoke"`), and per-loop INFO logging in a three-million-record run is the right shape to cost this
  much throughput. Also on the same suspect list: the coherence check in `describeProgress()` and the
  pause-observation fields on `BrokerPollSystem`. One term at a time, same lane, control arm - the
  method that diagnosis used on itself.
<!-- post-merge: checked-end -->
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
