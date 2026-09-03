# Infer's annotations: `@ThreadConfined` is the first one in use, and the rest are a blind spot

<!-- inflight-type: bug -->
<!-- inflight-impact: blind-spot -->
<!-- inflight-labels: concurrency -->

The infer lane (`bin/infer-test.sh`, register: [`static-infer-findings.md`](static-infer-findings.md))
runs RacerD, pulse and starvation over core main code and gates on an identity ratchet. Every one
of those checkers reads annotations from `com.facebook.infer.annotation`, and until astubbs#225 the
jar was not even on the classpath: the analysis was running on what it could *infer* and nothing
the code *declared*. The first declaration is `@ThreadConfined(PartitionState.CONTROL_THREAD)` on
the recovery pass and the ledger replay (the rule is in
`parallel-consumer-core/src/main/java/bz/stub/parallelconsumer/AGENTS.md`, "Declare thread
confinement"). This note is the survey that follows: which of the other annotations would change
what the lane can see, and where the confinement one belongs next.

**The ratchet is the evidence.** `config/infer-known-findings.txt` fails both ways, so an
annotation that changes RacerD's verdict shows up as an identity that stopped firing (retire the
line, in the same commit, saying why) or one that started (a real find, or a wrong declaration).
An annotation that changes nothing is a comment the analyser happens to parse - still worth having
where it records a confinement decision, but not evidence of anything.

## Where `@ThreadConfined` belongs next - two experiments, each one commit

- **`lastCommitTime` on the processor.** Two of the ratchet's known races
  (`AbstractParallelEoSStreamProcessor.controlLoop`, `.isTimeToCommitNow`) are the same read of
  `lastCommitTime`, whose only writer is `commitOffsetsThatAreReady` and whose only reader is
  `isTimeToCommitNow` - both control-thread methods. If that is so, confining the field (or both
  methods) retires both identities honestly, and the retirement is the proof. If RacerD keeps
  reporting, one of the accesses is not on the thread we think, which is a finding of its own.
- **`RetryQueue$RetryQueueIterator.closed`.** Four known races on one boolean: an iterator handed
  out under the queue's read lock and closed by the thread that iterated. If an iterator is only
  ever used by the thread that opened it, `@ThreadConfined(ThreadConfined.ANY)` on the class says
  exactly that and retires four identities. If iterators can cross threads, the flag needs to be
  volatile and the races are real - either answer improves on "known".

After those, the candidates are every field the javadoc already calls control-thread-only or
poll-thread-only: the processor's state transitions and mailbox drain, `BrokerPollSystem`'s
poll-thread state (guarded at runtime by `ThreadConfinedConsumer`, declared to nothing), and the
replay-generation stamp at dispatch. Sweep by grepping the javadoc for "control thread" and "poll
thread"; each hit is a declaration not yet made.

## The other annotations, and which would move the lane

- **`@ThreadSafe` (class).** The one that turns discovery into declaration. RacerD only reports on
  code it believes is meant to be thread-safe - it infers that from lock use - so a class that
  shares state without locks can be *silent* rather than clean. Marking the classes that are shared
  by design (`WorkManager`, `ProducerManager`, `PCMetrics`, the shard map) tells RacerD to report
  every unsynchronized access in them. Expect the ratchet to grow the first time; that growth is
  the blind spot this note is named for.
- **`@Initializer`.** Methods that run before the object is published (`PCModule`'s wiring,
  `ConsumerManager.init`) are excluded from race reporting, which removes false positives that
  otherwise get baselined into the ratchet as "known".
- **`@Functional`.** A method whose result is a pure function of its inputs may be read racily
  without a report; for the metrics suppliers that read counters this is the honest description.
- **`@SynchronizedCollection`** on fields holding `Collections.synchronized*` wrappers, which RacerD
  otherwise cannot tell from a plain collection - the shape of the `PCMetrics.registeredMeters`
  defect in the register.
- **`@NonBlocking` / `@Lockless`** feed the starvation checker: a method so marked that then blocks
  on a lock is reported. The rebalance callbacks (`ArchitectureTest.rebalanceCallbacksMustNotBlock`
  enforces this by hand) are the obvious first sites.
- **The `Nullsafe` family** (`@Nullsafe`, `@Nullable` propagation, the `@SuppressField*` set) is a
  different checker mode: opt a class in and every unannotated nullable flow becomes a finding. The
  pulse `NULLPTR_DEREFERENCE` group in the ratchet - the `PartitionStateManager.getPartitionState`
  callers - is the same question the stale-arrival note
  (`core-stale-arrival-guard-needs-a-null-safety-decision.md`) has open, and belongs with it rather
  than here.
- Not for this codebase: `PrivacySource`/`Sink`, `IntegritySource`/`Sink` (taint), `Expensive`,
  `PerformanceCritical`, `IgnoreAllocations`, `NoAllocation` (the Android-oriented cost checkers),
  `OkToExtend`, `Cleanup`, `ReturnsOwnership`.

## Done when

Each experiment above has run once and its result is in the ratchet or in
[`static-infer-findings.md`](static-infer-findings.md); `@ThreadSafe` has been tried on at least one
shared class and the findings it surfaced are triaged; and the engine's `AGENTS.md` names which of
these the repo uses and which it does not, so the next reader does not re-derive this list.
