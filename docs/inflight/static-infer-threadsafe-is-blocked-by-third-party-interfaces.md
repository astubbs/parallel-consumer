# `@ThreadSafe` would move the infer lane most, and slf4j's `Logger` is what stops it

<!-- inflight-type: bug -->
<!-- inflight-impact: blind-spot -->
<!-- inflight-labels: concurrency -->

The infer lane (`bin/infer-test.sh`, register: [`static-infer-findings.md`](static-infer-findings.md))
runs RacerD, pulse and starvation over core main code and gates on an identity ratchet. Every one of
those checkers reads annotations from `com.facebook.infer.annotation`, and the jar was not on the
compile classpath at all until the change that opened this note: the analysis ran on what it could
*infer* and nothing the code *declared*.

Every annotation in that jar has now been applied and measured against the lane rather than reasoned
about from Infer's documentation. **The settled verdicts moved to
`parallel-consumer-core/src/main/java/bz/stub/parallelconsumer/AGENTS.md`, "Which of Infer's
annotations this repo uses", which owns them.** This note owns only what is still open: the one
annotation that would move the lane most, and the two things blocking it.

**The ratchet is the evidence, and it fails both ways.** `config/infer-known-findings.txt` reports an
identity that stopped firing as loudly as one that started, so an annotation that changes RacerD's
verdict shows up as a retirement (honest, if the code says so) or a new finding (a real find, or a
wrong declaration). An annotation that changes nothing is a comment the analyser happens to parse.

## The open question: `@ThreadSafe`, and the two things blocking it

RacerD only reports on code it believes is *meant* to be thread-safe, and it infers that from lock
use - so a class that shares state without locks can be **silent** rather than clean. `@ThreadSafe`
on a class is the declaration that turns that silence into reporting, and it is the only annotation
here that turns discovery into declaration.

**Measured, 2026-09-03**: applied to `WorkManager`, `ShardManager`, `ProcessingShard`,
`ConsumerManager`, `BrokerPollSystem`, `ProducerManager` and `PCMetrics` at once, the lane's finding
count went up roughly tenfold. Reproduce by adding `@com.facebook.infer.annotation.ThreadSafe` to
those seven type declarations and running
`INFER_KNOWN=/nonexistent bin/infer-test.sh` (report-only; the numbers are deliberately not written
down here - they are a command's answer, and were already wrong once this note was drafted).

**Blocker 1 - `INTERFACE_NOT_THREAD_SAFE` on interfaces this project cannot annotate.** Over half of
the new findings are of that type, and the two interfaces they name are `org.slf4j.Logger` and
`io.micrometer.core.instrument.Meter`. Both are thread-safe; neither carries `@ThreadSafe`, and
neither is ours to change. That means **every log call site in an annotated class is one finding**,
which no ratchet can usefully hold. Infer's `--disable-issue-type INTERFACE_NOT_THREAD_SAFE` is the
lever, and turning it on is a change to a hosted CI lane's checker set with its own review surface -
that is the first step for whoever picks this up, not an aside.

**Blocker 2 - the largest single group is an artefact of a test-only seam.** With
`INTERFACE_NOT_THREAD_SAFE` set aside, the remaining `THREAD_SAFETY_VIOLATION`s fall into about a
dozen groups keyed on one field each, and the biggest is every non-private `ShardManager` method
reading `processingShards` "racing with the write in `setProcessingShards`". That setter is
package-private and exists for tests; `ShardMapIsNeverReplacedArchTest` already forbids production
callers of it, and RacerD cannot see an ArchUnit rule. So the group is real about the seam and wrong
about the code, and adopting the annotation means answering what to do about the setter first.

**What the triage found in the rest**, which is the part worth not re-deriving:

- **`WorkManager.numberRecordsOutForProcessing`** - a plain `int`, the second-largest group, and the
  counter whose drift `OutForProcessingCounterDriftProbeTest` already probes.
- **`ConsumerManager.commitRequested` and the `noWakeups`/`erroneousWakups`/`correctPollWakeups`
  counters** - RacerD independently refinds what SpotBugs' `AT_*` family already reports, listed in
  `docs/refactoring.md`. Corroboration, not new ground.
- **`BrokerPollSystem.longPollTimeout`** - mutable `static` state with a setter reached from
  `AbstractParallelEoSStreamProcessor.setLongPollTimeout` for tests. The "Remove static state" item
  in `docs/refactoring.md` owns it; this is a second detector finding the same thing.
- **`ConsumerManager.closeInProgressSignal`** - a non-volatile `BooleanSupplier` field with a Lombok
  `@Setter`, wired by `BrokerPollSystem` at construction and read in the poll loop. **This is the
  `@Initializer` case in the flesh**: the write happens before the reading thread is started, so the
  edge exists and the finding is a false positive - which is exactly what `@Initializer` is for, and
  why it only becomes worth writing once `@ThreadSafe` is on.
- **`ShardManager.iterationResumePoint`, `ProcessingShard.slowWarningRateLimit.lastFireMs`** - small,
  and each a genuine confinement question of the shape the two settled `@ThreadConfined` experiments
  answered.

## Sequencing

The `@Lockless` measurement above and astubbs#431 reach
`ArchitectureTest.rebalanceCallbacksMustNotBlock` from opposite sides: astubbs#431 closes that
rule's **method-reference** blind spot and deletes six `KNOWN_BLOCKING_VIOLATIONS`, while
`@Lockless` is about the rule's other structural blind spot - a `synchronized` block is a
`MONITORENTER`, not a method call, so widening the walk does not reach it. Neither makes the other
wrong. `static-archunit-main-code-rules.md` is where the rule's blind spots are enumerated and it
does not yet name the `MONITORENTER` one; astubbs#431 was rewriting that file at the same time, so
the entry was deliberately left for whoever touches it next rather than written into a conflict.
<!-- post-merge: checked -->

## Done when

`INTERFACE_NOT_THREAD_SAFE` is either disabled on the lane or shown to be tolerable, the
`setProcessingShards` question is answered, and `@ThreadSafe` is on at least one shared class with
every finding it raises either fixed or in the ratchet with a reason. The remaining annotations
(`@Initializer`, `@Functional`, `@SynchronizedCollection`) only become writable at that point, and
the engine's `AGENTS.md` already says so.

The `Nullsafe` family is deliberately not here: it is a different checker mode, and the pulse
`NULLPTR_DEREFERENCE` group in the ratchet is the same question
[`core-stale-arrival-guard-needs-a-null-safety-decision.md`](core-stale-arrival-guard-needs-a-null-safety-decision.md)
has open. It belongs with that decision, not this one.
