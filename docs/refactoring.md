# Refactoring backlog

Deferred internal refactors - improvements noticed while working that are too big
or too risky to fold into the change at hand, to be picked up **when things are
quiet**. This is a solo-maintainer backlog, not an issue tracker: entries live
here (versioned, greppable, zero per-item ceremony) instead of as GitHub issues.

**This is the index for all refactor work.** It also catalogues the abandoned
draft branches and prior closed PRs so their ideas aren't lost - each with what it
did, whether it's still relevant, and any linked issue. The *full verdicts* on the
prior closed PRs stay in `src/docs/development/upstream-pr-analysis.adoc` (Part 2);
this file keeps the actionable pointers.

## How to use it

- **Capture at the point of noticing** with a code marker: `// TODO(refactor): <one line>`
  right where you spot it. `grep -rn "TODO(refactor)" --include=*.java` lists them.
- **Write it up here** only when it's big enough to warrant context (where, why
  deferred, size/risk, payoff, links). Small point-fixes can stay as bare markers.
- **Grouped by file** so items surface when you're editing that file and so edits
  cluster (fewer merge conflicts); cross-cutting/architectural items go up top.
- **Graduation rule:** an item only becomes a branch/PR when you actually start it.
  If it maps to an upstream issue, link it - don't duplicate it.
- **Not** for: in-flight work (`docs/inflight.md`), fork↔upstream mapping
  (`src/docs/development/upstream-map.yaml`), solved problems (`docs/solutions/`),
  or PR-specific review feedback (raise that on the PR).

**Reference convention** (same as the changelog): bare `#NN` = this fork
(astubbs/parallel-consumer); `upstream #NN` = confluentinc/parallel-consumer.
Prior draft PRs (`upstream #NNN`) are astubbs's closed-unmerged upstream PRs, and
`origin/<branch>` are abandoned fork drafts - both **design references, not code to
resurrect** (they've bitrotted).

---

## Cross-cutting / architectural

Large, mostly interdependent, several **undecided**. Most trace to upstream #200.
Do not start one casually.

### Thread model: eliminate the separate poller thread (MASSIVE, UNDECIDED)
- **upstream #200** - "Consider a shared-nothing architecture, to reduce thread
  complexity" - the canonical tracker ("the ultimate simplification would be to
  eliminate the separate poller thread"). Would also kill the #857 deadlock class
  (poll vs control thread on `commitCommand`).
- Design ref: draft `upstream #270` (Partition Events). Abandoned branches:
  `origin/improvements/interrupt-reason` (the interrupting-poll model - wake a
  blocking poll when work arrives), `origin/improvements/poller-bus-actor` (poller
  as an actor), `origin/improvements/rebalance-messages` (rebalance via messages),
  `origin/refactor/control-loop`, `origin/refactor/extract-controller` (extract a
  `SubscriptionHandler` interface, pull Poll up), `origin/refactor/infinite-retry`
  (move timeout-retry into the controller; poller just forwards the error),
  `origin/refactor/function-runner`, `origin/massive-refactor` (the umbrella attempt).

### Decompose the God class - `AbstractParallelEoSStreamProcessor` (1533 lines)
- Control loop + lifecycle/state machine + commit orchestration + threading +
  rebalance listener + deprecated options in one class. Design ref: draft
  `upstream #488`. Branch `origin/refactor/state-machine` (extract the lifecycle
  state machine). Do alongside the #200 work; high risk.

### Actor / IPC message bus for commits & results
- Replace shared-state coordination with a lightweight actor/mailbox. Design refs:
  drafts `upstream #524` (commit-command actor), `upstream #325` (lambda actor queue
  IPC). Branches: `origin/improvements/lambda-actor-bus` (the bus),
  `origin/improvements/commit-command-actor`,
  `origin/improvements/async-process-send-results-using-actor` (process send-results
  via actor instead of a blocking `future.get` - relates to draft `upstream #356`),
  `origin/improvements/transactions-dont-block` (non-blocking tx, depends on the
  actor system), `origin/improvements/{scheduled-commit,actor-scheduled,remove-commit-queue}`.
  Only meaningful as part of the #200 rework.

### Remove static state (unblocks parallel test execution)
- Several classes hold static state only to satisfy tests, forcing serial test
  runs. Design refs: drafts `upstream #405` (remove static state), `upstream #126`
  (remove static manipulation in tests) → enables `upstream #143` (parallel tests).
  Branches `origin/improvements/remove-static` and `.../remove-static-use-pcmodule`
  (replace static state with PCModule DI). Concrete sites under the offsets/state
  files below. **Still relevant.**

### Thread-safe public API surface
- **upstream #186** - "Ensure all PC APIs are thread safe" (labelled *blocker,
  ver:1.0*). Cross-cutting audit; pairs with the thread-model work.

### Performance
- **upstream #884** - "Parallel Consumer is 30x slower than normal consumer" - the
  headline perf issue to characterise before/after any hot-path change.
- Shard-count caching (O(n) scan → cached): design ref draft `upstream #530`;
  branches `origin/improvements/{cache-counts,set-to-list,headset}` (cache / headSet
  the counts; "push TreeSet construction up to source"). **Concrete, still relevant.**
- Engine/queue and encoding experiments: see the idea-bank section below.

---

## By file / module (parallel-consumer-core)

### offsets/OffsetMapCodecManager.java — upstream #233 (central)
- Encoding and decoding are conflated; the class needs a `Consumer` only for one
  decode-on-assignment method (L131), is passed `null` elsewhere, and is created as
  throwaway instances. Refactor: split encode/decode, drop the consumer dependency,
  remove the `null` usage. (static-state at L51-65; see "Remove static state".)
- L30: prune the "keep multiple encodings for comparison" analysis-only code once
  the encoding choice is settled. L33/L35: `sneaky throws` IO handling; missing
  `max-uncommitted < Short.MAX` bound.

### offsets/OffsetSimultaneousEncoder.java
- L218: large offset ranges (→ `Integer.MAX_VALUE`) are slow - scans could be
  skipped by passing in the known incompletes map (draft: `origin/refactor/encode-with-incompletes-direct`).
- L214: run-length range capped at `Short.MAX_VALUE`, could double. L227: move the
  per-offset loop into the encoder subtypes. L212: inline into the WorkManager
  partition loop. L90-91: static state for test serialisation (see cross-cutting).

### offsets/BitSetEncoder.java & OffsetBitSet.java
- Unify the V1/V2 init paths (BitSetEncoder L90); merge/clarify why `OffsetBitSet`
  is separate at all (OffsetBitSet L21).

### offsets/OffsetRunLength.java
- L92: possibly avoid creating offset metadata at all in some cases.

### offsets/OffsetDecodingError.java
- L13: should it extend `java.lang.Error`? (exception-hierarchy design)

### state/PartitionState.java (715 lines)
- L96: concurrent commit-data collection exists only because control/poller threads
  share state - removed under shared-nothing (upstream #200). L491: `null` passed to
  the codec manager (upstream #233). L327: visibility widened for legacy tests.

### state/PartitionStateManager.java
- L123 was a throwaway `OffsetMapCodecManager` per assignment (upstream #233); PR #57
  cached it (the #859 leak site), but the broader #233 refactor remains.

### state/WorkContainer.java
- L42: instance field working around static state - folds into static-state removal.

### internal/AbstractParallelEoSStreamProcessor.java
- God class (see cross-cutting). L930: `todo move into WorkManager` (misplaced
  "enough work?" check). L531: brittle Kafka-consumer-by-classname string check.
  Deprecated `commitInterval` options to delete at next major (L87-103).

### internal/ProducerManager.java
- L162: `syncBeginTransaction()` is `private synchronized` (locks on `this`) -
  lock-hygiene: a dedicated private lock is safer (same idea as the PCMetrics #859
  fix); low priority, separate concern. L265: brute-force transaction-commit retry.

### internal/DynamicLoadFactor.java
- L90: `doStep()` is `private synchronized` (locks on `this`) - same lock-hygiene
  note as ProducerManager; low priority.

### internal/ExternalEngine.java
- L52: avoid the extra thread (go straight from the control thread). L91: method may
  be redundant now that modules don't use the internal threading system.

### ParallelConsumerOptions.java (573 lines)
- Accreting deprecated fields (L282-286, L361-363, L500-502) to remove at next major;
  L564 temporary Kafka-compat work-around flag to retire.

### ParallelEoSStreamProcessor.java
- L80: extract the wrapping function into its own class so it's directly reusable.

### metrics/PCMetricsDef.java
- L43/L46: two unimplemented metric definitions - implement or drop.

---

## Abandoned draft branches (idea bank)

Never-merged fork branches - **design references only** (bitrotted). The
thread-model / actor / static-state / shard-caching clusters are listed under
Cross-cutting above; the rest:

**Perf: engine & queue experiments** → mostly dead-ends; ideas for upstream #884:
- `origin/features/disrupter` - LMAX Disruptor engine experiment.
- `origin/direct-ringbuffer`, `origin/ringbuffer-batch` - ring-buffer engine.
- `origin/refactor/double-ended-queue` - block on work submission to the pool
  instead of on results (backpressure).
- `origin/refactor/worker-queues` - worker-queue rework.
- `origin/refactor/gpt3-central-queue-direct-pull` - central queue, direct pull
  (noted: poller-throttling issue, didn't help).
- `origin/refactor/gpt3-queue-management-with-msg-push` - central distribution via
  actor message, batch-100.
- `origin/external-engine-higher-pressure` - backpressure/pressure system for the
  vertx/reactor external engines (fractional steps).
- `origin/predictive-offset-payloads` - approximate in-flight per partition from the
  offset range.
- `origin/features/least-loaded` - incomplete futures as a loading proxy (→ draft
  `upstream #473` / issue `upstream #394`, least-loaded broker).

**Offset encoding** → relevant to the offsets/*Encoder items above:
- `origin/refactor/encode-with-incompletes-direct` - invoke the encoder with known
  incompletes directly instead of iterating (the `OffsetSimultaneousEncoder` L218 hot-spot).
- `origin/refactor/continuous-encode-22`, `origin/continuous-encode` - split
  run-length sequence/entry; continuous encoding (draft `upstream #46`).
- `origin/encoders-truncate-themselves` - push truncation into the encoders.

**Offsets/state classes** → tie to upstream #233 / #200:
- `origin/refactors/offsets-class`, `.../offsets-class-partition-state` - introduce
  an `Offset` type used by `PartitionState`.
- `origin/refactors/refactor-psm-and-ps`, `origin/partition-state` - PSM/PS rework.

**API / interface**:
- `origin/features/producer-facade` - **DEAD-END, conclusion recorded**: "doesn't
  make sense to have a producer facade." Don't revisit.
- `origin/features/consumer-interface`, `origin/refactor/interface` - Consumer /
  interface naming (→ cohesive-API draft `upstream #303`).
- `origin/refactor/deprecate-jstream` - deprecate the JStream API.
- `origin/move-cons-to-pc` - move the consumer into PC (old/new styles verified equal).
- `origin/refactor/minor-changes` - rename enum to the standard pattern.
- `origin/improvements/nonnull-default` - adopt `@ParametersAreNonnullByDefault`.
- `origin/improvements/module-info` - add JPMS `module-info`.
- `origin/improvements/loom` - Loom/Virtual-Threads POC → **superseded by upstream #908**.
- `origin/custom-thread-pool` - customisable `ThreadPoolExecutor` (→ upstream #78; also
  subsumed by #908).

**Test infrastructure**:
- `origin/refactor/chaos-broker`, `.../chaos-broker-challage-test`,
  `.../test-consumer-disconnect` - ChaosBroker / broker-disconnect testing (draft
  `upstream #345`, issue `upstream #203`).
- `origin/refactor/test-hardening` - OOM diagnostics for `LargeVolumeInMemoryTests` at 1M.
- `origin/refactor/empty-tests` - remove/implement the empty placeholder tests (draft `upstream #496`).
- `origin/improvements/test-perf`, `.../multi-topic-test` - test perf / multi-topic.
- `origin/client-factory` - client-factory config to prevent client reuse (draft `upstream #106`).
- `origin/slf4j-no-logger` - warn when no SLF4J logger is bound (→ `upstream #139`; UX, not a refactor).

---

## Prior closed PRs (idea bank — full verdicts in the .adoc)

`src/docs/development/upstream-pr-analysis.adoc` Part 2 keeps the full catalogue and
verdicts for ~53 closed-unmerged upstream PRs. The refactor/perf-relevant ones, as
specs (not branches):
- **Perf:** `upstream #530` (shard-count caching), `#356` (async producing, #29),
  `#408` (run-length v3 with Longs), `#46` (continuous encoding), `#237` (shard starvation #236).
- **Architecture:** `upstream #488` (God class), `#270` (shared-nothing #200),
  `#524` (commit actor), `#325` (lambda-actor IPC), `#271` (package restructure),
  `#303` (cohesive Consumer/Function API), `#405` (remove static state).
- **Test infra:** `upstream #345` (ChaosBroker #203), `#126` (remove static in
  tests) → `#143` (parallel tests), `#106` (client factory), `#492`/`#494`/`#496`.

Other open refactor issues: `upstream #200`, `#233`, `#290` (refactor test base),
`#186` (thread-safe APIs, 1.0 blocker), `#192` (unique thread names), `#78` (custom
ThreadPoolExecutor), `#172` (1.0 release train); fork `#40` (dedupe MockConsumer* tests).

---

_Seeded 2026-07-28 from a code scan (TODO/FIXME + large-class signals) and a
branch/issue/prior-PR sweep. Keep it pruned: delete items when done, and promote to
a branch/PR only when you actually start one._
