# Refactoring backlog

Deferred internal refactors - improvements noticed while working that are too big
or too risky to fold into the change at hand, to be picked up **when things are
quiet**. This is a solo-maintainer backlog, not an issue tracker: entries live
here (versioned, greppable, zero per-item ceremony) instead of as GitHub issues.

## How to use it

- **Capture at the point of noticing** with a code marker: `// TODO(refactor): <one line>`
  right where you spot it. `grep -rn "TODO(refactor)" --include=*.java` lists them.
- **Write it up here** only when it's big enough to warrant context (where, why
  deferred, size/risk, payoff, links). Small point-fixes can stay as bare markers.
- **Grouped by file** so items surface when you're editing that file and so edits
  cluster (fewer merge conflicts); cross-cutting/architectural items that have no
  single home file go in the first section.
- **Graduation rule:** an item only becomes a branch/PR when you actually start it
  (same rule as `upstream-map.yaml`). If it maps to an upstream issue, link it -
  don't duplicate it.
- **Not** for: in-flight work (`docs/inflight.md`), fork↔upstream mapping
  (`src/docs/development/upstream-map.yaml`), solved problems (`docs/solutions/`),
  or PR-specific review feedback (raise that on the PR).

**Reference convention** (same as the changelog): bare `#NN` = this fork
(astubbs/parallel-consumer); `upstream #NN` = confluentinc/parallel-consumer.
Prior draft PRs (`upstream #NNN`) are astubbs's closed-unmerged upstream PRs -
use them as design references, not branches to resurrect (they've bitrotted).
`origin/<branch>` names are abandoned fork drafts kept only as design refs.

---

## Cross-cutting / architectural

These are large, mostly interdependent, and several are **undecided**. Most trace
back to upstream #200. Do not start one casually.

### Thread model: eliminate the separate poller thread (MASSIVE, UNDECIDED)
- **upstream #200** - "Consider a shared-nothing architecture, to reduce thread
  complexity." The canonical tracker for merging/simplifying the controller /
  poller / worker threads ("the ultimate simplification would be to eliminate the
  separate poller thread"). Undecided whether it'll ever be done.
- Motivates the #857 deadlock class (poll vs control thread on `commitCommand`) -
  a single-threaded core would kill that bug class outright.
- Design refs (bitrotted): draft `upstream #270` (Partition Events); branch cluster
  `origin/improvements/{lambda-actor-bus,commit-command-actor,poller-bus-actor,rebalance-messages,transactions-dont-block}`,
  `origin/refactor/{control-loop,extract-controller,state-machine}`, `origin/massive-refactor`.

### Decompose the God class - `AbstractParallelEoSStreamProcessor` (1533 lines)
- Control loop + lifecycle/state machine + commit orchestration + threading +
  rebalance listener + deprecated options all in one class. Design ref: `upstream #488`
  (Refactor God class to components). `state-machine` extraction lives in
  `origin/refactor/state-machine`. Size/risk: high; do alongside the #200 work.

### Remove static state (unblocks parallel test execution)
- Several classes hold static state only to satisfy tests, which forces
  serial test runs. Design refs: `upstream #405` (remove static state), `upstream #126`
  (remove static manipulation in tests) → enables `upstream #143` (parallel tests in CI).
  Branches `origin/improvements/{remove-static,remove-static-use-pcmodule}`.
  Concrete sites listed under the offsets/state files below.

### Actor / IPC message bus for commits & results
- Replace shared-state coordination with a lightweight actor/mailbox. Design refs:
  `upstream #524` (Actor for commit commands), `upstream #325` (Lambda actor queue IPC).
  Part of the #200 cluster; only meaningful as part of that rework.

### Thread-safe public API surface
- **upstream #186** - "Ensure all PC APIs are thread safe" (labelled *blocker, ver:1.0*).
  Cross-cutting audit; pairs with the thread-model work.

### Performance
- **upstream #884** - "Parallel Consumer is 30x slower than normal consumer" - the
  headline perf issue to characterise before/after any hot-path refactor.
- Shard-count caching (O(n) scan → cached): design ref `upstream #530`; branches
  `origin/improvements/{cache-counts,set-to-list,headset}`.

---

## By file / module (parallel-consumer-core)

### offsets/OffsetMapCodecManager.java — upstream #233 (central)
- Encoding and decoding are conflated; the class needs a `Consumer` only for one
  decode-on-assignment method (L131), is passed `null` elsewhere, and is created as
  throwaway instances. Refactor: split encode/decode, drop the consumer dependency,
  remove the `null` usage. (`// todo ... #233` at L131; static-state at L51-65.)
- L30: prune the "keep multiple encodings for comparison" analysis-only code once
  the encoding choice is settled.
- L33/L35: `sneaky throws` IO-error handling; missing `max-uncommitted < Short.MAX` bound.

### offsets/OffsetSimultaneousEncoder.java
- L218: large offset ranges (→ `Integer.MAX_VALUE`) are slow - encoding scans could
  be skipped by passing in the known incompletes map (algorithmic hot spot).
- L214: run-length range capped at `Short.MAX_VALUE`; could double it.
- L227: move the per-offset loop into the encoder subtypes (RunLength doesn't need it).
- L212: inline into the WorkManager partition-iteration loop (cross-class coupling).
- L90-91: static state kept only to serialise tests (see "Remove static state").

### offsets/BitSetEncoder.java & OffsetBitSet.java
- Unify the V1/V2 init paths (BitSetEncoder L90), and merge/clarify why
  `OffsetBitSet` is a separate class at all (OffsetBitSet L21).

### offsets/OffsetRunLength.java
- L92: possibly avoid creating offset metadata at all in some cases.

### offsets/OffsetDecodingError.java
- L13: should it extend `java.lang.Error`? (exception-hierarchy design)

### state/PartitionState.java (715 lines)
- L96: concurrent commit-data collection exists only because control/poller threads
  share state - removed under shared-nothing (upstream #200).
- L491: `null` passed to the codec manager (upstream #233 - see above).
- L327: visibility widened for legacy tests - encapsulation leak to close once the
  static-state/test refactor lands.

### state/PartitionStateManager.java
- L123 was a throwaway `OffsetMapCodecManager` per assignment (upstream #233); PR #57
  cached it (the #859 leak site), but the broader #233 refactor remains.

### state/WorkContainer.java
- L42: instance field working around static state - folds into the static-state removal.

### internal/AbstractParallelEoSStreamProcessor.java
- God class (see cross-cutting). L930: `todo move into WorkManager` (misplaced
  "enough work?" responsibility). L531: brittle Kafka-consumer-by-classname string
  check. Deprecated `commitInterval` options to delete at next major (L87-103).

### internal/ProducerManager.java
- L265: brute-force transaction-commit retry - explore alternatives.

### internal/ExternalEngine.java
- L52: avoid the extra thread (go straight from the control thread). L91: method may
  be redundant now that modules don't use the internal threading system.

### ParallelConsumerOptions.java (573 lines)
- Accreting deprecated fields (L282-286, L361-363, L500-502) to remove at next major;
  L564 temporary Kafka-compat work-around flag to retire.

### ParallelEoSStreamProcessor.java
- L80: extract the wrapping function into its own class so it's directly reusable.

### metrics/PCMetricsDef.java
- L43/L46: two unimplemented metric definitions - implement or drop (and add to
  `Metrics.adoc` when implemented).

---

## Prior drafts / idea bank

Beyond the design refs cited above, `src/docs/development/upstream-pr-analysis.adoc`
Part 2 catalogues ~53 of astubbs's closed-unmerged upstream PRs, including refactor
and perf work worth mining as specs (not branches): `upstream #356` (async producing),
`upstream #408` (run-length v3 with Longs), `upstream #271` (package restructure),
`upstream #303` (cohesive Consumer/Function API), `upstream #345` (ChaosBroker test
infra), plus the Group B/C/D entries. The abandoned `origin/refactor/*`,
`origin/improvements/*`, `origin/refactors/*` and engine-experiment branches
(`direct-ringbuffer`, `features/disrupter`, `refactor/double-ended-queue`, …) are
their fork-side implementation attempts - design references only.

Other open refactor issues: upstream #290 (refactor test base), #192 (unique thread
names), #78 (customisable ThreadPoolExecutor), #172 (release train for 1.0); fork
#40 (reduce duplication in MockConsumer* test classes).

---

_Seeded 2026-07-28 from a code scan (TODO/FIXME + large-class signals) and a
branch/issue/prior-PR sweep. Keep it pruned: delete items when done, and promote to
a branch/PR only when you actually start one._
