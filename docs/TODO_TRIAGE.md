# TODO triage

Companion to [`docs/TODO_INDEX.md`](TODO_INDEX.md), which is **generated** and lists every marker.
This file is **hand-curated**: it groups those markers into themes and puts them in priority order,
so the analysis is done once instead of being re-derived by whoever next looks at the tree.

**Line numbers deliberately omitted** - they rot. The index has current ones; this file names classes
and themes, which are stable.

Triaged 2026-08-01 against 91 markers. It does not need to be exhaustive or perfectly current to be
useful: if a group's rationale still reads true, it is still good.

---

## How to read the priorities

- **P1** - actively causing pain now (flakes, misleading behaviour, blocked work).
- **P2** - real debt with a clear payoff, safe to schedule.
- **P3** - worth doing when adjacent code is touched; not worth a dedicated slot.
- **P4** - notes-to-self and open questions. Leave them in the code; do *not* convert to tasks.

Anything a person should actually schedule belongs in [`docs/inflight.md`](inflight.md) with a link
here - not restated as a copy of the marker text.

---

## P1 - Test-infrastructure statics and timing (causing flakes today)

**Files:** `OffsetMapCodecManager` ("remove static state manipulation from tests (make non static)",
"refactored to constant in the remove statics branch"), `PartitionStateManager` ("remove static"),
`OffsetEncodingTests` / `OffsetEncodingBackPressureTest` / `OffsetEncodingBackPressureUnitTest`
("don't use static public accessors to change things - makes parallel testing harder and is smelly",
"wow this is smelly, but convenient"), `ParallelEoSStreamProcessorTest` (four markers: "async commit
can be slow - todo change this to event based", "remove all wait nevers in favour of triggers as it
slows down test").

**Why P1, not P3:** this group has already cost real time. The mutable-static codec state produced the
`largeOffsetMap` flake (diagnosed and locked in the test-integrity branch); the timing-based awaits in
`ParallelEoSStreamProcessorTest` are on the repo's known-flake list. These are not stylistic notes -
they are the mechanism behind intermittent CI failures.

**Ordering:** the codec statics first (self-contained, test-only blast radius, already parked as a
low-priority easy win in `inflight.md` - the sketch is written). The `awaitForSomeLoopCycles` →
event-driven conversion is larger and can follow.

## P1 - The #233 cluster: `OffsetMapCodecManager` conflates encode, decode and consumer

**Files:** `OffsetMapCodecManager` ("remove consumer #233", "this is the only method that needs the
consumer - offset encoding is being conflated with decoding upon assignment #233", "this should be
controlled for - improve consumer management so that this can't happen"), `PartitionStateManager`
("remove throw away instance creation - #233"), `PartitionState` ("refactor use of null shouldn't be
needed. Is OffsetMapCodecManager stateful? remove null #233").

**Why grouped:** five markers across three classes all carry the same issue number and describe **one**
design problem - a class that is simultaneously an encoder, a decoder and a consumer-holder. They are
not five tasks.

**Why P1:** this is the only cluster where markers name a shared root cause, and it has already been
paid for piecemeal - PR #57 removed one throw-away instantiation site as a partial step, and the
static `errorPolicy` (set-once per JVM, so multiple PC instances in one JVM clash) is a live
consequence. Each partial fix pays interest; the split is the fix.

**Shape:** separate encode from decode, drop the consumer dependency, make the remaining state
instance state. Note this overlaps the P1 statics group above - doing #233 properly likely subsumes
`OffsetMapCodecManager`'s share of it.

## P2 - Public API surface that should shrink (needs a major version)

**Files:** `AbstractParallelEoSStreamProcessor` ("delete in next major version" ×2, "make package
level"), `ParallelConsumerOptions` ("delete in next major version"), `WorkManager` ("make private"
×2), `OffsetMapCodecManager` ("make package private?", "rename"), `WorkContainer` ("change to enum,
remove setter - #241").

**Why P2 and why together:** each is trivial alone but breaking, so they are gated on the same event -
the next major. Doing them one at a time across releases spends the breaking-change budget repeatedly
for no benefit. **Actionable now:** collect them into a single "next major" checklist so the window is
used once. Worth cross-referencing the 0.7.x / Java-baseline planning in `inflight.md`, which is the
next natural breaking boundary.

## P2 - Offset-encoding performance

**Files:** `OffsetSimultaneousEncoder` ("optimisation - inline this into the partition iteration loop",
"could double the run-length range from Short.MAX_VALUE (~33,000)", "VERY large offset ranges is slow
(Integer.MAX_VALUE) - encoding scans could be avoided if passing in map of incompletes which should
already be known", "refactor this loop into the encoders"), `OffsetRunLength`, `BitSetEncoder`
("refactor initV2 and V1 together"), `OffsetBitSet` ("unify or refactor with BitSetEncoder. Why was it
ever separate?").

**Why P2:** the "VERY large offset ranges is slow" marker is a real scalability limit on the commit
hot path, not a nicety, and the encoder duplication (`BitSetEncoder` vs `OffsetBitSet`, V1 vs V2) is
the kind of parallel implementation that drifts. **Caveat before touching:** this is the most
correctness-critical code in the project - offset encoding decides what gets replayed or lost. Treat
`OffsetEncodingTests` and the chaos ledger as the safety net, and change one encoder at a time.

## P3 - Threading and lifecycle questions

**Files:** `ExternalEngine` ("optimise thread usage by not using any extra thread here at all - go
straight from the control thread", "now that the modules don't use the internal threading systems at
all, is this method redundant"), `VertxParallelEoSStreamProcessor` (the *same* thread-usage marker,
duplicated), `AbstractParallelEoSStreamProcessor` ("can sleep for less than this time? is this lower
bound required?", "move into WorkManager as it's specific to WM"), `ConsumerOffsetCommitter` ("keep
work in limbo until async response is received?").

**Why P3:** genuine design questions, but each needs measurement or a decision rather than an edit.
Two notes: the duplicated thread-usage marker in core and vertx is one question, not two. And the
`ConsumerOffsetCommitter` async-limbo marker is adjacent to the commit-path work in #100 - worth
re-reading whenever that path is next opened, since `PERIODIC_CONSUMER_ASYNCHRONOUS` currently
delivers commit failures to a callback that only logs.

## P3 - "todo fix AK mock consumer" (×4, one upstream problem)

**Files:** `CoreAppTest`, `CoreAppMetricsIntegrationTest`, `ReactorAppTest`, `VertxAppTest` - all
stubbing `mockConsumer.groupMetadata()` to work around Apache Kafka's `MockConsumer` not implementing
it.

**Why grouped:** four markers, one defect, and it is **not ours**. Fixing means either an upstream
Kafka fix or a shared test helper here. Cheap improvement: replace the four copies with one helper so
the workaround has a single home and disappears in one edit when Kafka is fixed. Re-check on the Kafka
4.x upgrade (tracked in `inflight.md`) - the behaviour may already have changed.

## P4 - Notes, questions and small cleanups (leave in place)

Examples: `EncodedOffsetPair` ("why is this needed? what's not covered?"), `OffsetDecodingError`
("should extend java.lang.Error?"), `ParallelStreamProcessor` ("why isn't this in ParallelConsumer?"),
`PartitionState` ("rename isRecordComplete()", "add support for this to TruthGen"),
`DynamicLoadFactor` ("make so can be fractional"), `ProducerManager` ("consider wrapping all client
calls…", "talk about alternatives to this brute force approach"), `PCMetricsDef` ("Not implemented yet
- add to Metrics.adoc when implemented" ×2), `ShardKeyTest` ("split up"), `CloseAndOpenOffsetTest`,
`VertxTest` ("how is this different from #failingHttpCall?"), `BrokerIntegrationTest`, the vertx
generics note, and `parallel-consumer-core/pom.xml` ("check legacy is recursive").

**These are working as intended.** They are context for whoever next edits that line, which is exactly
where such context belongs. Do not convert them into tickets; the index makes them discoverable.

Two are worth a second look if you are already in the file: the `PCMetricsDef` pair describe metrics
that are *declared but not implemented*, which is a documentation-vs-reality gap rather than a note;
and `EncodedOffsetPair`'s "why is this needed" guards a throw on a supposedly unreachable state, which
is the kind of thing chaos testing eventually answers.

---

## Cross-cutting observation

The two P1 groups are the same story told twice: **state that should be instance state is static, and
tests reach in to mutate it.** The codec statics cause flakes; the #233 conflation is why the class has
static state at all. Whoever takes either should read both sections - fixing #233 properly is likely to
resolve most of the P1 statics group as a side effect, which makes it better value than its size
suggests.
