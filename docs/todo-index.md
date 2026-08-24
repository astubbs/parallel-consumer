# TODO index

**Generated file - do not edit by hand.** Regenerate with `bin/todo-index.sh`
(`bin/todo-index.sh --check` fails if this file is stale).

Every `TODO` / `FIXME` / `XXX` marker in the tracked tree, grouped by module.

## Prioritised? See the refactoring backlog

This file is a raw, generated inventory - deliberately **not** prioritised, because it is rewritten
wholesale on every run. Triage lives in [`docs/refactoring.md`](refactoring.md), which is the
repo's existing backlog: markers that turn out to be real deferred work get written up there (grouped
by file, with breaking changes in their own release-gated section). Do **not** start a parallel
priority list - that was tried and duplicated the backlog.

Most markers are notes-to-self and should stay exactly where they are; this index makes them
discoverable in aggregate without promoting them to tasks.

## Finding one

Entries carry no line number - grep the text instead:

```bash
grep -rn "check legacy is recursive"
```

A line number would be wrong within a day and would drag this file into every unrelated diff. The
marker's own text is stable until someone edits the marker.

For the same reason this file carries no marker **count**: it would be a second, drifting statement
of something the list below already says exactly, and the two would disagree the moment either moved.
Count the entries if you need a number. `bin/todo-index.sh` prints one to the console when it runs,
where it cannot go stale.

## How to use this

Markers here are *not* a backlog - most are notes-to-self left next to the code they concern, and
that is the right place for them. This index exists so they are **discoverable in aggregate**: to
spot clusters (several markers around one class usually means a design that wants revisiting), and
so that the backlog can point at the code that motivates an item instead of restating it.

The durable rule of thumb: if a marker describes work someone should actually schedule, write it up
in `docs/refactoring.md` (with a link back to the code). If it is context for whoever next edits
that line, leave it in the code - it will show up here.

### parallel-consumer-core

**`parallel-consumer-core/pom.xml`**

- todo check legacy is recursive

**`parallel-consumer-core/src/main/java/bz/stub/parallelconsumer/ParallelConsumerOptions.java`**

- todo delete in next major version

**`parallel-consumer-core/src/main/java/bz/stub/parallelconsumer/ParallelEoSStreamProcessor.java`**

- todo refactor to it's own class, so that the wrapping function can be used directly from

**`parallel-consumer-core/src/main/java/bz/stub/parallelconsumer/ParallelStreamProcessor.java`**

- todo why isn't this in ParallelConsumer ?

**`parallel-consumer-core/src/main/java/bz/stub/parallelconsumer/internal/AbstractParallelEoSStreamProcessor.java`**

- todo delete in next major version
- todo delete in next major version
- todo make package level
- todo move into {@link WorkManager} as it's specific to WM having enough work?
- todo can sleep for less than this time? is this lower bound required? given that if we're starved - the failed work will most likely be selected? And even if not selected - then we will no longer be starved.

**`parallel-consumer-core/src/main/java/bz/stub/parallelconsumer/internal/ConsumerManager.java`**

- TODO(refactor): a user-facing failure wants a PC-named type - see

**`parallel-consumer-core/src/main/java/bz/stub/parallelconsumer/internal/ConsumerOffsetCommitter.java`**

- todo keep work in limbo until async response is received?
- TODO(refactor): a user-facing failure wants a PC-named type, not "internal runtime" -
- TODO(refactor): a user-facing failure wants a PC-named type - see

**`parallel-consumer-core/src/main/java/bz/stub/parallelconsumer/internal/DynamicLoadFactor.java`**

- todo make so can be fractional like 50% - this is because some systems need a fractional factor, like 1.1 or 1.2 rather than 2

**`parallel-consumer-core/src/main/java/bz/stub/parallelconsumer/internal/ExternalEngine.java`**

- TODO optimise thread usage by not using any extra thread here at all - go straight from the control thread to
- TODO: Now that the modules don't use the internal threading systems at all, is this method redundant as all work from a module extension would return true

**`parallel-consumer-core/src/main/java/bz/stub/parallelconsumer/internal/ProducerManager.java`**

- TODO(refactor): InternalRuntimeException misnames a failed send; throw a specific subclass and rename `exception` to `sendFailure`
- todo consider wrapping all client calls with a catch and new exception in the ProducerWrapper, so can get stack traces
- TODO talk about alternatives to this brute force approach for retrying committing transactions

**`parallel-consumer-core/src/main/java/bz/stub/parallelconsumer/metrics/PCMetricsDef.java`**

- TODO: Not implemented yet - add to Metrics.adoc when implemented
- TODO: Not implemented yet - add to Metrics.adoc when implemented

**`parallel-consumer-core/src/main/java/bz/stub/parallelconsumer/offsets/BitSetEncoder.java`**

- TODO refactor inivtV2 and V1 together, passing in the Short or Integer

**`parallel-consumer-core/src/main/java/bz/stub/parallelconsumer/offsets/EncodedOffsetPair.java`**

- throw new InternalRuntimeException("Invalid state"); // todo why is this needed? what's not covered?

**`parallel-consumer-core/src/main/java/bz/stub/parallelconsumer/offsets/OffsetBitSet.java`**

- todo unify or refactor with {@link BitSetEncoder}. Why was it ever separate?

**`parallel-consumer-core/src/main/java/bz/stub/parallelconsumer/offsets/OffsetDecodingError.java`**

- TODO should extend java.lang.Error ?

**`parallel-consumer-core/src/main/java/bz/stub/parallelconsumer/offsets/OffsetMapCodecManager.java`**

- TODO: consider IO exception management - question sneaky throws usage?
- TODO: enforce max uncommitted {@literal <} encoding length (Short.MAX)
- todo remove static state manipulation from tests (make non static)
- todo refactored to constant in the remove statics branch
- todo change to List as Sets have no order
- todo remove consumer - confluentinc#233
- todo this is the only method that needs the consumer - offset encoding is being conflated with decoding upon assignment - confluentinc#233
- todo make package private?
- todo rename
- todo this should be controlled for - improve consumer management so that this can't happen

**`parallel-consumer-core/src/main/java/bz/stub/parallelconsumer/offsets/OffsetRunLength.java`**

- TODO: look at offset encoding logic - maybe in those cases we should not create metadata at all?

**`parallel-consumer-core/src/main/java/bz/stub/parallelconsumer/offsets/OffsetSimultaneousEncoder.java`**

- TODO: optimisation - inline this into the partition iteration loop in {@link WorkManager}
- TODO: optimisation - could double the run-length range from Short.MAX_VALUE (~33,000) to Short.MAX_VALUE * 2
- TODO VERY large offset ranges is slow (Integer.MAX_VALUE) - encoding scans could be avoided if passing in map of incompletes which should already be known
- todo refactor this loop into the encoders (or sequential vs non sequential encoders) as RunLength doesn't need

**`parallel-consumer-core/src/main/java/bz/stub/parallelconsumer/state/PartitionState.java`**

- todo rename isRecordComplete()
- todo add support for this to TruthGen
- todo refactor use of null shouldn't be needed. Is OffsetMapCodecManager stateful? remove null - confluentinc#233

**`parallel-consumer-core/src/main/java/bz/stub/parallelconsumer/state/PartitionStateManager.java`**

- todo remove static
- OffsetMapCodecManager<K, V> om = new OffsetMapCodecManager<>(module); // todo remove throw away instance creation - confluentinc#233

**`parallel-consumer-core/src/main/java/bz/stub/parallelconsumer/state/WorkContainer.java`**

- todo change to enum, remove setter - confluentinc#241

**`parallel-consumer-core/src/main/java/bz/stub/parallelconsumer/state/WorkManager.java`**

- todo make private
- todo make private

**`parallel-consumer-core/src/test-integration/java/bz/stub/parallelconsumer/integrationTests/BrokerIntegrationTest.java`**

- todo need to customise this for this test

**`parallel-consumer-core/src/test-integration/java/bz/stub/parallelconsumer/integrationTests/CloseAndOpenOffsetTest.java`**

- todo remove - not even relevant to this test? smelly
- TODO: fatal vs retriable exceptions. Retry limits particularly for draining state?

**`parallel-consumer-core/src/test-integration/java/bz/stub/parallelconsumer/integrationTests/KafkaSanityTests.java`**

- todo remove static dependencies

**`parallel-consumer-core/src/test-integration/java/bz/stub/parallelconsumer/integrationTests/LargeVolumeInMemoryTests.java`**

- TODO: Assert process ordering

**`parallel-consumer-core/src/test-integration/java/bz/stub/parallelconsumer/integrationTests/MultiInstanceHighVolumeTest.java`**

- todo multi commit mode, multi partition count, multi instance count? 2,3,10,100? more instances than partitions, more partitions than instances

**`parallel-consumer-core/src/test-integration/java/bz/stub/parallelconsumer/integrationTests/MultiTopicTest.java`**

- When consumer-interface #XXX is merged, could just poll PC directly (see commented out assertCommit below).

**`parallel-consumer-core/src/test-integration/java/bz/stub/parallelconsumer/integrationTests/PartitionOrderProcessingTest.java`**

- todo refactor move up

**`parallel-consumer-core/src/test-integration/java/bz/stub/parallelconsumer/integrationTests/RebalanceTest.java`**

- todo refactor move up

**`parallel-consumer-core/src/test-integration/java/bz/stub/parallelconsumer/integrationTests/TransactionAndCommitModeTest.java`**

- todo performance: tighten up progress check (<2)

**`parallel-consumer-core/src/test-integration/java/bz/stub/parallelconsumer/integrationTests/TransactionMarkersTest.java`**

- todo move to super?
- todo can these gaps also be created by log compaction? If so, is the solution the same?

**`parallel-consumer-core/src/test-integration/java/bz/stub/parallelconsumer/integrationTests/utils/KafkaClientUtils.java`**

- todo docs

**`parallel-consumer-core/src/test/java/bz/stub/parallelconsumer/AbstractParallelEoSStreamProcessorTestBase.java`**

- todo migrate commit assertion methods in to a Truth Subject

**`parallel-consumer-core/src/test/java/bz/stub/parallelconsumer/JStreamParallelEoSStreamProcessorTest.java`**

- TODO this class shouldn't have access to the non streaming async consumer - refactor out another super class layer

**`parallel-consumer-core/src/test/java/bz/stub/parallelconsumer/ParallelEoSStreamProcessorTest.java`**

- awaitForSomeLoopCycles(50); // async commit can be slow - todo change this to event based
- awaitForSomeLoopCycles(3); // async commit can be slow - todo change this to event based
- awaitForSomeLoopCycles(3); // async commit can be slow - todo change this to event based

**`parallel-consumer-core/src/test/java/bz/stub/parallelconsumer/internal/admission/FalsifierScenarios.java`**

- TODO(refactor): wire pauseCycling into the falsifier suite in U6 (plan 2026-08-24-003, R14) - it
- TODO(refactor): wire rebalanceShrink into the falsifier suite in U6 (plan 2026-08-24-003, KTD4) - it
- TODO(refactor): wire floorPin into the falsifier suite in U6 (plan 2026-08-24-003, the escape's

**`parallel-consumer-core/src/test/java/bz/stub/parallelconsumer/internal/utils/KafkaTestUtils.java`**

- todo not used anymore - delete?
- todo move to specific assertion utils class, along with other legacy assertion utils?

**`parallel-consumer-core/src/test/java/bz/stub/parallelconsumer/offsets/OffsetEncodingBackPressureTest.java`**

- todo refactor test to use the new DI system, to manipulate one of the mocks to force test scenario, instead of messing with static state
- todo - very smelly - store for restoring
- todo don't use static public accessors to change things - makes parallel testing harder and is smelly
- todo restore static defaults - lazy way to override settings at runtime but causes bugs by allowing them to be statically changeable
- OffsetMapCodecManager.DefaultMaxMetadataSize = realMax; // todo wow this is smelly, but convenient

**`parallel-consumer-core/src/test/java/bz/stub/parallelconsumer/offsets/OffsetEncodingBackPressureUnitTest.java`**

- todo - very smelly - store for restoring
- todo don't use static public accessors to change things - makes parallel testing harder and is smelly
- todo restore static defaults - lazy way to override settings at runtime but causes bugs by allowing them to be statically changeable
- OffsetMapCodecManager.DefaultMaxMetadataSize = realMax; // todo wow this is smelly, but convenient

**`parallel-consumer-core/src/test/java/bz/stub/parallelconsumer/offsets/OffsetEncodingTests.java`**

- todo don't use static public accessors to change things - makes parallel testing harder and is smelly

**`parallel-consumer-core/src/test/java/bz/stub/parallelconsumer/offsets/WorkManagerOffsetMapCodecManagerTest.java`**

- todo refactor - remove tests which use hard coded state vs dynamic state - #compressionCycle, #selialiseCycle, #runLengthEncoding, #loadCompressedRunLengthRncoding

**`parallel-consumer-core/src/test/java/bz/stub/parallelconsumer/state/ShardKeyTest.java`**

- todo split up

**`parallel-consumer-core/src/test/java/bz/stub/parallelconsumer/truth/TruthGeneratorTests.java`**

- todo check legacy's also contribute to subject graph


### parallel-consumer-examples/parallel-consumer-example-core

**`parallel-consumer-examples/parallel-consumer-example-core/src/test/java/bz/stub/parallelconsumer/examples/core/CoreAppTest.java`**

- .thenReturn(new ConsumerGroupMetadata("groupid")); // todo fix AK mock consumer


### parallel-consumer-examples/parallel-consumer-example-metrics

**`parallel-consumer-examples/parallel-consumer-example-metrics/src/test/java/bz/stub/parallelconsumer/examples/metrics/integrationTests/CoreAppMetricsIntegrationTest.java`**

- when(mockConsumer.groupMetadata()).thenReturn(new ConsumerGroupMetadata("groupid")); // todo fix AK mock consumer


### parallel-consumer-examples/parallel-consumer-example-reactor

**`parallel-consumer-examples/parallel-consumer-example-reactor/src/test/java/bz/stub/parallelconsumer/examples/reactor/ReactorAppTest.java`**

- Mockito.when(mockConsumer.groupMetadata()).thenReturn(new ConsumerGroupMetadata("groupid")); // todo fix AK mock consumer


### parallel-consumer-examples/parallel-consumer-example-vertx

**`parallel-consumer-examples/parallel-consumer-example-vertx/src/test/java/bz/stub/parallelconsumer/examples/vertx/VertxAppTest.java`**

- Mockito.when(mockConsumer.groupMetadata()).thenReturn(new ConsumerGroupMetadata("groupid")); // todo fix AK mock consumer


### parallel-consumer-vertx

**`parallel-consumer-vertx/src/main/java/bz/stub/parallelconsumer/vertx/JStreamVertxParallelEoSStreamProcessor.java`**

- todo change to class generic type variables? 2 fields become 1. Not worth the hassle atm.

**`parallel-consumer-vertx/src/main/java/bz/stub/parallelconsumer/vertx/VertxParallelEoSStreamProcessor.java`**

- TODO optimise thread usage by not using any extra thread here at all - go straight from the control thread to

**`parallel-consumer-vertx/src/test/java/bz/stub/parallelconsumer/vertx/VertxTest.java`**

- todo how is this different from #failingHttpCall ?

