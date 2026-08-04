# TODO index

**Generated file - do not edit by hand.** Regenerate with `bin/todo-index.sh`
(`bin/todo-index.sh --check` fails if this file is stale).

Every `TODO` / `FIXME` / `XXX` marker in the tracked tree, grouped by module. **90 marker(s)** at
the time of generation.

## Prioritised? See the refactoring backlog

This file is a raw, generated inventory - deliberately **not** prioritised, because it is rewritten
wholesale on every run. Triage lives in [`docs/refactoring.md`](refactoring.md), which is the
repo's existing backlog: markers that turn out to be real deferred work get written up there (grouped
by file, with breaking changes in their own release-gated section). Do **not** start a parallel
priority list - that was tried and duplicated the backlog.

Most markers are notes-to-self and should stay exactly where they are; this index makes them
discoverable in aggregate without promoting them to tasks.

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

- L144 - todo check legacy is recursive

**`parallel-consumer-core/src/main/java/io/confluent/parallelconsumer/internal/AbstractParallelEoSStreamProcessor.java`**

- L90 - todo delete in next major version
- L102 - todo delete in next major version
- L123 - todo make package level
- L930 - todo move into {@link WorkManager} as it's specific to WM having enough work?
- L1237 - todo can sleep for less than this time? is this lower bound required? given that if we're starved - the failed work will most likely be selected? And even if not selected - then we will no longer be starved.

**`parallel-consumer-core/src/main/java/io/confluent/parallelconsumer/internal/ConsumerOffsetCommitter.java`**

- L104 - todo keep work in limbo until async response is received?

**`parallel-consumer-core/src/main/java/io/confluent/parallelconsumer/internal/DynamicLoadFactor.java`**

- L20 - todo make so can be fractional like 50% - this is because some systems need a fractional factor, like 1.1 or 1.2 rather than 2

**`parallel-consumer-core/src/main/java/io/confluent/parallelconsumer/internal/ExternalEngine.java`**

- L52 - TODO optimise thread usage by not using any extra thread here at all - go straight from the control thread to
- L91 - TODO: Now that the modules don't use the internal threading systems at all, is this method redundant as all work from a module extension would return true

**`parallel-consumer-core/src/main/java/io/confluent/parallelconsumer/internal/ProducerManager.java`**

- L245 - todo consider wrapping all client calls with a catch and new exception in the ProducerWrapper, so can get stack traces
- L265 - TODO talk about alternatives to this brute force approach for retrying committing transactions

**`parallel-consumer-core/src/main/java/io/confluent/parallelconsumer/metrics/PCMetricsDef.java`**

- L43 - TODO: Not implemented yet - add to Metrics.adoc when implemented
- L46 - TODO: Not implemented yet - add to Metrics.adoc when implemented

**`parallel-consumer-core/src/main/java/io/confluent/parallelconsumer/offsets/BitSetEncoder.java`**

- L90 - TODO refactor inivtV2 and V1 together, passing in the Short or Integer

**`parallel-consumer-core/src/main/java/io/confluent/parallelconsumer/offsets/EncodedOffsetPair.java`**

- L100 - throw new InternalRuntimeException("Invalid state"); // todo why is this needed? what's not covered?

**`parallel-consumer-core/src/main/java/io/confluent/parallelconsumer/offsets/OffsetBitSet.java`**

- L21 - todo unify or refactor with {@link BitSetEncoder}. Why was it ever separate?

**`parallel-consumer-core/src/main/java/io/confluent/parallelconsumer/offsets/OffsetDecodingError.java`**

- L13 - TODO should extend java.lang.Error ?

**`parallel-consumer-core/src/main/java/io/confluent/parallelconsumer/offsets/OffsetMapCodecManager.java`**

- L33 - TODO: consider IO exception management - question sneaky throws usage?
- L35 - TODO: enforce max uncommitted {@literal <} encoding length (Short.MAX)
- L54 - todo remove static state manipulation from tests (make non static)
- L65 - todo refactored to constant in the remove statics branch
- L93 - todo change to List as Sets have no order
- L114 - todo remove consumer #233
- L131 - todo this is the only method that needs the consumer - offset encoding is being conflated with decoding upon assignment #233
- L132 - todo make package private?
- L133 - todo rename
- L136 - todo this should be controlled for - improve consumer management so that this can't happen

**`parallel-consumer-core/src/main/java/io/confluent/parallelconsumer/offsets/OffsetRunLength.java`**

- L92 - TODO: look at offset encoding logic - maybe in those cases we should not create metadata at all?

**`parallel-consumer-core/src/main/java/io/confluent/parallelconsumer/offsets/OffsetSimultaneousEncoder.java`**

- L212 - TODO: optimisation - inline this into the partition iteration loop in {@link WorkManager}
- L214 - TODO: optimisation - could double the run-length range from Short.MAX_VALUE (~33,000) to Short.MAX_VALUE * 2
- L218 - TODO VERY large offset ranges is slow (Integer.MAX_VALUE) - encoding scans could be avoided if passing in map of incompletes which should already be known
- L227 - todo refactor this loop into the encoders (or sequential vs non sequential encoders) as RunLength doesn't need

**`parallel-consumer-core/src/main/java/io/confluent/parallelconsumer/ParallelConsumerOptions.java`**

- L285 - todo delete in next major version

**`parallel-consumer-core/src/main/java/io/confluent/parallelconsumer/ParallelEoSStreamProcessor.java`**

- L80 - todo refactor to it's own class, so that the wrapping function can be used directly from

**`parallel-consumer-core/src/main/java/io/confluent/parallelconsumer/ParallelStreamProcessor.java`**

- L36 - todo why isn't this in ParallelConsumer ?

**`parallel-consumer-core/src/main/java/io/confluent/parallelconsumer/state/PartitionState.java`**

- L236 - todo rename isRecordComplete()
- L237 - todo add support for this to TruthGen
- L491 - todo refactor use of null shouldn't be needed. Is OffsetMapCodecManager stateful? remove null #233

**`parallel-consumer-core/src/main/java/io/confluent/parallelconsumer/state/PartitionStateManager.java`**

- L51 - todo remove static
- L124 - OffsetMapCodecManager<K, V> om = new OffsetMapCodecManager<>(module); // todo remove throw away instance creation - #233

**`parallel-consumer-core/src/main/java/io/confluent/parallelconsumer/state/WorkContainer.java`**

- L60 - todo change to enum, remove setter - #241

**`parallel-consumer-core/src/main/java/io/confluent/parallelconsumer/state/WorkManager.java`**

- L49 - todo make private
- L53 - todo make private

**`parallel-consumer-core/src/test-integration/java/io/confluent/parallelconsumer/integrationTests/BrokerIntegrationTest.java`**

- L115 - todo need to customise this for this test

**`parallel-consumer-core/src/test-integration/java/io/confluent/parallelconsumer/integrationTests/CloseAndOpenOffsetTest.java`**

- L96 - todo remove - not even relevant to this test? smelly
- L162 - TODO test for event/trigger instead - could consume offsets topic but have to decode the binary
- L357 - TODO: fatal vs retriable exceptions. Retry limits particularly for draining state?

**`parallel-consumer-core/src/test-integration/java/io/confluent/parallelconsumer/integrationTests/KafkaSanityTests.java`**

- L70 - todo remove static dependencies

**`parallel-consumer-core/src/test-integration/java/io/confluent/parallelconsumer/integrationTests/LargeVolumeInMemoryTests.java`**

- L107 - TODO: Assert process ordering

**`parallel-consumer-core/src/test-integration/java/io/confluent/parallelconsumer/integrationTests/MultiInstanceHighVolumeTest.java`**

- L53 - todo multi commit mode, multi partition count, multi instance count? 2,3,10,100? more instances than partitions, more partitions than instances

**`parallel-consumer-core/src/test-integration/java/io/confluent/parallelconsumer/integrationTests/MultiTopicTest.java`**

- L77 - When consumer-interface #XXX is merged, could just poll PC directly (see commented out assertCommit below).

**`parallel-consumer-core/src/test-integration/java/io/confluent/parallelconsumer/integrationTests/PartitionOrderProcessingTest.java`**

- L48 - todo refactor move up

**`parallel-consumer-core/src/test-integration/java/io/confluent/parallelconsumer/integrationTests/RebalanceTest.java`**

- L54 - todo refactor move up

**`parallel-consumer-core/src/test-integration/java/io/confluent/parallelconsumer/integrationTests/TransactionAndCommitModeTest.java`**

- L209 - todo rounds should be 1? progress should always be made
- L272 - todo performance: tighten up progress check (<2)

**`parallel-consumer-core/src/test-integration/java/io/confluent/parallelconsumer/integrationTests/TransactionMarkersTest.java`**

- L66 - todo move to super?
- L95 - todo can these gaps also be created by log compaction? If so, is the solution the same?

**`parallel-consumer-core/src/test-integration/java/io/confluent/parallelconsumer/integrationTests/utils/KafkaClientUtils.java`**

- L118 - todo docs

**`parallel-consumer-core/src/test/java/io/confluent/csid/utils/KafkaTestUtils.java`**

- L46 - todo not used anymore - delete?
- L301 - todo move to specific assertion utils class, along with other legacy assertion utils?

**`parallel-consumer-core/src/test/java/io/confluent/parallelconsumer/AbstractParallelEoSStreamProcessorTestBase.java`**

- L57 - todo migrate commit assertion methods in to a Truth Subject

**`parallel-consumer-core/src/test/java/io/confluent/parallelconsumer/JStreamParallelEoSStreamProcessorTest.java`**

- L26 - TODO this class shouldn't have access to the non streaming async consumer - refactor out another super class layer

**`parallel-consumer-core/src/test/java/io/confluent/parallelconsumer/offsets/OffsetEncodingBackPressureTest.java`**

- L74 - todo refactor test to use the new DI system, to manipulate one of the mocks to force test scenario, instead of messing with static state
- L85 - todo - very smelly - store for restoring
- L88 - todo don't use static public accessors to change things - makes parallel testing harder and is smelly
- L292 - todo restore static defaults - lazy way to override settings at runtime but causes bugs by allowing them to be statically changeable
- L293 - OffsetMapCodecManager.DefaultMaxMetadataSize = realMax; // todo wow this is smelly, but convenient

**`parallel-consumer-core/src/test/java/io/confluent/parallelconsumer/offsets/OffsetEncodingBackPressureUnitTest.java`**

- L55 - todo - very smelly - store for restoring
- L58 - todo don't use static public accessors to change things - makes parallel testing harder and is smelly
- L185 - todo restore static defaults - lazy way to override settings at runtime but causes bugs by allowing them to be statically changeable
- L186 - OffsetMapCodecManager.DefaultMaxMetadataSize = realMax; // todo wow this is smelly, but convenient

**`parallel-consumer-core/src/test/java/io/confluent/parallelconsumer/offsets/OffsetEncodingTests.java`**

- L185 - todo don't use static public accessors to change things - makes parallel testing harder and is smelly

**`parallel-consumer-core/src/test/java/io/confluent/parallelconsumer/offsets/WorkManagerOffsetMapCodecManagerTest.java`**

- L51 - todo refactor - remove tests which use hard coded state vs dynamic state - #compressionCycle, #selialiseCycle, #runLengthEncoding, #loadCompressedRunLengthRncoding

**`parallel-consumer-core/src/test/java/io/confluent/parallelconsumer/ParallelEoSStreamProcessorTest.java`**

- L496 - awaitForSomeLoopCycles(50); // async commit can be slow - todo change this to event based
- L518 - awaitForSomeLoopCycles(3); // async commit can be slow - todo change this to event based
- L528 - awaitForSomeLoopCycles(3); // async commit can be slow - todo change this to event based
- L695 - verify(producerSpy, after(verificationWaitDelay).never()).commitTransaction(); // todo remove all wait nevers in favour of triggers as it slows down test

**`parallel-consumer-core/src/test/java/io/confluent/parallelconsumer/state/ShardKeyTest.java`**

- L44 - todo split up

**`parallel-consumer-core/src/test/java/io/confluent/parallelconsumer/truth/TruthGeneratorTests.java`**

- L27 - todo check legacy's also contribute to subject graph


### parallel-consumer-examples/parallel-consumer-example-core

**`parallel-consumer-examples/parallel-consumer-example-core/src/test/java/io/confluent/parallelconsumer/examples/core/CoreAppTest.java`**

- L78 - .thenReturn(new ConsumerGroupMetadata("groupid")); // todo fix AK mock consumer


### parallel-consumer-examples/parallel-consumer-example-metrics

**`parallel-consumer-examples/parallel-consumer-example-metrics/src/test/java/io/confluent/parallelconsumer/examples/metrics/integrationTests/CoreAppMetricsIntegrationTest.java`**

- L46 - when(mockConsumer.groupMetadata()).thenReturn(new ConsumerGroupMetadata("groupid")); // todo fix AK mock consumer


### parallel-consumer-examples/parallel-consumer-example-reactor

**`parallel-consumer-examples/parallel-consumer-example-reactor/src/test/java/io/confluent/parallelconsumer/examples/reactor/ReactorAppTest.java`**

- L65 - Mockito.when(mockConsumer.groupMetadata()).thenReturn(new ConsumerGroupMetadata("groupid")); // todo fix AK mock consumer


### parallel-consumer-examples/parallel-consumer-example-vertx

**`parallel-consumer-examples/parallel-consumer-example-vertx/src/test/java/io/confluent/parallelconsumer/examples/vertx/VertxAppTest.java`**

- L71 - Mockito.when(mockConsumer.groupMetadata()).thenReturn(new ConsumerGroupMetadata("groupid")); // todo fix AK mock consumer


### parallel-consumer-vertx

**`parallel-consumer-vertx/src/main/java/io/confluent/parallelconsumer/vertx/JStreamVertxParallelEoSStreamProcessor.java`**

- L153 - todo change to class generic type variables? 2 fields become 1. Not worth the hassle atm.

**`parallel-consumer-vertx/src/main/java/io/confluent/parallelconsumer/vertx/VertxParallelEoSStreamProcessor.java`**

- L119 - TODO optimise thread usage by not using any extra thread here at all - go straight from the control thread to

**`parallel-consumer-vertx/src/test/java/io/confluent/parallelconsumer/vertx/VertxTest.java`**

- L113 - todo how is this different from #failingHttpCall ?


### src/docs/development

**`src/docs/development/upstream-map.yaml`**

- L59 - notes free text (internal; may be scratch/TODO phrasing)
- L64 - todo OPTIONAL list of outstanding actions (e.g. merge the open PR,
- L66 - (and advance `status`) once done. `upstream-map.py todo` lists them.

