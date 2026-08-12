package io.confluent.parallelconsumer.streams;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.internals.ProcessorContextImpl;
import org.apache.kafka.streams.processor.internals.ProcessorNode;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.processor.internals.ProcessorStateManager;
import org.apache.kafka.streams.processor.internals.RecordCollector;
import org.apache.kafka.streams.processor.internals.RecordQueue;
import org.apache.kafka.streams.processor.internals.SourceNode;
import org.apache.kafka.streams.processor.internals.StreamTask;
import org.apache.kafka.streams.processor.internals.Task;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.internals.ThreadCache;
import org.assertj.core.groups.Tuple;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * The load-bearing change of this module, asserted directly: Kafka Streams' per-task mutable state is safe to
 * touch from more than one thread at a time.
 * <p>
 * Stock Streams keeps the "which record am I on" and "which node am I in" state in two {@code protected} fields
 * on {@link org.apache.kafka.streams.processor.internals.AbstractProcessorContext}, because exactly one
 * StreamThread ever owns a task. PC's whole proposition is N worker threads inside one task at once, so those
 * two fields become the first thing to corrupt - and they corrupt <em>silently</em>, as a record processed
 * under another record's headers, offset and topic.
 * <p>
 * The three record-context tests are behavioural, against a real patched {@link ProcessorContextImpl}. The
 * two {@code StreamTask} tests are not: a real StreamTask needs a broker, a consumer and a producer, so they
 * assert on the real (reflectively reached) {@code AbstractPartitionGroup.RecordInfo} and on StreamTask's
 * compiled shape instead. The behavioural cover for those lives in the integration arm.
 *
 * @see ShadowedClassLoadingTest for the proof that these are the patched classes and not the jar's
 */
@Slf4j
class ProcessorContextConfinementTest {

    private static final String TOPIC = "confinement-input";
    private static final long TIMEOUT_SECONDS = 30;

    /** Which barrier phase a thread performs its "fetch" in - fixes the interleaving so neither arm can pass by luck. */
    private static final int FETCHES_FIRST = 0;
    private static final int FETCHES_SECOND = 1;

    private ProcessorContextImpl context;
    private ExecutorService workers;

    @BeforeEach
    void setUp() {
        final StreamsMetricsImpl streamsMetrics =
                new StreamsMetricsImpl(new Metrics(), "confinement-test", StreamsConfig.METRICS_LATEST, Time.SYSTEM);

        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "confinement-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        final ProcessorStateManager stateManager = mock(ProcessorStateManager.class);
        when(stateManager.taskType()).thenReturn(Task.TaskType.ACTIVE);

        context = new ProcessorContextImpl(
                new TaskId(0, 0),
                new StreamsConfig(props),
                stateManager,
                streamsMetrics,
                new ThreadCache(new LogContext("confinement-test "), 0L, streamsMetrics));

        // Gives the context a StreamTask to hand terminal-node latency to. A mock, because building a real one
        // needs a broker - and nothing here asserts on it.
        context.transitionToActive(mock(StreamTask.class), mock(RecordCollector.class), context.cache());

        workers = Executors.newFixedThreadPool(3);
    }

    @AfterEach
    void tearDown() {
        workers.shutdownNow();
    }

    /**
     * The core claim. One context instance, two threads, two different records - each thread must read back its
     * own. Before the patch both threads share one field and the second {@code setRecordContext} silently
     * redefines what the first thread thinks it is processing.
     * <p>
     * The barrier ordering is what makes this deterministic rather than lucky: both threads set, then both
     * threads read. On the unpatched class the reads are guaranteed to be wrong, not merely likely to be.
     */
    @Test
    void twoThreadsEachReadBackTheirOwnRecordContext() throws Exception {
        final ProcessorRecordContext first = recordContextFor(0, 10L);
        final ProcessorRecordContext second = recordContextFor(1, 20L);

        final CyclicBarrier bothHaveSet = new CyclicBarrier(2);

        final Future<ProcessorRecordContext> firstRead = workers.submit(readBackAfter(first, bothHaveSet));
        final Future<ProcessorRecordContext> secondRead = workers.submit(readBackAfter(second, bothHaveSet));

        assertThat(firstRead.get(TIMEOUT_SECONDS, TimeUnit.SECONDS))
                .as("the thread that set partition 0 / offset 10 must still see it after another thread set "
                        + "partition 1 / offset 20 on the same context instance")
                .isSameAs(first);
        assertThat(secondRead.get(TIMEOUT_SECONDS, TimeUnit.SECONDS))
                .as("and symmetrically for the other thread")
                .isSameAs(second);
    }

    /**
     * A thread that has never set a record context must see the undefined-context dummies, not whatever another
     * thread happens to have left in the field. This is the failure mode that would be hardest to spot in
     * production: an unrelated punctuation or standby read picking up a live record's topic and offset.
     */
    @Test
    void aThreadThatNeverSetAContextSeesNothingLeftOverFromAnother() throws Exception {
        final CyclicBarrier otherHasSet = new CyclicBarrier(2);

        final Future<?> setter = workers.submit(() -> {
            context.setRecordContext(recordContextFor(7, 700L));
            await(otherHasSet);
            await(otherHasSet); // hold the context set until the observer has finished reading
            return null;
        });

        final Future<?> observer = workers.submit(() -> {
            await(otherHasSet);
            assertThat(context.recordContext()).as("must not inherit the other thread's record context").isNull();
            assertThat(context.recordMetadata()).as("recordMetadata must be empty, not the other thread's").isEmpty();
            assertThat(context.topic()).as("undefined-context dummy for topic is null").isNull();
            assertThat(context.partition()).as("undefined-context dummy for partition is -1").isEqualTo(-1);
            assertThat(context.offset()).as("undefined-context dummy for offset is -1").isEqualTo(-1L);
            assertThat(context.timestamp()).as("undefined-context dummy for timestamp is 0").isEqualTo(0L);
            assertThat(context.headers()).as("undefined-context dummy for headers is empty").isEmpty();
            await(otherHasSet);
            return null;
        });

        observer.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        setter.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    /**
     * {@code forward()}'s save/restore of the record context is a stack, and it only works if the save and the
     * restore hit the same storage. Routing {@link ProcessorContextImpl} onto the accessors is exactly the step
     * that keeps it a stack per thread rather than a stack shared between threads - and it is the step that
     * fails silently, because leaving the direct field access in place still compiles and still passes any
     * single-threaded test.
     * <p>
     * Two levels of forwarding, on two threads at once, with different record contexts and timestamps. Each
     * thread must see its own context all the way down and its own original context on the way back out.
     */
    @Test
    void nestedForwardRestoresEachThreadsOwnContext() throws Exception {
        final Map<String, ProcessorRecordContext> seenAtLeaf = new ConcurrentHashMap<>();
        final Map<String, ProcessorRecordContext> seenAtMiddle = new ConcurrentHashMap<>();

        final ProcessorNode<Object, Object, Object, Object> leaf = node("leaf", record -> {
            seenAtLeaf.put(Thread.currentThread().getName(), context.recordContext());
        });
        final ProcessorNode<Object, Object, Object, Object> middle = node("middle", record -> {
            seenAtMiddle.put(Thread.currentThread().getName(), context.recordContext());
            // forward again with a different timestamp, so the nested call really does replace the context
            context.forward(new Record<>(record.key(), record.value(), record.timestamp() + 1, record.headers()));
        });
        final ProcessorNode<Object, Object, Object, Object> root = node("root", record -> { });

        root.addChild(middle);
        middle.addChild(leaf);
        middle.init(context);
        leaf.init(context);

        final CyclicBarrier bothInside = new CyclicBarrier(2);

        final Future<ProcessorRecordContext> firstOut = workers.submit(forwardThrough(root, recordContextFor(0, 10L), 100L, bothInside));
        final Future<ProcessorRecordContext> secondOut = workers.submit(forwardThrough(root, recordContextFor(1, 20L), 200L, bothInside));

        final ProcessorRecordContext firstRestored = firstOut.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        final ProcessorRecordContext secondRestored = secondOut.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        assertThat(firstRestored.partition()).as("thread one's context must be restored after forward returns").isEqualTo(0);
        assertThat(firstRestored.offset()).isEqualTo(10L);
        assertThat(secondRestored.partition()).as("thread two's context must be restored after forward returns").isEqualTo(1);
        assertThat(secondRestored.offset()).isEqualTo(20L);

        assertThat(seenAtMiddle).as("both threads must have reached the middle node").hasSize(2);
        assertThat(seenAtLeaf).as("both threads must have reached the leaf node").hasSize(2);

        // Inside the chain each thread must see its own partition/offset, carried down from its own root record.
        assertThat(seenAtMiddle.values())
                .as("the middle node must never see a partition/offset pair that belongs to the other thread")
                .extracting(ProcessorRecordContext::partition, ProcessorRecordContext::offset)
                .containsExactlyInAnyOrder(Tuple.tuple(0, 10L), Tuple.tuple(1, 20L));
        assertThat(seenAtLeaf.values())
                .as("nor may the leaf node, one level deeper into the save/restore stack")
                .extracting(ProcessorRecordContext::partition, ProcessorRecordContext::offset)
                .containsExactlyInAnyOrder(Tuple.tuple(0, 10L), Tuple.tuple(1, 20L));
    }

    /**
     * {@code StreamTask.recordInfo} is the other half of the same defect. {@code partitionGroup.nextRecord()}
     * <em>writes</em> into it, and {@code process()} reads {@code partition()} and {@code queue()} back out of
     * it <em>after</em> the record has been processed - so one shared instance means one thread can attribute
     * its consumed offset to another thread's partition.
     * <p>
     * The control arm here is the point: it drives the same barrier ordering through a single shared
     * {@code RecordInfo} and asserts the cross-talk is actually observed. Without it, the per-record arm below
     * would pass for a design that never had the defect in the first place.
     * <p>
     * What this does <em>not</em> prove is that {@code StreamTask} actually allocates one per record - that is
     * {@link #streamTaskPerTaskStateIsDeclaredForConcurrentUse()}, and it is why the two are a pair.
     */
    @Test
    void twoRecordInfoConsumersDoNotSeeEachOthersPartitionOrNode() throws Exception {
        final TopicPartition partitionA = new TopicPartition(TOPIC, 0);
        final TopicPartition partitionB = new TopicPartition(TOPIC, 1);
        final SourceNode<String, String> nodeA = sourceNode("source-a");
        final SourceNode<String, String> nodeB = sourceNode("source-b");
        final RecordQueue queueA = queueFor(partitionA, nodeA);
        final RecordQueue queueB = queueFor(partitionB, nodeB);

        // Control arm: the pre-patch design - one RecordInfo for the whole task. A fetches first, B fetches
        // second, then both read - the exact interleaving process() is exposed to once more than one thread
        // is inside it.
        final Object shared = newRecordInfo();
        final CyclicBarrier sharedPhases = new CyclicBarrier(2);
        final Future<Object[]> sharedA = workers.submit(fetchThenRead(shared, queueA, sharedPhases, FETCHES_FIRST));
        final Future<Object[]> sharedB = workers.submit(fetchThenRead(shared, queueB, sharedPhases, FETCHES_SECOND));
        final Object[] sharedReadA = sharedA.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        sharedB.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        assertThat(sharedReadA)
                .as("control arm: with one shared RecordInfo, thread A reads back the partition and source node "
                        + "of the record thread B fetched. If this ever stops holding, the harness has lost the "
                        + "ability to detect the defect and the per-record arm below proves nothing.")
                .containsExactly(partitionB, nodeB);

        // The patched design: a RecordInfo per record, same interleaving.
        final CyclicBarrier ownPhases = new CyclicBarrier(2);
        final Future<Object[]> ownA = workers.submit(fetchThenRead(newRecordInfo(), queueA, ownPhases, FETCHES_FIRST));
        final Future<Object[]> ownB = workers.submit(fetchThenRead(newRecordInfo(), queueB, ownPhases, FETCHES_SECOND));

        assertThat(ownA.get(TIMEOUT_SECONDS, TimeUnit.SECONDS))
                .as("with a RecordInfo per record, thread A keeps its own partition and source node")
                .containsExactly(partitionA, nodeA);
        assertThat(ownB.get(TIMEOUT_SECONDS, TimeUnit.SECONDS))
                .as("and thread B keeps its own")
                .containsExactly(partitionB, nodeB);
    }

    /**
     * The structural half of the {@code StreamTask} change. These fields cannot be exercised behaviourally
     * without a broker (a real {@code StreamTask} needs a consumer, a producer and a partition group), so they
     * are asserted on the compiled shape instead - and the compiled shape is what actually decides whether two
     * PC worker threads corrupt the task's offset bookkeeping.
     * <p>
     * The behavioural cover for these lives in the integration arm.
     */
    @Test
    void streamTaskPerTaskStateIsDeclaredForConcurrentUse() throws Exception {
        assertThat(Modifier.isFinal(fieldOf(StreamTask.class, "recordInfo").getModifiers()))
                .as("recordInfo must no longer be a single instance fixed for the task's lifetime - it is now "
                        + "re-assigned per record, which is what stops nextRecord() scribbling on a record that "
                        + "another thread is still finishing with")
                .isFalse();

        assertThat(StreamTask.class.getDeclaredMethod("doProcess", long.class,
                Class.forName("org.apache.kafka.streams.processor.internals.AbstractPartitionGroup$RecordInfo"),
                Class.forName("org.apache.kafka.streams.processor.internals.StampedRecord")))
                .as("doProcess must take both the RecordInfo and the record as parameters. If either is still "
                        + "read from a field, pinning it in process() buys nothing - the RecordInfo read happens "
                        + "after the record was processed, and on the PC path there is no single 'current "
                        + "record' for a task at all, there are up to poolSize of them at once.")
                .isNotNull();

        // U9 strengthened this from "the cross-thread write must be volatile" to "the cross-thread write
        // must not exist" for RECORD-COMPLETION state: workers report completion only through the
        // dispatcher's mailbox, PC's frontier is the commit source, and commitNeeded went back to being a
        // plain stock-path field. Volatile reappearing here would mean a worker-side completion write
        // crept back in - which is exactly what this assertion refuses.
        assertThat(Modifier.isVolatile(fieldOf(StreamTask.class, "commitNeeded").getModifiers()))
                .as("commitNeeded must be a plain field again: since U9 workers report record completion "
                        + "only through the dispatcher's mailbox and PC's frontier. If this is volatile "
                        + "again, a cross-thread completion write came back, and the mailbox discipline has "
                        + "been broken.")
                .isFalse();

        // The ONE sanctioned cross-thread commit write, found by the U9 review: a processor calling
        // context.commit() runs ProcessorContextImpl.commit -> requestCommit on a PC WORKER thread, so
        // commitRequested is genuinely written cross-thread and must stay volatile. This pairs with the
        // assertion above: completion state has no cross-thread writes, the commit REQUEST has exactly one.
        assertThat(Modifier.isVolatile(fieldOf(StreamTask.class, "commitRequested").getModifiers()))
                .as("commitRequested is written from PC worker threads (context.commit() -> requestCommit) "
                        + "and read by the StreamThread - it must be volatile, and it is the only commit "
                        + "field allowed a cross-thread write")
                .isTrue();

        assertThat(fieldOf(StreamTask.class, "processTimeMs").getType())
                .as("processTimeMs is accumulated concurrently, so a plain long loses updates")
                .isEqualTo(LongAdder.class);
    }

    // --- helpers -------------------------------------------------------------------------------------------

    private Callable<ProcessorRecordContext> readBackAfter(final ProcessorRecordContext toSet,
                                                           final CyclicBarrier bothHaveSet) {
        return () -> {
            context.setRecordContext(toSet);
            await(bothHaveSet);
            return context.recordContext();
        };
    }

    private Callable<ProcessorRecordContext> forwardThrough(final ProcessorNode<Object, Object, Object, Object> root,
                                                            final ProcessorRecordContext own,
                                                            final long recordTimestamp,
                                                            final CyclicBarrier bothInside) {
        return () -> {
            context.setRecordContext(own);
            context.setCurrentNode(root);
            await(bothInside);
            context.forward(new Record<>("k", "v", recordTimestamp, new RecordHeaders()));
            return context.recordContext();
        };
    }

    /**
     * Mimics one {@code process()} call: hand the RecordInfo to the "partition group" (which writes into it),
     * wait for the other thread to do the same, then read back where the record came from - which is exactly
     * the ordering real {@code process()} has, and exactly where the shared instance loses.
     */
    private Callable<Object[]> fetchThenRead(final Object recordInfo,
                                             final RecordQueue queue,
                                             final CyclicBarrier phases,
                                             final int fetchPhase) {
        return () -> {
            for (int phase = 0; phase < 2; phase++) {
                if (phase == fetchPhase) {
                    setQueue(recordInfo, queue);
                }
                await(phases);
            }
            return new Object[]{invokeOn(recordInfo, "partition"), invokeOn(recordInfo, "node")};
        };
    }

    private static ProcessorRecordContext recordContextFor(final int partition, final long offset) {
        return new ProcessorRecordContext(offset * 1000, offset, partition, TOPIC, new RecordHeaders());
    }

    private static ProcessorNode<Object, Object, Object, Object> node(final String name,
                                                                     final Processor<Object, Object, Object, Object> processor) {
        return new ProcessorNode<>(name, processor, null);
    }

    private static SourceNode<String, String> sourceNode(final String name) {
        return new SourceNode<>(name, Serdes.String().deserializer(), Serdes.String().deserializer());
    }

    private static RecordQueue queueFor(final TopicPartition partition, final SourceNode<String, String> source) {
        final RecordQueue queue = mock(RecordQueue.class);
        when(queue.partition()).thenReturn(partition);
        // doReturn, not when(...).thenReturn: source() is declared SourceNode<?, ?> and the wildcard capture
        // makes the type-safe form unusable here.
        doReturn(source).when(queue).source();
        return queue;
    }

    private static void await(final CyclicBarrier barrier) throws Exception {
        barrier.await(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    private static Field fieldOf(final Class<?> owner, final String name) throws Exception {
        final Field field = owner.getDeclaredField(name);
        field.setAccessible(true);
        return field;
    }

    /**
     * {@code AbstractPartitionGroup.RecordInfo} is package-private, and this test deliberately lives in this
     * module's own package rather than Kafka's - so it is reached reflectively. The object under test is still
     * the real one.
     */
    private static Object newRecordInfo() throws Exception {
        final Constructor<?> constructor = Class
                .forName("org.apache.kafka.streams.processor.internals.AbstractPartitionGroup$RecordInfo")
                .getDeclaredConstructor();
        constructor.setAccessible(true);
        return constructor.newInstance();
    }

    private static void setQueue(final Object recordInfo, final RecordQueue queue) throws Exception {
        final Field field = recordInfo.getClass().getDeclaredField("queue");
        field.setAccessible(true);
        field.set(recordInfo, queue);
    }

    private static Object invokeOn(final Object recordInfo, final String methodName) throws Exception {
        final Method method = recordInfo.getClass().getDeclaredMethod(methodName);
        method.setAccessible(true);
        return method.invoke(recordInfo);
    }
}
