package io.confluent.parallelconsumer.streams;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.internal.EpochAndRecordsMap;
import io.confluent.parallelconsumer.internal.PCModule;
import io.confluent.parallelconsumer.state.WorkManager;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.api.parallel.Isolated;

import pl.tlinkowski.unij.api.UniSets;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongFunction;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The seam itself, with Kafka Streams taken out of the picture.
 * <p>
 * Everything asserted here is a property of {@link PcTaskDispatcher} and the Parallel Consumer machinery
 * behind it - work registration, KEY-ordered selection, the worker pool, failure handling. Running it without
 * a broker or a topology is deliberate: when the end-to-end run misbehaves, these tests say whether the
 * dispatcher or the Kafka Streams integration is at fault, and a seam that cannot localise its own failures
 * produces verdicts nobody can act on.
 * <p>
 * The end-to-end half lives in
 * {@code io.confluent.parallelconsumer.streams.integrationTests.PcDrivenStreamsDispatchTest}.
 *
 * @author Antony Stubbs
 */
@Slf4j
// The switch and the process-wide counters in PcDispatchCounters are global by necessity (there is no seam
// through KafkaStreams to inject a collaborator), and this module inherits the project's concurrent JUnit
// execution from core's junit-platform.properties. Left concurrent, these methods read each other's counters
// and toggle each other's switch. Per-dispatcher counters cover most of it; this covers the switch.
@Execution(ExecutionMode.SAME_THREAD)
// And @Isolated, not merely same-thread: these tests call PcTaskDispatcher.abortAllActive(), which kills
// every live dispatcher in the JVM - including one belonging to a concurrently running test class.
// SAME_THREAD only serialises this class's own methods; the sibling IT classes carry @Isolated for the
// same process-wide reason.
@Isolated
class PcTaskDispatcherTest {

    private static final String TOPIC = "pc-streams-in";
    private static final TopicPartition PARTITION = new TopicPartition(TOPIC, 0);
    private static final Set<TopicPartition> INPUT_PARTITIONS = UniSets.of(PARTITION);

    private static final Duration PUMP_TIMEOUT = Duration.ofSeconds(60);

    private PcTaskDispatcher dispatcher;

    @BeforeEach
    void resetCounters() {
        PcDispatchCounters.reset();
    }

    @AfterEach
    void closeDispatcher() {
        if (dispatcher != null) {
            dispatcher.close();
            dispatcher = null;
        }
        // Back to the artifact's default (on), not to off - a teardown that parks the switch somewhere other
        // than where the JVM found it is the implicit coupling this default flip exists to remove.
        PcDispatchSwitch.resetToDefault();
    }

    /**
     * The first trap. {@code WorkManager} is a {@code ConsumerRebalanceListener}, but here Kafka Streams owns
     * the consumer, so PC's assignment lifecycle is never driven unless the bridge drives it - and the way it
     * fails is to register nothing at all, quietly.
     */
    @Test
    void registerWorkAcceptsEveryRecordOnceThePartitionHasAnAssignmentEpoch() {
        dispatcher = new PcTaskDispatcher("task-epoch", INPUT_PARTITIONS, 4);

        List<ConsumerRecord<byte[], byte[]>> batch = records(20, offset -> "key-" + (offset % 5));
        dispatcher.registerRecords(PARTITION, batch);

        assertThat(dispatcher.getRecordsOffered())
                .as("every record in the batch must be offered")
                .isEqualTo(20);
        assertThat(dispatcher.getRecordsAccepted())
                .as("no record may be skipped for want of an epoch - EpochAndRecordsMap drops those with "
                        + "nothing but a log.warn, so a gap here is a silent zero-record registration")
                .isEqualTo(20);
        assertThat(dispatcher.getWorkManager().getNumberOfWorkQueuedInShardsAwaitingSelection())
                .as("and the records must actually be sitting in PC's shards, ready to be selected")
                .isEqualTo(20);
    }

    /**
     * The control for the test above: the same registration, against a {@code WorkManager} that was never told
     * about the assignment. This is the failure mode the dispatcher's constructor exists to prevent, asserted
     * directly so that the bootstrap cannot be quietly deleted by a later refactor without a red test.
     */
    @Test
    void withoutAnAssignmentEpochEveryRecordIsSilentlyDropped() {
        ParallelConsumerOptions<byte[], byte[]> options = ParallelConsumerOptions.<byte[], byte[]>builder()
                .consumer(new MockConsumer<byte[], byte[]>(OffsetResetStrategy.NONE))
                .ordering(ParallelConsumerOptions.ProcessingOrder.KEY)
                .build();
        WorkManager<byte[], byte[]> unassigned = new PCModule<>(options).workManager();

        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> byPartition = new HashMap<>();
        byPartition.put(PARTITION, records(20, offset -> "key-" + offset));
        EpochAndRecordsMap<byte[], byte[]> epochTagged =
                new EpochAndRecordsMap<>(new ConsumerRecords<>(byPartition), unassigned.getPm());

        assertThat(epochTagged.count())
                .as("with no assignment epoch, EpochAndRecordsMap keeps nothing - and says so only at WARN. "
                        + "This is what a missing onPartitionsAssigned looks like: an idle-looking topology.")
                .isZero();
    }

    /**
     * The default, asserted rather than assumed - this is the test that would go red if someone flipped it
     * back. Depending on this artifact is the opt-in, so a {@code StreamTask} constructed with nothing
     * configured gets a dispatcher; turning the switch off gets the stock partition group back, and then the
     * marker cannot move, which is what makes a non-zero marker elsewhere mean something.
     * <p>
     * {@code resetToDefault()} rather than reading {@code isEnabled()} straight off: this class mutates the
     * process-wide switch, so by the time this method runs the switch holds whatever the last test left, and
     * an unqualified read here would be asserting on test-ordering rather than on the default.
     */
    @Test
    void theSwitchIsOnByDefaultAndTurningItOffLeavesTheStockPathInPlace() {
        PcDispatchSwitch.resetToDefault();

        assertThat(PcDispatchSwitch.isEnabled())
                .as("PC dispatch must default to ON - putting this artifact on the classpath IS the opt-in, "
                        + "and a second, hidden opt-in step buys nobody anything")
                .isTrue();
        // Assigned to the field so @AfterEach closes its worker pool.
        dispatcher = PcTaskDispatcher.createIfEnabled("task-default-on", INPUT_PARTITIONS);
        assertThat(dispatcher)
                .as("so a task built with nothing configured must get a dispatcher")
                .isNotNull();
        assertThat(dispatcher.getRecordsDispatched())
                .as("merely existing dispatches nothing")
                .isZero();

        dispatcher.close();
        dispatcher = null;

        PcDispatchSwitch.disable();
        assertThat(PcTaskDispatcher.createIfEnabled("task-off", INPUT_PARTITIONS))
                .as("and turning it off must give the stock path back - no dispatcher means StreamTask keeps "
                        + "its own partition group")
                .isNull();
        assertThat(PcDispatchCounters.getRecordsDispatchedToPool())
                .as("nothing reached a worker pool in either half")
                .isZero();
    }

    /**
     * The property is the only way an application (as opposed to a test) can get the stock path back, so its
     * parsing is worth an assertion of its own - including that a typo fails loudly rather than being read as
     * "off", which would silently produce a run that looks like a control arm and is not.
     */
    @Test
    void theEnabledPropertyTurnsTheSeamOffAndRejectsAnythingItCannotUnderstand() {
        final String original = System.getProperty(PcDispatchSwitch.ENABLED_PROPERTY);
        try {
            System.setProperty(PcDispatchSwitch.ENABLED_PROPERTY, "false");
            PcDispatchSwitch.resetToDefault();
            assertThat(PcDispatchSwitch.isEnabled())
                    .as("-D%s=false is how an A/B run gets stock Kafka Streams dispatch back",
                            PcDispatchSwitch.ENABLED_PROPERTY)
                    .isFalse();

            System.setProperty(PcDispatchSwitch.ENABLED_PROPERTY, "TRUE");
            PcDispatchSwitch.resetToDefault();
            assertThat(PcDispatchSwitch.isEnabled()).as("and case must not matter").isTrue();

            System.setProperty(PcDispatchSwitch.ENABLED_PROPERTY, "flase");
            assertThatThrownBy(PcDispatchSwitch::resetToDefault)
                    .as("a typo must not be silently read as 'off'")
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining(PcDispatchSwitch.ENABLED_PROPERTY);
        } finally {
            if (original == null) {
                System.clearProperty(PcDispatchSwitch.ENABLED_PROPERTY);
            } else {
                System.setProperty(PcDispatchSwitch.ENABLED_PROPERTY, original);
            }
            PcDispatchSwitch.resetToDefault();
        }
    }

    /**
     * The whole point: records that stock Kafka Streams would run one after another on the StreamThread are in
     * the chain at the same time.
     */
    @Test
    void aPoolOfFourRunsAtLeastThreeRecordsAtOnce() {
        dispatcher = new PcTaskDispatcher("task-concurrency", INPUT_PARTITIONS, 4);
        ConcurrencyProbe probe = new ConcurrencyProbe(Duration.ofMillis(300));

        // Distinct keys, so KEY ordering imposes no serialisation of its own and the pool is the only limit.
        dispatcher.registerRecords(PARTITION, records(12, offset -> "key-" + offset));
        pumpToQuiescence(probe);

        assertThat(probe.completed).hasSize(12);
        assertThat(probe.peakConcurrency.get())
                .as("a pool of 4 and a 300ms processor must put at least 3 records in the chain at once - "
                        + "anything less and records are still being run one at a time")
                .isGreaterThanOrEqualTo(3);
        assertThat(dispatcher.getRecordsDispatched())
                .as("the dispatch marker is the only proof these records did not take the stock path")
                .isEqualTo(12);
    }

    /**
     * KEY ordering is what lets a Streams topology survive parallel dispatch: a shard hands out at most one
     * record per key at a time, so per-key sequencing is preserved even though the run as a whole is
     * concurrent.
     */
    @Test
    void twoRecordsSharingAKeyAreNeverInFlightAtTheSameTime() {
        dispatcher = new PcTaskDispatcher("task-key-order", INPUT_PARTITIONS, 4);
        ConcurrencyProbe probe = new ConcurrencyProbe(Duration.ofMillis(40));

        // Three keys, interleaved, so each key has several records and the pool has room to overlap them.
        dispatcher.registerRecords(PARTITION, records(24, offset -> "key-" + (offset % 3)));
        pumpToQuiescence(probe);

        assertThat(probe.completed).hasSize(24);
        assertThat(probe.keysSeenConcurrentlyWithThemselves)
                .as("a key found running twice at once means PC's shard invariant did not hold, and every "
                        + "per-key ordering guarantee a Streams topology relies on is gone")
                .isEmpty();

        for (Map.Entry<String, List<Long>> perKey : probe.completionOrderByKey().entrySet()) {
            assertThat(perKey.getValue())
                    .as("key %s must be processed in offset order", perKey.getKey())
                    .isSorted();
        }
    }

    /**
     * The other half of the ordering property - it would be trivially satisfiable by never running anything in
     * parallel at all.
     */
    @Test
    void recordsWithDistinctKeysDoRunConcurrently() {
        dispatcher = new PcTaskDispatcher("task-distinct-keys", INPUT_PARTITIONS, 4);
        ConcurrencyProbe probe = new ConcurrencyProbe(Duration.ofMillis(200));

        dispatcher.registerRecords(PARTITION, records(8, offset -> "key-" + offset));
        pumpToQuiescence(probe);

        assertThat(probe.completed).hasSize(8);
        assertThat(probe.largestSetOfKeysInFlightTogether.get())
                .as("distinct keys must overlap - otherwise KEY ordering is being satisfied by serialising "
                        + "everything, which is stock Streams with extra threads")
                .isGreaterThanOrEqualTo(2);
    }

    /**
     * Failure semantics, and the retry divergence this module deliberately accepts. PC would normally
     * re-dispatch a failed record, which here would re-run the processor chain including {@code forward()}
     * calls that already emitted downstream - duplicates stock Streams never produces. Retries are therefore
     * disabled, and the failure is surfaced instead.
     */
    @Test
    void aFailingRecordSurfacesOnceAndIsNeverRetriedWhileOtherKeysKeepFlowing() {
        dispatcher = new PcTaskDispatcher("task-failure", INPUT_PARTITIONS, 4);

        String poisonKey = "key-2";
        AtomicInteger poisonAttempts = new AtomicInteger();
        List<String> completedKeys = new CopyOnWriteArrayList<>();

        PcTaskDispatcher.WorkPreparer preparer = record -> {
            String key = new String(record.key(), StandardCharsets.UTF_8);
            return () -> {
                if (poisonKey.equals(key)) {
                    poisonAttempts.incrementAndGet();
                    throw new IllegalStateException("processor blew up on " + key);
                }
                completedKeys.add(key);
            };
        };

        dispatcher.registerRecords(PARTITION, records(12, offset -> "key-" + (offset % 4)));
        boolean quiescent = dispatcher.pumpUntilQuiescent(preparer, PUMP_TIMEOUT);

        assertThat(quiescent)
                .as("a failed record must not wedge the dispatcher - with retries disabled its key's shard "
                        + "blocks, and everything else must still drain")
                .isTrue();
        assertThat(poisonAttempts.get())
                .as("exactly one attempt: a retry would re-run the whole chain and re-emit records the first "
                        + "attempt already forwarded downstream")
                .isEqualTo(1);
        assertThat(completedKeys)
                .as("every record of every other key must still have been processed")
                .hasSize(9);
        assertThat(completedKeys).doesNotContain(poisonKey);

        assertThat(dispatcher.pollFailure())
                .as("the failure must be retrievable so the StreamThread can surface it the way stock does")
                .isInstanceOf(IllegalStateException.class);
        assertThat(dispatcher.pollFailure())
                .as("and cleared once taken, so it is reported once rather than on every pump")
                .isNull();

        assertThat(dispatcher.getRecordsFailed()).isEqualTo(1);
        assertThat(dispatcher.getRecordsSucceeded()).isEqualTo(9);
        // The two later records of the poison key are still queued behind it - blocked, not lost.
        assertThat(dispatcher.getRecordsDispatched()).isEqualTo(10);
        assertThat(dispatcher.getRecordsOffered()).isEqualTo(12);
    }

    // ---------------------------------------------------------------------------------------------------

    private void pumpToQuiescence(final ConcurrencyProbe probe) {
        boolean quiescent = dispatcher.pumpUntilQuiescent(probe, PUMP_TIMEOUT);
        assertThat(quiescent).as("all work must drain within %s", PUMP_TIMEOUT).isTrue();
    }

    /**
     * @param keyForOffset lets a test choose how keys map onto offsets - all-distinct for a concurrency test,
     *                     modulo-N for an ordering one
     */
    private static List<ConsumerRecord<byte[], byte[]>> records(final int count,
                                                                final LongFunction<String> keyForOffset) {
        return records(PARTITION, count, keyForOffset);
    }

    /**
     * The same batch builder for a partition other than the default one - needed by the U10 partition-update
     * tests, which are about a second partition arriving and leaving. An overload rather than a copy, so the
     * record shape cannot drift between the two.
     */
    private static List<ConsumerRecord<byte[], byte[]>> records(final TopicPartition partition,
                                                                final int count,
                                                                final LongFunction<String> keyForOffset) {
        List<ConsumerRecord<byte[], byte[]>> batch = new ArrayList<>();
        for (long offset = 0; offset < count; offset++) {
            batch.add(new ConsumerRecord<>(
                    partition.topic(),
                    partition.partition(),
                    offset,
                    keyForOffset.apply(offset).getBytes(StandardCharsets.UTF_8),
                    ("value-" + offset).getBytes(StandardCharsets.UTF_8)));
        }
        return batch;
    }

    /**
     * A stand-in processor that records what was running at the same time as what. It is the instrument the
     * concurrency and ordering assertions read from - output correctness would not distinguish "ran in
     * parallel" from "ran one at a time, quickly".
     */
    private static final class ConcurrencyProbe implements PcTaskDispatcher.WorkPreparer {

        private final Duration workDuration;

        private final AtomicInteger inFlight = new AtomicInteger();
        private final AtomicInteger peakConcurrency = new AtomicInteger();
        private final AtomicInteger largestSetOfKeysInFlightTogether = new AtomicInteger();

        private final Map<String, AtomicInteger> inFlightPerKey = new ConcurrentHashMap<>();
        private final Set<String> keysSeenConcurrentlyWithThemselves =
                Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
        private final List<Completion> completed = new CopyOnWriteArrayList<>();

        private ConcurrencyProbe(final Duration workDuration) {
            this.workDuration = workDuration;
        }

        @Override
        public Runnable prepare(final ConsumerRecord<byte[], byte[]> record) {
            String key = new String(record.key(), StandardCharsets.UTF_8);
            long offset = record.offset();
            return () -> {
                int concurrent = inFlight.incrementAndGet();
                peakConcurrency.accumulateAndGet(concurrent, Math::max);

                int sameKeyConcurrent = inFlightPerKey
                        .computeIfAbsent(key, ignored -> new AtomicInteger())
                        .incrementAndGet();
                if (sameKeyConcurrent > 1) {
                    keysSeenConcurrentlyWithThemselves.add(key);
                }
                largestSetOfKeysInFlightTogether.accumulateAndGet(distinctKeysInFlight(), Math::max);

                try {
                    Thread.sleep(workDuration.toMillis());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    inFlightPerKey.get(key).decrementAndGet();
                    inFlight.decrementAndGet();
                    completed.add(new Completion(key, offset));
                }
            };
        }

        private int distinctKeysInFlight() {
            int distinct = 0;
            for (AtomicInteger count : inFlightPerKey.values()) {
                if (count.get() > 0) {
                    distinct++;
                }
            }
            return distinct;
        }

        private Map<String, List<Long>> completionOrderByKey() {
            Map<String, List<Long>> byKey = new LinkedHashMap<>();
            for (Completion completion : completed) {
                byKey.computeIfAbsent(completion.key, ignored -> new ArrayList<>()).add(completion.offset);
            }
            return byKey;
        }
    }

    // ------- the commit surface -------------------------------------------------------------------------

    /**
     * The return value is a progress signal the caller paces on, not a count of pool submissions - and the
     * two only agree on the happy path, which is why every other test here agreed with the broken definition.
     * The patched {@code process()} returns this, and stock's {@code TaskExecutor} reads a false as "this task
     * made no progress" and hands the StreamThread back to a blocking poll. So a batch consumed entirely by
     * routes that never reach the pool must still report its full count: reporting zero there is the lie that
     * stalls a topology for a poll cycle per batch of corrupted records.
     * <p>
     * Both no-pool routes are covered because they are separate branches with separate bookkeeping - a drop
     * completes the work, a preparation failure records a failure - and a regression could plausibly hit one
     * and not the other.
     */
    @Test
    void recordsConsumedWithoutReachingThePoolStillCountAsProgress() {
        dispatcher = new PcTaskDispatcher("task-dropped", INPUT_PARTITIONS, 4);
        dispatcher.registerRecords(PARTITION, records(6, offset -> "key-" + offset));

        // Returns null for every record: consumed and completed on this thread, nothing handed to a worker.
        int consumed = dispatcher.dispatchAvailable(record -> null);

        assertThat(consumed)
                .as("all six were taken off the WorkManager, so all six are progress - counting pool "
                        + "submissions instead would report 0 and stall the caller that paces on this")
                .isEqualTo(6);
        assertThat(PcDispatchCounters.getRecordsDispatchedToPool())
                .as("and none of them reached the pool, which is precisely why the two definitions differ")
                .isZero();
    }

    @Test
    void recordsThatFailPreparationStillCountAsProgress() {
        dispatcher = new PcTaskDispatcher("task-prep-failure", INPUT_PARTITIONS, 4);
        dispatcher.registerRecords(PARTITION, records(6, offset -> "key-" + offset));

        // Throws for every record: preparation failed on the StreamThread, as a deserialisation error would.
        int consumed = dispatcher.dispatchAvailable(record -> {
            throw new IllegalStateException("deserialisation blew up");
        });

        assertThat(consumed)
                .as("a record consumed by failing preparation has still left the queue, so it is progress")
                .isEqualTo(6);
        assertThat(PcDispatchCounters.getRecordsDispatchedToPool())
                .as("none reached the pool")
                .isZero();
        assertThat(PcDispatchCounters.getRecordsFailed())
                .as("and the failures must be accounted for, not silently swallowed")
                .isEqualTo(6);
    }

    /**
     * The commit protocol's read side, seen from the StreamThread: a worker's completion becomes visible to
     * hasCommitDataOutstanding and collectCommitData through the mailbox drain those methods perform, and the
     * collected map carries the frontier - the lowest incomplete offset - not the highest completed one.
     */
    @Test
    void completedWorkBecomesCommitDataAtTheFrontier() {
        dispatcher = new PcTaskDispatcher("task-commit", INPUT_PARTITIONS, 4);

        // Three same-key records: offsets 0..2. Workers complete them; the frontier follows contiguity.
        dispatcher.registerRecords(PARTITION, records(3, offset -> "one-key"));
        assertThat(dispatcher.hasCommitDataOutstanding())
                .as("nothing has completed yet - registration alone is not commit-worthy")
                .isFalse();

        boolean quiescent = dispatcher.pumpUntilQuiescent(record -> () -> { }, PUMP_TIMEOUT);
        assertThat(quiescent).as("three no-op records must drain").isTrue();

        assertThat(dispatcher.hasCommitDataOutstanding())
                .as("completed, uncommitted work must report as commit-outstanding")
                .isTrue();
        assertThat(dispatcher.collectCommitData())
                .as("the frontier after 0..2 all complete is 3 - the next offset to resume from")
                .containsKey(PARTITION)
                .satisfies(map -> assertThat(map.get(PARTITION).offset()).isEqualTo(3L));
    }

    /**
     * The write side: only {@link PcTaskDispatcher#onCommitSuccess} clears the dirty state - collection alone
     * must not, or a failed commit would strand its records (collection is a read, success is the ack).
     */
    @Test
    void collectionDoesNotClearDirtyButTheSuccessAckDoes() {
        dispatcher = new PcTaskDispatcher("task-ack", INPUT_PARTITIONS, 4);
        dispatcher.registerRecords(PARTITION, records(2, offset -> "one-key"));
        dispatcher.pumpUntilQuiescent(record -> () -> { }, PUMP_TIMEOUT);

        var collected = dispatcher.collectCommitData();
        assertThat(dispatcher.hasCommitDataOutstanding())
                .as("collecting is a READ - a commit that later fails must find the partition still dirty")
                .isTrue();
        assertThat(dispatcher.collectCommitData())
                .as("and a second collection (the retry after a failed commit) returns the same data")
                .isEqualTo(collected);

        dispatcher.onCommitSuccess(collected);
        assertThat(dispatcher.hasCommitDataOutstanding())
                .as("the success ack is what marks the covered work clean")
                .isFalse();
    }

    /**
     * The crash-injection surface. abortClose is idempotent, deregisters from the registry (abortAllActive
     * must not abort it twice), and a crashed dispatcher accepts no further dispatch.
     */
    @Test
    void abortCloseIsACrashNotAShutdown() {
        dispatcher = new PcTaskDispatcher("task-abort", INPUT_PARTITIONS, 4);
        dispatcher.registerRecords(PARTITION, records(2, offset -> "k"));

        dispatcher.abortClose();
        dispatcher.abortClose(); // idempotent - a second crash of a dead dispatcher is a no-op

        assertThat(dispatcher.dispatchAvailable(record -> () -> { }))
                .as("a crashed dispatcher hands out nothing")
                .isZero();

        // abortAllActive after the abort must not touch this dispatcher again (it deregistered) - and with
        // no other dispatcher alive in this test, the call is a clean no-op rather than an error.
        PcTaskDispatcher.abortAllActive();
    }

    /** abortAllActive reaches every live dispatcher, not only the most recent one. */
    @Test
    void abortAllActiveCrashesEveryLiveDispatcher() {
        dispatcher = new PcTaskDispatcher("task-multi-a", INPUT_PARTITIONS, 4);
        PcTaskDispatcher second = new PcTaskDispatcher("task-multi-b", INPUT_PARTITIONS, 4);
        try {
            PcTaskDispatcher.abortAllActive();
            assertThat(dispatcher.dispatchAvailable(record -> () -> { }))
                    .as("first dispatcher crashed")
                    .isZero();
            assertThat(second.dispatchAvailable(record -> () -> { }))
                    .as("second dispatcher crashed too - the registry covers every live instance")
                    .isZero();
        } finally {
            second.close();
        }
    }

    // ------------------------------------------------------------------------------------------------
    // U10 (astubbs#255): task lifecycle - the uncommitted-work predicate, the owner-thread rebind, the
    // partition update, and the teardown contract that recycling leaked.
    // ------------------------------------------------------------------------------------------------

    /**
     * <b>The distinction the whole clean-close fix rests on.</b>
     * <p>
     * "Is a commit worth attempting" and "is it safe to walk away" are the same question on the stock path,
     * because processing is synchronous and a record is either done or not yet started. Asynchronous dispatch
     * creates a third state - running - and this asserts the two predicates disagree about it. If they ever
     * agree here, {@code validateClean()} is back to letting a clean close discard live work.
     */
    @Test
    void inFlightWorkIsUncommittedWorkButIsNotYetCommitData() throws Exception {
        dispatcher = new PcTaskDispatcher("task-inflight-predicate", INPUT_PARTITIONS, 4);

        assertThat(dispatcher.hasUncommittedWork())
                .as("a dispatcher holding nothing has nothing uncommitted")
                .isFalse();
        assertThat(dispatcher.hasCommitDataOutstanding())
                .as("and nothing to commit")
                .isFalse();

        final CountDownLatch inChain = new CountDownLatch(1);
        final CountDownLatch releaseWorker = new CountDownLatch(1);
        dispatcher.registerRecords(PARTITION, records(1, offset -> "held-key"));
        dispatcher.dispatchAvailable(record -> () -> {
            inChain.countDown();
            awaitLatch(releaseWorker);
        });

        assertThat(inChain.await(30, TimeUnit.SECONDS))
                .as("the worker must actually be inside the chain before the predicates are read, or this "
                        + "test proves nothing about the in-flight state")
                .isTrue();

        try {
            assertThat(dispatcher.getInFlightCount())
                    .as("precondition: exactly one record is running")
                    .isEqualTo(1);
            assertThat(dispatcher.hasCommitDataOutstanding())
                    .as("nothing has COMPLETED, so there is no commit worth attempting - the frontier has "
                            + "not moved")
                    .isFalse();
            assertThat(dispatcher.hasUncommittedWork())
                    .as("but the task is emphatically not safe to close clean: a record is inside the "
                            + "processor chain and no commit covers it")
                    .isTrue();
        } finally {
            releaseWorker.countDown();
        }

        dispatcher.pumpUntilQuiescent(record -> () -> { }, PUMP_TIMEOUT);

        assertThat(dispatcher.hasCommitDataOutstanding())
                .as("once complete and fed back, the work is commit data")
                .isTrue();
        assertThat(dispatcher.hasUncommittedWork())
                .as("and still uncommitted - completion is not a commit")
                .isTrue();

        dispatcher.onCommitSuccess(dispatcher.collectCommitData());

        assertThat(dispatcher.hasCommitDataOutstanding()).as("the ack clears the commit data").isFalse();
        assertThat(dispatcher.hasUncommittedWork())
                .as("and clears the uncommitted-work answer too - both predicates agree once the work is "
                        + "genuinely committed, which is the only state where they must")
                .isFalse();
    }

    /**
     * A failed record must not make the task permanently un-closable.
     * <p>
     * With retries disabled a failed record blocks its KEY shard forever and the records behind it stay
     * <em>available</em> in PC's counters. Defining uncommitted work as "PC still holds records" would
     * therefore make one poison pill enough to keep {@code validateClean()} throwing for the rest of the
     * task's life - the same trap {@link PcTaskDispatcher#isQuiescent()} was written to avoid. Nothing is
     * lost by closing: the failed record never completed, so the frontier never rose over it.
     */
    @Test
    void aFailedRecordDoesNotLeaveTheTaskPermanentlyUncloseable() {
        dispatcher = new PcTaskDispatcher("task-failed-not-stuck", INPUT_PARTITIONS, 4);
        dispatcher.registerRecords(PARTITION, records(3, offset -> "same-key"));

        dispatcher.pumpUntilQuiescent(record -> () -> {
            throw new IllegalStateException("KABOOM");
        }, PUMP_TIMEOUT);

        assertThat(dispatcher.getRecordsFailed())
                .as("precondition: the record really did fail")
                .isGreaterThan(0);
        assertThat(dispatcher.hasUncommittedWork())
                .as("a failed record is not uncommitted WORK - it will never complete, so it can never be "
                        + "committed, and counting it would deadlock every future clean close")
                .isFalse();
    }

    /**
     * The owner-thread bind moves with the task. Before U10 it was captured in the constructor, which is
     * correct only while a task is created and driven by one thread forever - a recycled or reassigned task
     * carried a stale owner and the guard threw {@link IllegalStateException} on a <b>legitimate</b> call.
     */
    @Test
    void rebindingHandsTheCommitSurfaceToTheNewOwnerAndRevokesItFromTheOld() throws Exception {
        dispatcher = new PcTaskDispatcher("task-rebind", INPUT_PARTITIONS, 4);
        final Thread constructingThread = Thread.currentThread();

        assertThat(dispatcher.hasCommitDataOutstanding())
                .as("precondition: the constructing thread owns the commit surface")
                .isFalse();

        runOnNewThread("new-owner", () -> {
            assertThatThrownBy(dispatcher::hasCommitDataOutstanding)
                    .as("before rebinding, the new thread is a stranger")
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("owner-thread-only")
                    .hasMessageContaining("bindToCurrentThread");

            dispatcher.bindToCurrentThread();

            assertThat(dispatcher.hasCommitDataOutstanding())
                    .as("after rebinding the new owner may drive the commit surface - this is the "
                            + "legitimate call the construction-time bind used to reject")
                    .isFalse();
        });

        assertThatThrownBy(dispatcher::hasCommitDataOutstanding)
                .as("and ownership MOVED rather than being shared - the old owner is now the stranger, or "
                        + "the guard would be protecting nothing")
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(constructingThread.getName());
    }

    /**
     * <b>Regression test for a live defect: the state updater is a legitimate foreign caller.</b>
     * <p>
     * Kafka Streams' {@code DefaultStateUpdater} calls {@code StreamTask.maybeCheckpoint} from its own
     * thread for restoring and standby tasks, and that method gates on "is there work outstanding". Under
     * stock, that gate is a plain field read. Routing it through a draining, owner-thread-guarded call turned
     * it into concurrent mutation of PC's shard and partition state from a second thread - which the guard
     * then reported as {@code IllegalStateException ... called from '...-StateUpdater-1'}, killing the client
     * at startup.
     * <p>
     * The guard was right and the call site was wrong. So the <em>query</em> is now genuinely a query -
     * readable by anyone, mutating nothing - while everything that touches {@code WorkManager} stays
     * owner-thread-only. This pins that split: if a later change reintroduces a drain or a guard here, the
     * state updater dies again, and it dies in CI rather than in somebody's cluster.
     */
    @Test
    void theOutstandingWorkQueryIsAnswerableFromAForeignThread() throws Exception {
        dispatcher = new PcTaskDispatcher("task-foreign-query", INPUT_PARTITIONS, 4);
        dispatcher.registerRecords(PARTITION, records(2, offset -> "k" + offset));
        dispatcher.pumpUntilQuiescent(record -> () -> { }, PUMP_TIMEOUT);

        assertThat(dispatcher.hasUncommittedWork())
                .as("precondition: there is completed, uncommitted work to report")
                .isTrue();

        runOnNewThread("StateUpdater-1", () -> {
            assertThat(dispatcher.hasUncommittedWork())
                    .as("a foreign thread must get the SAME answer without throwing - this is the exact "
                            + "call DefaultStateUpdater makes through maybeCheckpoint")
                    .isTrue();
            assertThat(dispatcher.getInFlightCount())
                    .as("and the rest of the read-only surface is equally safe from there")
                    .isZero();
            assertThat(dispatcher.isClosed()).isFalse();
        });

        dispatcher.onCommitSuccess(dispatcher.collectCommitData());

        runOnNewThread("StateUpdater-2", () ->
                assertThat(dispatcher.hasUncommittedWork())
                        .as("and the foreign reader sees the commit, so the published state is not stale - "
                                + "a query that never updates would gate checkpointing forever")
                        .isFalse());
    }

    /**
     * The guard and the wake signal are one seam. Moving the owner without moving the signal would leave a
     * dispatcher whose guard admits the new thread while its workers still wake the old one - a stall rather
     * than an exception, and therefore the worse of the two failures.
     */
    @Test
    void rebindingMovesTheWakeSignalWithTheOwner() throws Exception {
        dispatcher = new PcTaskDispatcher("task-rebind-signal", INPUT_PARTITIONS, 4);

        assertThat(PcWorkSignal.registeredDispatchersOnCurrentThread())
                .as("precondition: the constructing thread's signal speaks for this dispatcher")
                .isEqualTo(1);

        runOnNewThread("new-signal-owner", () -> {
            dispatcher.bindToCurrentThread();
            assertThat(PcWorkSignal.registeredDispatchersOnCurrentThread())
                    .as("the new owner's signal now speaks for the dispatcher, so a worker completion wakes "
                            + "the thread that is actually waiting")
                    .isEqualTo(1);
        });

        assertThat(PcWorkSignal.registeredDispatchersOnCurrentThread())
                .as("and the old owner's signal no longer does - otherwise that thread keeps taking the "
                        + "split-wait branch for work it no longer drives")
                .isZero();
    }

    /** Rebinding to the thread that already owns the dispatcher is a no-op, not a churn of registrations. */
    @Test
    void rebindingToTheSameThreadChangesNothing() {
        dispatcher = new PcTaskDispatcher("task-rebind-idempotent", INPUT_PARTITIONS, 4);

        dispatcher.bindToCurrentThread();
        dispatcher.bindToCurrentThread();

        assertThat(PcWorkSignal.registeredDispatchersOnCurrentThread())
                .as("Kafka drives the assignment paths on every rebalance whether or not the owner changed; "
                        + "re-registering each time would grow the signal's set without bound")
                .isEqualTo(1);
        assertThat(dispatcher.hasUncommittedWork()).isFalse();
    }

    /** A closed dispatcher cannot be handed to a live thread - that is the revive hazard in another shape. */
    @Test
    void aClosedDispatcherRefusesToBeBound() {
        dispatcher = new PcTaskDispatcher("task-bind-closed", INPUT_PARTITIONS, 4);
        dispatcher.close();

        assertThatThrownBy(dispatcher::bindToCurrentThread)
                .as("binding a closed dispatcher would accept records and never dispatch them - the silent "
                        + "stall the revive guard exists to prevent")
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("closed PC dispatcher");
    }

    /**
     * Cooperative rebalancing changes a live task's partitions through {@code updateInputPartitions}, which
     * never reached PC. A newly assigned partition therefore had no assignment epoch, and
     * {@code EpochAndRecordsMap} drops every record of a partition whose epoch is null - zero registered, no
     * exception, a topology that just looks idle.
     */
    @Test
    void newlyAssignedPartitionsBecomeRegisterableAndRevokedOnesDoNot() {
        final TopicPartition second = new TopicPartition(TOPIC, 1);
        dispatcher = new PcTaskDispatcher("task-partition-update", INPUT_PARTITIONS, 4);

        dispatcher.registerRecords(second, records(second, 4, offset -> "k" + offset));
        assertThat(dispatcher.getRecordsAccepted())
                .as("control arm: before the update, records for an unassigned partition are silently "
                        + "dropped for want of an epoch - this is the defect, asserted directly")
                .isZero();

        dispatcher.updatePartitions(UniSets.of(PARTITION, second));

        dispatcher.registerRecords(second, records(second, 4, offset -> "k" + offset));
        assertThat(dispatcher.getRecordsAccepted())
                .as("after the update the new partition has an epoch and its records are accepted")
                .isEqualTo(4);

        assertThat(dispatcher.getWorkManager().getNumberOfWorkQueuedInShardsAwaitingSelection())
                .as("precondition: that work is queued and selectable")
                .isEqualTo(4);

        dispatcher.updatePartitions(UniSets.of(PARTITION));

        assertThat(dispatcher.getWorkManager().getNumberOfWorkQueuedInShardsAwaitingSelection())
                .as("revocation discards the queued work for the revoked partition - it is the new owner's "
                        + "to process now, and handing it to a worker here would duplicate it")
                .isZero();
    }

    /**
     * The epoch fence, which is the reason revocation can abandon in-flight work instead of draining it. An
     * outcome arriving for a partition revoked while its record was running must not advance a frontier the
     * new owner is now responsible for.
     */
    @Test
    void anOutcomeForARevokedPartitionIsDroppedRatherThanCommitted() throws Exception {
        final TopicPartition second = new TopicPartition(TOPIC, 1);
        dispatcher = new PcTaskDispatcher("task-revoked-outcome", UniSets.of(PARTITION, second), 4);

        final CountDownLatch inChain = new CountDownLatch(1);
        final CountDownLatch releaseWorker = new CountDownLatch(1);
        dispatcher.registerRecords(second, records(second, 1, offset -> "doomed"));
        dispatcher.dispatchAvailable(record -> () -> {
            inChain.countDown();
            awaitLatch(releaseWorker);
        });
        assertThat(inChain.await(30, TimeUnit.SECONDS)).as("the record is in flight").isTrue();

        // The rebalance lands while the record is still inside the chain.
        dispatcher.updatePartitions(UniSets.of(PARTITION));
        releaseWorker.countDown();

        dispatcher.pumpUntilQuiescent(record -> () -> { }, PUMP_TIMEOUT);

        assertThat(dispatcher.collectCommitData())
                .as("the late outcome belongs to a partition this instance no longer owns - committing its "
                        + "offset would tell the group that work the new owner has not done is done")
                .doesNotContainKey(second);
    }

    /** A rebalance that changes nothing must not bump an epoch and strand live work. */
    @Test
    void anIdenticalPartitionSetIsANoOp() {
        dispatcher = new PcTaskDispatcher("task-partition-noop", INPUT_PARTITIONS, 4);
        dispatcher.registerRecords(PARTITION, records(3, offset -> "k" + offset));

        dispatcher.updatePartitions(UniSets.of(PARTITION));

        dispatcher.pumpUntilQuiescent(record -> () -> { }, PUMP_TIMEOUT);
        assertThat(dispatcher.getRecordsSucceeded())
                .as("Kafka calls the assignment paths on every rebalance; a spurious epoch bump here would "
                        + "discard work that was never reassigned")
                .isEqualTo(3);
    }

    /**
     * <b>The recycle leak, asserted as the contract that closes it.</b>
     * <p>
     * {@code prepareRecycle()} tore the task down without routing through {@code close(boolean)}, so a
     * recycled task left four things behind: its entry in the live-dispatcher registry, its worker pool, its
     * {@link PcWorkSignal} registration, and its partitions in the WorkManager. This pins all four to the one
     * call {@code prepareRecycle} now makes, so a teardown path that forgets it is caught by whichever of the
     * four the author did not think about.
     */
    @Test
    void closingReleasesEveryResourceARecycleUsedToLeak() {
        final int activeBefore = PcTaskDispatcher.activeDispatcherCount();
        dispatcher = new PcTaskDispatcher("task-teardown-contract", INPUT_PARTITIONS, 4);
        dispatcher.registerRecords(PARTITION, records(2, offset -> "k" + offset));
        dispatcher.pumpUntilQuiescent(record -> () -> { }, PUMP_TIMEOUT);

        assertThat(PcTaskDispatcher.activeDispatcherCount())
                .as("precondition: the dispatcher is live in the JVM-wide registry")
                .isEqualTo(activeBefore + 1);
        assertThat(PcWorkSignal.registeredDispatchersOnCurrentThread())
                .as("precondition: and its owner's wake signal speaks for it")
                .isEqualTo(1);

        dispatcher.close();

        assertThat(PcTaskDispatcher.activeDispatcherCount())
                .as("leak 1: the registry entry is static, so a leaked dispatcher outlives the task forever")
                .isEqualTo(activeBefore);
        assertThat(dispatcher.isClosed())
                .as("leak 2: the worker pool - close() shuts it down and awaits termination")
                .isTrue();
        assertThat(PcWorkSignal.registeredDispatchersOnCurrentThread())
                .as("leak 3: a stale signal registration keeps the StreamThread on the split-wait branch "
                        + "for work that can never complete")
                .isZero();
        assertThat(dispatcher.getWorkManager().getNumberOfWorkQueuedInShardsAwaitingSelection())
                .as("leak 4: the WorkManager's partition state - close() revokes what it held")
                .isZero();
        assertThat(dispatcher.hasUncommittedWork())
                .as("and the published dirty state follows the revoke, not just the drain that precedes it - "
                        + "a closed dispatcher owns nothing and will never commit again, which is what "
                        + "Kafka's shouldClearCommitStatusesInCloseDirty asserts through commitNeeded()")
                .isFalse();

        dispatcher = null; // already closed; keep the teardown from double-closing
    }

    /** Ownership having moved must not stop the teardown releasing the NEW owner's registration. */
    @Test
    void closingAfterARebindReleasesTheCurrentOwnersSignal() throws Exception {
        dispatcher = new PcTaskDispatcher("task-teardown-after-rebind", INPUT_PARTITIONS, 4);

        runOnNewThread("rebound-owner", () -> {
            dispatcher.bindToCurrentThread();
            dispatcher.close();
            assertThat(PcWorkSignal.registeredDispatchersOnCurrentThread())
                    .as("the close must release whichever signal currently speaks for the dispatcher, not "
                            + "the one it was constructed with")
                    .isZero();
        });

        dispatcher = null;
    }

    /**
     * KTD5's safety argument, encoded. The stale-ack hazard on {@code onOffsetCommitSuccess} stays
     * unreachable only because the acknowledgement and the partition update are both owner-thread-only, so
     * they cannot interleave. Pinning both is what turns that argument into something a later change can
     * falsify loudly instead of silently.
     */
    @Test
    void theCommitAckAndThePartitionUpdateAreBothOwnerThreadOnly() throws Exception {
        dispatcher = new PcTaskDispatcher("task-epoch-reachability", INPUT_PARTITIONS, 4);

        runOnNewThread("interloper", () -> {
            assertThatThrownBy(() -> dispatcher.updatePartitions(UniSets.of(PARTITION)))
                    .as("half the argument: a rebalance cannot be applied from another thread")
                    .isInstanceOf(IllegalStateException.class);
            assertThatThrownBy(() -> dispatcher.onCommitSuccess(Collections.emptyMap()))
                    .as("the other half: an ack cannot arrive from another thread. Two owner-thread-only "
                            + "calls cannot interleave, so no ack can cross a revocation boundary")
                    .isInstanceOf(IllegalStateException.class);
        });
    }

    /**
     * Runs the body on a fresh thread and rethrows whatever it threw on the caller's thread, so an assertion
     * failure inside is a test failure rather than a stack trace on stderr and a green run.
     */
    private static void runOnNewThread(final String name, final Runnable body) throws Exception {
        final AtomicReference<Throwable> thrown = new AtomicReference<>();
        Thread thread = new Thread(() -> {
            try {
                body.run();
            } catch (Throwable t) {
                thrown.set(t);
            }
        }, name);
        thread.start();
        thread.join(60_000);
        assertThat(thread.isAlive()).as("the helper thread must finish").isFalse();
        if (thrown.get() != null) {
            if (thrown.get() instanceof AssertionError) {
                throw (AssertionError) thrown.get();
            }
            throw new IllegalStateException("thread '" + name + "' failed", thrown.get());
        }
    }

    private static void awaitLatch(final CountDownLatch latch) {
        try {
            if (!latch.await(60, TimeUnit.SECONDS)) {
                throw new IllegalStateException("latch never released - the test would otherwise hang here");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    private static final class Completion {
        private final String key;
        private final long offset;

        private Completion(final String key, final long offset) {
            this.key = key;
            this.offset = offset;
        }
    }
}
