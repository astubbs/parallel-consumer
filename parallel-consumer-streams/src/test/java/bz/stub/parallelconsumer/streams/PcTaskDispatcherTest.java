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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongFunction;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

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
        List<ConsumerRecord<byte[], byte[]>> batch = new ArrayList<>();
        for (long offset = 0; offset < count; offset++) {
            batch.add(new ConsumerRecord<>(
                    TOPIC,
                    PARTITION.partition(),
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

    /**
     * The defect that made this method a query rather than a drain, reproduced without Kafka Streams.
     * <p>
     * Kafka Streams' {@code DefaultStateUpdater} calls {@code StreamTask.maybeCheckpoint} <b>from its own
     * thread</b> for every task it is restoring, and the patched {@code maybeCheckpoint} asks the dispatcher
     * whether a commit is outstanding before refreshing changelog offsets. So this question arrives on a
     * second thread, concurrently with the StreamThread, and it arrives in production - it is not a
     * hypothetical a guard invented. While the answer came from a mailbox drain, that meant
     * {@code WorkManager} and its partition state, neither of them thread-safe, touched by two threads at
     * once; the owner-thread guard converted that silent race into a deterministic
     * {@code IllegalStateException} that took the integration suite down.
     * <p>
     * Three properties, and the middle one is the one worth guarding: the call must not throw, the answer
     * must be <em>right</em> from over there, and it must be reached without draining. A thread-safe answer
     * that reported "nothing to commit" about finished work would be a worse defect than the crash, because
     * it loses records instead of stopping.
     */
    @Test
    void commitOutstandingIsAnswerableFromAnotherThreadWithoutDraining() throws InterruptedException {
        dispatcher = new PcTaskDispatcher("task-cross-thread-query", INPUT_PARTITIONS, 4);
        dispatcher.registerRecords(PARTITION, records(1, offset -> "one-key"));

        assertThat(dispatcher.dispatchAvailable(record -> () -> { }))
                .as("the single record goes to a worker")
                .isEqualTo(1);

        // inFlight is decremented in runOnWorker's finally block, AFTER the completion has been published,
        // so zero in flight means the completion is sitting in the mailbox. Nothing has drained it: only the
        // owner thread drains, and the owner thread is this one, parked here.
        await().atMost(PUMP_TIMEOUT).until(() -> dispatcher.getInFlightCount() == 0);

        assertThat(dispatcher.getWorkManager().isDirty())
                .as("precondition: PC itself does not know the record completed yet - which is precisely the "
                        + "state a draining query would have had to mutate away, from the wrong thread")
                .isFalse();

        AtomicReference<Boolean> answer = new AtomicReference<>();
        Throwable thrown = runOffOwnerThread("test-StateUpdater-1",
                () -> answer.set(dispatcher.hasCommitDataOutstanding()));

        assertThat(thrown)
                .as("the state updater asks this from its own thread on every restoring task; a query that "
                        + "throws there takes the application down, deterministically")
                .isNull();
        assertThat(answer.get())
                .as("and the answer must be right from over there: the record has completed and no commit "
                        + "has covered it, so a thread-safe FALSE would lose it")
                .isTrue();
        assertThat(dispatcher.getWorkManager().isDirty())
                .as("and it must not have drained to work that out - the drain is what reached WorkManager "
                        + "from the wrong thread")
                .isFalse();

        assertThat(dispatcher.collectCommitData())
                .as("the owner-thread path still folds the completion in, so nothing is stranded by the "
                        + "query staying passive")
                .containsKey(PARTITION)
                .satisfies(map -> assertThat(map.get(PARTITION).offset()).isEqualTo(1L));
    }

    /**
     * The accuracy trap in the other direction, and the reason the query counts successes rather than the
     * mailbox's size: a failed record leaves its offset incomplete, PC never turns the partition dirty, and
     * there is nothing new to commit. Answering "outstanding" here would be permanent - retries are disabled,
     * so the record never succeeds - and {@code validateClean} would turn it into a spurious
     * {@code TaskMigratedException} on an otherwise clean close.
     */
    @Test
    void aFailedCompletionIsNotCommitOutstanding() {
        dispatcher = new PcTaskDispatcher("task-failure-not-outstanding", INPUT_PARTITIONS, 4);
        dispatcher.registerRecords(PARTITION, records(1, offset -> "one-key"));

        dispatcher.dispatchAvailable(record -> () -> {
            throw new IllegalStateException("processing blew up");
        });
        await().atMost(PUMP_TIMEOUT).until(() -> dispatcher.getRecordsFailed() == 1);

        assertThat(dispatcher.hasCommitDataOutstanding())
                .as("a failure is not commit data - counting the mailbox rather than its successes would "
                        + "report true here, and never stop")
                .isFalse();

        dispatcher.collectCommitData();
        assertThat(dispatcher.hasCommitDataOutstanding())
                .as("and still not, once the failure has actually been fed back to PC")
                .isFalse();
    }

    /**
     * The other half of the same rule: the two methods that really do reach {@code WorkManager} keep the
     * owner-thread guard. {@link PcTaskDispatcher#hasCommitDataOutstanding()} lost it by becoming a query, not
     * by the rule being relaxed, and the difference is what this pins - deleting the guard would restore the
     * silent cross-thread corruption it was added to expose.
     */
    @Test
    void theWorkManagerTouchingCommitMethodsStayOwnerThreadOnly() throws InterruptedException {
        dispatcher = new PcTaskDispatcher("task-owner-guard", INPUT_PARTITIONS, 4);

        assertThat(runOffOwnerThread("test-StateUpdater-1", dispatcher::collectCommitData))
                .as("collecting drains the mailbox into PC, so it stays owner-thread-only")
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("collectCommitData");

        assertThat(runOffOwnerThread("test-StateUpdater-1",
                () -> dispatcher.onCommitSuccess(Collections.emptyMap())))
                .as("and acknowledging a commit writes partition state, so it does too")
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("onCommitSuccess");
    }

    /**
     * Runs {@code body} on a thread named after Kafka Streams' state updater - the thread this module's
     * commit surface is actually reached from - and returns whatever it threw, or null.
     */
    private static Throwable runOffOwnerThread(final String threadName, final Runnable body)
            throws InterruptedException {
        AtomicReference<Throwable> thrown = new AtomicReference<>();
        Thread thread = new Thread(() -> {
            try {
                body.run();
            } catch (Throwable t) { //NOSONAR - the point is to report whatever the call did, including Errors
                thrown.set(t);
            }
        }, threadName);
        thread.start();
        thread.join(PUMP_TIMEOUT.toMillis());
        assertThat(thread.isAlive())
                .as("the off-thread call must return rather than block - a query that can block is not one")
                .isFalse();
        return thrown.get();
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
