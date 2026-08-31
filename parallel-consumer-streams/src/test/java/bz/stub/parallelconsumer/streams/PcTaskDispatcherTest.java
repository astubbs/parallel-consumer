package bz.stub.parallelconsumer.streams;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.internal.EpochAndRecordsMap;
import bz.stub.parallelconsumer.internal.PCModule;
import bz.stub.parallelconsumer.state.WorkManager;
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
 * {@code bz.stub.parallelconsumer.streams.integrationTests.PcDrivenStreamsDispatchTest}.
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
        // Back to where the JVM found it, not to a value this class prefers - a teardown that parks the
        // switch somewhere else is exactly the implicit coupling that stating each arm's requirement removes.
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
     * The default, asserted rather than assumed - this is the test that would go red if someone flipped it.
     * The seam is an opt-in preview while unsupported topology shapes go unrefused, so a {@code StreamTask}
     * constructed with nothing configured must get <b>no</b> dispatcher and keep its own partition group;
     * turning the switch on gets one. With the switch off the dispatch marker cannot move, which is what
     * makes a non-zero marker elsewhere mean something.
     * <p>
     * <b>Read the reasoning in {@link PcDispatchSwitch} before flipping this.</b> The default was ON in the
     * feasibility study and this reverses that, on a different argument from the one that decision beat -
     * flipping it back to make a test convenient would be the third round trip.
     * <p>
     * {@code resetToDefault()} rather than reading {@code isEnabled()} straight off: this class mutates the
     * process-wide switch, so by the time this method runs the switch holds whatever the last test left, and
     * an unqualified read here would be asserting on test-ordering rather than on the default.
     */
    @Test
    void theSwitchIsOffByDefaultAndTurningItOnPutsPcInThePath() {
        PcDispatchSwitch.resetToDefault();

        assertThat(PcDispatchSwitch.isEnabled())
                .as("PC dispatch must default to OFF while unsupported topology shapes are dispatched rather "
                        + "than refused - see PcDispatchSwitch for why, and for what would change it back")
                .isFalse();
        assertThat(PcTaskDispatcher.createIfEnabled("task-default-off", INPUT_PARTITIONS))
                .as("so a task built with nothing configured keeps its own partition group - no dispatcher")
                .isNull();

        PcDispatchSwitch.enable(2);
        // Assigned to the field so @AfterEach closes its worker pool.
        dispatcher = PcTaskDispatcher.createIfEnabled("task-on", INPUT_PARTITIONS);
        assertThat(dispatcher)
                .as("and opting in must give a dispatcher")
                .isNotNull();
        assertThat(dispatcher.getRecordsDispatched())
                .as("merely existing dispatches nothing")
                .isZero();

        assertThat(PcDispatchCounters.getRecordsDispatchedToPool())
                .as("nothing reached a worker pool in either half")
                .isZero();
    }

    /**
     * The property is the only way an application (as opposed to a test) can opt in, so its parsing is worth
     * an assertion of its own - including that a typo fails loudly rather than being read as the default,
     * which would silently produce a run that looks like the arm it was asked for and is not.
     */
    @Test
    void theEnabledPropertyTurnsTheSeamOnAndRejectsAnythingItCannotUnderstand() {
        final String original = System.getProperty(PcDispatchSwitch.ENABLED_PROPERTY);
        try {
            System.setProperty(PcDispatchSwitch.ENABLED_PROPERTY, "true");
            PcDispatchSwitch.resetToDefault();
            assertThat(PcDispatchSwitch.isEnabled())
                    .as("-D%s=true is how an application opts a whole JVM in",
                            PcDispatchSwitch.ENABLED_PROPERTY)
                    .isTrue();

            System.setProperty(PcDispatchSwitch.ENABLED_PROPERTY, "TRUE");
            PcDispatchSwitch.resetToDefault();
            assertThat(PcDispatchSwitch.isEnabled()).as("and case must not matter").isTrue();

            System.setProperty(PcDispatchSwitch.ENABLED_PROPERTY, "false");
            PcDispatchSwitch.resetToDefault();
            assertThat(PcDispatchSwitch.isEnabled())
                    .as("and =false is how an A/B run states the stock arm explicitly")
                    .isFalse();

            System.setProperty(PcDispatchSwitch.ENABLED_PROPERTY, "ture");
            assertThatThrownBy(PcDispatchSwitch::resetToDefault)
                    .as("a typo must not be silently read as the default")
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
     *
     * <p><b>The "other keys keep flowing" half of this test was deliberately narrowed</b> (astubbs#255), and
     * it is a narrowing rather than a regression. It used to assert that a poison key blocked only its own
     * shard while every other key drained to completion. That is still the right description of the
     * <em>shards</em> under KEY ordering - but it was also, accidentally, the dispatcher's policy after a
     * failure it had already seen, and that policy was wrong. Stock Kafka Streams stops processing at the
     * throw: the exception leaves {@code process()}, reaches the uncaught-exception handler, and the thread
     * dies. Here the throw happens on a worker and reaches the StreamThread later, so continuing to hand out
     * work in between produced {@code forward()} side effects for a task that was already dying, bounded by
     * nothing but the poll budget.
     */
    @Test
    void aFailingRecordSurfacesOnceIsNeverRetriedAndStopsFurtherDispatch() {
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
                .as("work already in flight when the failure happened is left to finish, not interrupted "
                        + "mid-chain")
                .isNotEmpty();
        assertThat(completedKeys)
                .as("STRICTLY fewer than the 9 the pre-bar dispatcher completed. isNotEmpty() alone would be "
                        + "a weakened assertion - 9 satisfies it, so the test would pass identically with the "
                        + "bar deleted and would pin nothing")
                .hasSizeLessThan(9);
        assertThat(completedKeys).doesNotContain(poisonKey);
        assertThat(dispatcher.hasPendingFailure())
                .as("the dispatcher knows a record failed, and that bar is what stops further dispatch")
                .isTrue();
        assertThat(dispatcher.getRecordsDispatched())
                .as("dispatch stopped at the failure - STRICTLY fewer than the 10 the pre-bar dispatcher "
                        + "handed out")
                .isLessThan(10);

        PcTaskDispatcher.Failure failure = dispatcher.pollFailure();
        assertThat(failure)
                .as("the failure must be retrievable so the StreamThread can surface it the way stock does")
                .isNotNull();
        assertThat(failure.getCause()).isInstanceOf(IllegalStateException.class);
        assertThat(failure.getTopic())
                .as("and must name the record, because it is one of several running concurrently")
                .isEqualTo(TOPIC);
        assertThat(dispatcher.pollFailure())
                .as("and cleared once taken, so it is reported once rather than on every pump")
                .isNull();
        assertThat(dispatcher.hasPendingFailure())
                .as("but the DISPATCH BAR outlives the poll that cleared the failure - suspend()'s drain runs "
                        + "next, and an open bar there would hand out the entire remaining backlog")
                .isTrue();

        assertThat(dispatcher.getRecordsFailed()).isEqualTo(1);
        assertThat(dispatcher.getRecordsSucceeded()).isEqualTo(completedKeys.size());
        assertThat(dispatcher.getRecordsSucceeded() + dispatcher.getRecordsFailed())
                .as("every record handed to the pool reported an outcome back to PC")
                .isEqualTo(dispatcher.getRecordsDispatched());
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

        // Probed through collectCommitData, not hasCommitDataOutstanding: the latter stopped being guarded
        // when it became a genuine query, so it can no longer witness a guard at all. collectCommitData
        // reaches WorkManager and therefore still carries the rule this test is about.
        assertThat(dispatcher.collectCommitData())
                .as("precondition: the constructing thread owns the commit surface")
                .isEmpty();

        runOnNewThread("new-owner", () -> {
            assertThatThrownBy(dispatcher::collectCommitData)
                    .as("before rebinding, the new thread is a stranger")
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("owner-thread-only")
                    .hasMessageContaining("bindToCurrentThread");

            dispatcher.bindToCurrentThread();

            assertThat(dispatcher.collectCommitData())
                    .as("after rebinding the new owner may drive the commit surface - this is the "
                            + "legitimate call the construction-time bind used to reject")
                    .isEmpty();
        });

        assertThatThrownBy(dispatcher::collectCommitData)
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

    /**
     * A closed dispatcher cannot be handed to a live thread. That is the revival hazard in its smallest
     * shape: the patched {@code StreamTask.revive()} builds a <em>new</em> dispatcher rather than re-binding
     * the dead one, and this is the guard that makes taking the other route fail loudly.
     */
    @Test
    void aClosedDispatcherRefusesToBeBound() {
        dispatcher = new PcTaskDispatcher("task-bind-closed", INPUT_PARTITIONS, 4);
        dispatcher.close();

        assertThatThrownBy(dispatcher::bindToCurrentThread)
                .as("binding a closed dispatcher would accept records and never dispatch them - the silent "
                        + "stall a revived task used to hit")
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("closed PC dispatcher");
    }

    /**
     * <b>Revival, at the dispatcher's own level.</b>
     * <p>
     * {@code closeDirtyAndRevive} resurrects the same {@code StreamTask} instance, so the seam's answer is a
     * <em>replacement</em> dispatcher over the partitions the task holds now - never a resurrection of the
     * closed one, which has no route back to a running worker pool. This asserts the property the patched
     * {@code revive()} depends on: the fresh dispatcher works, and it starts with a clean commit ledger
     * rather than inheriting the dead one's.
     */
    @Test
    void aReplacementDispatcherOverTheSamePartitionsRunsWorkTheClosedOneCouldNot() {
        final PcTaskDispatcher closedOne = new PcTaskDispatcher("task-revive-old", INPUT_PARTITIONS, 4);
        closedOne.registerRecords(PARTITION, records(2, offset -> "k" + offset));
        closedOne.pumpUntilQuiescent(record -> () -> { }, PUMP_TIMEOUT);
        closedOne.close();

        assertThat(closedOne.isClosed()).as("precondition: the old dispatcher went down with the task").isTrue();

        dispatcher = new PcTaskDispatcher("task-revive-new", INPUT_PARTITIONS, 4);

        assertThat(dispatcher.hasUncommittedWork())
                .as("the replacement starts clean - it inherits no commit state from the instance that "
                        + "died, and Kafka was never told about work the closed one had not committed")
                .isFalse();

        dispatcher.registerRecords(PARTITION, records(3, offset -> "revived-" + offset));
        dispatcher.pumpUntilQuiescent(record -> () -> { }, PUMP_TIMEOUT);

        assertThat(dispatcher.getRecordsSucceeded())
                .as("and it dispatches, which is the whole difference between a revived task that runs and "
                        + "one that accepts records into a shut-down pool and says nothing")
                .isEqualTo(3);
        assertThat(dispatcher.hasCommitDataOutstanding())
                .as("its own work is commit data on its own ledger")
                .isTrue();
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
        final int activeBefore = PcTaskDispatcher.activeCount();
        dispatcher = new PcTaskDispatcher("task-teardown-contract", INPUT_PARTITIONS, 4);
        dispatcher.registerRecords(PARTITION, records(2, offset -> "k" + offset));
        dispatcher.pumpUntilQuiescent(record -> () -> { }, PUMP_TIMEOUT);

        assertThat(PcTaskDispatcher.activeCount())
                .as("precondition: the dispatcher is live in the JVM-wide registry")
                .isEqualTo(activeBefore + 1);
        assertThat(PcWorkSignal.registeredDispatchersOnCurrentThread())
                .as("precondition: and its owner's wake signal speaks for it")
                .isEqualTo(1);

        dispatcher.close();

        assertThat(PcTaskDispatcher.activeCount())
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

    // ------------------------------------------------------------------------------------------------
    // Backpressure occupancy (astubbs#255). What the patched StreamTask compares against
    // buffered.records.per.partition to decide whether to pause the consumer - so a count that drifts LOW
    // silently removes the memory bound, and one that drifts HIGH pauses a partition for good. These pin
    // both directions.
    // ------------------------------------------------------------------------------------------------

    /** Prepares work that never finishes, so records stay in flight and cannot be drained mid-assertion. */
    private static PcTaskDispatcher.WorkPreparer blockingPreparer(final CountDownLatch release) {
        return record -> () -> awaitLatch(release);
    }

    private static PcTaskDispatcher.WorkPreparer noopPreparer() {
        return record -> () -> {
        };
    }

    @Test
    void registeringRecordsRaisesTheBufferedCountByTheNumberPcTookOn() {
        dispatcher = new PcTaskDispatcher("task-buffered-register", INPUT_PARTITIONS, 4);

        assertThat(dispatcher.getBufferedRecordCount(PARTITION))
                .as("a fresh dispatcher holds nothing")
                .isZero();

        dispatcher.registerRecords(PARTITION, records(7, offset -> "key-" + offset));

        assertThat(dispatcher.getBufferedRecordCount(PARTITION))
                .as("every registered record is buffered until a worker starts it")
                .isEqualTo(7);
        assertThat(dispatcher.getBufferedRecordCount())
                .as("the total is the sum over the partitions this task holds")
                .isEqualTo(7);
    }

    @Test
    void aRecordPcHasAlreadyCompletedDoesNotRaiseTheBufferedCount() {
        dispatcher = new PcTaskDispatcher("task-buffered-refused", INPUT_PARTITIONS, 4);

        List<ConsumerRecord<byte[], byte[]>> batch = records(3, offset -> "key-" + offset);
        dispatcher.registerRecords(PARTITION, batch);
        dispatcher.pumpUntilQuiescent(noopPreparer(), PUMP_TIMEOUT);

        assertThat(dispatcher.getBufferedRecordCount(PARTITION))
                .as("everything registered has been consumed")
                .isZero();

        dispatcher.registerRecords(PARTITION, batch);

        assertThat(dispatcher.getBufferedRecordCount(PARTITION))
                .as("PC refuses offsets it has already completed, so nothing was taken on")
                .isZero();
    }

    /**
     * The ranked defect this rung was asked to close, and the reason the occupancy count is derived from
     * PC's incomplete set rather than predicted at registration.
     * <p>
     * Kafka can hand the same offset to {@code addRecords} twice without PC having completed it - a
     * {@code seek} backwards, an offset reset, corruption recovery. PC keeps the container it already has
     * ({@code ProcessingShard.addWorkContainer} drops the arrival when a live one for that offset is
     * resident) and will therefore only ever hand that record out ONCE. A count raised per re-delivered
     * record is raised twice and lowered once: it never returns to zero, and since the count is what pauses
     * the partition, that partition is paused for good with no symptom but a topology that has gone quiet.
     * <p>
     * The second half of this test is the one that would have caught it: the count must come back to zero.
     */
    @Test
    void aRedeliveredOffsetTheShardDropsDoesNotRaiseTheBufferedCount() {
        dispatcher = new PcTaskDispatcher("task-buffered-redelivered", INPUT_PARTITIONS, 4);

        // One key, so KEY ordering keeps all three resident in the shard rather than letting a pump take
        // them - the state in which a re-delivery is dropped rather than replacing anything.
        List<ConsumerRecord<byte[], byte[]>> batch = records(3, offset -> "the-one-key");
        dispatcher.registerRecords(PARTITION, batch);
        assertThat(dispatcher.getBufferedRecordCount(PARTITION)).isEqualTo(3);

        dispatcher.registerRecords(PARTITION, batch);

        assertThat(dispatcher.getBufferedRecordCount(PARTITION))
                .as("the same three offsets re-delivered are the same three records - PC drops the arriving "
                        + "containers, so counting them would leave the partition paused for ever")
                .isEqualTo(3);

        dispatcher.pumpUntilQuiescent(noopPreparer(), PUMP_TIMEOUT);

        assertThat(dispatcher.getBufferedRecordCount(PARTITION))
                .as("and it comes back to zero, which is what a permanent-pause residue could not do")
                .isZero();
        assertThat(dispatcher.getBufferedUnderflowCount()).isZero();
    }

    @Test
    void theBufferedCountReturnsToZeroAndNeverUnderflowsAcrossRepeatedBatches() {
        dispatcher = new PcTaskDispatcher("task-buffered-balance", INPUT_PARTITIONS, 4);

        List<ConsumerRecord<byte[], byte[]>> batch = records(6, offset -> "key-" + (offset % 3));

        for (int round = 0; round < 3; round++) {
            // The same batch every round: after the first, every offset has already been completed, so PC
            // refuses all six and the count must not move. That is the drift-high direction.
            dispatcher.registerRecords(PARTITION, batch);
            assertThat(dispatcher.getBufferedRecordCount(PARTITION))
                    .as("round %s registered only what PC had not already completed", round)
                    .isEqualTo(round == 0 ? 6 : 0);

            dispatcher.pumpUntilQuiescent(noopPreparer(), PUMP_TIMEOUT);

            assertThat(dispatcher.getBufferedRecordCount(PARTITION))
                    .as("round %s consumed everything it registered", round)
                    .isZero();
        }

        assertThat(dispatcher.getBufferedUnderflowCount())
                .as("a negative occupancy is the drift direction that disables the pause, and nothing else "
                        + "reports it")
                .isZero();
    }

    /**
     * Kafka's own {@code shouldResumePartitionWhenSkippingOverRecordsWithInvalidTs} drives five same-key
     * records against a threshold of three and expects the partition still paused after one pump. Under KEY
     * ordering one shard hands out one record at a time, so one pump must consume exactly one and leave four
     * held - if it consumed more, occupancy would fall to the threshold and the resume would be correct by
     * its own rule while being wrong against Kafka.
     */
    @Test
    void onePumpOverOneKeyConsumesExactlyOneRecord() {
        dispatcher = new PcTaskDispatcher("task-buffered-one-key", INPUT_PARTITIONS, 4);
        CountDownLatch release = new CountDownLatch(1);

        dispatcher.registerRecords(PARTITION, records(5, offset -> "the-one-key"));

        int consumed = dispatcher.dispatchAvailable(blockingPreparer(release));

        try {
            assertThat(consumed)
                    .as("KEY ordering hands out at most one record per key at a time")
                    .isEqualTo(1);
            assertThat(dispatcher.getBufferedRecordCount(PARTITION))
                    .as("four of the five are still held, which is above a threshold of three - so a resume "
                            + "computed from this number cannot fire yet")
                    .isEqualTo(4);
        } finally {
            release.countDown();
        }
    }

    @Test
    void aDispatchedRecordLeavesTheBufferEvenWhileItIsStillRunning() {
        dispatcher = new PcTaskDispatcher("task-buffered-inflight", INPUT_PARTITIONS, 2);
        CountDownLatch release = new CountDownLatch(1);

        dispatcher.registerRecords(PARTITION, records(5, offset -> "key-" + offset));
        assertThat(dispatcher.getBufferedRecordCount(PARTITION)).isEqualTo(5);

        int consumed = dispatcher.dispatchAvailable(blockingPreparer(release));

        try {
            assertThat(consumed)
                    .as("the pool bounds how many can start at once")
                    .isEqualTo(2);
            assertThat(dispatcher.getBufferedRecordCount(PARTITION))
                    .as("buffered means not yet started, so the two in flight have left the buffer")
                    .isEqualTo(3);
            assertThat(dispatcher.getInFlightCount()).isEqualTo(2);
        } finally {
            release.countDown();
        }
    }

    /**
     * The objection that kept this count standalone on the branch this work is reconstructed from: PC's
     * incomplete set holds a failed record for ever, because retries are disabled, so backpressure derived
     * from it would pause that partition permanently. Answered rather than ignored - a failed record is
     * never subtracted back out of the handed-out tally, so it cancels itself out of the derivation.
     */
    @Test
    void aFailedRecordStaysOutOfTheBufferedCountForever() {
        dispatcher = new PcTaskDispatcher("task-buffered-poison", INPUT_PARTITIONS, 4);

        dispatcher.registerRecords(PARTITION, records(1, offset -> "key-a"));
        dispatcher.pumpUntilQuiescent(record -> () -> {
            throw new IllegalStateException("boom");
        }, PUMP_TIMEOUT);

        assertThat(dispatcher.getRecordsFailed()).isEqualTo(1);
        assertThat(dispatcher.getBufferedRecordCount(PARTITION))
                .as("PC still counts the failed offset as incomplete and always will - but it is not "
                        + "BUFFERED, and reporting it as such would pause this partition for ever")
                .isZero();
        assertThat(dispatcher.getBufferedUnderflowCount()).isZero();
    }

    /**
     * A record dropped on the way in never reaches a worker, but it HAS left the buffer - it is consumed.
     * Counting only pool submissions would leave the count high and pause the partition over records that no
     * longer exist.
     */
    @Test
    void aRecordDroppedDuringPreparationStillLeavesTheBuffer() {
        dispatcher = new PcTaskDispatcher("task-buffered-dropped", INPUT_PARTITIONS, 4);

        dispatcher.registerRecords(PARTITION, records(4, offset -> "key-" + offset));
        dispatcher.pumpUntilQuiescent(record -> null, PUMP_TIMEOUT);

        assertThat(dispatcher.getBufferedRecordCount(PARTITION))
                .as("dropped is consumed")
                .isZero();
    }

    @Test
    void bufferedCountsAreKeptPerPartitionAndRevokedWithTheirPartition() {
        final TopicPartition second = new TopicPartition(TOPIC, 1);
        dispatcher = new PcTaskDispatcher("task-buffered-partitions", UniSets.of(PARTITION, second), 4);

        dispatcher.registerRecords(PARTITION, records(PARTITION, 3, offset -> "key-" + offset));
        dispatcher.registerRecords(second, records(second, 5, offset -> "key-" + offset));

        assertThat(dispatcher.getBufferedRecordCount(PARTITION)).isEqualTo(3);
        assertThat(dispatcher.getBufferedRecordCount(second)).isEqualTo(5);
        assertThat(dispatcher.getBufferedRecordCount()).isEqualTo(8);

        dispatcher.updatePartitions(UniSets.of(PARTITION));

        assertThat(dispatcher.getBufferedRecordCount(second))
                .as("a revoked partition holds nothing - the new owner re-reads it. A count left behind here "
                        + "would keep a partition paused by a task that no longer owns it")
                .isZero();
        assertThat(dispatcher.getBufferedRecordCount(PARTITION))
                .as("the surviving partition is untouched")
                .isEqualTo(3);
        assertThat(dispatcher.getBufferedRecordCount())
                .as("and the total follows the assignment")
                .isEqualTo(3);
    }

    @Test
    void theBufferedCountIsReadableFromAThreadThatIsNotTheOwner() throws InterruptedException {
        dispatcher = new PcTaskDispatcher("task-buffered-foreign-read", INPUT_PARTITIONS, 4);
        dispatcher.registerRecords(PARTITION, records(6, offset -> "key-" + offset));

        AtomicInteger seen = new AtomicInteger(-1);
        // The memory-bound proof samples occupancy from a watcher thread while the run is in flight, so this
        // is the call shape that must not hit the owner-thread guard.
        Throwable thrown = runOffOwnerThread("not-the-owner",
                () -> seen.set(dispatcher.getBufferedRecordCount(PARTITION)));

        assertThat(thrown)
                .as("a question may not be owner-thread-guarded")
                .isNull();
        assertThat(seen.get()).isEqualTo(6);
    }

    @Test
    void closingClearsTheBufferedCounts() {
        dispatcher = new PcTaskDispatcher("task-buffered-close", INPUT_PARTITIONS, 4);
        dispatcher.registerRecords(PARTITION, records(4, offset -> "key-" + offset));
        assertThat(dispatcher.getBufferedRecordCount()).isEqualTo(4);

        dispatcher.close();

        assertThat(dispatcher.getBufferedRecordCount())
                .as("a closed dispatcher holds nothing, and a count left behind outlives the task that "
                        + "paused the partition")
                .isZero();
    }

    // ------------------------------------------------------------------------------------------------
    // Failure surfacing (astubbs#255): the type, the timing, and the commit fence.
    // ------------------------------------------------------------------------------------------------

    @Test
    void aFailurePollCarriesTheRecordThatCausedIt() {
        dispatcher = new PcTaskDispatcher("task-failure-record", INPUT_PARTITIONS, 1);
        RuntimeException boom = new IllegalStateException("boom");

        dispatcher.registerRecords(PARTITION, records(1, offset -> "key-a"));
        dispatcher.pumpUntilQuiescent(record -> () -> {
            throw boom;
        }, PUMP_TIMEOUT);

        PcTaskDispatcher.Failure failure = dispatcher.pollFailure();

        assertThat(failure).isNotNull();
        assertThat(failure.getCause()).isSameAs(boom);
        assertThat(failure.getTopic()).isEqualTo(TOPIC);
        assertThat(failure.getPartition()).isEqualTo(PARTITION.partition());
        assertThat(failure.getOffset())
                .as("which of the concurrently running records failed is the first question anyone asks")
                .isZero();
        assertThat(dispatcher.pollFailure())
                .as("cleared as it is handed over")
                .isNull();
    }

    /**
     * {@code pollFailure()} clears the failure as it hands it to the StreamThread, so a bar that read only
     * that field would be open again immediately - and {@code StreamTask.suspend()}'s drain, which runs
     * next, would dispatch the whole remaining backlog of a task that is already dying.
     */
    @Test
    void aSurfacedFailureStillBarsDispatchSoTheSuspendDrainCannotRunTheBacklog() {
        dispatcher = new PcTaskDispatcher("task-failure-sticky", INPUT_PARTITIONS, 1);
        AtomicInteger started = new AtomicInteger();

        dispatcher.registerRecords(PARTITION, records(20, offset -> "key-" + offset));
        dispatcher.pumpUntilQuiescent(record -> () -> {
            started.incrementAndGet();
            throw new IllegalStateException("boom");
        }, PUMP_TIMEOUT);

        int startedBeforeSurfacing = started.get();
        assertThat(dispatcher.hasPendingFailure()).isTrue();

        // The StreamThread takes the exception - which clears firstFailure - and the task is then suspended.
        assertThat(dispatcher.pollFailure()).isNotNull();
        assertThat(dispatcher.hasPendingFailure())
                .as("the bar must outlive the poll that cleared the failure")
                .isTrue();

        boolean quiesced = dispatcher.pumpUntilQuiescent(record -> () -> {
            started.incrementAndGet();
            throw new IllegalStateException("boom");
        }, PUMP_TIMEOUT);

        assertThat(started.get())
                .as("the suspend-shaped drain must not hand out the backlog")
                .isEqualTo(startedBeforeSurfacing);
        assertThat(quiesced)
                .as("and must still reach quiescence, or suspend() sits out its whole drain timeout")
                .isTrue();
    }

    @Test
    void recordsAlreadyInFlightWhenAFailureOccursStillCompleteAndReachPcsAccounting() {
        dispatcher = new PcTaskDispatcher("task-failure-inflight", INPUT_PARTITIONS, 4);
        AtomicInteger completed = new AtomicInteger();

        dispatcher.registerRecords(PARTITION, records(8, offset -> "key-" + offset));
        dispatcher.pumpUntilQuiescent(record -> () -> {
            if (record.offset() == 0) {
                throw new IllegalStateException("boom");
            }
            completed.incrementAndGet();
        }, PUMP_TIMEOUT);

        assertThat(dispatcher.getRecordsFailed()).isEqualTo(1);
        assertThat(completed.get())
                .as("in-flight work is left to finish rather than interrupted mid-chain")
                .isPositive();
        assertThat(dispatcher.getRecordsSucceeded() + dispatcher.getRecordsFailed())
                .as("every dispatched record reported an outcome to PC")
                .isEqualTo(dispatcher.getRecordsDispatched());
    }

    @Test
    void aFailedRecordIsNeverHandedOutAgain() {
        dispatcher = new PcTaskDispatcher("task-failure-no-retry", INPUT_PARTITIONS, 1);
        List<Long> seen = new CopyOnWriteArrayList<>();

        dispatcher.registerRecords(PARTITION, records(1, offset -> "key-a"));
        dispatcher.pumpUntilQuiescent(record -> {
            seen.add(record.offset());
            return () -> {
                throw new IllegalStateException("boom");
            };
        }, PUMP_TIMEOUT);
        dispatcher.pollFailure();
        dispatcher.pumpUntilQuiescent(record -> {
            seen.add(record.offset());
            return () -> {
            };
        }, PUMP_TIMEOUT);

        assertThat(seen)
                .as("retries are disabled on purpose - a retry re-runs a chain that already called forward()")
                .containsExactly(0L);
    }

    /**
     * The TIMING half of the typed-exception fix, at unit level: a pump that has nothing left to hand out
     * waits for the outcome of what it already dispatched, so a failure raised on a worker is available to
     * the very next line rather than a pump - or a whole {@code runOnce} - later.
     */
    @Test
    void awaitOutcomeReturnsAsSoonAsAWorkerReportsAFailure() {
        dispatcher = new PcTaskDispatcher("task-settle-failure", INPUT_PARTITIONS, 1);
        CountDownLatch startedRunning = new CountDownLatch(1);
        CountDownLatch releaseWorker = new CountDownLatch(1);

        dispatcher.registerRecords(PARTITION, records(1, offset -> "key-a"));
        int consumed = dispatcher.dispatchAvailable(record -> () -> {
            startedRunning.countDown();
            awaitLatch(releaseWorker);
            throw new IllegalStateException("boom");
        });
        assertThat(consumed).isEqualTo(1);
        awaitLatch(startedRunning);

        assertThat(dispatcher.pollFailure())
                .as("nothing has failed yet - without the wait this is what the pump would see and act on")
                .isNull();

        releaseWorker.countDown();

        assertThat(dispatcher.awaitOutcome(PUMP_TIMEOUT))
                .as("the wait ends because there is something to see, not because it ran out")
                .isTrue();
        assertThat(dispatcher.pollFailure())
                .as("and the failure is now available on THIS pump")
                .isNotNull();
    }

    @Test
    void awaitOutcomeReturnsImmediatelyWhenNothingIsInFlight() {
        dispatcher = new PcTaskDispatcher("task-settle-idle", INPUT_PARTITIONS, 1);

        long startedAt = System.nanoTime();
        boolean sawSomething = dispatcher.awaitOutcome(Duration.ofSeconds(30));
        Duration waited = Duration.ofNanos(System.nanoTime() - startedAt);

        assertThat(sawSomething).isTrue();
        assertThat(waited)
                .as("an idle dispatcher must not cost the StreamThread anything - the wait is for outcomes "
                        + "of work in flight, and there is none")
                .isLessThan(Duration.ofSeconds(5));
    }

    /**
     * The discriminator that keeps the settle wait out of the saturated case, and it is not an optimisation.
     * A pump consumes nothing for two quite different reasons: PC had nothing to hand out (idle - the
     * outcome of running work is the only thing that can change anything, so wait) or the pool was full
     * (saturated - there is plenty of work and nowhere to put it, and the StreamThread's next act would have
     * been to poll). Waiting in the second case throttles INTAKE, measured at sixteen-fold with the pause
     * switched off, which silently supplied a second memory bound and made the memory-bound proof's control
     * arm look almost bounded.
     */
    @Test
    void aFullPoolDeclinesToWaitSoThatIntakeIsNotThrottled() {
        dispatcher = new PcTaskDispatcher("task-settle-saturated", INPUT_PARTITIONS, 1);
        CountDownLatch startedRunning = new CountDownLatch(1);
        CountDownLatch releaseWorker = new CountDownLatch(1);

        // Two distinct keys, so PC genuinely has more to give and only the single pool slot stops it.
        dispatcher.registerRecords(PARTITION, records(2, offset -> "key-" + offset));
        assertThat(dispatcher.dispatchAvailable(record -> () -> {
            startedRunning.countDown();
            awaitLatch(releaseWorker);
        })).isEqualTo(1);
        awaitLatch(startedRunning);

        try {
            assertThat(dispatcher.dispatchAvailable(noopPreparer()))
                    .as("the pool is full, so this pump hands out nothing - which is the state the wait must "
                            + "NOT be taken in")
                    .isZero();

            long startedAt = System.nanoTime();
            boolean waited = dispatcher.awaitOutcomeIfIdle();
            Duration elapsed = Duration.ofNanos(System.nanoTime() - startedAt);

            assertThat(waited).isFalse();
            assertThat(elapsed)
                    .as("it must return at once rather than holding the StreamThread away from poll() for "
                            + "the settle budget on every saturated pump")
                    .isLessThan(PcTaskDispatcher.OUTCOME_SETTLE_BUDGET);
        } finally {
            releaseWorker.countDown();
        }
    }

    /**
     * The other half of the discriminator: a pump that ran out of WORK while the pool still had room does
     * wait, because there is nothing else it could usefully do.
     */
    @Test
    void aPumpThatRanOutOfWorkWithASpareSlotDoesWait() {
        dispatcher = new PcTaskDispatcher("task-settle-idle-pump", INPUT_PARTITIONS, 4);
        CountDownLatch startedRunning = new CountDownLatch(1);
        CountDownLatch releaseWorker = new CountDownLatch(1);

        // One key, so after the first record is handed out KEY ordering leaves nothing selectable while
        // three pool slots stand empty - PC ran out of work, the pool did not.
        dispatcher.registerRecords(PARTITION, records(2, offset -> "the-one-key"));
        assertThat(dispatcher.dispatchAvailable(record -> () -> {
            startedRunning.countDown();
            awaitLatch(releaseWorker);
            throw new IllegalStateException("boom");
        })).isEqualTo(1);
        awaitLatch(startedRunning);

        assertThat(dispatcher.dispatchAvailable(noopPreparer()))
                .as("nothing selectable, but the pool has room")
                .isZero();

        releaseWorker.countDown();

        assertThat(dispatcher.awaitOutcomeIfIdle())
                .as("so it waits, and the failure is available on this pump rather than the next")
                .isTrue();
        assertThat(dispatcher.pollFailure()).isNotNull();
    }

    @Test
    void awaitOutcomeGivesUpAtItsBudgetWhenAWorkerIsStillRunning() {
        dispatcher = new PcTaskDispatcher("task-settle-budget", INPUT_PARTITIONS, 1);
        CountDownLatch startedRunning = new CountDownLatch(1);
        CountDownLatch releaseWorker = new CountDownLatch(1);

        dispatcher.registerRecords(PARTITION, records(1, offset -> "key-a"));
        dispatcher.dispatchAvailable(record -> () -> {
            startedRunning.countDown();
            awaitLatch(releaseWorker);
        });
        awaitLatch(startedRunning);

        try {
            assertThat(dispatcher.awaitOutcome(Duration.ofMillis(50)))
                    .as("a worker that neither finishes nor fails must not hold the StreamThread past the "
                            + "budget - the wait is bounded, which is what makes it safe to take at all")
                    .isFalse();
        } finally {
            releaseWorker.countDown();
        }
    }

    /**
     * Control arm for {@link #aWorkerFailureFencesTheCommitFrontier}. Same shape, no failure: the frontier
     * IS offered, so the fenced arm below is showing the fence rather than a dispatcher that never offers
     * commit data at all.
     */
    @Test
    void withNoFailureTheCommitFrontierIsOffered() {
        dispatcher = new PcTaskDispatcher("task-fence-control", INPUT_PARTITIONS, 4);

        dispatcher.registerRecords(PARTITION, records(4, offset -> "key-" + offset));
        dispatcher.pumpUntilQuiescent(noopPreparer(), PUMP_TIMEOUT);

        assertThat(dispatcher.getRecordsSucceeded()).isEqualTo(4);
        assertThat(dispatcher.collectCommitData())
                .as("four records completed and no commit has covered them, so there is a frontier to commit")
                .isNotEmpty();
    }

    /**
     * astubbs/parallel-consumer#271's review thread, "a worker's processing failure can be committed past",
     * answered as a property rather than as prose.
     * <p>
     * Key A fails; keys B, C, D succeed. On the stock path the throw leaves {@code process()} and the
     * {@code runOnce} iteration never reaches its commit. Here the throw happens on a worker, so without the
     * fence a scheduled commit landing before the failure is surfaced would make the other keys' offsets
     * durable for a task that is about to be closed dirty and rewound - and for a
     * {@code TaskCorruptedException} that is worse than a duplicate, because recovery wipes the state the
     * committed offsets claim to cover.
     */
    @Test
    void aWorkerFailureFencesTheCommitFrontier() {
        dispatcher = new PcTaskDispatcher("task-fence", INPUT_PARTITIONS, 4);

        dispatcher.registerRecords(PARTITION, records(4, offset -> "key-" + offset));
        dispatcher.pumpUntilQuiescent(record -> () -> {
            if (record.offset() == 0) {
                throw new IllegalStateException("boom");
            }
        }, PUMP_TIMEOUT);

        assertThat(dispatcher.getRecordsFailed())
                .as("the arm is only meaningful if a record really failed")
                .isEqualTo(1);
        assertThat(dispatcher.getRecordsSucceeded())
                .as("and only if OTHER records really succeeded - those are the offsets that would be "
                        + "committed past the failure")
                .isPositive();
        assertThat(dispatcher.hasCommitDataOutstanding())
                .as("PC does hold completed, uncommitted work: the fence is what withholds it, not an "
                        + "absence of anything to withhold")
                .isTrue();

        assertThat(dispatcher.collectCommitData())
                .as("nothing may be committed past a record that failed")
                .isEmpty();
    }

    /**
     * The fence must not be paid for by making a failed task look CLEAN to close. Those are opposite
     * questions and were deliberately kept apart: {@code StreamTask.validateClean} turns this one into a
     * {@code TaskMigratedException} so the TaskManager closes the task dirty instead.
     */
    @Test
    void theCommitFenceDoesNotMakeAFailedTaskLookSafeToCloseClean() {
        dispatcher = new PcTaskDispatcher("task-fence-clean", INPUT_PARTITIONS, 4);

        dispatcher.registerRecords(PARTITION, records(4, offset -> "key-" + offset));
        dispatcher.pumpUntilQuiescent(record -> () -> {
            if (record.offset() == 0) {
                throw new IllegalStateException("boom");
            }
        }, PUMP_TIMEOUT);

        assertThat(dispatcher.hasUncommittedWork())
                .as("work completed that no commit covers - and the fence is why no commit will cover it, "
                        + "which makes closing clean MORE wrong rather than less")
                .isTrue();
    }
}
