package bz.stub.parallelconsumer.integrationTests;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.ParallelEoSStreamProcessor;
import bz.stub.parallelconsumer.integrationTests.utils.KafkaClientUtils;
import bz.stub.parallelconsumer.internal.PCModule;
import bz.stub.parallelconsumer.internal.ConsumerManager;
import bz.stub.parallelconsumer.internal.ProducerManager;
import bz.stub.parallelconsumer.internal.ProducerWrapper;
import bz.stub.parallelconsumer.internal.utils.ThreadUtils;
import bz.stub.parallelconsumer.state.WorkManager;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import pl.tlinkowski.unij.api.UniLists;
import pl.tlinkowski.unij.api.UniSets;

import java.time.Duration;
import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static bz.stub.parallelconsumer.ParallelConsumerOptions.CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER;
import static bz.stub.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.PARTITION;
import static com.google.common.truth.Truth.assertWithMessage;
import static java.time.Duration.ofSeconds;
import static org.awaitility.Awaitility.await;

/**
 * MEASUREMENT PROBE for the unbounded revoke wait in
 * {@link ParallelConsumerOptions.CommitMode#PERIODIC_TRANSACTIONAL_PRODUCER} - the defect behind
 * astubbs/parallel-consumer#44 (confluentinc#803, <i>"Transactional Producer instance gets timeout
 * getting commit lock while second instance starts"</i>), the only issue upstream ever labelled
 * <i>verified bug</i>.
 * <p>
 * NOT a candidate for merging as a normal test - it is an instrument, and on the defect arm it is
 * <b>expected to fail</b>. It exists to make the overrun observable before any bound is designed.
 *
 * <h2>The defect being probed</h2>
 * {@code AbstractParallelEoSStreamProcessor.onPartitionsRevoked} opens with
 * <pre>{@code
 *     while (isTransactionCommittingInProgress())
 *         Thread.sleep(100); //wait for the transaction to finish committing
 * }</pre>
 * That predicate is {@code producerTransactionLock.isWriteLocked()}, and the control thread takes
 * that write lock in {@link ProducerManager#preAcquireOffsetsToCommit()} before every transactional
 * commit. The callback runs on {@code pc-broker-poll} <i>inside</i> {@code poll()}, so the wait is
 * bounded by nothing except {@code max.poll.interval.ms} - and breaching that evicts the member,
 * which is the "group is already rebalancing" ending astubbs#44 reports.
 * <p>
 * This is <b>not</b> the AB-BA cycle of astubbs#29. That cycle's second edge lives in
 * {@code ConsumerOffsetCommitter}, which is only constructed for the consumer-commit modes, so it
 * cannot close here. The two defects are in mutually exclusive modes;
 * {@code Rebalance857CommitSyncDeadlockProbeIT} on astubbs#29 is the sibling instrument for the
 * other one, and this class deliberately copies its shape rather than its file.
 *
 * <h2>How the window is opened deterministically</h2>
 * The control-arm method of {@code docs/investigating.md}: inject a delay that forces the window
 * open, and hold every other term identical. {@link DwellingProducerManager} extends the real
 * {@link ProducerManager} and dwells {@link #COMMIT_DWELL_MS} <i>after</i>
 * {@code preAcquireOffsetsToCommit()} has taken the producer write lock. So for the whole dwell
 * {@code isTransactionCommittingInProgress()} is true, and a revoke landing in that span spins in
 * the loop above. The commit interval is 1s and processing is deliberately slow against a 500
 * record backlog, so the {@code WorkManager} stays dirty and a commit is always in flight - the
 * same reason the sibling probe needs {@link #PROCESSING_DELAY_MS}.
 *
 * <h2>The two arms</h2>
 * One term differs between them, {@code -Dprobe857tx.dwellMs}:
 * <ul>
 *   <li><b>defect arm</b> (default, {@value #DEFAULT_DWELL_MS}ms) - the dwell exceeds
 *       {@link #MAX_POLL_INTERVAL_MS}, so the revoke wait breaches the poll interval and the member
 *       is evicted. <b>Expected to FAIL on current code.</b> A bound on the holder is what should
 *       turn this arm green.</li>
 *   <li><b>control arm</b> ({@code -Dprobe857tx.dwellMs=2000}) - the dwell is well under the poll
 *       interval, so the same forced overlap is harmless and every assertion passes. This arm
 *       proves the instrument can go green at all; it says nothing about the code being fixed.</li>
 * </ul>
 *
 * <h2>Two traps this probe is built to avoid</h2>
 * Both voided runs of the sibling instrument, and both are cheap to avoid:
 * <ul>
 *   <li><b>Confirm the arm actually engaged.</b> The resolved dwell is logged and asserted, and
 *       {@link #dwellsEntered} counts the commits that actually held the lock. If the revoke never
 *       overlapped a commit the wait is ~0 and every assertion below would pass <i>vacuously</i> on
 *       the defect arm - a false green that reads exactly like a fix. {@link #WINDOW_OPENED_FLOOR_MS}
 *       turns that into an explicit INCONCLUSIVE failure instead.</li>
 *   <li><b>Run with {@code -Dpc.log.level=info}</b> or the revoke path's log lines are filtered at
 *       the default test verbosity, and their absence is indistinguishable from the race never
 *       happening.</li>
 * </ul>
 * Do <b>not</b> try to reproduce this by replaying a captured chaos seed. A seed fixes the
 * conductor's schedule, not the poll-versus-control interleaving this turns on; the family ledger
 * records that it does not reproduce this class of defect, and it was re-derived the hard way on
 * 2026-08-31.
 *
 * <h2>Calibration status</h2>
 * <b>2026-09-01</b>, both arms run locally on one shared TestContainers broker, five repetitions
 * each, {@code -Dpc.log.level=info}. Read this before running, so a result already established is
 * not re-derived:
 * <ul>
 *   <li><b>control arm</b> ({@code -Dprobe857tx.dwellMs=2000}) - <b>5/5 pass</b>. Fifteen revokes
 *       fired: ten on {@code pc-broker-poll} (the rebalance path, two per repetition) and five on
 *       {@code pc-control} (the close path). Measured waits quantised to the dwell: 2001ms, 4110ms,
 *       5416ms. The instrument can go green, and it goes green for the expected reason.</li>
 *   <li><b>defect arm</b> (default {@value #DEFAULT_DWELL_MS}ms) - <b>5/5 fail</b>,
 *       {@code VERDICT=POLL_INTERVAL_BREACHED}, every breaching revoke on {@code pc-broker-poll}.
 *       The callback held the poll thread <b>79,394ms</b> against a 10,000ms
 *       {@code max.poll.interval.ms}.</li>
 * </ul>
 * Two things that measurement establishes beyond "the wait is unbounded":
 * <ul>
 *   <li><b>79s from a 20s dwell.</b> The wait is not bounded by <i>one</i> transaction. The commit
 *       interval is 1s, so the control thread re-acquires the write lock as soon as it releases it,
 *       and the waiter is starved across successive commits - {@code isTransactionCommittingInProgress()}
 *       never reads false long enough for the loop to exit. Any bound placed on a single
 *       transaction's duration would therefore <b>not</b> fix this; the deadline has to be on the
 *       wait itself or on the holder's willingness to keep re-taking the lock.</li>
 *   <li><b>The ambient probe corroborates it independently</b> - the failure carries
 *       {@code ZOMBIE_MEMBER/REBALANCE_BLOCKED: group dwelling in CompletingRebalance for 15s - a
 *       member is not answering the rebalance (protocol-unresponsive)}. That is astubbs#44's
 *       reported symptom, reached from a different instrument than this class's own assertion.</li>
 * </ul>
 * <b>Known confound, not yet controlled:</b> {@code junit.jupiter.execution.parallel} runs the five
 * repetitions concurrently against one broker, so five PC instances contend and {@link #dwellsEntered}
 * is shared across them. It did not change either verdict - the arms separate cleanly - but a
 * per-repetition dwell count is not trustworthy until the class is pinned to one thread.
 */
@Slf4j
class Revoke857TransactionalWaitProbeIT extends BrokerIntegrationTest<String, String> {

    static final long DEFAULT_DWELL_MS = 20_000L;

    /**
     * The bound the revoke wait must respect. Set explicitly rather than left to the client default
     * so the assertion below is comparing against a number this test controls, and so a defect-arm
     * run finishes in test time rather than in the default five minutes.
     */
    static final long MAX_POLL_INTERVAL_MS = 10_000L;

    /**
     * How long the control thread holds the producer write lock per commit. The arm selector - see
     * the class javadoc. Defect arm exceeds {@link #MAX_POLL_INTERVAL_MS}; control arm is under it.
     */
    static final long COMMIT_DWELL_MS = Long.getLong("probe857tx.dwellMs", DEFAULT_DWELL_MS);

    /**
     * A revoke that waited less than this never overlapped a commit, so the run measured nothing.
     * Reported as INCONCLUSIVE rather than allowed to pass - see the first trap in the class javadoc.
     */
    static final long WINDOW_OPENED_FLOOR_MS = 500L;

    /**
     * Per-record processing delay. With instant processing the backlog is committed long before the
     * rebalance, the {@code WorkManager} is clean, and the control thread never enters the commit
     * path during the revoke: no window, nothing measured.
     */
    static final long PROCESSING_DELAY_MS = 25L;

    static final long FIRST_BATCH = 500L;

    static final long SECOND_BATCH = 100L;

    Consumer<String, String> consumer;

    Producer<String, String> producer;

    ParallelEoSStreamProcessor<String, String> pc;

    CountDownLatch firstRevokeCompleted;

    /**
     * Wall time spent inside the real {@code onPartitionsRevoked} - dominated by the wait loop, and
     * also covering the revoke-path commit and truncation that follow it. The outcome variable.
     */
    AtomicLong revokeCallbackTookMs;

    /** Commits that actually took the write lock and dwelled. Proof the instrument was armed. */
    static final AtomicLong dwellsEntered = new AtomicLong();

    {
        super.numPartitions = 2;
    }

    /**
     * Holds the producer write lock open for {@link #COMMIT_DWELL_MS} on every transactional commit,
     * by dwelling once {@code super} has acquired it. Overriding the acquire rather than
     * {@code commitOffsets} keeps the dwell strictly inside the lock-held window without changing
     * anything about the commit itself.
     */
    static class DwellingProducerManager<K, V> extends ProducerManager<K, V> {

        DwellingProducerManager(ProducerWrapper<K, V> producerWrapper,
                                ConsumerManager<K, V> consumerManager,
                                WorkManager<K, V> workManager,
                                ParallelConsumerOptions<K, V> options) {
            super(producerWrapper, consumerManager, workManager, options);
        }

        @Override
        protected void preAcquireOffsetsToCommit() throws java.util.concurrent.TimeoutException, InterruptedException {
            super.preAcquireOffsetsToCommit();
            long entered = dwellsEntered.incrementAndGet();
            log.info("PROBE857TX: commit #{} holds the producer write lock, dwelling {}ms - a revoke landing now " +
                    "spins in onPartitionsRevoked", entered, COMMIT_DWELL_MS);
            ThreadUtils.sleepQuietly(COMMIT_DWELL_MS);
        }
    }

    /**
     * Exists only to hand {@link DwellingProducerManager} to PC in place of the real one. The
     * components are read here rather than inside {@link DwellingProducerManager} because
     * {@code PCModule}'s accessors are protected: they are reachable through {@code this} in a
     * subclass, but not through another instance from a different package.
     */
    static class DwellingModule<K, V> extends PCModule<K, V> {

        private ProducerManager<K, V> dwelling;

        DwellingModule(ParallelConsumerOptions<K, V> options) {
            super(options);
        }

        @Override
        protected ProducerManager<K, V> producerManager() {
            if (dwelling == null) {
                dwelling = new DwellingProducerManager<>(producerWrap(), consumerManager(), workManager(), options());
            }
            return dwelling;
        }
    }

    private static Properties shortPollInterval() {
        Properties props = new Properties();
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, String.valueOf(MAX_POLL_INTERVAL_MS));
        return props;
    }

    @BeforeEach
    void setup() {
        firstRevokeCompleted = new CountDownLatch(1);
        revokeCallbackTookMs = new AtomicLong(-1);
        dwellsEntered.set(0);
        setupTopic();

        log.info("PROBE857TX: dwell arm = {}ms against max.poll.interval.ms = {}ms - expecting {}",
                COMMIT_DWELL_MS, MAX_POLL_INTERVAL_MS,
                COMMIT_DWELL_MS > MAX_POLL_INTERVAL_MS ? "DEFECT ARM (overrun, should fail on current code)"
                        : "CONTROL ARM (no overrun, should pass)");

        producer = getKcu().createNewProducer(KafkaClientUtils.ProducerMode.TRANSACTIONAL);
        // true = mint a fresh group; the joining consumer below REUSES it, which is what makes its
        // arrival a rebalance of this group rather than an unrelated one.
        consumer = getKcu().createNewConsumer(true, shortPollInterval());

        var pcOptions = ParallelConsumerOptions.<String, String>builder()
                .commitMode(PERIODIC_TRANSACTIONAL_PRODUCER)
                .consumer(consumer)
                .producer(producer)
                // must outlast the dwell, or the produce path fails for a reason that is not the
                // defect under test and the run measures the wrong thing
                .produceLockAcquisitionTimeout(Duration.ofMinutes(2))
                .ordering(PARTITION) // no keys needed
                .build();

        pc = new ParallelEoSStreamProcessor<>(pcOptions, new DwellingModule<>(pcOptions)) {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                log.info("PROBE857TX: revoke callback entered on thread {} for {} - about to wait for any " +
                        "in-flight transaction", Thread.currentThread().getName(), partitions);
                long start = System.currentTimeMillis();
                try {
                    super.onPartitionsRevoked(partitions);
                } finally {
                    long took = System.currentTimeMillis() - start;
                    revokeCallbackTookMs.compareAndSet(-1, took);
                    log.info("PROBE857TX: revoke callback returned after {}ms (max.poll.interval.ms is {}ms)",
                            took, MAX_POLL_INTERVAL_MS);
                    firstRevokeCompleted.countDown();
                }
            }
        };

        pc.subscribe(UniSets.of(topic));
    }

    @AfterEach
    void cleanup() {
        pc.close();
    }

    @SneakyThrows
    @RepeatedTest(5)
    void revokeMustNotWaitOnATransactionPastTheMaxPollInterval() {
        var count = new AtomicLong();

        getKcu().produceMessages(topic, FIRST_BATCH);
        pc.setTimeBetweenCommits(ofSeconds(1));
        pc.poll(recordContexts -> {
            ThreadUtils.sleepQuietly(PROCESSING_DELAY_MS);
            count.incrementAndGet();
        });

        await().timeout(ofSeconds(60)).until(() -> count.get() > 5);
        log.info("PROBE857TX: records are flowing, joining a second consumer to trigger the revoke");

        try (var joiner = getKcu().createNewConsumer(false, shortPollInterval())) {
            joiner.subscribe(UniLists.of(topic));
            joiner.poll(ofSeconds(5));

            boolean revokeCompleted = firstRevokeCompleted.await(120, TimeUnit.SECONDS);
            assertWithMessage("PROBE857TX VERDICT=WEDGED: the revoke callback did not return within 120s - "
                    + "pc-broker-poll is stuck in the transaction wait")
                    .that(revokeCompleted).isTrue();
        }

        long took = revokeCallbackTookMs.get();

        // Guard against a vacuous pass: if the revoke never overlapped a commit, nothing was measured
        // and the assertion below would report success on the defect arm.
        assertWithMessage("PROBE857TX VERDICT=INCONCLUSIVE: the revoke returned in %sms having entered %s dwells, "
                + "so it never overlapped an in-flight transaction and this run measured nothing", took, dwellsEntered.get())
                .that(took).isGreaterThan(WINDOW_OPENED_FLOOR_MS);

        // The defect. The callback runs on the poll thread inside poll(), so anything it spends here
        // counts against max.poll.interval.ms; overrunning it evicts the member, which is astubbs#44.
        assertWithMessage("PROBE857TX VERDICT=POLL_INTERVAL_BREACHED: the revoke callback held the poll thread "
                + "for %sms, past the %sms max.poll.interval.ms - the member is evicted mid-rebalance. This is "
                + "astubbs/parallel-consumer#44 (confluentinc#803): the wait on an in-flight transaction has no "
                + "deadline.", took, MAX_POLL_INTERVAL_MS)
                .that(took).isLessThan(MAX_POLL_INTERVAL_MS);

        // Liveness: work produced after the rebalance must still flow.
        getKcu().produceMessages(topic, SECOND_BATCH);
        long total = FIRST_BATCH + SECOND_BATCH;
        await().timeout(ofSeconds(120))
                .until(() -> count.get() >= total || pc.isClosedOrFailed() || pc.getFailureCause() != null);

        assertWithMessage("PROBE857TX VERDICT=CONTROL_THREAD_DIED: PC recorded a failure cause after the forced "
                + "revoke-during-transaction overlap: %s", pc.getFailureCause())
                .that(pc.getFailureCause()).isNull();
        assertWithMessage("PROBE857TX VERDICT=STALLED: only %s of %s records processed - no crash, but "
                + "post-rebalance work did not flow", count.get(), total)
                .that(count.get()).isAtLeast(total);

        log.info("PROBE857TX VERDICT=OK: revoke callback {}ms, {} dwells entered, {} records processed",
                took, dwellsEntered.get(), count.get());
    }
}
