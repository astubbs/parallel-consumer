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
 * getting commit lock while second instance starts"</i>), which carries upstream's <i>verified bug</i>
 * label - one of a couple of dozen that do, not the only one.
 * <p>
 * Built as an instrument to make the overrun observable before any bound was designed - on the defect
 * arm, against the unfixed code, it was <b>expected to fail</b>. The fix has since landed on this
 * branch, so it is now the <b>regression test</b> for it: both arms are expected to pass, and the
 * defect arm going red again means the callback has started waiting on the transaction lock once more.
 * <p>
 * <b>A green run is only meaningful alongside a nonzero {@link DwellingProducerManager#revocationDeclines()}</b> - see that
 * field. The fixed callback returns in milliseconds because it declined, which is indistinguishable
 * from a run where no commit was ever in flight unless the decline is counted.
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
 *       {@link #MAX_POLL_INTERVAL_MS}, so a revoke that <i>waits</i> breaches the poll interval and the
 *       member is evicted. This arm fails on the unfixed code and passes on the fixed code; it is the
 *       one that carries the regression signal.</li>
 *   <li><b>control arm</b> ({@code -Dprobe857tx.dwellMs=2000}) - the dwell is well under the poll
 *       interval, so even a waiting revoke stays inside budget. It proves the instrument can go green
 *       at all, and it passes on both the fixed and the unfixed code - so on its own it says nothing
 *       about whether the defect is present.</li>
 * </ul>
 *
 * <h2>Two traps this probe is built to avoid</h2>
 * Both voided runs of the sibling instrument, and both are cheap to avoid:
 * <ul>
 *   <li><b>Confirm the arm actually engaged.</b> The resolved dwell is logged, and
 *       {@link DwellingProducerManager#dwellsEntered()} counts the commits that actually held the lock. A
 *       revoke that never overlapped a commit returns in ~0ms and would pass every assertion below
 *       <i>vacuously</i> - a false green that reads exactly like a fix. The window-opened gate turns that
 *       into an explicit INCONCLUSIVE failure, and it accepts <b>either</b> a long wait (unfixed code) or a
 *       nonzero {@link DwellingProducerManager#revocationDeclines()} (fixed code), because after the fix
 *       "it was fast" is no longer evidence of anything on its own.</li>
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
 * Both arms, five repetitions each, one shared TestContainers broker, {@code -Dpc.log.level=info}. Read this
 * before running, so a result already established is not re-derived.
 *
 * <p><b>2026-09-01, BEFORE the fix</b> - the measurement the fix was designed against:
 * <ul>
 *   <li><b>defect arm</b> (default {@value #DEFAULT_DWELL_MS}ms) - <b>5/5 fail</b>,
 *       {@code VERDICT=POLL_INTERVAL_BREACHED}, every breaching revoke on {@code pc-broker-poll}. The callback
 *       held the poll thread <b>79,394ms</b> against a 10,000ms {@code max.poll.interval.ms}.</li>
 *   <li><b>control arm</b> ({@code -Dprobe857tx.dwellMs=2000}) - <b>5/5 pass</b>, waits quantised to the dwell
 *       at 2001/4110/5416ms. Proof the instrument could go green at all; it said nothing about the code.</li>
 * </ul>
 *
 * <p><b>2026-09-01, AFTER the fix</b> (the revoke path declines the commit lock instead of waiting):
 * <ul>
 *   <li><b>defect arm</b> - <b>5/5 pass</b>. Revoke callback <b>6ms</b>, down from 79,394ms; 3 dwells entered,
 *       <b>2 revocations declined</b>, 605 records processed. The decline count is the part that matters: it
 *       proves the window opened and the fix path ran, rather than the run being fast because nothing
 *       happened.</li>
 *   <li><b>control arm</b> - <b>5/5 pass</b>, callback 11ms, 3 dwells, 1 decline, 600 records. Note this arm
 *       changed meaning: it used to wait out the short dwell, and now declines it like any other.</li>
 * </ul>
 *
 * <p>What the pre-fix measurement established beyond "the wait is unbounded", and what ruled out the
 * deadline-the-holder design: <b>79s came out of a 20s dwell.</b> The commit interval is 1s, so the control
 * thread re-acquires the write lock as soon as it releases it and the waiter is starved across <i>successive</i>
 * commits - {@code isTransactionCommittingInProgress()} never reads false long enough for the loop to exit. Any
 * bound on a single transaction's duration would therefore <b>not</b> have fixed this. The ambient probe
 * corroborated it independently, with
 * {@code ZOMBIE_MEMBER/REBALANCE_BLOCKED: group dwelling in CompletingRebalance for 15s}, which is astubbs#44's
 * reported symptom reached from a different instrument.
 *
 * <p><b>Two traps met while re-running this, both self-inflicted and both cheap to avoid.</b> First: with the
 * dwell left armed for the whole run it fired 45 times, holding the producer write lock 20s out of every commit,
 * and post-rebalance work could not drain inside the liveness window - PC was demonstrably healthy throughout
 * (no failure cause, not closed), so that was the instrument throttling the product. {@code disarmDwell()}
 * exists for that. Second: running any other Maven command against this worktree while a failsafe JVM is live
 * rewrites {@code target/test-classes} underneath it, which surfaces as a bogus
 * {@code NoClassDefFoundError: ChaosSeed$Holder} in teardown - a build collision, not a defect.
 *
 * <b>Shared-broker contention is the wanted condition, not a confound - do not "fix" it by pinning
 * this class to one thread.</b> An earlier version of this paragraph called
 * {@code junit.jupiter.execution.parallel} an uncontrolled confound and proposed serialising the
 * repetitions. That is backwards, and
 * the confluentinc#857 revoke-path cluster decomposition plan establishes why on
 * the sibling defect: forking one broker per fork <b>removes the window</b>, which is how the
 * integration suite went green for months while the deadlock sat untouched. Contention is what opens
 * the window this class exists to measure.
 * <p>
 * The bookkeeping used to be shared - both counters were {@code static}, so a figure read off them belonged
 * to no particular repetition, and one repetition declining would have satisfied the window-opened gate for
 * all five. They are now fields of {@link DwellingProducerManager}, one per PC instance, so each repetition
 * asserts on its own evidence.
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

    /** The instrument, held so its per-instance counters can be asserted on. */
    DwellingModule<String, String> dwellingModule;

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

        /**
         * Turned off once the revoke under test has returned. The dwell exists to hold the window open across
         * ONE rebalance; leaving it on afterwards holds the producer write lock 20s out of every commit for the
         * rest of the run, which starves the produce path and makes the liveness check below measure the
         * instrument rather than the product. Measured: with it left on, 45 dwells fired and post-rebalance work
         * could not drain inside 120s while PC stayed demonstrably healthy - no failure cause, not closed.
         */
        private volatile boolean dwellArmed = true;

        /** Commits that took the write lock and dwelled. Per instance - see the class javadoc. */
        private final AtomicLong dwellsEntered = new AtomicLong();

        /**
         * Revocations that found the lock held and <b>declined</b> - the fix path executing.
         * <p>
         * This is what makes a green run mean anything, and it is the lesson
         * the confluentinc#857 revoke-path cluster decomposition plan paid for on the
         * sibling defect: <i>"A clean fixed arm with a zero skip-count would be indistinguishable from a probe
         * that never opened the window, which is exactly how this fix looked unproven for four months."</i>
         * <p>
         * Before the fix the outcome variable carried that proof itself - a revoke that waited 79s had
         * self-evidently overlapped a commit. After the fix the callback returns in milliseconds precisely
         * <em>because</em> it declined, so "it was fast" no longer distinguishes a working fix from a window
         * that never opened. The count does.
         */
        private final AtomicLong revocationDeclines = new AtomicLong();

        void disarmDwell() {
            dwellArmed = false;
        }

        long dwellsEntered() {
            return dwellsEntered.get();
        }

        long revocationDeclines() {
            return revocationDeclines.get();
        }

        @Override
        protected void preAcquireOffsetsToCommit() throws java.util.concurrent.TimeoutException, InterruptedException {
            super.preAcquireOffsetsToCommit();
            if (!dwellArmed) {
                return;
            }
            long entered = dwellsEntered.incrementAndGet();
            log.info("PROBE857TX: commit #{} holds the producer write lock, dwelling {}ms - a revoke landing now " +
                    "spins in onPartitionsRevoked", entered, COMMIT_DWELL_MS);
            ThreadUtils.sleepQuietly(COMMIT_DWELL_MS);
        }

        @Override
        public boolean tryAcquireCommitLockForRevocation() {
            boolean acquired = super.tryAcquireCommitLockForRevocation();
            if (!acquired) {
                long declines = revocationDeclines.incrementAndGet();
                log.info("PROBE857TX: revocation #{} DECLINED the commit lock - the fix path executed", declines);
            }
            return acquired;
        }
    }

    /**
     * Exists only to hand {@link DwellingProducerManager} to PC in place of the real one. The
     * components are read here rather than inside {@link DwellingProducerManager} because
     * {@code PCModule}'s accessors are protected: they are reachable through {@code this} in a
     * subclass, but not through another instance from a different package.
     */
    static class DwellingModule<K, V> extends PCModule<K, V> {

        private DwellingProducerManager<K, V> dwelling;

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

        /** Null until PC first asks for the producer manager, which it does during construction. */
        DwellingProducerManager<K, V> dwellingManager() {
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

        dwellingModule = new DwellingModule<>(pcOptions);
        pc = new ParallelEoSStreamProcessor<>(pcOptions, dwellingModule) {
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
                    // The window this instrument exists to force is now closed. Leaving the dwell armed would
                    // hold the producer write lock for COMMIT_DWELL_MS out of every subsequent commit, starving
                    // the produce path for the rest of the run - which makes the liveness check below a
                    // measurement of the instrument rather than of PC.
                    dwellingModule.dwellingManager().disarmDwell();
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
        //
        // TWO WAYS THE WINDOW CAN BE SHOWN TO HAVE OPENED, and which one applies tells you which code you are
        // running. Before the fix, the revoke WAITED, so a long callback was itself the proof. After the fix it
        // DECLINES, so the callback is fast precisely because the window opened - and "it was fast" would
        // otherwise be indistinguishable from a run where no commit was ever in flight. Requiring either keeps
        // one assertion honest against both arms; requiring only the first would fail every fixed run, and
        // "relax the guard until it passes" is how a fix gets declared against an instrument that stopped
        // looking.
        var instrument = dwellingModule.dwellingManager();
        boolean windowOpened = instrument.revocationDeclines() > 0 || took > WINDOW_OPENED_FLOOR_MS;
        assertWithMessage("PROBE857TX VERDICT=INCONCLUSIVE: the revoke returned in %sms having entered %s dwells "
                + "and declined %s times, so it never overlapped an in-flight transaction and this run measured "
                + "nothing", took, instrument.dwellsEntered(), instrument.revocationDeclines())
                .that(windowOpened).isTrue();

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

        log.info("PROBE857TX VERDICT=OK: revoke callback {}ms, {} dwells entered, {} revocations declined, "
                + "{} records processed", took, instrument.dwellsEntered(), instrument.revocationDeclines(),
                count.get());
    }
}
