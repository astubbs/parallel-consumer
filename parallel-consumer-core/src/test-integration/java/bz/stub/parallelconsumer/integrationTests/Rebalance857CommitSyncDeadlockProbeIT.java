package bz.stub.parallelconsumer.integrationTests;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.ParallelEoSStreamProcessor;
import bz.stub.parallelconsumer.internal.PCModule;
import bz.stub.parallelconsumer.internal.utils.ThreadUtils;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import pl.tlinkowski.unij.api.UniLists;
import pl.tlinkowski.unij.api.UniSets;

import java.time.Duration;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static bz.stub.parallelconsumer.ParallelConsumerOptions.CommitMode.PERIODIC_CONSUMER_SYNC;
import static bz.stub.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.PARTITION;
import static bz.stub.parallelconsumer.integrationTests.utils.KafkaClientUtils.GroupOption.NEW_GROUP;
import static bz.stub.parallelconsumer.integrationTests.utils.KafkaClientUtils.GroupOption.REUSE_GROUP;
import static java.time.Duration.ofSeconds;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.number.OrderingComparison.greaterThan;

/**
 * MEASUREMENT PROBE for the confluentinc#857 AB-BA deadlock, in the only commit mode where the
 * cycle can close: {@link ParallelConsumerOptions.CommitMode#PERIODIC_CONSUMER_SYNC}.
 * <p>
 * NOT a candidate for merging as-is - this is the instrument for an A/B soak experiment comparing
 * origin/master (defect) against the astubbs#29 fix branch. It must be byte-identical on both arms.
 * <p>
 * <b>Mechanism being probed.</b> In PERIODIC_CONSUMER_SYNC:
 * <ul>
 * <li>Edge 1: the control thread's periodic commit takes the commit lock
 * ({@code synchronized(commitCommand)} on master, {@code commitLock} on the fix branch) and then
 * blocks in {@code ConsumerOffsetCommitter.commitAndWait()} waiting for a {@code CommitResponse}
 * that ONLY the pc-broker-poll thread can produce (via {@code maybeDoCommit()} between polls).</li>
 * <li>Edge 2: a rebalance fires {@code onPartitionsRevoked} ON the pc-broker-poll thread, inside
 * {@code poll()}. On master that callback calls {@code commitOffsetsThatAreReady()}, which blocks
 * on the commit lock held by edge 1. The poll thread can now never answer the control thread's
 * commit request: AB-BA. Bounded only by {@code offsetCommitTimeout} (10s), after which the
 * control thread dies with "Timeout waiting for commit response" and the PC instance fails.</li>
 * </ul>
 * <b>How the window is opened deterministically</b> (the control-arm method of
 * docs/investigating.md - inject a delay that opens the window, hold everything else identical):
 * the revoke callback dwells {@link #REVOKE_DWELL_MS} (4s) BEFORE attempting the revoke-path
 * commit. The control thread commits every 1s, so during the dwell it is guaranteed to have sent
 * a commit request and be blocked holding the lock - the poll thread (us, in this callback) is the
 * only thread that could answer it. The dwell is identical bytes on both arms; on the fixed arm
 * the revoke path uses tryLock and skips, so the same forced overlap is claimed to be harmless.
 * <p>
 * <b>Outcome variables (per iteration)</b> - effect-based, reachable on BOTH arms, unlike the
 * original {@code RebalanceEoSDeadlockTest} latch which the fix branch's private
 * {@code tryCommitOffsetsOnRevoke()} bypasses:
 * <ol>
 * <li>the revoke callback completes (hard-deadlock detector),</li>
 * <li>the PC control thread survives: {@code getFailureCause() == null} and
 * {@code !isClosedOrFailed()},</li>
 * <li>liveness: records produced AFTER the rebalance are processed (count reaches TOTAL).</li>
 * </ol>
 * Expected signature on the defect arm: control thread crash ~10s after the revoke, cause chain
 * containing "Timeout waiting for commit response". Expected on the fixed arm: INFO log
 * "Skipping offset commit during partition revocation" and all assertions pass.
 */
@Slf4j
class Rebalance857CommitSyncDeadlockProbeIT extends BrokerIntegrationTest<String, String> {

    /**
     * Must comfortably exceed the 1s commit interval so the control thread is guaranteed to be
     * mid-commit (blocked in commitAndWait, holding the commit lock) when the revoke-path commit
     * attempt starts. Well under offsetCommitTimeout (10s) and all broker/rebalance timeouts.
     */
    static final long REVOKE_DWELL_MS = 4_000L;

    static final long FIRST_BATCH = 500L;
    static final long SECOND_BATCH = 100L;

    /**
     * Per-record processing delay. The control loop's commit gate is
     * {@code isTimeToCommitNow() && wm.isDirty() && !isRebalanceInProgress} - probe v1 proved
     * (10/10 green on the DEFECT arm, revoke commit attempts 0-3ms) that with instant processing
     * everything is committed long before the rebalance, the WorkManager is clean, and the control
     * thread never enters the commit path during the dwell: no window, nothing measured. Slow
     * processing plus a 500-record backlog keeps completions flowing into the mailbox throughout
     * the dwell, so the WorkManager stays dirty and the 1s-interval commit is guaranteed to fire
     * - and block - while we hold the poll thread in the revoke callback.
     */
    static final long PROCESSING_DELAY_MS = 25L;

    Consumer<String, String> consumer;

    ParallelEoSStreamProcessor<String, String> pc;

    CountDownLatch firstRevokeCompleted;

    AtomicLong revokeCommitAttemptTookMs;

    {
        super.numPartitions = 2;
    }

    @BeforeEach
    void setup() {
        firstRevokeCompleted = new CountDownLatch(1);
        revokeCommitAttemptTookMs = new AtomicLong(-1);
        setupTopic();
        consumer = getKcu().createNewConsumer(NEW_GROUP);
        var pcOptions = ParallelConsumerOptions.<String, String>builder()
                .commitMode(PERIODIC_CONSUMER_SYNC)
                .consumer(consumer)
                .offsetCommitTimeout(ofSeconds(10)) // the bound on the deadlock - explicit, not default-dependent
                .ordering(PARTITION) // no keys needed
                .build();

        pc = new ParallelEoSStreamProcessor<>(pcOptions, new PCModule<>(pcOptions)) {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                log.info("PROBE857: revoke callback entered on thread {} for {}, dwelling {}ms so the control " +
                                "thread is mid-commit (holding the commit lock, blocked on a response only this " +
                                "thread can send)",
                        Thread.currentThread().getName(), partitions, REVOKE_DWELL_MS);
                ThreadUtils.sleepQuietly(REVOKE_DWELL_MS);
                long start = System.currentTimeMillis();
                try {
                    super.onPartitionsRevoked(partitions);
                } finally {
                    long took = System.currentTimeMillis() - start;
                    revokeCommitAttemptTookMs.compareAndSet(-1, took);
                    log.info("PROBE857: revoke-path commit attempt + truncation took {}ms", took);
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
    @RepeatedTest(20)
    void revokeWhileControlThreadMidCommitMustNotDeadlockOrKillTheConsumer() {
        var count = new AtomicLong();

        getKcu().produceMessages(topic, FIRST_BATCH);
        pc.setTimeBetweenCommits(ofSeconds(1));
        pc.poll(recordContexts -> {
            ThreadUtils.sleepQuietly(PROCESSING_DELAY_MS);
            long now = count.incrementAndGet();
            log.debug("Processed record, count now {} - offset: {}", now, recordContexts.offset());
        });

        await().timeout(ofSeconds(30)).untilAtomic(count, is(greaterThan(5L)));
        log.info("PROBE857: records are being consumed, triggering rebalance by joining a second consumer");

        try (var newConsumer = getKcu().createNewConsumer(REUSE_GROUP)) {
            newConsumer.subscribe(UniLists.of(topic));
            newConsumer.poll(ofSeconds(5));

            boolean revokeCompleted = firstRevokeCompleted.await(60, TimeUnit.SECONDS);
            Assertions.assertTrue(revokeCompleted,
                    "PROBE857 VERDICT=HARD_DEADLOCK: revoke callback did not complete within 60s - " +
                            "pc-broker-poll is wedged in onPartitionsRevoked");
            log.info("PROBE857: revoke callback completed, commit attempt took {}ms", revokeCommitAttemptTookMs.get());
        }
        // second consumer has left; PC re-acquires all partitions

        // liveness check: work produced AFTER the rebalance must flow. On the defect arm the
        // control thread died ~10s after the revoke started, so count freezes.
        getKcu().produceMessages(topic, SECOND_BATCH);
        long total = FIRST_BATCH + SECOND_BATCH;
        await().timeout(ofSeconds(90))
                .until(() -> count.get() >= total || pc.isClosedOrFailed() || pc.getFailureCause() != null);

        var failureCause = pc.getFailureCause();
        if (failureCause != null) {
            log.error("PROBE857 VERDICT=CONTROL_THREAD_DIED: {}", failureCause.toString());
        }
        Assertions.assertNull(failureCause,
                "PROBE857 VERDICT=CONTROL_THREAD_DIED: PC recorded a failure cause after the forced " +
                        "revoke-during-commit overlap: " + failureCause);
        Assertions.assertFalse(pc.isClosedOrFailed(),
                "PROBE857 VERDICT=PC_CLOSED: PC closed or failed after the forced revoke-during-commit overlap");
        Assertions.assertTrue(count.get() >= total,
                "PROBE857 VERDICT=STALLED: only " + count.get() + "/" + total + " records processed - " +
                        "no crash recorded, but post-rebalance work did not flow");
        log.info("PROBE857 VERDICT=OK: revoke commit attempt {}ms, {} records processed, PC healthy",
                revokeCommitAttemptTookMs.get(), count.get());
    }
}
