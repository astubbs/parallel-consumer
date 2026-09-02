/*-
 * Copyright (C) 2020-2023 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */
package bz.stub.parallelconsumer.integrationTests;

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.ParallelEoSStreamProcessor;
import bz.stub.parallelconsumer.integrationTests.utils.DeclineCountingProducerManager;
import bz.stub.parallelconsumer.integrationTests.utils.DeclineCountingProducerManager.DeclineCountingModule;
import bz.stub.parallelconsumer.integrationTests.utils.KafkaClientUtils;
import bz.stub.parallelconsumer.internal.utils.ThreadUtils;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import pl.tlinkowski.unij.api.UniLists;
import pl.tlinkowski.unij.api.UniSets;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static bz.stub.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.PARTITION;
import static bz.stub.parallelconsumer.integrationTests.utils.KafkaClientUtils.GroupOption.REUSE_GROUP;
import static java.time.Duration.ofSeconds;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.number.OrderingComparison.greaterThan;

/**
 * Regression guard for the confluentinc#541 rebalance deadlock, fixed by upstream PR confluentinc#548 (2023).
 * <p>
 * <b>The defect this guards</b> (from the commit record - upstream {@code 2738fb3d5}): in
 * {@link ParallelConsumerOptions.CommitMode#PERIODIC_TRANSACTIONAL_PRODUCER} (EoS), commit initiation lives on
 * {@code pc-control} while the rebalance callback runs on {@code pc-broker-poll}, and the two take the same pair of
 * locks in opposite orders:
 * <ul>
 * <li>{@code pc-control}: acquires the producer transaction <b>write lock (A)</b> early
 * ({@code maybeAcquireCommitLock} → {@code preAcquireOffsetsToCommit}, the record-sending barrier), then the
 * <b>commit mutex (B)</b> inside {@code commitOffsetsThatAreReady()}.</li>
 * <li>{@code pc-broker-poll}, pre-fix: {@code onPartitionsRevoked} entered the commit path directly - <b>B</b> first,
 * then <b>A</b> inside {@code retrieveOffsetsAndCommit} → {@code acquireCommitLock}.</li>
 * </ul>
 * AB-BA: the poll thread wedges inside the rebalance callback (or dies on {@code ProducerManager}'s
 * cross-thread-write-lock {@code ConcurrentModificationException}), the member is eventually kicked from the group,
 * and the offsets of the revoked partitions are never committed. confluentinc#548's fence: the revoke callback first
 * waited out any in-flight transactional commit ({@code while (isTransactionCommittingInProgress()) sleep} - the
 * predicate is "write lock A held"), and {@code isRebalanceInProgress} stops {@code pc-control} starting a new commit
 * cycle mid-rebalance. That wait had no deadline, and on a busy transactional instance it held the poll thread past
 * {@code max.poll.interval.ms} - astubbs#44 (confluentinc#803). astubbs#408 replaced it: <b>the revoke path now
 * declines both locks with a bare {@code tryLock()} and skips its commit when either is held.</b> The revoked
 * partitions' completed-but-uncommitted work is then redelivered to the next owner - the at-least-once contract,
 * and the cost {@code docs/features/rebalance-behaviour.yaml} records. The decision and the alternative not taken
 * (a bounded wait, viable once producer fencing is recoverable - astubbs#225) are in
 * {@code docs/inflight/bug-857-transactional-revoke-wait.md}.
 * <p>
 * <b>What the detector observes - behaviour, not internals.</b> An earlier version of this test proved "the revoke
 * path committed" by overriding {@code commitOffsetsThatAreReady()} and counting a latch when the override ran on
 * {@code pc-broker-poll}. That pinned the test to a method name: when the revoke path moved to the private
 * {@code tryCommitOffsetsOnRevoke()} (the confluentinc#857 fix), the commit still happened but the latch never
 * counted - the test passed 5/5 on the defective build and failed 5/5 on the fixed one, reporting a working fix as a
 * regression (the worked example in
 * {@code docs/solutions/workflow-issues/prove-the-problem-exists-before-writing-the-fix.md}).
 * This version asserts the outcome instead:
 * <ol>
 * <li><b>Deadlock bound</b>: the revoke callback, entered while {@code pc-control} is provably mid-commit-cycle
 * (holding lock A), completes within 30s.</li>
 * <li><b>Not by waiting</b>: the callback returns inside {@value #REVOKE_MUST_RETURN_WITHIN_MS}ms, a fraction of
 * the {@value #CONTROL_COMMIT_DELAY_MS}ms dwell it was timed into. The pre-astubbs#408 spin returned only once
 * the dwell ended, so it fails this by construction; a deadlock fails the first.</li>
 * <li><b>The window was resolved, not skipped</b>: either the revocation declined the held lock - counted by
 * {@link DeclineCountingProducerManager}, the same instrument {@code Revoke857TransactionalWaitProbeIT} uses, and
 * the only thing that distinguishes a fast fixed callback from one that never met a commit - or, if the dwell had
 * already ended by the time the callback got there, it committed inline and the group's committed offsets for the
 * revoked partitions moved. Until astubbs#408 the second was the only accepted outcome, and it held on master
 * <em>because of</em> the unbounded wait.</li>
 * <li><b>The instance survives</b>: no recorded failure cause, and processing continues after the rebalance.</li>
 * </ol>
 * <b>How the overlap is forced deterministically</b> (the same control-arm technique as
 * {@code Rebalance857CommitSyncDeadlockProbeIT}): the {@code commitOffsetsThatAreReady()} override makes each
 * {@code pc-control} commit cycle dwell {@value #CONTROL_COMMIT_DELAY_MS}ms between acquiring lock A and touching
 * lock B, and the revoke callback waits (pre-{@code super}, bounded) for a <i>fresh</i> dwell to begin before
 * proceeding - so the callback body always runs against a control thread that holds A and has not yet taken B, the
 * exact pre-fix fatal window. A slow-flowing backlog keeps the WorkManager dirty so control keeps entering commit
 * cycles; {@code overlapForced} makes the forcing itself assertable, so the test cannot silently go vacuous.
 * <p>
 * <b>Mode is deliberate, and scope is deliberate.</b> confluentinc#541 is a transactional-mode defect - lock A is the
 * producer transaction lock, which exists only in {@code PERIODIC_TRANSACTIONAL_PRODUCER} - so that mode is kept.
 * Consequently this test does NOT and cannot exercise the confluentinc#857 AB-BA cycle: that cycle's second edge
 * lives in {@code ConsumerOffsetCommitter}, which is only constructed for the consumer-commit modes.
 * {@code Rebalance857CommitSyncDeadlockProbeIT} covers confluentinc#857 in {@code PERIODIC_CONSUMER_SYNC}.
 *
 * @author Nacho Munoz
 * @author Antony Stubbs
 */
@Slf4j
class RebalanceEoSDeadlockTest extends BrokerIntegrationTest<String, String> {

    private static final String PC_CONTROL = "pc-control";

    /**
     * How long each pc-control commit cycle dwells between acquiring the produce/transaction write lock (A) and
     * entering the locked commit section (B). Widens the pre-fix fatal window so the revoke callback can be timed
     * into it deterministically. Must comfortably exceed the revoke callback's own work (baseline offset read +
     * commit attempt, well under 1s) so that on a defective build no control-thread commit can complete - and move
     * the committed offsets - between the baseline read and the callback returning.
     */
    static final long CONTROL_COMMIT_DELAY_MS = 4_000L;

    /**
     * Bound on waiting for pc-control to begin a fresh dwell. Control enters a commit cycle roughly every
     * {@code timeBetweenCommits(1s) + CONTROL_COMMIT_DELAY_MS + commit}, so ~6s; 20s is generous, and a miss is
     * asserted on ({@link #overlapForced}), never silently ignored.
     */
    static final long OVERLAP_WAIT_BOUND_MS = 20_000L;

    /**
     * How long the real {@code onPartitionsRevoked} may take once entered against a dwelling pc-control. A declining
     * callback measured 4-11ms on a shared broker ({@code Revoke857TransactionalWaitProbeIT}); the deleted spin
     * returned only when the {@value #CONTROL_COMMIT_DELAY_MS}ms dwell ended. A quarter of the dwell leaves two
     * orders of magnitude of headroom for the fix and still fails the wait even when the baseline offset read has
     * eaten most of the dwell before the callback enters.
     */
    static final long REVOKE_MUST_RETURN_WITHIN_MS = CONTROL_COMMIT_DELAY_MS / 4;

    /**
     * Enough backlog that the WorkManager stays dirty - so pc-control keeps entering commit cycles - through
     * setup, the forced overlap and the post-rebalance liveness check. ~{@value #PROCESSING_DELAY_MS}ms per record
     * across 2 partitions ≈ 80 records/s ≈ 50s of work.
     */
    static final long RECORDS_TO_PRODUCE = 4_000L;

    /** Per-record processing delay pre-revoke: keeps completions (uncommitted, committable work) flowing. */
    static final long PROCESSING_DELAY_MS = 25L;

    /**
     * Per-record processing delay after the first revocation. Slowing redelivery keeps the committed-offset
     * assertion honest: on a defective build the revoked partitions' records are redelivered and reprocessed, which
     * would eventually advance the committed offsets anyway - at this rate not within the assertion's read, which
     * happens well under a second after the callback returns.
     */
    static final long POST_REVOKE_PROCESSING_DELAY_MS = 500L;

    Consumer<String, String> consumer;
    Producer<String, String> producer;

    ParallelEoSStreamProcessor<String, String> pc;

    /** Hands PC a producer manager that counts declined revocations - the instrument behind assertion 3. */
    DeclineCountingModule<String, String> module;

    /** Counts down when the first (forced-overlap) revocation's callback returns - the hard-deadlock detector. */
    CountDownLatch firstRevokeCompleted;

    /** Wall time the real {@code onPartitionsRevoked} took on the forced-overlap revocation; -1 until it returns. */
    volatile long revokeCallbackTookMs = -1;

    /** Incremented each time pc-control enters the artificial dwell (holding lock A, before lock B). */
    final AtomicLong controlDwellEpoch = new AtomicLong();

    /** Whether the revoke callback confirmed pc-control was mid-commit-cycle before proceeding - the vacuity guard. */
    final AtomicBoolean overlapForced = new AtomicBoolean(false);

    final AtomicBoolean firstRevokeSeen = new AtomicBoolean(false);

    /** Committed offsets for the input topic, read at revoke entry with the forced overlap in place. */
    volatile Map<TopicPartition, Long> committedAtRevokeEntry;

    /** The input-topic partitions the first revocation revoked. */
    volatile Collection<TopicPartition> revokedPartitions;

    volatile boolean slowProcessingAfterRevoke = false;

    {
        super.numPartitions = 2;
    }

    private String outputTopic;

    @BeforeEach
    void setup() {
        firstRevokeCompleted = new CountDownLatch(1);
        setupTopic();
        // a genuinely separate output topic. The pre-repair version called setupTopic("output-topic") here, which
        // OVERWRITES the inherited `topic` field - so PC subscribed to, consumed from, and produced back into the
        // "output" topic in a feedback loop, and the test's input/output split existed in name only.
        outputTopic = "output-" + topic;
        ensureTopic(outputTopic, numPartitions);
        producer = getKcu().createNewProducer(KafkaClientUtils.ProducerMode.TRANSACTIONAL);
        consumer = getKcu().createNewConsumer(KafkaClientUtils.GroupOption.NEW_GROUP);
        var pcOptions = ParallelConsumerOptions.<String, String>builder()
                .commitMode(ParallelConsumerOptions.CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER)
                .consumer(consumer)
                .produceLockAcquisitionTimeout(Duration.ofMinutes(2))
                .producer(producer)
                .ordering(PARTITION) // just so we dont need to use keys
                .build();

        module = new DeclineCountingModule<>(pcOptions);
        pc = new ParallelEoSStreamProcessor<>(pcOptions, module) {

            @Override
            protected void commitOffsetsThatAreReady() throws TimeoutException, InterruptedException {
                final var isControlThread = Thread.currentThread().getName().contains(PC_CONTROL);
                if (isControlThread) {
                    // at this point in a transactional commit cycle, pc-control already holds the
                    // produce/transaction write lock (A) via maybeAcquireCommitLock, and has not yet taken the
                    // commit mutex (B) - the exact window the pre-confluentinc#548 revoke path deadlocked in
                    controlDwellEpoch.incrementAndGet();
                    log.info("Dwelling pc-control {}ms between produce-lock acquisition and the locked commit " +
                            "section, to hold open the confluentinc#548 window", CONTROL_COMMIT_DELAY_MS);
                    ThreadUtils.sleepQuietly(CONTROL_COMMIT_DELAY_MS);
                }
                super.commitOffsetsThatAreReady();
            }

            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                boolean firstRealRevoke = !partitions.isEmpty() && firstRevokeSeen.compareAndSet(false, true);
                if (!firstRealRevoke) {
                    super.onPartitionsRevoked(partitions);
                    return;
                }
                slowProcessingAfterRevoke = true;
                revokedPartitions = partitions.stream().filter(tp -> tp.topic().equals(topic))
                        .collect(java.util.stream.Collectors.toList());

                // force the confluentinc#548 overlap: wait for pc-control to begin a FRESH dwell, so the whole
                // callback below runs while control provably holds lock A and has ~CONTROL_COMMIT_DELAY_MS before
                // it touches lock B. A fresh dwell (epoch increase), not just "currently dwelling", so the
                // baseline read + super() below fit inside the dwell with seconds to spare.
                long epochAtEntry = controlDwellEpoch.get();
                long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(OVERLAP_WAIT_BOUND_MS);
                while (System.nanoTime() < deadlineNanos) {
                    if (controlDwellEpoch.get() > epochAtEntry) {
                        overlapForced.set(true);
                        break;
                    }
                    ThreadUtils.sleepQuietly(10);
                }
                log.info("Revocation of {} entered on {}; overlap with a mid-commit pc-control forced: {}",
                        partitions, Thread.currentThread().getName(), overlapForced.get());

                committedAtRevokeEntry = readCommittedOffsetsForInputTopic();

                long start = System.currentTimeMillis();
                try {
                    super.onPartitionsRevoked(partitions);
                } finally {
                    revokeCallbackTookMs = System.currentTimeMillis() - start;
                    log.info("Revocation callback returned after {}ms", revokeCallbackTookMs);
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

    /**
     * Committed offsets for the input topic's partitions, from the group coordinator - the observable outcome the
     * revoke-time commit exists to produce. Null (asserted on later) rather than an exception if the read fails,
     * so an admin hiccup cannot crash the rebalance callback this is called from.
     */
    private Map<TopicPartition, Long> readCommittedOffsetsForInputTopic() {
        try {
            var committed = getKcu().getAdmin()
                    .listConsumerGroupOffsets(getKcu().getGroupId())
                    .partitionsToOffsetAndMetadata()
                    .get(10, TimeUnit.SECONDS);
            Map<TopicPartition, Long> result = new ConcurrentHashMap<>();
            committed.forEach((tp, oam) -> {
                if (tp.topic().equals(topic) && oam != null) {
                    result.put(tp, oam.offset());
                }
            });
            return result;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Interrupted reading committed offsets", e);
            return null;
        } catch (Exception e) {
            log.error("Failed to read committed offsets", e);
            return null;
        }
    }

    @SneakyThrows
    @RepeatedTest(5)
    void noDeadlockOnRevoke() {
        var count = new AtomicLong();

        getKcu().produceMessages(topic, RECORDS_TO_PRODUCE);
        pc.setTimeBetweenCommits(ofSeconds(1));
        // consume some records
        pc.pollAndProduce((recordContexts) -> {
            ThreadUtils.sleepQuietly(slowProcessingAfterRevoke ? POST_REVOKE_PROCESSING_DELAY_MS : PROCESSING_DELAY_MS);
            count.getAndIncrement();
            log.debug("Processed record, count now {} - offset: {}", count, recordContexts.offset());
            return new ProducerRecord<>(outputTopic, recordContexts.key(), recordContexts.value());
        });

        await().timeout(Duration.ofSeconds(30)).untilAtomic(count, is(greaterThan(5L)));
        log.debug("Records are getting consumed");

        // cause rebalance
        log.debug("Creating new consumer in same group and subscribing to same topic set - the revocation this " +
                "triggers is timed (in the onPartitionsRevoked override) into pc-control's commit cycle");
        try (var newConsumer = getKcu().createNewConsumer(REUSE_GROUP)) {
            newConsumer.subscribe(UniLists.of(topic));

            // keep the new consumer polling while we wait: the rebalance protocol advances inside poll(), and the
            // forced overlap holds our member's revocation callback open for several seconds
            long revokeDeadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            while (firstRevokeCompleted.getCount() > 0 && System.nanoTime() < revokeDeadlineNanos) {
                newConsumer.poll(Duration.ofMillis(500));
            }
            if (firstRevokeCompleted.getCount() > 0) {
                Assertions.fail("confluentinc#548 regression: onPartitionsRevoked did not complete within 30s - " +
                        "pc-broker-poll is wedged in the revocation callback while pc-control is mid-commit");
            }

            // read the outcome IMMEDIATELY: on a defective build, redelivered records would eventually re-advance
            // these offsets - POST_REVOKE_PROCESSING_DELAY_MS makes that take minutes, this read takes milliseconds
            var committedAtRevokeReturn = readCommittedOffsetsForInputTopic();

            Assertions.assertTrue(overlapForced.get(),
                    "Vacuous run: the revocation was never timed into a pc-control commit cycle, so the " +
                            "confluentinc#548 window was not exercised - check that the backlog kept the " +
                            "WorkManager dirty (control only enters commit cycles when there is work to commit)");
            var baseline = committedAtRevokeEntry;
            Assertions.assertNotNull(baseline, "Baseline committed-offset read at revoke entry failed");
            Assertions.assertNotNull(committedAtRevokeReturn, "Committed-offset read at revoke return failed");
            Assertions.assertFalse(revokedPartitions.isEmpty(), "Revocation carried no input-topic partitions");

            // assertion 2: returned, and not by waiting the dwell out
            long took = revokeCallbackTookMs;
            Assertions.assertTrue(took >= 0 && took < REVOKE_MUST_RETURN_WITHIN_MS,
                    "confluentinc#803 regression: the revocation callback took " + took + "ms against a bound of " +
                            REVOKE_MUST_RETURN_WITHIN_MS + "ms, timed into a " + CONTROL_COMMIT_DELAY_MS + "ms " +
                            "pc-control commit dwell. It waited the in-flight commit out - the unbounded spin " +
                            "astubbs#408 deleted - instead of declining. That wait runs inside poll() and is charged " +
                            "against max.poll.interval.ms");

            // assertion 3: the window was resolved, not skipped - declined (the fix path, counted), or committed
            // inline because the dwell had already ended when the callback reached the lock
            boolean anyRevokedPartitionAdvanced = false;
            for (var tp : revokedPartitions) {
                long before = baseline.getOrDefault(tp, -1L);
                long after = committedAtRevokeReturn.getOrDefault(tp, -1L);
                log.info("Committed offset for {}: {} at revoke entry -> {} at revoke return", tp, before, after);
                anyRevokedPartitionAdvanced |= after > before;
            }
            long declines = module.manager().revocationDeclines();
            log.info("Forced-overlap revocation: {} decline(s), committed offsets advanced inside the callback: {}",
                    declines, anyRevokedPartitionAdvanced);
            Assertions.assertTrue(declines > 0 || anyRevokedPartitionAdvanced,
                    "Vacuous run: the revocation neither declined the producer transaction lock (" + declines +
                            " declines counted) nor committed inline (no revoked partition's committed offset " +
                            "moved between revoke entry and return), so it never met the in-flight pc-control " +
                            "commit the forced overlap exists to put in its way - the window was not exercised");

            Assertions.assertNull(pc.getFailureCause(),
                    "PC recorded a failure after the forced revoke-during-commit overlap: " + pc.getFailureCause());
            Assertions.assertFalse(pc.isClosedOrFailed(),
                    "PC closed or failed after the forced revoke-during-commit overlap");

            // liveness: the control thread must still be committing and distributing work after the rebalance
            long countAtRevoke = count.get();
            await().timeout(Duration.ofSeconds(30)).untilAtomic(count, is(greaterThan(countAtRevoke)));
            log.debug("Test finished - processing continued after the rebalance");
        }
    }
}
