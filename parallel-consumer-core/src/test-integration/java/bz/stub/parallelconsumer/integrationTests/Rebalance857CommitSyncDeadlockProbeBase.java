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
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.BeforeEach;
import pl.tlinkowski.unij.api.UniLists;
import pl.tlinkowski.unij.api.UniSets;

import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static bz.stub.parallelconsumer.ParallelConsumerOptions.CommitMode.PERIODIC_CONSUMER_SYNC;
import static bz.stub.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.PARTITION;
import static java.time.Duration.ofSeconds;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.number.OrderingComparison.greaterThan;

/**
 * MEASUREMENT PROBE for the confluentinc#857 AB-BA deadlock, in the only commit mode where the
 * cycle can close: {@link ParallelConsumerOptions.CommitMode#PERIODIC_CONSUMER_SYNC}.
 * <p>
 * <b>Current role: this GATES.</b> The probe runs in the gating integration suite, split across four
 * one-line subclasses of this base ({@code Rebalance857CommitSyncDeadlockProbeIT} and {@code ..2IT},
 * {@code ..3IT}, {@code ..4IT}) so that failsafe can pack it across forks - as one class it was a
 * ~342s unsplittable floor that set the wall for whichever CI shard held it. The first three are
 * named in {@code HEAVY_CLASSES} in {@code bin/ci-integration-test.sh}; the fourth rides the
 * catch-all. Do not collapse them back into one class without reading that script's sizing preamble,
 * and do not remove one thinking it is redundant - the repetition count is the detection power.
 * <p>
 * HISTORY, and it is dated: until astubbs/parallel-consumer#442 (2026-09) this was NOT a merge
 * candidate. It was the instrument for an A/B soak experiment comparing origin/master (defect)
 * against the astubbs#29 fix branch, and had to stay byte-identical on both arms - which is why it
 * carried {@code @RepeatedTest(20)}. That constraint ended with the experiment. It is recorded here
 * because a maintainer finding this class in the gating suite would otherwise read the old rule as
 * live and either remove a deliberate gate or refuse a legitimate edit; historical byte-identity is
 * no longer required of it. Reps are now {@code 5} per subclass, chosen for gate cost rather than
 * for soak depth.
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
/**
 * <b>Calibration status</b>, 2026-08-31 - read this before running, so a result already established
 * is not re-derived. Run as four cells against a control cut from this branch's HEAD with
 * {@code commitLock.tryLock()} replaced by {@code commitLock.lock()} (blocking rather than
 * declining), twenty repetitions each:
 * <ul>
 *   <li>fix + eager - every repetition passes, 23 revoke-path declines</li>
 *   <li>fix + cooperative - every repetition passes, 20 declines</li>
 *   <li>pre-fix control + eager - every repetition FAILS</li>
 *   <li>pre-fix control + cooperative - every repetition FAILS</li>
 * </ul>
 * So the AB-BA cycle is not eager-specific, and the fix holds on both assignors. The declines are
 * what make a green cell mean anything: they prove the window opened. Run with
 * {@code -Dpc.log.level=info} or the revoke fork's two log lines are filtered out and every cell
 * reports zero declines - which reads exactly like a run in which the race never happened.
 */
abstract class Rebalance857CommitSyncDeadlockProbeBase extends BrokerIntegrationTest<String, String> {

    /*
     * SPLIT INTO FOUR CONCRETE CLASSES, five repetitions each - see the four
     * Rebalance857CommitSyncDeadlockProbe*IT subclasses. The twenty repetitions, every assertion and
     * the 4s dwell are UNCHANGED; only the packaging moved.
     *
     * Why: surefire/failsafe forks pull whole CLASSES from one queue, so a single class is never
     * split across forks. At @RepeatedTest(20) this instrument was 339s of a 420s forked failsafe
     * wall - a hard floor that no fork count could lower, and the largest single term in the
     * integration gate. Four classes of five let the forks spread it.
     *
     * The instrument's calibration is unaffected: each repetition is byte-identical to before, and
     * the four cells the class javadoc records are selected by system property, not by class.
     */

    /**
     * Must comfortably exceed the 1s commit interval so the control thread is guaranteed to be
     * mid-commit (blocked in commitAndWait, holding the commit lock) when the revoke-path commit
     * attempt starts. Well under offsetCommitTimeout (10s) and all broker/rebalance timeouts.
     */
    /**
     * Runs this instrument on the COOPERATIVE assignor instead of the default eager one:
     * {@code -Dprobe857.cooperative=true}. Off by default, so the gating configuration is unchanged.
     * <p>
     * <b>Why this switch exists.</b> astubbs#29's evidence came from this probe on the eager path,
     * while the family's twentieth capture is a COOPERATIVE revoke. That the fix covers both is an
     * inference - it sits on the revoke path, which should be assignor-independent - and inference is
     * not measurement. The seed replay that was supposed to settle it could not: a chaos seed fixes
     * the conductor's schedule, not the poll-versus-control interleaving the AB-BA close races on, so
     * the control arm never reproduced (recorded in
     * {@code docs/inflight/test-857-revoke-under-work-sightings.md}). This instrument does not depend
     * on that luck - it forces the window open with {@link #REVOKE_DWELL_MS} against a one-second
     * commit interval - so pointing it at the cooperative assignor asks the same question with the
     * schedule controlled rather than sampled.
     * <p>
     * Both consumers in the group must agree on the assignor, so the property feeds
     * {@link #assignorProps()} and is applied to the PC consumer and to the joining consumer whose
     * arrival triggers the revoke. The strategy string is the one the chaos suite already uses.
     */
    static final boolean COOPERATIVE = Boolean.getBoolean("probe857.cooperative");

    /**
     * Consumer properties selecting the arm. Empty for eager, which is the default assignor - so the
     * eager arm is byte-for-byte the configuration that produced this probe's original result.
     */
    private static Properties assignorProps() {
        Properties props = new Properties();
        if (COOPERATIVE) {
            props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
                    "org.apache.kafka.clients.consumer.CooperativeStickyAssignor");
        }
        return props;
    }

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
        // true = mint a fresh group; the joining consumer below passes false to REUSE it, which is
        // what makes its arrival a rebalance of this group rather than an unrelated one.
        consumer = getKcu().createNewConsumer(true, assignorProps());
        log.info("PROBE857: assignor arm = {}", COOPERATIVE ? "COOPERATIVE" : "EAGER (default)");
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

    /**
     * Five repetitions of the probe, INHERITED by each concrete subclass - four of them, so twenty
     * repetitions in total, spread over four classes that failsafe's forks schedule independently.
     * <p>
     * The annotation lives here rather than being redeclared in each subclass so the body exists
     * once. JUnit collects inherited non-private test methods, so every subclass runs it; this
     * class is abstract, so it never runs on its own.
     */
    @RepeatedTest(5)
    @SneakyThrows
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

        try (var newConsumer = getKcu().createNewConsumer(false, assignorProps())) {
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
