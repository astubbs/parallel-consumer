package bz.stub.parallelconsumer;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */


import bz.stub.parallelconsumer.internal.ConsumerOffsetCommitter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.RebalanceInProgressException;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static bz.stub.parallelconsumer.internal.utils.ThreadUtils.sleepOrFail;
import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.parallel.ResourceAccessMode.READ_WRITE;
import static pl.tlinkowski.unij.api.UniLists.of;

/**
 * "{@code InternalRuntimeException: Timeout waiting for commit response}" as users actually meet it -
 * astubbs#177, confluentinc#833: a consumer that runs for a while under a high failure rate across many keys and
 * then exits with that message.
 * <p>
 * The message is a <b>symptom</b>, and this class pins both halves of that:
 * <ol>
 *     <li>the trigger must not happen - a commit rejected mid-rebalance is deferred, so the
 *         broker-poll thread survives and no waiter ever times out
 *         ({@link #aRebalanceStormUnderAHighFailureRateNeitherStallsNorKillsTheConsumer()});</li>
 *     <li>when the poll thread dies for some <em>other</em> reason, the user must be told what that
 *         reason was, at the moment it happens, rather than being handed the timeout, which names
 *         neither the failing subsystem nor the failure
 *         ({@link #aDeadPollThreadReportsItsOwnCauseNotTheCommitResponseTimeout()});</li>
 *     <li>and when the poll thread is merely <em>slow</em>, it must be waited on rather than declared
 *         failed, because a slow poller and a dead one produce the same observable for the waiter and
 *         only the dead one is unrecoverable
 *         ({@link #aLivePollThreadThatIsMerelySlowIsWaitedOutRatherThanDeclaredDead()});</li>
 *     <li>and the fourth case the other three leave: a poll thread that is alive, has not died, and is
 *         <em>not</em> working towards the commit either, because it is wedged somewhere outside the
 *         commit path. No affirmative event can arrive, so a last-resort backstop ends the wait fatally
 *         rather than never ({@link #aPollThreadWedgedOutsideTheCommitPathIsFatalAtTheBackstop()}).</li>
 * </ol>
 * The second is the part the fixes for the known triggers could never cover: they removed two ways to
 * kill that thread, and every remaining way still produced the same uninformative message. The third
 * pinned, until the commit-failure seam (astubbs#317) landed, that the waiter's own
 * {@code offsetCommitTimeout} deadline reported itself truthfully; that deadline is now gone - every
 * exit is an affirmatively published event (typed response or poller death), because a local deadline
 * spent the same option the poll side uses as its retry budget, on an earlier clock, so it always
 * fired first and the budget's terminal outcome could never be delivered - and the scenario now pins
 * the survival that replaced it.
 * <p>
 * Deliberately not a subclass of {@link CommitRejectionTestBase}. That base pins a different property
 * - one rejection reason at a time, rejected only at start-up, asserting the offsets are not recorded
 * as successful - on a workload (ten records, one key, no retries) chosen to isolate it. What is under
 * test here is the reported <em>workload</em>: rejections recurring for the whole run against a large
 * backlog of retrying records, where a fix that deferred a commit but never re-requested one would
 * pass the base and stall here.
 * <p>
 * Nor of {@link MockConsumerTestBase}, and this is the closer call, since that base does own the
 * manual rebalance dance these tests repeat. Four things it fixes are variables here: it hardcodes
 * <em>one</em> partition and one record key, where the reported workload is {@value #KEYS} keys across
 * {@value #PARTITIONS} partitions under {@code KEY} ordering - which is what makes the rejections land
 * against a real backlog rather than a queue of one; it builds PC in {@code @BeforeEach} from
 * per-<em>class</em> options, while these three scenarios need three different consumers and two
 * different {@code offsetCommitTimeout}s; and its teardown asserts the failure cause is null, which two
 * of the three deliberately produce. Extending it would mean overriding the assignment, the record
 * model and the teardown, and splitting one story into three files - so the wiring would be inherited
 * and everything that matters overridden. Generalising the base to take a partition count and a key
 * supplier would be the real fix, and it belongs in a change that can re-verify the six classes already
 * on it, not in this one.
 */
@Slf4j
@Timeout(180)
class CommitResponseTimeoutSymptomTest {

    private static final String TOPIC = CommitResponseTimeoutSymptomTest.class.getSimpleName();

    /** As reported: a thousand keys. Each is its own shard under {@link ParallelConsumerOptions.ProcessingOrder#KEY}. */
    private static final int KEYS = 1000;

    private static final int PARTITIONS = 4;

    /** As reported: about half the records fail. Every second key fails its first attempt. */
    private static final int FAILING_KEY_MODULO = 2;

    /** The group rebalances constantly, so a commit keeps landing inside one. */
    private static final int REJECT_EVERY_NTH_COMMIT = 3;

    /**
     * The report is of a consumer that "runs for a while" before dying, so the records arrive over
     * time rather than in one batch. Fed in one go the whole backlog drains inside about four commit
     * cycles, which is too few for a rejection <em>storm</em> to mean anything - the first measured
     * version of this test saw one rejection.
     */
    private static final int FEED_BATCHES = 20;

    private static final Duration FEED_INTERVAL = Duration.ofMillis(150);

    /**
     * Lower bound on rejections, from {@link #FEED_BATCHES} x {@link #FEED_INTERVAL} of feeding at a
     * 100ms commit interval - comfortably under the count a healthy run produces. A slower machine
     * feeds over a longer wall-clock window and rejects more, never fewer, so load cannot flake this.
     */
    private static final int MIN_REJECTIONS = 4;

    private ParallelEoSStreamProcessor<String, String> parallelConsumer;

    @AfterEach
    void closePc() {
        if (parallelConsumer != null && !parallelConsumer.isClosedOrFailed()) {
            parallelConsumer.closeDontDrainFirst();
        }
    }

    /**
     * The reported scenario end to end. Rejections recur for the whole run, so deferral has to be
     * genuinely re-requested each cycle rather than merely survived once.
     * <p>
     * Discriminating: with the {@link RebalanceInProgressException} catch removed, the poll thread dies
     * on the first rejection and the run ends with the reported
     * {@code Timeout waiting for commit response} - the records never finish and the failure cause is
     * not null.
     */
    @Test
    void aRebalanceStormUnderAHighFailureRateNeitherStallsNorKillsTheConsumer() {
        final AtomicInteger commitAttempts = new AtomicInteger();
        final AtomicInteger commitsRejected = new AtomicInteger();

        var mockConsumer = new MockConsumer<String, String>(OffsetResetStrategy.EARLIEST) {
            @Override
            public synchronized void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
                if (commitAttempts.incrementAndGet() % REJECT_EVERY_NTH_COMMIT == 0) {
                    commitsRejected.incrementAndGet();
                    throw new RebalanceInProgressException("Offset commit cannot be completed since the "
                            + "consumer is undergoing a rebalance for auto partition assignment (mocking)");
                }
                super.commitSync(offsets);
            }
        };

        parallelConsumer = startPc(mockConsumer, Duration.ofSeconds(10));
        var partitions = assignPartitions(mockConsumer, parallelConsumer);
        var recordsPerPartition = expectedRecordsPerPartition();

        // every key must fail its first attempt, then succeed - the reported ~50% failure rate
        final Map<String, AtomicInteger> attemptsPerKey = new ConcurrentHashMap<>();
        final Set<String> succeeded = ConcurrentHashMap.newKeySet();

        parallelConsumer.poll(context -> context.forEach(record -> {
            String key = record.key();
            int attempt = attemptsPerKey.computeIfAbsent(key, k -> new AtomicInteger()).incrementAndGet();
            if (shouldFail(key) && attempt == 1) {
                throw new FakeRuntimeException("Simulated user function failure for " + key);
            }
            succeeded.add(key);
        }));

        feedRecordsOverTime(mockConsumer);

        // the whole backlog drains despite rejections continuing throughout
        Awaitility.await().atMost(Duration.ofSeconds(120)).untilAsserted(() ->
                assertThat(succeeded).hasSize(KEYS));

        // the rejection path was exercised repeatedly, not just once at start-up - otherwise this
        // test could pass by never reaching the behaviour it exists to pin.
        //
        // Awaited rather than read once: the await above is satisfied by the USER FUNCTION's set,
        // while this counter is incremented on the broker-poll thread inside commitSync. Those are
        // different threads and neither orders the other, so reading this one at the instant the
        // other finishes assumes an ordering nothing provides. The margin is large (feeding alone
        // spans ~19x FEED_INTERVAL at a 100ms commit interval, so tens of commits precede the last
        // batch) - but "large margin" is what every latent race says before it loses. Awaiting the
        // value actually asserted is strictly stronger and cannot mask anything: if the rejections
        // never arrive, this still fails.
        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(commitsRejected.get()).isAtLeast(MIN_REJECTIONS));
        log.info("Commit attempts: {}, rejected: {}", commitAttempts.get(), commitsRejected.get());

        // and every partition really is committed to the end, so deferral postponed rather than dropped
        Awaitility.await().atMost(Duration.ofSeconds(60)).untilAsserted(() -> {
            var committed = mockConsumer.committed(new HashSet<>(partitions));
            for (TopicPartition tp : partitions) {
                assertThat(committed.get(tp)).isNotNull();
                assertThat(committed.get(tp).offset()).isEqualTo(recordsPerPartition.get(tp));
            }
        });

        assertThat(parallelConsumer.getFailureCause()).isNull();
        assertThat(parallelConsumer.isClosedOrFailed()).isFalse();
    }

    /**
     * When the poll thread dies of something PC does not classify, the reported failure must name that
     * something - and must not wait out {@code offsetCommitTimeout} first.
     * <p>
     * {@link AuthorizationException} stands in for the open-ended set of ways that thread can die
     * (broker down, offset encoding, authorization) which no per-exception fix covers. The control
     * thread is already blocked in {@code commitAndWait()} when it happens, so the poller's own error
     * is otherwise never reported at all.
     * <p>
     * <b>The timeout is the control, not the mechanism.</b> {@code offsetCommitTimeout} is set to a
     * minute here while the await allows half of it: the poller publishes its death through
     * {@link ConsumerOffsetCommitter#notifyPollerDied}, so the waiter is released by an <em>event</em>
     * and this finishes in well under a second. Remove that call and the only way out is the timeout,
     * which cannot arrive inside the await - so the test fails, and it fails without asserting on a
     * clock. The chain assertion below is the same statement made causally: a timeout report appearing
     * at all means the event path did not fire.
     * <p>
     * Discriminating: without the death notification the run ends after a full minute with
     * {@code Timeout waiting for commit response} and this exception nowhere in the chain - exactly the
     * position the reporter of astubbs#177 was left in.
     */
    @Test
    void aDeadPollThreadReportsItsOwnCauseNotTheCommitResponseTimeout() {
        final Duration commitTimeout = Duration.ofMinutes(1);
        final String pollThreadFailure = "Not authorized to commit (mocking)";

        var mockConsumer = new MockConsumer<String, String>(OffsetResetStrategy.EARLIEST) {
            @Override
            public synchronized void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
                throw new AuthorizationException(pollThreadFailure);
            }
        };

        parallelConsumer = startPc(mockConsumer, commitTimeout);
        assignPartitions(mockConsumer, parallelConsumer);
        addRecords(mockConsumer, 0, PARTITIONS); // enough to make PC want to commit

        parallelConsumer.poll(context -> context.forEach(record -> log.trace("Processing {}", record.key())));

        // Awaiting isClosedOrFailed() and then asserting getFailureCause() is the safe direction, and
        // deliberately so: supervisorLoop() assigns failureReason BEFORE doClose() (which sets
        // state=CLOSED) and before the throw that completes controlThreadFuture, and
        // isClosedOrFailed() reads only those two later signals. So the awaited signal LAGS the
        // asserted value rather than leading it - the inverse of the trap in
        // docs/solutions/test-flakiness/vacuous-await-condition-brokerpoller-backpressure-2026-07-31.md
        // ("await the value you assert, never a proxy that leads it"). The FutureTask state read in
        // isDone() also gives the happens-before edge that publishes failureReason to this thread.
        // Do not "fix" this into awaiting getFailureCause(): a null cause is exactly what a
        // regression here would produce, and awaiting it would hide that.
        // Half of offsetCommitTimeout. Reaching this ceiling means the waiter was NOT released by the
        // death event and is sitting out its full minute - see the class-level note above.
        Awaitility.await().atMost(commitTimeout.dividedBy(2)).untilAsserted(() ->
                assertThat(parallelConsumer.isClosedOrFailed()).isTrue());

        Exception failureCause = parallelConsumer.getFailureCause();
        assertThat(failureCause).isNotNull();

        var chain = causeChain(failureCause);
        var everyMessage = chain.stream()
                .flatMap(t -> Stream.concat(Stream.of(t), java.util.Arrays.stream(t.getSuppressed())))
                .map(t -> String.valueOf(t.getMessage()))
                .collect(java.util.stream.Collectors.toList());

        // the real reason must be reachable, not replaced by the symptom
        assertThat(chain.stream().anyMatch(t -> t instanceof AuthorizationException)).isTrue();
        assertThat(chain.stream().anyMatch(t -> String.valueOf(t.getMessage()).contains(pollThreadFailure))).isTrue();

        // the waiter was told, so it says so
        assertThat(everyMessage.stream().anyMatch(m -> m.contains("broker poll thread has died"))).isTrue();

        // and the symptom is absent entirely rather than merely demoted: its presence would mean the
        // waiter timed out, which on this path is the regression itself.
        assertThat(everyMessage.stream().noneMatch(m -> m.contains("Timeout waiting for commit response"))).isTrue();
    }

    /**
     * The other branch: the poll thread is <b>alive</b> and simply has not answered in time. The waiter
     * must wait it out - released by the commit's own (late) answer - rather than declare a failure on
     * a clock of its own.
     * <p>
     * This is the branch the death event deliberately does not cover, so it is the one that proves the
     * two are not conflated: a commit blocking on the poll thread produces the identical observable for
     * the control thread as a dead poller, and only the dead one may be fatal. Before the commit-failure
     * seam (astubbs#317) this scenario pinned the previous answer - a local {@code offsetCommitTimeout}
     * deadline whose report carefully claimed no more than it knew ("has not died with an exception").
     * That deadline spent the SAME option the poll side uses as its retry budget, on an earlier clock,
     * so whenever a commit was genuinely failing the waiter deterministically died first and the
     * budget's terminal outcome - the event the seam hands to the {@link CommitFailureHandler} - could
     * never be delivered. The rework that removed it is pinned here from the waiter's side; the
     * seam-side half (a held-open commit attempt still reaching the handler) lives in
     * {@link MockConsumerCommitFailureDecisionTest#theWaiterOutlivesACommitAttemptHeldOpenPastTheOldDeadline()}.
     * <p>
     * Discriminating: reintroduce any waiter-side deadline at or below {@code offsetCommitTimeout} and
     * this fails - the instance dies at 1s where the commit answers, successfully, at 5s.
     */
    @Test
    void aLivePollThreadThatIsMerelySlowIsWaitedOutRatherThanDeclaredDead() {
        final Duration commitTimeout = Duration.ofSeconds(1);
        final Duration commitBlocksFor = commitTimeout.multipliedBy(5);

        var mockConsumer = new MockConsumer<String, String>(OffsetResetStrategy.EARLIEST) {
            @Override
            // DELIBERATELY NOT synchronized, unlike the overrides in the other two scenarios. They throw
            // immediately; this one sleeps, and MockConsumer guards poll, addRecord, commitSync and close
            // with one monitor - so sleeping while holding it parks PC's own polling and the teardown
            // close, not just this commit. What the test needs is the POLL THREAD blocked inside
            // commitSync, which happens either way; holding the monitor only adds collateral. Measured:
            // with `synchronized`, green locally in 8.6s and dead on CI at the 60s await
            // (run 32216929470). MockConsumerTestBase#addRecordsInBackground documents this hazard -
            // inheriting that warning is part of what extending it would have bought, per the class
            // javadoc above.
            public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
                // the poll thread stays alive and healthy - it is simply in here, not answering yet
                sleepOrFail(commitBlocksFor, "Interrupted while blocking the commit");
                super.commitSync(offsets);
            }
        };

        parallelConsumer = startPc(mockConsumer, commitTimeout);
        var partitions = assignPartitions(mockConsumer, parallelConsumer);
        addRecords(mockConsumer, 0, PARTITIONS); // enough to make PC want to commit

        parallelConsumer.poll(context -> context.forEach(record -> log.trace("Processing {}", record.key())));

        // the slow commit completes and is recorded - the waiter sat through a block five times its old
        // deadline and was released by the answer, not by a clock
        Awaitility.await().atMost(Duration.ofSeconds(60)).untilAsserted(() -> {
            var committed = mockConsumer.committed(new HashSet<>(partitions));
            assertThat(partitions.stream().anyMatch(tp -> committed.get(tp) != null)).isTrue();
        });

        // slow is not dead: no failure was declared and the instance is still running
        assertThat(parallelConsumer.getFailureCause()).isNull();
        assertThat(parallelConsumer.isClosedOrFailed()).isFalse();
    }

    /**
     * The poll thread is alive, healthy, and wedged <b>outside</b> the commit path - so it will never service the
     * commit request, and neither affirmative exit exists: it publishes no death (it has not died) and no typed
     * response (it is not in the commit path to produce one). Waiting is then not patience but a permanent hang,
     * with PC neither committing nor reporting, so a last-resort backstop ends it fatally.
     * <p>
     * <b>This is the case removing the waiter's deadline reopened.</b> The deadline used to break exactly this
     * cycle - crudely, and at a length that also killed healthy slow commits, which is why it went - so the
     * replacement is deliberately generous: four commit budgets plus a fixed floor, minutes rather than seconds.
     * The scenario above ({@link #aLivePollThreadThatIsMerelySlowIsWaitedOutRatherThanDeclaredDead()}) is the
     * control arm that the two are not conflated: it blocks a commit for five times {@code offsetCommitTimeout}
     * and must still complete, which it does because the backstop is nowhere near it.
     * <p>
     * The AB-BA deadlock of astubbs#29 / confluentinc#857 is the real-world shape of a poll thread wedged this
     * way (its callback half is cut in {@code AbstractParallelEoSStreamProcessor#tryCommitOffsetsOnRevoke}, and
     * pinned in {@code MockConsumerCommitFailureHandlerFreeExitsTest}); a {@code poll()} that never returns is
     * the same thing without the lock, and is what this drives.
     * <p>
     * The floor is shortened through its test seam, the same shape as {@code BrokerPollSystem.longPollTimeout} -
     * a test cannot outlive the real one.
     * <p>
     * Discriminating: remove the backstop and this hangs until the class {@code @Timeout}, having reported
     * nothing. Report it as the seam's budget exhaustion instead, and the type assertion below fails - the two
     * are opposite claims, one saying a commit failed and the other that no commit is being attempted at all.
     */
    @Test
    // GLOBAL, not a named resource: the backstop floor is a JVM-wide static, so shortening it shortens the
    // backstop of every PC alive in this JVM. Measured: with a named lock, the merely-slow scenario above ran
    // concurrently, inherited a 5s backstop against its deliberate 5s commit block, and died. A named lock only
    // excludes tests that name the same resource - and the whole point of that scenario is that it does not
    // know this static exists.
    @ResourceLock(value = Resources.GLOBAL, mode = READ_WRITE)
    void aPollThreadWedgedOutsideTheCommitPathIsFatalAtTheBackstop() {
        final Duration commitTimeout = Duration.ofMillis(250);
        // backstop = 4 x 250ms + 1s = 2s, so the whole scenario runs in seconds rather than the real minute-plus
        final Duration backstopFloor = Duration.ofSeconds(1);
        Duration originalFloor = ConsumerOffsetCommitter.getPollerWedgedBackstopFloor();
        ConsumerOffsetCommitter.setPollerWedgedBackstopFloor(backstopFloor);

        var wedgePoll = new AtomicBoolean(false);
        var pollWedged = new CountDownLatch(1);
        var releasePoll = new CountDownLatch(1);
        var mockConsumer = new MockConsumer<String, String>(OffsetResetStrategy.EARLIEST) {
            @Override
            // deliberately NOT synchronized - see the sibling scenario above for why parking under the
            // MockConsumer monitor takes the teardown path down with it
            public ConsumerRecords<String, String> poll(Duration timeout) {
                if (wedgePoll.get()) {
                    pollWedged.countDown();
                    try {
                        // bounded only so a test failure cannot wedge the JVM; nothing here releases it in time
                        if (!releasePoll.await(120, SECONDS)) {
                            throw new FakeRuntimeException("the wedged poll was never released - test failure");
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    wedgePoll.set(false);
                }
                return super.poll(timeout);
            }
        };

        try {
            // one record is held in flight, because a commit cycle only runs when something is dirty - its
            // completion is what sends the control thread into a commit AFTER the poll thread is wedged, and a
            // wedged poller can deliver no new work to dirty anything with
            var heldRecordEntered = new CountDownLatch(1);
            var heldRecordRelease = new CountDownLatch(1);
            final String heldKey = "key-0";
            parallelConsumer = startPc(mockConsumer, commitTimeout);
            assignPartitions(mockConsumer, parallelConsumer);
            addRecords(mockConsumer, 0, PARTITIONS); // enough to make PC want to commit
            parallelConsumer.poll(context -> context.forEach(record -> {
                if (heldKey.equals(record.key())) {
                    heldRecordEntered.countDown();
                    try {
                        if (!heldRecordRelease.await(120, SECONDS)) {
                            throw new FakeRuntimeException("the held record was never released - test failure");
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
                log.trace("Processing {}", record.key());
            }));

            // wedge the poller once it has delivered the work, with one record still in flight
            assertWithMessage("the held record was never reached")
                    .that(heldRecordEntered.await(30, SECONDS)).isTrue();
            wedgePoll.set(true);
            assertWithMessage("the poll thread never wedged").that(pollWedged.await(30, SECONDS)).isTrue();
            heldRecordRelease.countDown(); // dirty, so a commit cycle runs - and can never be answered

            // generous against the 2s backstop, and it is the backstop that must end this, not this deadline:
            // see the class note on awaiting isClosedOrFailed() rather than the cause
            Awaitility.await().atMost(Duration.ofSeconds(60)).untilAsserted(() ->
                    assertThat(parallelConsumer.isClosedOrFailed()).isTrue());

            Exception failureCause = parallelConsumer.getFailureCause();
            assertThat(failureCause).isNotNull();
            var chain = causeChain(failureCause);
            var everyMessage = chain.stream()
                    .map(t -> String.valueOf(t.getMessage()))
                    .collect(java.util.stream.Collectors.toList());

            assertWithMessage("the report must name the wedged poller, not a generic timeout")
                    .that(everyMessage.stream().anyMatch(m -> m.contains("has not serviced or answered commit "
                            + "request"))).isTrue();
            assertWithMessage("...and must say plainly that it is not the commit-failure seam's budget exhaustion")
                    .that(everyMessage.stream().anyMatch(m -> m.contains("NOT the commit-failure seam's budget "
                            + "exhaustion"))).isTrue();
            assertWithMessage("a wedged poller is not a commit failure - no handler decision could answer it")
                    .that(chain.stream().anyMatch(t -> t instanceof OffsetCommitBudgetExceededException)).isFalse();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new FakeRuntimeException("interrupted waiting for the poll thread to wedge");
        } finally {
            releasePoll.countDown();
            ConsumerOffsetCommitter.setPollerWedgedBackstopFloor(originalFloor);
        }
    }

    private boolean shouldFail(String key) {
        return Integer.parseInt(key.substring("key-".length())) % FAILING_KEY_MODULO == 0;
    }

    private ParallelEoSStreamProcessor<String, String> startPc(MockConsumer<String, String> mockConsumer,
                                                               Duration offsetCommitTimeout) {
        var options = ParallelConsumerOptions.<String, String>builder()
                .consumer(mockConsumer)
                .ordering(ParallelConsumerOptions.ProcessingOrder.KEY) // as reported - keys are the unit of ordering
                .commitMode(ParallelConsumerOptions.CommitMode.PERIODIC_CONSUMER_SYNC) // the only mode that waits on a commit response
                .commitInterval(Duration.ofMillis(100))
                .offsetCommitTimeout(offsetCommitTimeout)
                .defaultMessageRetryDelay(Duration.ofMillis(50)) // keep the retry backlog moving
                .build();
        var pc = new ParallelEoSStreamProcessor<String, String>(options);
        pc.subscribe(of(TOPIC));
        return pc;
    }

    /** MockConsumer does not honour the Consumer contract for rebalances - drive it by hand. */
    private List<TopicPartition> assignPartitions(MockConsumer<String, String> mockConsumer,
                                                  ParallelEoSStreamProcessor<String, String> pc) {
        List<TopicPartition> partitions = new ArrayList<>();
        Map<TopicPartition, Long> beginningOffsets = new HashMap<>();
        for (int p = 0; p < PARTITIONS; p++) {
            TopicPartition tp = new TopicPartition(TOPIC, p);
            partitions.add(tp);
            beginningOffsets.put(tp, 0L);
        }
        mockConsumer.rebalance(partitions);
        pc.onPartitionsAssigned(partitions);
        mockConsumer.updateBeginningOffsets(beginningOffsets);
        return partitions;
    }

    /**
     * Feeds all {@link #KEYS} records in {@link #FEED_BATCHES} batches, so the consumer runs across
     * many commit cycles as reported rather than draining one buffer. Synchronous: PC polls on its own
     * threads, so the test thread is free, and there is no background feeder to outlive the test and
     * call {@code addRecord} on a closed consumer.
     */
    private void feedRecordsOverTime(MockConsumer<String, String> mockConsumer) {
        int batchSize = KEYS / FEED_BATCHES;
        for (int batch = 0; batch < FEED_BATCHES; batch++) {
            addRecords(mockConsumer, batch * batchSize, batchSize);
            if (batch < FEED_BATCHES - 1) {
                sleepOrFail(FEED_INTERVAL, "Interrupted while feeding records");
            }
        }
    }

    private void addRecords(MockConsumer<String, String> mockConsumer, int firstKey, int count) {
        for (int i = firstKey; i < firstKey + count; i++) {
            int partition = i % PARTITIONS;
            // keys are dealt round-robin across partitions, so a key's offset is its index within its
            // own partition's share
            long offset = i / PARTITIONS;
            mockConsumer.addRecord(new ConsumerRecord<>(TOPIC, partition, offset, "key-" + i, "value-" + i));
        }
    }

    /** @return the number of records each partition receives, i.e. its expected final committed offset */
    private Map<TopicPartition, Long> expectedRecordsPerPartition() {
        Map<TopicPartition, Long> counts = new HashMap<>();
        for (int i = 0; i < KEYS; i++) {
            counts.merge(new TopicPartition(TOPIC, i % PARTITIONS), 1L, Long::sum);
        }
        return counts;
    }

    private static List<Throwable> causeChain(Throwable throwable) {
        List<Throwable> chain = new ArrayList<>();
        for (Throwable t = throwable; t != null && !chain.contains(t); t = t.getCause()) {
            chain.add(t);
        }
        return chain;
    }
}
