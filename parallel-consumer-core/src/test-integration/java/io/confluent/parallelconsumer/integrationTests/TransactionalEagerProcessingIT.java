package io.confluent.parallelconsumer.integrationTests;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.ParallelEoSStreamProcessor;
import io.confluent.parallelconsumer.ProvesClaim;
import io.confluent.parallelconsumer.TransactionalClaim;
import io.confluent.parallelconsumer.integrationTests.utils.KafkaClientUtils.GroupOption;
import io.confluent.parallelconsumer.integrationTests.utils.TransactionalTopicVerifier;
import io.confluent.parallelconsumer.integrationTests.utils.WorkerFunctionFailureCapture;
import io.confluent.parallelconsumer.internal.PCModule;
import io.confluent.parallelconsumer.internal.ProducerManager;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import pl.tlinkowski.unij.api.UniLists;
import pl.tlinkowski.unij.api.UniSets;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import static com.google.common.truth.Truth.assertWithMessage;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.UNORDERED;
import static io.confluent.parallelconsumer.integrationTests.utils.KafkaClientUtils.ProducerMode.TRANSACTIONAL;
import static java.time.Duration.ofMillis;
import static java.time.Duration.ofMinutes;
import static java.time.Duration.ofSeconds;
import static org.apache.commons.lang3.RandomUtils.nextInt;
import static org.awaitility.Awaitility.await;

/**
 * C13 - the documented cost of {@code ParallelConsumerOptions#allowEagerProcessingDuringTransactionCommit}:
 * <em>"this may cause side effect replay when the record is retried, otherwise there is no replay."</em>
 *
 * <h2>Why this is one test with two arms rather than two tests</h2>
 * The claim is <b>directional</b>. "May replay" is not falsifiable on its own - a run that happened not to replay
 * would satisfy it, and so would an assertion that permits any count. What is falsifiable is the DIFFERENCE:
 * under one forced trigger, applied identically, the eager arm re-runs the user function for the retried record
 * and the strict arm does not. So both arms run in one method against one trigger, and the assertion compares
 * them. The replay is asserted as observed, not merely permitted.
 *
 * <h2>The trigger, and why it is the produce lock rather than a crash</h2>
 * {@code ParallelEoSStreamProcessor#processAndProduceResults} takes the produce lock in one of two places
 * depending on the option, and that placement is the whole mechanism:
 * <ul>
 *     <li><b>strict</b> (the default, eager disabled) - {@code beginProducing} runs BEFORE the user function, so a
 *     failure to acquire it means the user function never ran at all;</li>
 *     <li><b>eager</b> - {@code beginProducing} runs AFTER the user function has returned its result records, so a
 *     failure to acquire it discards work the user function already did, side effects included.</li>
 * </ul>
 * Both raise the same failure, PC's catch-all fails the record, and it is retried. The trigger is therefore simply
 * to make one acquisition time out, which this does by holding the write side of
 * {@code ProducerManager#producerTransactionLock} shut - the same lock a commit holds, taken the same way, for
 * exactly as long as it takes the timeout to be OBSERVED rather than for a guessed duration.
 * <p>
 * The lock is taken directly by the test thread rather than by stalling a real commit, because a stalled commit
 * runs on the CONTROL thread: while it is inside {@code preAcquireOffsetsToCommit} the controller is not running
 * {@code retrieveAndDistributeNewWork} either, so the victim record would never be dispatched to a worker and
 * would never attempt the lock. Held from outside, the controller stays free to dispatch while the lock is shut.
 * <p>
 * That has one hazard, and {@link Arm.ObservableModule} closes it: {@code ProducerManager#acquireCommitLock}
 * throws {@link java.util.concurrent.ConcurrentModificationException} if the write lock is held by ANOTHER
 * thread, so a commit attempted during the hold would kill the instance. The hold is therefore only ever taken
 * once the arm is quiescent - every record committed, so {@code wm.isDirty()} is false and
 * {@code maybeAcquireCommitLock} does not even try - and the module counts and interlocks any attempt that
 * happens anyway, so the failure is a legible "the trigger never fired, and N commits were attempted during the
 * hold" rather than an instance death.
 *
 * <h2>What is counted</h2>
 * User-function invocations per input KEY, from an in-process counter - that is what a "side effect" is here.
 * Never offsets: commit markers occupy offsets, so the output topic is read through
 * {@link TransactionalTopicVerifier}, which counts the records it actually consumed.
 *
 * @author Antony Stubbs
 * @see TransactionalClaim#EAGER_PROCESSING_MAY_REPLAY
 * @see TransactionTimeoutsTest#produceTimeout the prior art for forcing a produce-lock timeout
 * @see TransactionalCrashReplayIT
 */
@Tag("transactions")
@Slf4j
class TransactionalEagerProcessingIT extends BrokerIntegrationTest<String, String> {

    /**
     * Substring of BOTH produce-lock timeout messages raised by
     * {@code ParallelEoSStreamProcessor#processAndProduceResults}, used to recognise the forced trigger without
     * committing to which arm raised it.
     */
    private static final String PRODUCE_LOCK_TIMEOUT = "acquire produce lock";

    /**
     * The strict arm's message - main's own words for "the lock was taken before the user function could start".
     */
    private static final String BEFORE_THE_USER_FUNCTION = "early acquire produce lock";

    /**
     * The eager arm's message - main's own words for "the lock was taken at produce time, after the user
     * function had already run".
     */
    private static final String AFTER_THE_USER_FUNCTION = "late acquire produce lock";

    private static final String WARMUP = "warmup";

    private static final String RESULT = "result|";

    private static final String PRIMED_KEYS = "primed-";

    private static final String VICTIM_KEYS = "victim-";

    /**
     * Records fed before the trigger. Their only job is to prove the pipeline processes and COMMITS normally, so
     * that the arm can be brought to a quiescent state before the hold - and to give the verifier something
     * committed to be caught up to.
     */
    private static final int PRIMING_RECORDS = 3;

    private static final int BATCH_SIZE = 1;

    /**
     * One worker. The trigger concerns a single record's single acquisition attempt, and a wider pool would only
     * add records racing for the same lock without making the observation any sharper.
     */
    private static final int MAX_CONCURRENCY = 1;

    /**
     * How long a worker waits for the produce lock before failing its record. Short, because the hold is only
     * released once the resulting timeout has been observed - this is the length of the trigger, not a race
     * against it.
     */
    private static final Duration PRODUCE_LOCK_TIMEOUT_AFTER = ofSeconds(2);

    /**
     * Deliberately long. The commit lock is never contended in this test: the hold is taken while nothing is
     * dirty, and {@link Arm.ObservableModule} interlocks any commit attempt made during it anyway. A short value
     * here would only add a second, uninteresting way for the instance to die.
     */
    private static final Duration COMMIT_LOCK_TIMEOUT = ofSeconds(60);

    private static final Duration COMMIT_INTERVAL = ofMillis(200);

    /**
     * Long enough that the victim is retried promptly after the hold is released, short enough that the retry is
     * not itself a wait worth watching.
     */
    private static final Duration RETRY_DELAY = ofMillis(500);

    /**
     * Far longer than this test runs. A reaped transaction would give every visibility assertion here a second,
     * uninteresting explanation.
     */
    private static final Duration NO_REAP_WHILE_WE_RUN = ofMinutes(5);

    private static final Duration PROGRESS_TIMEOUT = ofSeconds(120);

    /**
     * How long the hold waits for the produce-lock timeout it exists to cause. Generous: the wait is a poll, a
     * dispatch and one {@link #PRODUCE_LOCK_TIMEOUT_AFTER}, and the interesting outcome is the timeout firing,
     * not how quickly.
     */
    private static final Duration TRIGGER_TIMEOUT = ofSeconds(60);

    /**
     * Watched after the replay has demonstrably finished, so that an output record produced late by a retry
     * cannot slip in behind the exactly-once assertion.
     */
    private static final Duration STRAGGLER_WINDOW = ofSeconds(3);

    // -------------------------------------------------------------------------------------------------------------
    // The test
    // -------------------------------------------------------------------------------------------------------------

    /**
     * C13, both halves, under one trigger.
     * <p>
     * Each arm: run some records normally, wait until the instance is quiescent, hold the produce lock shut, feed
     * ONE more record, and wait for that record's produce-lock acquisition to time out. Then release, let the
     * record be retried to success, and count how many times its user function ran.
     * <p>
     * What each arm asserts, and why each is needed:
     * <ol>
     *     <li><b>Where the lock is taken.</b> Inside the user function, the worker thread's own read-hold count on
     *     {@code producerTransactionLock} is 1 in the strict arm and 0 in the eager arm - a direct reading of
     *     "before the user function runs" versus "at produce time". The arm-specific timeout message
     *     ({@value #BEFORE_THE_USER_FUNCTION} / {@value #AFTER_THE_USER_FUNCTION}) says the same thing in main's
     *     own words.</li>
     *     <li><b>Non-vacuity.</b> The trigger must be observed to have fired in BOTH arms. Without it the strict
     *     arm's "no replay" is a statement about a record that was never retried, which is trivially true and
     *     worth nothing.</li>
     *     <li><b>The difference.</b> Strict runs the victim's user function exactly once; eager runs it more than
     *     once; and eager is strictly greater than strict. The last of the three is the claim - the first two are
     *     what stop it being satisfied by an arm that did nothing.</li>
     *     <li><b>What is NOT replayed.</b> The output topic still holds exactly one result per input record in
     *     both arms. The eager arm's discarded attempt never reached {@code produceMessages}, so what the option
     *     costs is the user's side effects, not duplicate output - which is what "doesn't interfere with the
     *     transaction itself, just reduces side effects" means, and it would be easy to mistake one for the
     *     other.</li>
     * </ol>
     */
    @Test
    @ProvesClaim(TransactionalClaim.EAGER_PROCESSING_MAY_REPLAY)
    void eagerProcessingReplaysTheSideEffectOfARetriedRecordWhereStrictProcessingDoesNot() {
        Arm strict = new Arm("strict", false);
        int strictInvocations = strict.runTheTrigger();

        Arm eager = new Arm("eager", true);
        int eagerInvocations = eager.runTheTrigger();

        strict.assertTheProduceLockWasTakenBeforeTheUserFunction();
        eager.assertTheProduceLockWasTakenAfterTheUserFunction();

        assertWithMessage("with eager processing disabled the victim's user function must have run exactly once - "
                        + "the failed attempt timed out on the produce lock BEFORE the function could start, so it "
                        + "cost no side effect, and the retry then succeeded. More than one is the replay the "
                        + "documentation says only happens with eager processing; none means the retry never "
                        + "succeeded. Invocations were %s", strict.userFunctionInvocations)
                .that(strictInvocations)
                .isEqualTo(1);

        assertWithMessage("the eager arm did not re-run the user function for the retried record, so the "
                        + "documented side-effect replay was not observed - invocations were %s. The trigger DID "
                        + "fire (%s), so this is the claim failing rather than the test not reaching it",
                eager.userFunctionInvocations, eager.produceLockTimeouts())
                .that(eagerInvocations)
                .isGreaterThan(1);

        assertWithMessage("the two arms ran the victim's user function the same number of times under the same "
                        + "forced trigger, so this run cannot distinguish 'eager may replay' from 'both replay' or "
                        + "'neither does' - the claim is directional and is untested by an equal result")
                .that(eagerInvocations)
                .isGreaterThan(strictInvocations);

        log.info("C13 observed: victim user function ran {}x with eager processing enabled and {}x with it "
                + "disabled, under the same forced produce-lock timeout", eagerInvocations, strictInvocations);

        strict.assertEachResultExistsExactlyOnce();
        eager.assertEachResultExistsExactlyOnce();
    }

    // -------------------------------------------------------------------------------------------------------------
    // One arm: an instance, its topics, and the trigger
    // -------------------------------------------------------------------------------------------------------------

    /**
     * One configuration of {@code ParallelConsumerOptions#allowEagerProcessingDuringTransactionCommit}, with its
     * own topics, instance and consumer group, so that the two arms cannot interfere with each other.
     */
    private final class Arm {

        private final String name;

        private final boolean eager;

        private final String nonce;

        private final String inputTopic;

        private final String outputTopic;

        /**
         * The side effect being counted: one entry per input key, incremented on entry to the user function.
         */
        private final Map<String, AtomicInteger> userFunctionInvocations = new ConcurrentHashMap<>();

        /**
         * The worker thread's own read-hold count on {@code producerTransactionLock}, sampled INSIDE the user
         * function - which is the direct reading of whether the produce lock was taken before it or after it.
         */
        private final List<Integer> produceLockHoldInsideUserFunction = new CopyOnWriteArrayList<>();

        /**
         * Commits attempted while the hold was in force. Expected to be zero - see the class javadoc - and
         * reported in the trigger's failure message, because a non-zero value explains exactly why the victim was
         * never dispatched.
         */
        private final AtomicInteger commitAttemptsDuringTheHold = new AtomicInteger();

        private final CountDownLatch holdReleased = new CountDownLatch(1);

        private volatile boolean holdActive;

        private final ObservableModule module;

        private final ParallelEoSStreamProcessor<String, String> pc;

        private final TransactionalTopicVerifier committed;

        private final WorkerFunctionFailureCapture workerFailures;

        private final String warmupMarker;

        private List<String> primedKeys;

        private String victimKey;

        @SneakyThrows
        private Arm(String name, boolean eager) {
            this.name = name;
            this.eager = eager;
            this.nonce = name + "-" + nextInt();
            this.inputTopic = setupTopic(TransactionalEagerProcessingIT.class.getSimpleName() + "-" + name + "-input");
            this.outputTopic = setupTopic(TransactionalEagerProcessingIT.class.getSimpleName() + "-" + name + "-output");

            // committed by a producer with its own random transactional.id, so it is fixture rather than part of
            // the story - its only job is to be the marker the verifier must be caught up to before it is
            // allowed to say anything about what it has NOT seen
            Producer<String, String> warmupProducer = register(getKcu().createAndInitNewTransactionalProducer());
            warmupMarker = WARMUP + "-" + nonce;
            warmupProducer.beginTransaction();
            warmupProducer.send(new ProducerRecord<>(outputTopic, warmupMarker, warmupMarker));
            warmupProducer.commitTransaction();

            committed = register(TransactionalTopicVerifier.readCommitted(getKcu(), nonce, outputTopic));
            committed.requireLiveAndCaughtUp(warmupMarker);

            workerFailures = register(new WorkerFunctionFailureCapture(nonce));

            KafkaProducer<String, String> producer = getKcu()
                    .createNewProducer(TRANSACTIONAL, NO_REAP_WHILE_WE_RUN, Optional.empty());
            KafkaConsumer<String, String> consumer = getKcu().createNewConsumer(GroupOption.NEW_GROUP);

            module = new ObservableModule(ParallelConsumerOptions.<String, String>builder()
                    .consumer(consumer)
                    .producer(producer)
                    .commitMode(PERIODIC_TRANSACTIONAL_PRODUCER)
                    .ordering(UNORDERED)
                    .maxConcurrency(MAX_CONCURRENCY)
                    .batchSize(BATCH_SIZE)
                    .commitInterval(COMMIT_INTERVAL)
                    .defaultMessageRetryDelay(RETRY_DELAY)
                    .produceLockAcquisitionTimeout(PRODUCE_LOCK_TIMEOUT_AFTER)
                    .commitLockAcquisitionTimeout(COMMIT_LOCK_TIMEOUT)
                    .allowEagerProcessingDuringTransactionCommit(eager)
                    .build());

            pc = register(new ParallelEoSStreamProcessor<>(module.options(), module));
            // names this instance's worker threads, which is how WorkerFunctionFailureCapture tells our worker
            // failures from those of the sibling arm sharing this JVM
            pc.setMyId(Optional.of(nonce));
            pc.subscribe(UniSets.of(inputTopic));
        }

        /**
         * A {@link PCModule} that does two things the test cannot do from outside the DI system.
         * <p>
         * It exposes the memoised {@link ProducerManager} - memoised matters, because a fresh one would carry a
         * fresh {@code producerTransactionLock} and the produce and commit locks would stop being two sides of
         * one lock, which is the entire mechanism under test.
         * <p>
         * And it interlocks commits against the test's hold. {@code ProducerManager#acquireCommitLock} throws
         * {@link java.util.concurrent.ConcurrentModificationException} when the write lock is held by another
         * thread, so a commit attempted during the hold would kill the instance and replace a legible test
         * failure with an instance death. The hold is taken while nothing is dirty precisely so that this never
         * runs; it counts what it catches so that, if it ever does run, the reason the victim was not dispatched
         * is in the failure message instead of being inferred.
         */
        private final class ObservableModule extends PCModule<String, String> {

            /**
             * This module's own memo. {@code PCModule} memoises into a private field this override never reaches,
             * so caching here is what keeps every caller - the PC and the test - on ONE
             * {@code producerTransactionLock}.
             */
            private ProducerManager<String, String> interlocked;

            private ObservableModule(ParallelConsumerOptions<String, String> options) {
                super(options);
            }

            @Override
            public ProducerManager<String, String> producerManager() {
                if (interlocked == null) {
                    interlocked = new ProducerManager<String, String>(producerWrap(), consumerManager(),
                            workManager(), options()) {
                        @Override
                        protected void preAcquireOffsetsToCommit()
                                throws java.util.concurrent.TimeoutException, InterruptedException {
                            if (holdActive) {
                                commitAttemptsDuringTheHold.incrementAndGet();
                                // never let the controller reach acquireCommitLock while the test thread holds
                                // the write lock - it would throw ConcurrentModificationException and kill the
                                // instance. Expected never to run; counted so that if it does, the trigger's
                                // failure message says so.
                                holdReleased.await();
                            }
                            super.preAcquireOffsetsToCommit();
                        }
                    };
                }
                return interlocked;
            }
        }

        /**
         * @return the read-lock/write-lock pair that the produce lock and the commit lock are the two sides of
         */
        private ReentrantReadWriteLock producerTransactionLock() {
            return module.producerManager().getProducerTransactionLock();
        }

        /**
         * Starts processing. One output record per input record, and the user-function invocation counted on
         * ENTRY - so a record whose attempt is later discarded still counts, which is precisely the side effect
         * the claim is about.
         */
        private void start() {
            log.info("Arm {}: starting instance (eager={})", name, eager);
            pc.pollAndProduce(context -> {
                ConsumerRecord<String, String> record = context.getSingleConsumerRecord();
                userFunctionInvocations.computeIfAbsent(record.key(), ignored -> new AtomicInteger())
                        .incrementAndGet();
                produceLockHoldInsideUserFunction.add(producerTransactionLock().getReadHoldCount());
                return new ProducerRecord<>(outputTopic, record.key(), RESULT + record.key());
            });
        }

        private int invocationsFor(String key) {
            AtomicInteger count = userFunctionInvocations.get(key);
            return count == null ? 0 : count.get();
        }

        private List<String> produceLockTimeouts() {
            return workerFailures.describe().stream()
                    .filter(failure -> failure.contains(PRODUCE_LOCK_TIMEOUT))
                    .collect(Collectors.toList());
        }

        private String resultFor(String key) {
            return RESULT + key;
        }

        private List<String> resultsFor(List<String> keys) {
            return keys.stream().map(this::resultFor).collect(Collectors.toList());
        }

        /**
         * Feed the priming records, wait until every one of their results is committed and visible, and only
         * then consider the instance quiescent.
         * <p>
         * Quiescence is what makes the hold safe: the partition is clean, so {@code maybeAcquireCommitLock}'s
         * {@code wm.isDirty()} gate is false and the controller does not attempt a commit while the write lock is
         * held by this thread.
         */
        @SneakyThrows
        private void primeAndQuiesce() {
            primedKeys = getKcu().produceMessages(inputTopic, PRIMING_RECORDS, name + "-" + PRIMED_KEYS);
            start();
            committed.awaitAllVisible(resultsFor(primedKeys), PROGRESS_TIMEOUT);
            log.info("Arm {}: quiescent - every priming result committed and visible", name);
        }

        /**
         * The forced trigger, and the whole point of the arm.
         * <p>
         * Holds the produce lock shut, feeds one record, and waits for THAT record's acquisition attempt to time
         * out - so the hold lasts exactly as long as it takes to be observed rather than a duration guessed in
         * advance. Then releases, and lets the record be retried to success.
         *
         * @return how many times the victim record's user function ran in total
         */
        @SneakyThrows
        private int runTheTrigger() {
            primeAndQuiesce();

            ReentrantReadWriteLock lock = producerTransactionLock();
            holdActive = true;
            lock.writeLock().lock();
            try {
                victimKey = getKcu().produceMessages(inputTopic, 1, name + "-" + VICTIM_KEYS).get(0);
                log.info("Arm {}: produce lock held shut, victim record {} fed", name, victimKey);

                await().atMost(TRIGGER_TIMEOUT)
                        .pollInterval(ofMillis(200))
                        .untilAsserted(() -> assertWithMessage("arm %s never raised a produce-lock timeout while "
                                        + "the lock was held shut, so nothing was retried and this arm proves "
                                        + "nothing about replay. Worker-path failures seen: %s. Commits attempted "
                                        + "during the hold (expected 0 - a non-zero count means the controller was "
                                        + "interlocked and never dispatched the victim): %s",
                                name, workerFailures.describe(), commitAttemptsDuringTheHold.get())
                                .that(produceLockTimeouts())
                                .isNotEmpty());
            } finally {
                holdActive = false;
                lock.writeLock().unlock();
                holdReleased.countDown();
            }
            log.info("Arm {}: trigger fired ({}), produce lock released", name, produceLockTimeouts());

            // the retry, which must succeed - otherwise the count below is of a record still failing rather than
            // of one that was retried across the window
            committed.awaitAllVisible(resultsFor(UniLists.of(victimKey)), PROGRESS_TIMEOUT);

            assertWithMessage("arm %s shut down or failed during the trigger, so nothing below is about the "
                            + "documented retry path", name)
                    .that(pc.isClosedOrFailed())
                    .isFalse();

            return invocationsFor(victimKey);
        }

        /**
         * @return the produce-lock timeouts that do NOT carry {@code expected} - empty is the assertion, and a
         *         non-empty result names the offenders rather than only reporting a count mismatch
         */
        private List<String> timeoutsNotSaying(String expected) {
            return produceLockTimeouts().stream()
                    .filter(failure -> !failure.contains(expected))
                    .collect(Collectors.toList());
        }

        private void assertTheProduceLockWasTakenBeforeTheUserFunction() {
            assertWithMessage("with eager processing DISABLED the worker must already hold the produce lock when "
                            + "the user function runs - processAndProduceResults takes it first. Hold counts "
                            + "sampled inside the user function were %s", produceLockHoldInsideUserFunction)
                    .that(new HashSet<>(produceLockHoldInsideUserFunction))
                    .containsExactly(1);

            assertWithMessage("with eager processing DISABLED every produce-lock timeout must be the one raised "
                    + "BEFORE the user function could start")
                    .that(timeoutsNotSaying(BEFORE_THE_USER_FUNCTION))
                    .isEmpty();
        }

        private void assertTheProduceLockWasTakenAfterTheUserFunction() {
            assertWithMessage("with eager processing ENABLED the worker must NOT hold the produce lock while the "
                            + "user function runs - it is taken at produce time, after the function returns. Hold "
                            + "counts sampled inside the user function were %s", produceLockHoldInsideUserFunction)
                    .that(new HashSet<>(produceLockHoldInsideUserFunction))
                    .containsExactly(0);

            assertWithMessage("with eager processing ENABLED every produce-lock timeout must be the one raised "
                    + "AFTER the user function returned")
                    .that(timeoutsNotSaying(AFTER_THE_USER_FUNCTION))
                    .isEmpty();

            // the lock must still have been taken by produce time, or the send would have been refused by
            // ProducerManager#ensureProduceStarted - so "0 inside the function" plus a committed result is
            // "taken after the user function returned" rather than "never taken"
            assertWithMessage("the eager arm never produced a result for the victim, so 'the lock is taken at "
                    + "produce time' is unsupported - a send without the produce lock would have been refused")
                    .that(committed.consumed())
                    .contains(resultFor(victimKey));
        }

        /**
         * The output side of the claim: what eager processing costs is the user's SIDE EFFECTS, not duplicate
         * output. The discarded attempt failed before {@code produceMessages}, so each input record still has
         * exactly one result on the topic.
         */
        private void assertEachResultExistsExactlyOnce() {
            long deadline = System.nanoTime() + STRAGGLER_WINDOW.toNanos();
            do {
                committed.poll();
            } while (System.nanoTime() < deadline);

            List<String> expected = new ArrayList<>(primedKeys);
            expected.add(victimKey);

            List<String> actual = committed.consumedRecords().stream()
                    .filter(record -> !record.value().equals(warmupMarker))
                    .map(ConsumerRecord::key)
                    .collect(Collectors.toList());

            assertWithMessage("arm %s produced a result more than once for some input record. The replay this "
                            + "test is about is of the user function's SIDE EFFECTS - the discarded attempt never "
                            + "reached the produce step, so the output topic must still hold one result per input "
                            + "record", name)
                    .that(actual)
                    .containsExactlyElementsIn(expected);
        }
    }
}
