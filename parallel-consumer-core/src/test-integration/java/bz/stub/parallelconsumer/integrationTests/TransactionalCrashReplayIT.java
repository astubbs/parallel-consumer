package bz.stub.parallelconsumer.integrationTests;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.ParallelEoSStreamProcessor;
import bz.stub.parallelconsumer.internal.utils.ThreadUtils;
import bz.stub.parallelconsumer.ProvesClaim;
import bz.stub.parallelconsumer.TransactionalClaim;
import bz.stub.parallelconsumer.integrationTests.utils.KafkaClientUtils.GroupOption;
import bz.stub.parallelconsumer.integrationTests.utils.TransactionalTopicVerifier;
import bz.stub.parallelconsumer.integrationTests.utils.WorkerFunctionFailureCapture;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InvalidProducerEpochException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import pl.tlinkowski.unij.api.UniSets;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.truth.Truth.assertWithMessage;
import static bz.stub.parallelconsumer.ParallelConsumerOptions.CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER;
import static bz.stub.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.UNORDERED;
import static bz.stub.parallelconsumer.integrationTests.utils.KafkaClientUtils.ProducerMode.TRANSACTIONAL;
import static java.time.Duration.ofMillis;
import static java.time.Duration.ofMinutes;
import static java.time.Duration.ofSeconds;
import static org.apache.commons.lang3.RandomUtils.nextInt;
import static org.awaitility.Awaitility.await;

/**
 * The two guarantees users actually buy exactly-once for, proved against a real broker by crashing an instance
 * mid-transaction and letting a replacement pick the work back up: C3 (a failed attempt is never visible and is
 * retried in <em>new</em> transactions, possibly regrouped) and C4 (a source offset and the records produced from
 * it commit together or not at all), plus C14 - the README's promise that "even under failure, the results will
 * exist exactly once in the Kafka output topic".
 *
 * <h2>Three mechanisms make this class a proof rather than theatre</h2>
 *
 * <b>1. There is no crash facility, and both close paths defeat the test.</b>
 * {@code AbstractParallelEoSStreamProcessor#innerDoClose} calls {@code commitOffsetsThatAreReady()} before it
 * touches the producer, so {@code closeDrainFirst()} and {@code closeDontDrainFirst()} both COMMIT the pending
 * transaction - the exact opposite of a crash. The crash here is therefore: never call {@code close()}, leave the
 * instance's transaction open, and let the replacement instance fence it.
 * <p>
 * <b>2. A STABLE {@code transactional.id}, or the test passes for the wrong reason.</b>
 * {@code KafkaClientUtils} gives every producer a random {@code transactional.id} by default, so a restarted
 * instance would never fence its predecessor. An unfenced abandoned transaction stays open and pins the last
 * stable offset for the whole {@code transaction.timeout.ms} window - and then a {@code read_committed} verifier
 * sees neither the abandoned records nor the replayed ones, so "no output record is visible" passes because
 * <em>everything</em> is invisible. Both instances here share one {@code transactional.id}, the timeout is set
 * long enough ({@link #ABANDONED_TRANSACTION_TIMEOUT}) that no assertion could be satisfied by waiting it out, and
 * {@link #assertTheAbandonedProducerWasFenced()} asserts the fencing happened instead of assuming it.
 * <p>
 * <b>3. One priming record, because an instance's FIRST commit ignores its commit interval.</b>
 * {@code isTimeToCommitNow()} treats a null {@code lastCommitTime} as a day elapsed, so however long the interval,
 * every instance commits once as soon as anything is dirty. A test that set a long interval and assumed "therefore
 * it never commits" would watch part of its supposedly-abandoned transaction become visible - it did here, on the
 * first run of this class, and cost eight records. So the first attempt is fed exactly ONE record
 * ({@link #PRIMING_RECORDS}), that startup commit is awaited and asserted, and only then is the payload produced -
 * into a transaction that now genuinely cannot commit before the crash.
 *
 * <h2>What is counted, and what is not</h2>
 * Never offsets. Commit and abort markers occupy offsets on a transactional topic, so record counts come from
 * {@link TransactionalTopicVerifier}, which counts what it consumed. Offsets are read only where they mean an
 * offset - the consumer group's committed position on the non-transactional INPUT topic, which is the other half
 * of C4's atomic pair. The one place output offsets are used is {@link #segmentByTransaction}, which reads the
 * <em>gaps</em> the markers leave to find transaction boundaries; it never treats an offset as a count.
 *
 * @author Antony Stubbs
 * @see TransactionalVisibilityIT
 * @see TransactionalClaim
 */
@Tag("transactions")
@Slf4j
// Per-method timeout, following DrainingMemberRebalanceIT and the rest of this package. Without one, a thread
// blocked on a broker call or a lock becomes a job that runs to the CI-level timeout with no failing test to
// point at. Comfortably above this class's own budget: crashAndReplay chains several 120s awaits - the priming
// commit, the source offset, the read_uncommitted control, FENCING_TIMEOUT, the replay, the replayed offset -
// plus the straggler window. Deliberately well under ABANDONED_TRANSACTION_TIMEOUT's role in the class: this is a
// backstop for a hang, not a second budget the arms are expected to approach.
@Timeout(900)
class TransactionalCrashReplayIT extends BrokerIntegrationTest<String, String> {

    private static final String ATTEMPT_ONE = "attempt-1";

    private static final String ATTEMPT_TWO = "attempt-2";

    private static final String WARMUP = "warmup";

    /**
     * Spends the first instance's unavoidable startup commit - see the class javadoc. Exactly one, so that the
     * commit cannot land half way through a set: the first moment any work is dirty is the moment this one record
     * succeeded, so this record is what that commit contains.
     */
    private static final int PRIMING_RECORDS = 1;

    /**
     * The payload volume for {@link #outputHoldsEachResultExactlyOnceAcrossTheReplayWhenBatching()}, and for that
     * arm alone. The produce-lock double-release defect it regression-tests was reported as 3/3 redeliveries at 6
     * records and 5/5 at 200, so this keeps the batched arm comfortably inside the defect's observed range - it is
     * calibration, not scale, and lowering it would weaken the regression test.
     */
    private static final int PAYLOAD_RECORDS = 200;

    /**
     * The payload for every arm whose mechanics do not depend on volume - crash, fence, replay and the exactly-once
     * multiset at {@code batchSize=1}. All of those need enough records for the replay to be a real replay and
     * nothing more, so they pay the smaller volume; only the batching arm above needs the calibrated one.
     */
    private static final int REPLAY_PAYLOAD_RECORDS = 40;

    /**
     * The payload for the recombination arm, which needs the replay to be slow enough to span several commit
     * intervals rather than to be large. Deliberately separate from {@link #REPLAY_PAYLOAD_RECORDS} even at the
     * same value, because what constrains it is {@link #RECOMBINATION_WORK_DELAY} x this count spanning several
     * {@link #REPLAY_COMMIT_INTERVAL}s.
     */
    private static final int RECOMBINATION_PAYLOAD_RECORDS = 40;

    private static final int MAX_CONCURRENCY = 4;

    /**
     * {@code transaction.timeout.ms} for both instances. Deliberately far longer than this test runs: if the
     * abandoned transaction could reap itself, every "not visible" assertion would have a second, uninteresting
     * explanation, and the prompt visibility of the replay would prove nothing about fencing.
     */
    private static final Duration ABANDONED_TRANSACTION_TIMEOUT = ofMinutes(5);

    /**
     * The first instance's commit interval. Longer than the test, so that after its one startup commit the
     * instance can only ever be abandoned mid-transaction.
     */
    private static final Duration ONE_COMMIT_ONLY = ofMinutes(10);

    private static final Duration REPLAY_COMMIT_INTERVAL = ofMillis(200);

    /**
     * Per-record dwell used only by the recombination arm, to spread the replay across several
     * {@link #REPLAY_COMMIT_INTERVAL}s so that its transaction grouping can actually differ from the original.
     */
    private static final Duration RECOMBINATION_WORK_DELAY = ofMillis(40);

    private static final Duration FENCING_TIMEOUT = ofSeconds(120);

    private static final Duration PROGRESS_TIMEOUT = ofSeconds(120);

    /**
     * How long a once-observed offset-without-records state is given to resolve before it counts as a violation.
     * Generous against what it is absorbing - concurrent transaction-marker delivery and one consumer fetch - and
     * far short of {@link #PROGRESS_TIMEOUT}, so a genuine one-sided commit still fails this arm rather than
     * timing the whole test out with a vaguer message.
     */
    private static final Duration ONE_SIDED_GRACE = ofSeconds(15);

    /**
     * How long the output topic is watched for stragglers after the replay has demonstrably finished (every
     * expected result visible AND the source offset committed). A duplicate produced by a redelivery lands after
     * the record it duplicates, so an exactly-once assertion made the instant the last expected record arrives
     * could miss it.
     */
    private static final Duration STRAGGLER_WINDOW = ofSeconds(5);

    private final Map<String, AtomicInteger> userFunctionInvocations = new ConcurrentHashMap<>();

    private String inputTopic;

    private String outputTopic;

    /**
     * Shared by both instances - see the class javadoc. Unique per test so that concurrently running methods in
     * this class never fence each other.
     */
    private String transactionalId;

    private String instanceNonce;

    private String primedKeyPrefix;

    private String payloadKeyPrefix;

    private int payloadRecords;

    private List<String> primedKeys;

    private List<String> payloadKeys;

    private String warmupMarker;

    private TransactionalTopicVerifier committed;

    private TransactionalTopicVerifier uncommitted;

    private WorkerFunctionFailureCapture workerFailures;

    private ParallelEoSStreamProcessor<String, String> abandoned;

    private ParallelEoSStreamProcessor<String, String> replacement;

    // Fixture

    /**
     * The invariant every arm in this class rests on: the awaits between abandoning a transaction and reading the
     * replayed results cannot run long enough for the broker to reap that transaction by itself.
     * <p>
     * Asserted on the constants, once per test, rather than on a measured elapsed time. Both waits throw the
     * instant they expire, so comparing the measured elapsed against {@link #ABANDONED_TRANSACTION_TIMEOUT} after
     * the fact could never be seen false - a guard nobody has watched fail is decoration. What can actually change
     * is the budget: widen either await past the abandoned transaction's own timeout and a broker reap becomes an
     * alternative explanation for the replay's prompt visibility, at which point every arm here proves less than
     * it claims.
     * <p>
     * A {@code @BeforeEach} rather than a call inside {@link #crashAndReplay}, because
     * {@link #abandonedTransactionIsInvisibleAndTheReplacementFencesItsProducer()} inlines the same
     * fence-then-await sequence instead of going through it. The constants are class-wide, so the check belongs
     * where every arm passes - including one written later that uses neither helper.
     */
    @BeforeEach
    void theAwaitBudgetMustNotReachTheAbandonedTransactionTimeout() {
        Duration worstCaseFromFencingToVisible = FENCING_TIMEOUT.plus(PROGRESS_TIMEOUT);
        assertWithMessage("this class's own await budget (%s fencing + %s progress) has grown to meet or exceed the "
                        + "abandoned transaction's timeout of %s. A run can now reach the point where the broker "
                        + "has reaped the abandoned transaction by itself, which would make the replayed results "
                        + "visible without any replacement having fenced anything - so either shorten the awaits or "
                        + "lengthen ABANDONED_TRANSACTION_TIMEOUT", FENCING_TIMEOUT, PROGRESS_TIMEOUT,
                ABANDONED_TRANSACTION_TIMEOUT)
                .that(worstCaseFromFencingToVisible.compareTo(ABANDONED_TRANSACTION_TIMEOUT) < 0)
                .isTrue();
    }

    /**
     * Input and output topics, the priming record, a committed warm-up record on the output topic, and two
     * verifiers that have both been proved live and caught up to it.
     * <p>
     * The warm-up record is committed by a producer with its own random {@code transactional.id}, so it fences
     * nothing and is not part of the crash story - its only job is to be the marker that
     * {@link TransactionalTopicVerifier#requireLiveAndCaughtUp} needs, since an absence asserted before the
     * verifier has read anything from the output topic is vacuous.
     * <p>
     * The payload is NOT produced here: it goes in only once the first instance has spent its startup commit -
     * see {@link #runFirstAttemptAndAbandonIt}.
     */
    @SneakyThrows
    private void prepare(String scenario, int payloadCount) {
        instanceNonce = scenario + "-" + nextInt();
        transactionalId = "crash-replay-" + instanceNonce;
        primedKeyPrefix = scenario + "-primed-";
        payloadKeyPrefix = scenario + "-payload-";
        payloadRecords = payloadCount;

        inputTopic = setupTopic(getClass().getSimpleName() + "-" + scenario + "-input");
        outputTopic = setupTopic(getClass().getSimpleName() + "-" + scenario + "-output");

        primedKeys = getKcu().produceMessages(inputTopic, PRIMING_RECORDS, primedKeyPrefix);

        Producer<String, String> warmupProducer = register(getKcu().createAndInitNewTransactionalProducer());
        warmupMarker = WARMUP + "-" + instanceNonce;
        warmupProducer.beginTransaction();
        warmupProducer.send(new ProducerRecord<>(outputTopic, warmupMarker, warmupMarker));
        warmupProducer.commitTransaction();

        committed = register(TransactionalTopicVerifier.readCommitted(getKcu(), scenario, outputTopic));
        uncommitted = register(TransactionalTopicVerifier.readUncommitted(getKcu(), scenario, outputTopic));
        committed.requireLiveAndCaughtUp(warmupMarker);
        uncommitted.requireLiveAndCaughtUp(warmupMarker);

        workerFailures = register(new WorkerFunctionFailureCapture(instanceNonce));
    }

    private static List<String> expectedResults(String attemptTag, List<String> keys) {
        return keys.stream()
                .map(key -> resultValue(attemptTag, key))
                .collect(Collectors.toList());
    }

    private static String resultValue(String attemptTag, String inputKey) {
        return attemptTag + "|" + inputKey;
    }

    /**
     * The value prefix identifying one attempt's results for the payload records - the records the crash is about.
     * The priming record's result carries the same attempt tag but a different key prefix, which is what keeps it
     * out of every assertion below.
     */
    private String resultPrefix(String attemptTag) {
        return attemptTag + "|" + payloadKeyPrefix;
    }

    /**
     * Builds an instance but does NOT start it consuming - construction alone initialises its transaction
     * session, which is what fences the previous holder of {@link #transactionalId}. Keeping those two moments
     * separate is what lets the replacement fence its predecessor <em>before</em> the predecessor has released
     * the consumer group.
     */
    private ParallelEoSStreamProcessor<String, String> buildInstance(int instanceNumber,
                                                                     int batchSize,
                                                                     Duration commitInterval,
                                                                     GroupOption groupOption) {
        KafkaProducer<String, String> producer = getKcu()
                .createNewProducer(TRANSACTIONAL, ABANDONED_TRANSACTION_TIMEOUT, Optional.of(transactionalId));
        KafkaConsumer<String, String> consumer = getKcu().createNewConsumer(groupOption);

        var pc = new ParallelEoSStreamProcessor<>(ParallelConsumerOptions.<String, String>builder()
                .consumer(consumer)
                .producer(producer)
                .commitMode(PERIODIC_TRANSACTIONAL_PRODUCER)
                .ordering(UNORDERED)
                .maxConcurrency(MAX_CONCURRENCY)
                .batchSize(batchSize)
                .commitInterval(commitInterval)
                .build());
        // names this instance's worker threads, which is how WorkerFunctionFailureCapture tells our worker
        // failures from those of any other test sharing this JVM
        pc.setMyId(Optional.of(instanceNonce + "-" + instanceNumber));
        pc.subscribe(UniSets.of(inputTopic));
        return register(pc);
    }

    private void start(ParallelEoSStreamProcessor<String, String> pc, String attemptTag, Duration workDelay) {
        log.info("Starting instance {} for attempt {}", pc.getMyId(), attemptTag);
        pc.pollAndProduceMany(context -> {
            List<ProducerRecord<String, String>> results = new ArrayList<>();
            for (ConsumerRecord<String, String> record : context.getConsumerRecordsFlattened()) {
                userFunctionInvocations.computeIfAbsent(record.key(), ignored -> new AtomicInteger())
                        .incrementAndGet();
                if (!workDelay.isZero()) {
                    ThreadUtils.sleepQuietly(workDelay);
                }
                results.add(new ProducerRecord<>(outputTopic, record.key(), resultValue(attemptTag, record.key())));
            }
            return results;
        });
    }

    // The crash, and the replay

    /**
     * Spends the first instance's startup commit on the priming record, then feeds it the payload and walks away
     * without closing it - leaving every payload result acknowledged by the broker and none of them committed.
     */
    @SneakyThrows
    private void runFirstAttemptAndAbandonIt(int batchSize, Duration workDelay) {
        abandoned = buildInstance(1, batchSize, ONE_COMMIT_ONLY, GroupOption.NEW_GROUP);
        start(abandoned, ATTEMPT_ONE, workDelay);

        // the startup commit, awaited rather than assumed: until it has landed, the instance still has one
        // interval-free commit in hand and could commit part of the payload
        committed.awaitAllVisible(expectedResults(ATTEMPT_ONE, primedKeys), PROGRESS_TIMEOUT);
        awaitSourceOffsetCommittedAt(PRIMING_RECORDS);

        payloadKeys = getKcu().produceMessages(inputTopic, payloadRecords, payloadKeyPrefix);

        // the control arm: the results really are on the broker, inside a transaction nobody has committed
        uncommitted.awaitAllVisible(expectedResults(ATTEMPT_ONE, payloadKeys), PROGRESS_TIMEOUT);

        committed.assertNeverVisible(resultPrefix(ATTEMPT_ONE));

        assertWithMessage("the abandoned instance committed a source offset past the priming record, so its "
                + "payload transaction was not abandoned mid-flight and nothing below would be about a failed "
                + "transaction at all")
                .that(committedSourceOffset())
                .isEqualTo(OptionalLong.of(PRIMING_RECORDS));

        log.info("Attempt one abandoned: {} results sent into an uncommitted transaction, source offset still at {}",
                payloadKeys.size(), PRIMING_RECORDS);
    }

    /**
     * Asserts the fencing that the whole class rests on: after a replacement has taken over the
     * {@code transactional.id}, the abandoned instance is REFUSED when it tries to commit.
     * <p>
     * Asking the abandoned instance to commit is the point. Left alone it would sit on an open transaction and
     * the test could only assume it had been fenced; poking it turns the assumption into an observation, and the
     * refusal is also what makes the abandoned instance release the consumer group so the replacement can start.
     */
    private void assertTheAbandonedProducerWasFenced() {
        abandoned.requestCommitAsap();

        await().atMost(FENCING_TIMEOUT)
                .pollInterval(ofMillis(200))
                .until(abandoned::isClosedOrFailed);

        Exception failure = abandoned.getFailureCause();
        assertWithMessage("the abandoned instance was asked to commit AFTER a replacement took over its "
                + "transactional.id '%s', and it shut down without a failure cause - so it was never fenced. "
                + "Every absence assertion in this test would then be about an open transaction pinning the "
                + "last stable offset, not about the guarantee", transactionalId)
                .that(failure)
                .isNotNull();

        Set<String> chain = causeChainTypes(failure);
        assertWithMessage("the abandoned instance failed, but not with the broker refusing a zombie producer. "
                + "Cause chain was %s. Without that refusal there is no evidence the replacement fenced its "
                + "predecessor rather than merely joining alongside it", chain)
                .that(chain)
                .containsAnyOf(ProducerFencedException.class.getName(),
                        InvalidProducerEpochException.class.getName());

        log.info("Abandoned instance was fenced as expected: {}", chain);
    }

    /**
     * The full crash-and-replay: attempt one sends the whole payload and is abandoned mid-transaction; the
     * replacement fences it, takes the partition back, and reprocesses from the last committed offset - which is
     * the priming record, so the whole payload is replayed.
     */
    private void crashAndReplay(int batchSize, Duration workDelay) {
        runFirstAttemptAndAbandonIt(batchSize, workDelay);

        // constructing the replacement initialises the shared transactional.id - this is the fencing act
        replacement = buildInstance(2, batchSize, REPLAY_COMMIT_INTERVAL, GroupOption.REUSE_GROUP);
        long fencedAt = System.nanoTime();

        assertTheAbandonedProducerWasFenced();

        start(replacement, ATTEMPT_TWO, workDelay);

        awaitResultsAndOffsetLandTogether(expectedResults(ATTEMPT_TWO, payloadKeys),
                PRIMING_RECORDS + payloadRecords);
        Duration untilVisible = Duration.ofNanos(System.nanoTime() - fencedAt);

        log.info("Replay complete: every result visible {} after the fencing, source offset committed at {}",
                untilVisible, PRIMING_RECORDS + payloadRecords);
    }

    // Offsets - read only where an offset means an offset

    /**
     * @return the consumer group's committed position for the INPUT partition, or empty if it has never
     *         committed. The input topic is written by a plain producer and carries no transaction markers, so
     *         this is a position, not a record count.
     */
    @SneakyThrows
    private OptionalLong committedSourceOffset() {
        Map<TopicPartition, OffsetAndMetadata> offsets = getKcu().getAdmin()
                .listConsumerGroupOffsets(getKcu().getGroupId())
                .partitionsToOffsetAndMetadata()
                .get(30, TimeUnit.SECONDS);
        OffsetAndMetadata offset = offsets.get(new TopicPartition(inputTopic, 0));
        return offset == null ? OptionalLong.empty() : OptionalLong.of(offset.offset());
    }

    /**
     * The atomicity assertion C4 actually rests on: sample the produced records and the committed source offset
     * <b>together</b>, and fail the moment the offset is seen to have landed without them.
     * <p>
     * <b>Why the obvious version does not prove the claim.</b> This used to be two sequential awaits - wait until
     * every result is visible, then wait until the offset reaches its target. An implementation that published the
     * results and committed the offset separately, with an arbitrarily wide window between them, satisfies both:
     * each eventually finishes inside its own timeout, and nothing ever looks at the pair. So the test could not
     * reject the one-sided state that the claim forbids, and C4 read {@code PROVED} on evidence that could not
     * fail. Codex review on astubbs#262 found that.
     * <p>
     * <b>The dangerous direction is offset-without-records</b>, and it is the only one asserted. A committed source
     * offset with its results not yet visible is the shape of silent data loss: a consumer restarting from that
     * offset skips work whose output nobody can see. Records-without-offset is the safe direction and is expected
     * transiently, because the offset is read through the admin API's consumer-group view, which lags the
     * transaction the records arrived in.
     * <p>
     * <b>A single observation of offset-without-records is NOT a violation, and an earlier revision of this helper
     * failed on one - which would have accused correct implementations.</b> Committing a transaction makes the
     * coordinator fan {@code WriteTxnMarkers} out concurrently to every partition it touched - the output topic's
     * AND {@code __consumer_offsets} - on different brokers, in no defined order. The group coordinator
     * materialises the offset when ITS marker lands; the output topic's last-stable-offset advances when ITS
     * marker lands. Neither ordering is guaranteed, so offset-first is an ordinary broker state. On top of that,
     * {@code consumed()} is what this verifier has POLLED, not what the broker has made visible, so one poll that
     * fetches nothing proves nothing at all.
     * <p>
     * What separates the two is whether it RESOLVES. A marker skew or a slow fetch closes within a moment; an
     * implementation that genuinely commits the offset without its records never does. So the first sighting opens
     * a bounded grace window and only its expiry is the failure - which keeps every falsifying case and drops the
     * false accusations. The read order is still offset-first, records-second, because that at least means the
     * grace window starts from a state actually observed rather than one inferred across the gap.
     * <p>
     * Shared by every {@link #crashAndReplay} caller, not just C4's: the pairing has to hold on every replay, and a
     * guard that only ran for the claim that names it would be the weaker instrument.
     * <p>
     * <b>What it does NOT check.</b> Only the TERMINAL pairing. The replay commits many transactions at
     * {@link #REPLAY_COMMIT_INTERVAL}, and the predicate is the final expected offset, so an implementation that
     * ran ahead on an intermediate transaction and then landed the last one correctly is not caught here. Nor is
     * records-without-offset asserted: that is the safe direction, and the concurrent-marker behaviour above makes
     * it indistinguishable from ordinary lag.
     * <p>
     * <b>Hand-rolled rather than awaitility, and that is not an oversight.</b> {@code await().untilAsserted(...)}
     * RETRIES a failing assertion, so it would swallow the single observation this whole helper exists to make -
     * the violation would be re-polled until it stopped being true, and the test would pass. Same reason
     * {@code TransactionalBatchVisibilityIT#watchUntilAllVisible} writes its own loop.
     */
    private void awaitResultsAndOffsetLandTogether(List<String> expectedResults, long expectedOffset) {
        Instant deadline = Instant.now().plus(PROGRESS_TIMEOUT);
        boolean resultsLanded = false;
        OptionalLong lastOffset = OptionalLong.empty();

        while (Instant.now().isBefore(deadline)) {
            // offset first, records second - see the javadoc; this ordering can only under-report
            OptionalLong offset = committedSourceOffset();
            committed.poll();
            List<String> visible = committed.consumed();

            boolean offsetLanded = offset.isPresent() && offset.getAsLong() == expectedOffset;
            resultsLanded = visible.containsAll(expectedResults);
            lastOffset = offset;

            if (offsetLanded && !resultsLanded) {
                // the grace window - see the javadoc; one sighting is marker skew until it fails to resolve
                resultsLanded = resultsBecomeVisibleWithin(expectedResults, ONE_SIDED_GRACE);
                if (!resultsLanded) {
                    List<String> missing = missingFrom(committed.consumed(), expectedResults);
                    assertWithMessage("the source offset reached %s and %s of its %s results were STILL invisible "
                                    + "to a read_committed consumer %s later - the offset and the records it "
                                    + "accounts for are supposed to land as one atomic set, and a consumer "
                                    + "restarting here would skip work whose output nobody can see. Missing: %s",
                            expectedOffset, missing.size(), expectedResults.size(), ONE_SIDED_GRACE, missing)
                            .fail();
                }
            }

            if (offsetLanded) {
                return;
            }
            ThreadUtils.sleepQuietly(ofMillis(200));
        }

        committed.poll();
        assertWithMessage("the replay never completed: source offset was %s, expected exactly %s; still missing %s "
                        + "of %s results: %s",
                lastOffset, expectedOffset, missingFrom(committed.consumed(), expectedResults).size(),
                expectedResults.size(), missingFrom(committed.consumed(), expectedResults))
                .fail();
    }

    /**
     * @return whether every expected result became visible inside {@code grace}. Polls rather than sleeping, so a
     *         resolution is seen as soon as it happens; used only to tell marker skew from a real one-sided commit.
     */
    private boolean resultsBecomeVisibleWithin(List<String> expectedResults, Duration grace) {
        Instant graceDeadline = Instant.now().plus(grace);
        while (Instant.now().isBefore(graceDeadline)) {
            committed.poll();
            if (committed.consumed().containsAll(expectedResults)) {
                return true;
            }
            ThreadUtils.sleepQuietly(ofMillis(200));
        }
        return false;
    }

    private static List<String> missingFrom(List<String> visible, List<String> expected) {
        return expected.stream().filter(e -> !visible.contains(e)).collect(Collectors.toList());
    }


    private void awaitSourceOffsetCommittedAt(long expected) {
        await().atMost(PROGRESS_TIMEOUT)
                .pollInterval(ofMillis(500))
                .untilAsserted(() -> assertWithMessage("the source offset never reached %s - the produced "
                        + "records and the offset that accounts for them are supposed to land as one atomic set",
                        expected)
                        .that(committedSourceOffset())
                        .isEqualTo(OptionalLong.of(expected)));
    }

    // Structure of the log: where one transaction ends and the next begins

    /**
     * Split consumed records into transactions using the offsets the commit and abort markers occupy.
     * <p>
     * A consumer never returns control records, so a transaction's own records are contiguous while the boundary
     * between two transactions always shows up as a gap of at least one offset. This reads the gaps as structure;
     * it never reads an offset as a count, which is the mistake that would silently inflate every "how many?"
     * answer on a transactional topic. Only sound because this topic has exactly one writer at a time.
     *
     * @param records consumed in offset order from a single partition
     */
    private static List<List<ConsumerRecord<String, String>>> segmentByTransaction(
            List<ConsumerRecord<String, String>> records) {
        List<List<ConsumerRecord<String, String>>> segments = new ArrayList<>();
        List<ConsumerRecord<String, String>> current = new ArrayList<>();
        long previousOffset = -1;
        for (ConsumerRecord<String, String> record : records) {
            if (!current.isEmpty() && record.offset() != previousOffset + 1) {
                segments.add(current);
                current = new ArrayList<>();
            }
            current.add(record);
            previousOffset = record.offset();
        }
        if (!current.isEmpty()) {
            segments.add(current);
        }
        return segments;
    }

    /**
     * @return the key sets of the transactions that carried results matching {@code valuePrefix}, in log order
     */
    private static List<Set<String>> transactionKeySetsFor(List<ConsumerRecord<String, String>> records,
                                                           String valuePrefix) {
        return segmentByTransaction(records).stream()
                .map(segment -> segment.stream()
                        .filter(record -> record.value().startsWith(valuePrefix))
                        .map(ConsumerRecord::key)
                        .collect(Collectors.toCollection(LinkedHashSet<String>::new)))
                .filter(keys -> !keys.isEmpty())
                .collect(Collectors.toList());
    }

    // Helpers

    private static Set<String> causeChainTypes(Throwable throwable) {
        Set<String> types = new LinkedHashSet<>();
        for (Throwable t = throwable; t != null && !types.contains(t.getClass().getName()); t = t.getCause()) {
            types.add(t.getClass().getName());
        }
        return types;
    }

    private void drainStragglers() {
        long deadline = System.nanoTime() + STRAGGLER_WINDOW.toNanos();
        do {
            committed.poll();
        } while (System.nanoTime() < deadline);
    }

    /**
     * @return the key of every visible result record, in log order and WITH repeats - the multiset the
     *         exactly-once claim is about. The warm-up marker is excluded: it is fixture, not a result.
     */
    private List<String> visibleResultKeys() {
        return committed.consumedRecords().stream()
                .filter(record -> !record.value().equals(warmupMarker))
                .map(ConsumerRecord::key)
                .collect(Collectors.toList());
    }

    /**
     * @return every input key exactly once - what the output topic must hold after the replay, regardless of
     *         which attempt produced each result
     */
    private List<String> allInputKeys() {
        return Stream.concat(primedKeys.stream(), payloadKeys.stream()).collect(Collectors.toList());
    }

    private List<String> keysProcessedMoreThanOnce() {
        return userFunctionInvocations.entrySet().stream()
                .filter(entry -> entry.getValue().get() > 1)
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());
    }

    private void assertNoWorkerThreadFailures() {
        assertWithMessage("the worker path threw, and nothing in main reads WorkContainer#future, so this "
                + "would otherwise have been invisible. PC's catch-all fails the WHOLE batch on such an "
                + "exception and redelivers records that had already succeeded")
                .that(workerFailures.describe())
                .isEmpty();
    }

    // Tests

    /**
     * C3, first half, and C4's negative side: an instance abandoned after its records were sent but before its
     * transaction committed leaves NOTHING visible, and no source offset committed for that work either - and the
     * replacement that takes over its {@code transactional.id} is observed to fence it.
     * <p>
     * The {@code read_uncommitted} control arm is what makes the absence a proof: it sees the very records the
     * {@code read_committed} arm cannot, so "absent" is the isolation level's doing rather than a send that never
     * happened. The fencing assertion is what stops the absence being explained by an open transaction that
     * simply pinned the last stable offset - see the class javadoc.
     */
    @Test
    @ProvesClaim({TransactionalClaim.FAILURE_INVISIBLE_AND_RECOMBINED,
            TransactionalClaim.OFFSET_AND_RECORDS_ATOMIC})
    void abandonedTransactionIsInvisibleAndTheReplacementFencesItsProducer() {
        prepare("crash", REPLAY_PAYLOAD_RECORDS);

        runFirstAttemptAndAbandonIt(1, Duration.ZERO);

        replacement = buildInstance(2, 1, REPLAY_COMMIT_INTERVAL, GroupOption.REUSE_GROUP);
        assertTheAbandonedProducerWasFenced();

        // the fenced instance's transaction is resolved as aborted, so the last stable offset advances and the
        // verifier can read PAST the abandoned region. Seeing the replay's results is what proves it read past -
        // "it never saw the abandoned ones" is then a statement about records it had every chance to see
        start(replacement, ATTEMPT_TWO, Duration.ZERO);
        committed.awaitAllVisible(expectedResults(ATTEMPT_TWO, payloadKeys), PROGRESS_TIMEOUT);

        committed.assertNoneSeen(resultPrefix(ATTEMPT_ONE));
        assertNoWorkerThreadFailures();
    }

    /**
     * C4: the replayed results and the source offset that accounts for them land together.
     * <p>
     * Both halves are asserted at both ends of the crash. Before: no payload result visible AND the source offset
     * still on the priming record. After: every result visible AND the offset moved to the end of the input. A
     * system that committed the offset without the records fails the pairing this asserts. The other direction,
     * records without the offset, is deliberately not asserted - see
     * {@link #awaitResultsAndOffsetLandTogether}, whose javadoc records why concurrent marker delivery makes it
     * indistinguishable from ordinary lag. A system failing the remaining halves fails one of the
     * four.
     */
    @Test
    @ProvesClaim(TransactionalClaim.OFFSET_AND_RECORDS_ATOMIC)
    void replayCommitsTheResultsAndTheirSourceOffsetTogether() {
        prepare("atomic", REPLAY_PAYLOAD_RECORDS);

        crashAndReplay(1, Duration.ZERO);

        committed.assertNoneSeen(resultPrefix(ATTEMPT_ONE));
        assertNoWorkerThreadFailures();
    }

    /**
     * C3, second half: the replay is free to REGROUP. The claim is not that the retry reproduces the original
     * transaction, only that what it commits is a valid set of results for the offsets it replayed.
     * <p>
     * Which is what is asserted: the abandoned attempt put every payload result in ONE transaction, the replay
     * spreads the same results over several, and the union of the replay's transactions is exactly the expected
     * set. A per-transaction comparison against the original would fail here, and should - it is not the
     * guarantee.
     */
    @Test
    @ProvesClaim(TransactionalClaim.FAILURE_INVISIBLE_AND_RECOMBINED)
    void theReplayRecombinesTheSameResultsIntoDifferentTransactions() {
        prepare("recombine", RECOMBINATION_PAYLOAD_RECORDS);

        crashAndReplay(1, RECOMBINATION_WORK_DELAY);

        // make sure the control arm has read the whole log, both attempts, before its structure is examined
        uncommitted.awaitAllVisible(expectedResults(ATTEMPT_TWO, payloadKeys), PROGRESS_TIMEOUT);

        List<Set<String>> original = transactionKeySetsFor(uncommitted.consumedRecords(),
                resultPrefix(ATTEMPT_ONE));
        List<Set<String>> retry = transactionKeySetsFor(uncommitted.consumedRecords(),
                resultPrefix(ATTEMPT_TWO));

        assertWithMessage("the abandoned attempt was supposed to hold every payload result in one open "
                + "transaction, so that the replay's grouping has something to differ FROM; it was spread over %s",
                original)
                .that(original)
                .hasSize(1);
        assertWithMessage("the abandoned attempt should have held a result for every payload record")
                .that(original.get(0))
                .containsExactlyElementsIn(payloadKeys);

        assertWithMessage("the replay committed its results in a single transaction, so this run cannot "
                + "distinguish 'may regroup' from 'reproduces the original grouping' - the claim is untested "
                + "rather than refuted. Retry grouping was %s", retry)
                .that(retry.size())
                .isAtLeast(2);

        List<String> replayed = retry.stream().flatMap(Set::stream).collect(Collectors.toList());
        assertWithMessage("the replay's transactions, taken together, must be a valid result set for the "
                + "offsets it replayed - every payload record accounted for, once")
                .that(replayed)
                .containsExactlyElementsIn(payloadKeys);

        assertNoWorkerThreadFailures();
    }

    /**
     * C14 - the README's promise, at {@code batchSize=1}: across a crash and a replay, each result exists exactly
     * once in the output topic, even though the INPUT side legitimately reprocesses.
     * <p>
     * The input-side assertion is not decoration. Without it a system that simply never replayed anything would
     * satisfy "no duplicates" trivially; requiring that some record demonstrably ran twice is what makes the
     * output-side assertion a statement about deduplication under replay rather than about a replay that never
     * happened.
     *
     * @see #outputHoldsEachResultExactlyOnceAcrossTheReplayWhenBatching() for the same assertion at
     *         {@code batchSize > 1}, which master fails
     */
    @Test
    @ProvesClaim(TransactionalClaim.RESULTS_EXACTLY_ONCE_UNDER_FAILURE)
    void outputHoldsEachResultExactlyOnceAcrossTheReplay() {
        prepare("exactly-once", REPLAY_PAYLOAD_RECORDS);

        crashAndReplay(1, Duration.ZERO);
        drainStragglers();

        assertWithMessage("the input side never reprocessed anything, so 'no duplicate output' here says "
                + "nothing about replay - the crash did not actually cost any work")
                .that(keysProcessedMoreThanOnce())
                .containsAtLeastElementsIn(payloadKeys);

        assertWithMessage("the output topic must hold each result exactly once at read_committed. Extra copies "
                + "are results whose source record was redelivered AFTER its output had already been committed")
                .that(visibleResultKeys())
                .containsExactlyElementsIn(allInputKeys());

        assertNoWorkerThreadFailures();
    }

    /**
     * The same claim at {@code batchSize > 1}, where master violates it - and the violation is worse than the
     * duplicate this arm was written to catch.
     * <p>
     * {@code AbstractParallelEoSStreamProcessor#addToMailbox} releases the produce lock once per WORK CONTAINER,
     * but {@code processAndProduceResults} acquires it once per {@code PollContextInternal}. With more than one
     * record in the context the second release trips {@code ProducerManager#ensureProduceStarted} - "Need to call
     * #beginProducing first" - and PC's catch-all in {@code runUserFunction} marks the WHOLE batch failed. Then
     * the asymmetry recorded in {@code CONCEPTS.md} takes over: only a success sets a partition dirty,
     * {@code PartitionState#onFailure} is a no-op, and the commit gate ANDs {@code wm.isDirty()} - so a partition
     * whose every batch fails is never dirty and NO COMMIT IS EVER ATTEMPTED.
     * <p>
     * What is observed is therefore a stall, not a duplicate: 5/5 runs at 200 records, with the replacement's
     * source offset frozen at 3 of 201 for the full await while its commit interval was 200ms, and no commit-path
     * error at all - commits were never attempted rather than attempted and failing. The results the README
     * promises "will exist exactly once" never come to exist. The same scenario at {@code batchSize = 1} passes
     * 4/4 on the same machine at the same volume. Full evidence in
     * {@code docs/solutions/test-issues/transactional-batching-stall-produce-lock-released-per-record-2026-08-08.md}.
     * <p>
     * This arm still asserts the multiset, because that is the claim; it simply never reaches it, failing earlier
     * on the source offset.
     * <p>
     * This arm was RED 5/5 before astubbs#257: the produce lock was acquired per {@code PollContext} but released
     * per record, so at {@code batchSize > 1} every batch failed, nothing ever marked the partition dirty, and the
     * instance stopped committing altogether - the source offset froze at 3 of 201. A stall, not the duplicate the
     * fix was originally written to catch. It is enabled now that astubbs#257 is merged in, and is the regression
     * test for that defect: a pipeline that has stopped committing cannot satisfy it.
     */
    @Test
    @ProvesClaim(TransactionalClaim.RESULTS_EXACTLY_ONCE_UNDER_FAILURE)
    void outputHoldsEachResultExactlyOnceAcrossTheReplayWhenBatching() {
        prepare("exactly-once-batched", PAYLOAD_RECORDS);

        crashAndReplay(3, Duration.ZERO);
        drainStragglers();

        assertWithMessage("the input side never reprocessed anything, so 'no duplicate output' here says "
                + "nothing about replay - the crash did not actually cost any work")
                .that(keysProcessedMoreThanOnce())
                .containsAtLeastElementsIn(payloadKeys);

        assertWithMessage("the output topic must hold each result exactly once at read_committed. Worker-path "
                + "failures captured during this run: %s", workerFailures.describe())
                .that(visibleResultKeys())
                .containsExactlyElementsIn(allInputKeys());

        // Not redundant with the message above, which is only read if the multiset assertion has already failed.
        // The defect this arm regression-tests has a worker-path failure as its literal signature - one per batch -
        // and a PARTIAL regression (some batches fail, are retried, and eventually succeed) still leaves each
        // result present exactly once. Without this the arm would go green with the defect live and logged.
        assertNoWorkerThreadFailures();
    }

}
