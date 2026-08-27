package bz.stub.parallelconsumer;

/*-
 * Copyright (C) 2020-2023 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import com.google.common.truth.Truth;
import bz.stub.parallelconsumer.internal.utils.KafkaTestUtils;
import bz.stub.parallelconsumer.internal.utils.LongPollingMockConsumer;
import bz.stub.parallelconsumer.ParallelConsumerOptions.ProcessingOrder;
import bz.stub.parallelconsumer.internal.AbstractParallelEoSStreamProcessor;
import bz.stub.parallelconsumer.internal.PCModule;
import bz.stub.parallelconsumer.model.CommitHistory;
import bz.stub.parallelconsumer.internal.UnmailboxableRecordException;
import bz.stub.parallelconsumer.state.WorkContainer;
import bz.stub.parallelconsumer.state.WorkManager;
import bz.stub.parallelconsumer.truth.CommitHistorySubject;
import bz.stub.parallelconsumer.truth.LongPollingMockConsumerSubject;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serdes;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import pl.tlinkowski.unij.api.UniLists;
import pl.tlinkowski.unij.api.UniMaps;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static bz.stub.parallelconsumer.internal.utils.LatchTestUtils.awaitLatch;
import static bz.stub.parallelconsumer.internal.utils.StringUtils.msg;
import static bz.stub.parallelconsumer.ParallelConsumerOptions.CommitMode.*;
import static bz.stub.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.UNORDERED;
import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.awaitility.Awaitility.waitAtMost;
import static org.mockito.Mockito.*;
import static pl.tlinkowski.unij.api.UniLists.of;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * @author Antony Stubbs
 * @see AbstractParallelEoSStreamProcessor
 */
// todo migrate commit assertion methods in to a Truth Subject
@Slf4j
public abstract class AbstractParallelEoSStreamProcessorTestBase {

    public String INPUT_TOPIC;
    public String OUTPUT_TOPIC;
    public String CONSUMER_GROUP_ID;

    public ConsumerGroupMetadata DEFAULT_GROUP_METADATA;

    /**
     * The frequency with which we pretend to poll the broker for records - actually the pretend long poll timeout. A
     * lower value shouldn't affect test speed much unless many batches of messages are "published" as test messages are
     * queued up at the beginning and the polled.
     *
     * @see LongPollingMockConsumer#poll(Duration)
     */
    public static final int DEFAULT_BROKER_POLL_FREQUENCY_MS = 500;

    /**
     * The commit interval for the main {@link AbstractParallelEoSStreamProcessor} control thread. Actually the timeout
     * that we poll the {@link LinkedBlockingQueue} for. A lower value will increase the frequency of control loop
     * cycles, making our test waiting go faster.
     *
     * @see AbstractParallelEoSStreamProcessor#workMailBox
     * @see AbstractParallelEoSStreamProcessor#processWorkCompleteMailBox
     */
    public static final int DEFAULT_COMMIT_INTERVAL_MAX_MS = 100;

    protected LongPollingMockConsumer<String, String> consumerSpy;
    protected MockProducer<String, String> producerSpy;

    protected AbstractParallelEoSStreamProcessor<String, String> parentParallelConsumer;

    public static int defaultTimeoutSeconds = 30;

    public static Duration defaultTimeout = ofSeconds(defaultTimeoutSeconds);
    protected static long defaultTimeoutMs = defaultTimeout.toMillis();
    protected static Duration effectivelyInfiniteTimeout = Duration.ofMinutes(20);

    ParallelEoSStreamProcessorTest.MyAction myRecordProcessingAction;

    ConsumerRecord<String, String> firstRecord;
    ConsumerRecord<String, String> secondRecord;

    protected KafkaTestUtils ktu;

    protected AtomicReference<Integer> loopCountRef;

    volatile CountDownLatch loopLatchV = new CountDownLatch(0);
    volatile CountDownLatch controlLoopPauseLatch = new CountDownLatch(0);
    protected AtomicReference<Integer> loopCount;

    /**
     * Time to wait to verify some assertion types
     */
    long verificationWaitDelay;
    protected TopicPartition topicPartition;

    /**
     * Unique topic names for each test method
     */
    public void setupTopicNames() {
        INPUT_TOPIC = "input-" + Math.random();
        OUTPUT_TOPIC = "output-" + Math.random();
        CONSUMER_GROUP_ID = "my-group" + Math.random();
        topicPartition = new TopicPartition(INPUT_TOPIC, 0);
        DEFAULT_GROUP_METADATA = new ConsumerGroupMetadata(CONSUMER_GROUP_ID);
    }

    @BeforeEach
    public void setupAsyncConsumerTestBase() {
        setupTopicNames();

        ParallelConsumerOptions<Object, Object> options = getOptions();
        setupParallelConsumerInstance(options);
    }

    protected ParallelConsumerOptions<Object, Object> getOptions() {
        ParallelConsumerOptions<Object, Object> options = getDefaultOptions()
                .build();
        return options;
    }

    protected ParallelConsumerOptions.ParallelConsumerOptionsBuilder<Object, Object> getDefaultOptions() {
        return ParallelConsumerOptions.builder()
                .commitMode(PERIODIC_CONSUMER_SYNC)
                .ordering(UNORDERED);
    }

    /**
     * Fails the test if PC terminated because a record could not be returned to the mailbox.
     * <p>
     * <b>Operator requirement: if this ever happens, it happens HERE and it is loud.</b> The defect it reports is
     * invisible by construction - the record is neither retried nor reported, no exception reaches the user, and the
     * committed offsets look correct - so a run that hit it and stayed green would be the worst outcome available.
     * The existing teardown only logs {@code "PC has error - test failed"} and closes, which does not fail anything.
     * <p>
     * Matched on TYPE rather than on the log banner, because a harness that matched the text would be one reword
     * away from silently never firing again. Checked as a cause chain, since the escalation wraps what
     * {@code addToMailbox} threw and a caller may wrap it again.
     */
    private void failLoudlyIfARecordCouldNotBeMailboxed() {
        // BOUNDED BY IDENTITY AND BY DEPTH, and the first version of this was neither - it broke the suite.
        //
        // It had `if (t.getCause() == t) break;`, a self-reference check, which is exactly the check
        // controlLoopSurvivesACyclicCauseChain's own javadoc says is defeated: initCause refuses A -> A, so the
        // only cycle you can build is A -> B -> A, and a self-reference test walks it forever. That test builds
        // one deliberately, so every run of this suite hung in teardown after it and the CI job died at its
        // 15-minute cap - a hang, which is the one failure mode a stack trace after the fact cannot explain.
        //
        // ThrowableUtils owns this problem in main code and guards both ways for the same reason; this is test
        // code and cannot reach its private walk, so the guard is repeated here rather than shared. Kept
        // deliberately small, because a drifted copy of a cause walk is its own hazard.
        var seen = java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<Throwable, Boolean>());
        Throwable t = parentParallelConsumer.getFailureCause();
        for (int depth = 0; t != null && depth < 100 && seen.add(t); depth++) {
            if (t instanceof UnmailboxableRecordException) {
                fail("PC TERMINATED: a record could not be returned to the mailbox, which is a bug in PC's own "
                        + "bookkeeping and means a record was neither retried nor reported. Read the terminal "
                        + "error in this test's log. Cause: " + t);
            }
            t = t.getCause();
        }
    }

    @AfterEach
    public void close() {
        failLoudlyIfARecordCouldNotBeMailboxed();

        // Reset Awaitility's global thread-local timeout state so per-test overrides
        // (e.g. setDefaultTimeout) don't leak into other tests under non-deterministic
        // test order (PIT baseline/mutations surface this; surefire's default ordering
        // happens to mask it). Runs even if the test body threw.
        Awaitility.reset();

        // don't try to close if error'd (at least one test purposefully creates an error to tests error handling) - we
        // don't want to bubble up an error here that we expect from here.
        if (!parentParallelConsumer.isClosedOrFailed()) {
            if (parentParallelConsumer.getFailureCause() != null) {
                log.error("PC has error - test failed");
            }
            log.debug("Test ended (maybe a failure), closing pc...");
            parentParallelConsumer.close();
        } else {
            log.debug("Test finished, pc already closed.");
        }
    }

    protected void injectWorkSuccessListener(WorkManager<String, String> wm, List<WorkContainer<String, String>> customSuccessfulWork) {
        wm.addSuccessfulWorkListener((work) -> {
            log.debug("Test work listener heard some successful work: {}", work);
            synchronized (customSuccessfulWork) {
                customSuccessfulWork.add(work);
            }
        });
    }

    protected void primeFirstRecord() {
        firstRecord = ktu.makeRecord("key-0", "v0-first-primed-record");
        consumerSpy.addRecord(firstRecord);
    }

    protected MockConsumer<String, String> setupClients() {
        instantiateConsumerProducer();
        ktu = new KafkaTestUtils(INPUT_TOPIC, CONSUMER_GROUP_ID, consumerSpy);
        return consumerSpy;
    }

    protected void instantiateConsumerProducer() {
        LongPollingMockConsumer<String, String> consumer = new LongPollingMockConsumer<>(OffsetResetStrategy.EARLIEST);
        MockProducer<String, String> producer = new MockProducer<>(true,
                Serdes.String().serializer(), Serdes.String().serializer());

        this.producerSpy = spy(producer);
        this.consumerSpy = spy(consumer);
        myRecordProcessingAction = spy(ParallelEoSStreamProcessorTest.MyAction.class);

        when(consumerSpy.groupMetadata()).thenReturn(DEFAULT_GROUP_METADATA);
    }

    /**
     * Need to make sure we only use {@link AbstractParallelEoSStreamProcessor#subscribe} methods, and not do manual
     * assignment, otherwise rebalance listeners don't fire (because there are never rebalances).
     */
    protected void subscribeParallelConsumerAndMockConsumerTo(String topic) {
        List<String> of = of(topic);
        parentParallelConsumer.subscribe(of);
        consumerSpy.subscribeWithRebalanceAndAssignment(of, 2);
    }

    protected void setupParallelConsumerInstance(ProcessingOrder order) {
        setupParallelConsumerInstance(ParallelConsumerOptions.builder().ordering(order).build());
    }

    protected void setupParallelConsumerInstance(ParallelConsumerOptions parallelConsumerOptions) {
        setupClients();

        var optionsWithClients = parallelConsumerOptions.toBuilder()
                .consumer(consumerSpy)
                .producer(producerSpy)
                .build();

        parentParallelConsumer = initAsyncConsumer(optionsWithClients);

        subscribeParallelConsumerAndMockConsumerTo(INPUT_TOPIC);

        parentParallelConsumer.setLongPollTimeout(ofMillis(DEFAULT_BROKER_POLL_FREQUENCY_MS));
        parentParallelConsumer.setTimeBetweenCommits(ofMillis(DEFAULT_COMMIT_INTERVAL_MAX_MS));

        verificationWaitDelay = parentParallelConsumer.getTimeBetweenCommits().multipliedBy(2).toMillis();

        loopCountRef = attachLoopCounter(parentParallelConsumer);
    }

    protected abstract AbstractParallelEoSStreamProcessor<String, String> initAsyncConsumer(ParallelConsumerOptions<String, String> parallelConsumerOptions);

    protected void sendSecondRecord(MockConsumer<String, String> consumer) {
        secondRecord = ktu.makeRecord("key-0", "v1");
        consumer.addRecord(secondRecord);
    }

    protected AtomicReference<Integer> attachLoopCounter(AbstractParallelEoSStreamProcessor parallelConsumer) {
        final AtomicReference<Integer> currentLoop = new AtomicReference<>(0);
        parentParallelConsumer.addLoopEndCallBack(() -> {
            Integer currentNumber = currentLoop.get();
            int newLoopNumber = currentNumber + 1;
            currentLoop.compareAndSet(currentNumber, newLoopNumber);
            log.trace("Counting down latch from {}", loopLatchV.getCount());
            loopLatchV.countDown();
            log.trace("Loop latch remaining: {}", loopLatchV.getCount());
            if (controlLoopPauseLatch.getCount() > 0) {
                log.debug("Waiting on pause latch ({})...", controlLoopPauseLatch.getCount());
                try {
                    controlLoopPauseLatch.await();
                } catch (InterruptedException e) {
                    log.error(e.getMessage(), e);
                }
                log.trace("Completed waiting on pause latch");
            }
            log.trace("Loop count {}", currentLoop.get());
        });
        return currentLoop;
    }

    /**
     * Pauses the control loop by awaiting this injected countdown lunch
     */
    protected void pauseControlLoop() {
        log.trace("Pause loop");
        controlLoopPauseLatch = new CountDownLatch(1);
    }

    /**
     * Resume is the controller by decrementing the injected countdown latch
     */
    protected void resumeControlLoop() {
        log.trace("Resume loop");
        controlLoopPauseLatch.countDown();
    }

    protected void awaitForOneLoopCycle() {
        awaitForSomeLoopCycles(1);
    }

    protected void awaitForSomeLoopCycles(int thisManyMore) {
        log.debug("Waiting for {} more iterations of the control loop.", thisManyMore);
        blockingLoopLatchTrigger(thisManyMore);
        log.debug("Completed waiting on {} loop(s)", thisManyMore);
    }

    protected void awaitUntilTrue(Callable<Boolean> booleanCallable) {
        waitAtMost(defaultTimeout).until(booleanCallable);
    }

    /**
     * Make sure the latch is attached, if this times out unexpectedly
     */
    @SneakyThrows
    private void blockingLoopLatchTrigger(int waitForCount) {
        log.debug("Waiting on {} cycles on loop latch for {}...", waitForCount, defaultTimeout);
        loopLatchV = new CountDownLatch(waitForCount);
        try {
            boolean timeout = !loopLatchV.await(defaultTimeoutSeconds, SECONDS);
            if (timeout || parentParallelConsumer.isClosedOrFailed())
                throw new TimeoutException(msg("Timeout of {}, waiting for {} counts, on latch with {} left", defaultTimeout, waitForCount, loopLatchV.getCount()));
        } catch (InterruptedException e) {
            log.error("Interrupted while waiting for loop latch - timeout was {}", defaultTimeout);
            throw e;
        }
    }

    @SneakyThrows
    private void awaitForLoopCount(int waitForCount) {
        log.debug("Waiting on {} cycles on loop latch...", waitForCount);
        waitAtMost(defaultTimeout.multipliedBy(100)).until(() -> loopCount.get() > waitForCount);
    }

    protected void awaitForCommit(int offset) {
        log.debug("Waiting for commit offset {}", offset);
        await().timeout(defaultTimeout)
                .untilAsserted(() -> assertCommitsContains(of(offset)));
    }

    /**
     * Waits until the commit history holds at least this many committed offsets, regardless of which offsets
     * they carry.
     * <p>
     * Use when the point being waited for is a commit <em>cycle</em> rather than a particular offset - notably
     * when a repeat commit of an already-committed base offset is expected, which
     * {@link #awaitForCommit(int)} cannot distinguish from the commit that came before it.
     * <p>
     * The count is of flattened per-partition entries, not of commit rounds: a round that commits two
     * partitions contributes two. Snapshot the count and wait for a delta rather than passing an absolute
     * figure, so the genesis commit ({@link KafkaTestUtils#trimAllGenesisOffset(List)}) cannot shift it.
     */
    protected void awaitForCommittedOffsetCount(int count) {
        log.debug("Waiting for {} committed offsets to have been emitted", count);
        await().timeout(defaultTimeout)
                .untilAsserted(() -> assertThat(getCommitHistoryFlattened()).hasSizeGreaterThanOrEqualTo(count));
    }

    protected void awaitForCommitExact(int offset) {
        log.debug("Waiting for EXACTLY commit offset {}", offset);
        await().timeout(defaultTimeout)
                .failFast(msg("Commit was not exact - contained offsets that weren't '{}'", offset), () -> {
                    List<Integer> offsets = extractAllPartitionsOffsetsSequentially(false);
                    return offsets.size() > 1 && !offsets.contains(offset);
                })
                .untilAsserted(() -> assertCommits(of(offset)));
    }

    public void assertCommitsContains(List<Integer> offsets) {
        List<Integer> commits = getCommitHistoryFlattened();
        assertThat(commits).containsAll(offsets);
    }

    protected List<Integer> getCommitHistoryFlattened() {
        return (isUsingTransactionalProducer())
                ? ktu.getProducerCommitsFlattened(producerSpy)
                : extractAllPartitionsOffsetsSequentially(false);
    }

    private List<OffsetAndMetadata> getCommitHistoryFlattenedMeta() {
        return (isUsingTransactionalProducer())
                ? ktu.getProducerCommitsMeta(producerSpy)
                : extractAllPartitionsOffsetsSequentiallyMeta(true);
    }

    public void assertCommits(List<Integer> offsets, String description) {
        assertCommits(offsets, Optional.of(description));
    }

    /**
     * Flattens the offsets of all partitions into a single sequential list. Removing the genesis commit (0) if it
     * exists, unless it's contained in the assertion.
     */
    public void assertCommits(List<Integer> offsets, Optional<String> description) {
        boolean trimGenesis = !offsets.contains(0);

        if (isUsingTransactionalProducer()) {
            ktu.assertCommits(producerSpy, offsets, description);
            assertThat(extractAllPartitionsOffsetsSequentially(trimGenesis)).isEmpty();
        } else {
            List<Integer> collect = extractAllPartitionsOffsetsSequentially(trimGenesis);

            // Repeat commits of the same base offset are expected - see KafkaTestUtils#collapseRepeatedCommits
            // for why - and this set-wise comparison already tolerates them. It is also order-insensitive,
            // which the producer-side branch is not, so unlike that branch it does NOT detect a committed
            // offset going backwards. See KafkaTestUtils#assertCommits for the difference and its cause.
            // is there a nicer optional way?
            // {@link Optional#ifPresentOrElse} only @since 9
            if (description.isPresent()) {
                assertThat(collect).as(description.get()).hasSameElementsAs(offsets);
            } else {
                assertThat(collect).hasSameElementsAs(offsets);
            }
            ktu.assertCommits(producerSpy, UniLists.of(), Optional.of("Empty"));
        }
    }

    /**
     * Flattens the offsets of all partitions into a single sequential list
     */
    protected List<Integer> extractAllPartitionsOffsetsSequentially(boolean trimGenesis) {
        return extractAllPartitionsOffsetsSequentiallyMeta(trimGenesis).stream().
                map(x -> (int) x.offset()) // int cast a luxury in test context - no big offsets
                .collect(Collectors.toList());
    }

    /**
     * Flattens the offsets of all partitions into a single sequential list
     */
    protected List<OffsetAndMetadata> extractAllPartitionsOffsetsSequentiallyMeta(boolean trimGenesis) {
        // copy the list for safe concurrent access
        List<Map<TopicPartition, OffsetAndMetadata>> history = new ArrayList<>(consumerSpy.getCommitHistoryInt());
        return history.stream()
                .flatMap(commits ->
                        {
                            var rawValues = new ArrayList<>(commits.values()).stream(); // 4 debugging
                            if (trimGenesis)
                                return rawValues.filter(x -> x.offset() != 0);
                            else
                                return rawValues; // int cast a luxury in test context - no big offsets
                        }
                ).collect(Collectors.toList());
    }


    protected List<OffsetAndMetadata> extractAllPartitionsOffsetsAndMetadataSequentially() {
        // copy the list for safe concurrent access
        List<Map<TopicPartition, OffsetAndMetadata>> history = new ArrayList<>(consumerSpy.getCommitHistoryInt());
        return history.stream()
                .flatMap(commits ->
                        {
                            Collection<OffsetAndMetadata> values = new ArrayList<>(commits.values());
                            return values.stream();
                        }
                ).collect(Collectors.toList());
    }

    public void assertCommits(List<Integer> offsets) {
        assertCommits(offsets, Optional.empty());
    }

    public CommitHistorySubject assertCommits() {
        List<OffsetAndMetadata> commitHistoryFlattened = getCommitHistoryFlattenedMeta();
        CommitHistory actual = new CommitHistory(commitHistoryFlattened);
        return CommitHistorySubject.assertThat(actual);
    }

    /**
     * Checks a list of commits of a list of partitions - outer list is partition, inner list is commits
     */
    public void assertCommitLists(List<List<Integer>> offsets) {
        if (isUsingTransactionalProducer()) {
            ktu.assertCommitLists(producerSpy, offsets, Optional.empty());
        } else {
            List<Map<String, Map<TopicPartition, OffsetAndMetadata>>> commitHistoryWithGropuId = consumerSpy.getCommitHistoryWithGroupId();
            ktu.assertCommitLists(commitHistoryWithGropuId, offsets, Optional.empty());
        }
    }

    protected List<Map<String, Map<TopicPartition, OffsetAndMetadata>>> getCommitHistory() {
        if (isUsingTransactionalProducer()) {
            return producerSpy.consumerGroupOffsetsHistory();
        } else {
            return consumerSpy.getCommitHistoryWithGroupId();
        }
    }

    protected boolean isUsingTransactionalProducer() {
        ParallelConsumerOptions.CommitMode commitMode = parentParallelConsumer.getWm().getOptions().getCommitMode();
        return commitMode.equals(PERIODIC_TRANSACTIONAL_PRODUCER);
    }

    protected boolean isUsingAsyncCommits() {
        ParallelConsumerOptions.CommitMode commitMode = parentParallelConsumer.getWm().getOptions().getCommitMode();
        return commitMode.equals(PERIODIC_CONSUMER_ASYNCHRONOUS);
    }

    protected void releaseAndWait(List<CountDownLatch> locks, List<Integer> lockIndexes) {
        for (Integer i : lockIndexes) {
            log.debug("Releasing {}...", i);
            locks.get(i).countDown();
        }
        awaitForSomeLoopCycles(1);
    }

    protected void releaseAndWait(List<CountDownLatch> locks, int lockIndex) {
        log.debug("Releasing {}...", lockIndex);
        locks.get(lockIndex).countDown();
        awaitForSomeLoopCycles(1);
    }

    protected abstract PCModule getModule();

    protected void pauseControlToAwaitForLatch(CountDownLatch latch) {
        pauseControlLoop();
        awaitLatch(latch);
        resumeControlLoop();
        awaitForOneLoopCycle();
    }

    /**
     * Assert {@link com.google.common.truth.Truth} on the test {@link Consumer} ({@link LongPollingMockConsumer}).
     */
    protected LongPollingMockConsumerSubject<String, String> assertThatConsumer(String msg) {
        return Truth.assertWithMessage(msg)
                .about(LongPollingMockConsumerSubject.<String, String>mockConsumers())
                .that(consumerSpy);
    }

}
