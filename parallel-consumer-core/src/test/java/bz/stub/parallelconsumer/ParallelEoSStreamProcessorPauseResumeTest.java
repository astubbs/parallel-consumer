package bz.stub.parallelconsumer;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions.CommitMode;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

import static com.google.common.truth.Truth.assertThat;
import static bz.stub.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.UNORDERED;

/**
 * Test for pause/resume feature of the parallel consumer (see {@code GH#193}).
 *
 * @author niels.oertel
 */
@Slf4j
class ParallelEoSStreamProcessorPauseResumeTest extends ParallelEoSStreamProcessorTestBase {

    private static final AtomicInteger MY_ID_GENERATOR = new AtomicInteger();

    private static final AtomicInteger RECORD_SET_KEY_GENERATOR = new AtomicInteger();

    private static class TestUserFunction implements Consumer<PollContext<String, String>> {

        private final AtomicInteger numProcessedRecords = new AtomicInteger();

        /**
         * The number of in flight records. Note that this may not exactly match the real number of in flight records as
         * parallel consumer has a wrapper around the user function so incrementing/decrementing the counter is a little
         * bit delayed.
         */
        private final AtomicInteger numInFlightRecords = new AtomicInteger();

        private final ReentrantLock mutex = new ReentrantLock();

        public void lockProcessing() {
            mutex.lock();
        }

        public void unlockProcessing() {
            log.debug("Unlocking processing");
            mutex.unlock();
        }

        @Override
        public void accept(PollContext<String, String> t) {
            log.debug("Received: {}", t);
            numInFlightRecords.incrementAndGet();
            try {
                lockProcessing();
                int numProcessed = numProcessedRecords.incrementAndGet();
                log.debug("Processed complete, incremented to {}", numProcessed);
            } finally {
                unlockProcessing();
                numInFlightRecords.decrementAndGet();
            }
        }

        public void reset() {
            numProcessedRecords.set(0);
        }
    }

    private ParallelConsumerOptions<String, String> getBaseOptions(final CommitMode commitMode, int maxConcurrency) {
        return ParallelConsumerOptions.<String, String>builder()
                .commitMode(commitMode)
                .consumer(consumerSpy)
                // UNORDERED so that we get nice linear offsets in our processing order (PARTITION has no concurrency, KEY depends on your keys
                .ordering(UNORDERED)
                .maxConcurrency(maxConcurrency)
                .build();
    }

    private void addRecordsWithSetKey(final int numRecords) {
        long recordSetKey = RECORD_SET_KEY_GENERATOR.incrementAndGet();
        log.debug("Producing {} records with set key {}.", numRecords, recordSetKey);
        for (int i = 0; i < numRecords; ++i) {
            consumerSpy.addRecord(ktu.makeRecord("key-" + recordSetKey + i, "v0-test-" + i));
        }
        log.debug("Finished producing {} records with set key {}.", numRecords, recordSetKey);
    }

    private void setupParallelConsumerInstance(final CommitMode commitMode, final int maxConcurrency) {
        setupParallelConsumerInstance(getBaseOptions(commitMode, maxConcurrency));

        // register unique ID on the parallel consumer
        String myId = "p/r-test-" + MY_ID_GENERATOR.incrementAndGet();
        parallelConsumer.setMyId(Optional.of(myId));
    }

    private TestUserFunction createTestSetup(final CommitMode commitMode, final int maxConcurrency) {
        setupParallelConsumerInstance(commitMode, maxConcurrency);
        TestUserFunction testUserFunction = new TestUserFunction();
        parallelConsumer.poll(testUserFunction);

        return testUserFunction;
    }

    /**
     * This test verifies that no new records are submitted to the workers once the consumer is paused.
     *
     * @param commitMode The commit mode to be configured for the parallel consumer.
     */
    @ParameterizedTest()
    @EnumSource(CommitMode.class)
    @SneakyThrows
    void pausingAndResumingProcessingShouldWork(final CommitMode commitMode) {
        int numTestRecordsPerSet = 1_000;
        int totalRecordsExpected = 2 * numTestRecordsPerSet;

        TestUserFunction testUserFunction = createTestSetup(commitMode, 3);

        // produce some messages
        addRecordsWithSetKey(numTestRecordsPerSet);

        // wait for processing to finish
        Awaitility
                .waitAtMost(defaultTimeout)
                .alias(numTestRecordsPerSet + " records should be processed")
                .untilAsserted(() -> assertThat(testUserFunction.numProcessedRecords.get()).isEqualTo(numTestRecordsPerSet));

        // overall committed offset should reach the same value
        awaitForCommit(numTestRecordsPerSet);

        //
        testUserFunction.reset();

        // pause parallel consumer and wait for control loops to catch up
        parallelConsumer.pauseIfRunning();

        awaitForOneLoopCycle();

        // produce more messages -> nothing should be processed
        addRecordsWithSetKey(numTestRecordsPerSet);

        awaitForSomeLoopCycles(2);

        // shouldn't have produced any records
        assertThat(testUserFunction.numProcessedRecords.get()).isEqualTo(0L);

        // overall committed offset should stay at old value
        awaitForCommit(numTestRecordsPerSet);

        // resume parallel consumer ->
        parallelConsumer.resumeIfPaused();

        // messages should be processed now
        Awaitility
                .waitAtMost(defaultTimeout)
                .alias(numTestRecordsPerSet + " records should be processed")
                .untilAsserted(() -> assertThat(testUserFunction.numProcessedRecords.get()).isEqualTo(numTestRecordsPerSet));

        // overall committed offset should reach the total of two batches that were processed
        awaitForCommit(totalRecordsExpected);
    }

    /**
     * This test verifies that in flight work is finished successfully when the consumer is paused. In flight work is
     * work that's currently being processed inside a user function has already been submitted to be processed based on
     * the dynamic load factor. The test also verifies that new offsets are committed once the in-flight work finishes
     * even if the consumer is still paused.
     *
     * @param commitMode The commit mode to be configured for the parallel consumer.
     */
    @ParameterizedTest()
    @EnumSource(CommitMode.class)
    @SneakyThrows
    void testThatInFlightWorkIsFinishedSuccessfullyAndOffsetsAreCommitted(final CommitMode commitMode) {
        int degreeOfParallelism = 3;
        int numTestRecordsPerSet = 1_000;

        TestUserFunction testUserFunction = createTestSetup(commitMode, degreeOfParallelism);
        // block processing in the user function to ensure we have in flight work once we pause the consumer
        testUserFunction.lockProcessing();

        // produce some messages
        addRecordsWithSetKey(numTestRecordsPerSet);

        // wait until we have enough records in flight
        Awaitility
                .waitAtMost(defaultTimeout)
                .alias(degreeOfParallelism + " records should be in flight processed")
                .untilAsserted(() -> assertThat(testUserFunction.numInFlightRecords.get()).isEqualTo(degreeOfParallelism));

        //
        assertCommits().isEmpty();

        // pause parallel consumer and wait for control loops to catch up
        parallelConsumer.pauseIfRunning();
        awaitForOneLoopCycle();

        // unlock the user function
        testUserFunction.unlockProcessing();

        // Every record that was already out at a worker when the pause landed must finish - that is the
        // guarantee a user depends on, and it is what both engines owe. How many MORE than that finish is not:
        // it is however much the engine had pushed into its executor queue ahead of the pause, which the
        // direct-pull engine (-Dpc.directPull=true) has none of. See the paused-buffer assertion below.
        Awaitility
                .waitAtMost(defaultTimeout)
                .alias("the " + degreeOfParallelism + " in-flight records should all complete")
                .untilAsserted(() -> assertThat(testUserFunction.numProcessedRecords.get()).isAtLeast(degreeOfParallelism));

        // overall committed offset should reach the same value
        awaitForCommit(testUserFunction.numProcessedRecords.get());

        // shouldn't have anymore in flight records now
        assertThat(testUserFunction.numInFlightRecords.get()).isEqualTo(0);
        assertThat(parallelConsumer.getWm().getNumberRecordsOutForProcessing()).isEqualTo(0);

        // The bound the original assertion never had, and it is the half of "pause works" that matters more:
        // whatever was already dispatched drains, but the pause stops the rest, so this must be nowhere near
        // the whole set. Without it, an engine that ignored the pause entirely would satisfy everything above.
        assertThat(testUserFunction.numProcessedRecords.get())
                .isLessThan(numTestRecordsPerSet);

        // The "strictly more than maxConcurrency finish" half of the original assertion has moved to
        // pausingDrainsThePreLoadedExecutorQueueAsWellAsTheInFlightRecords(), which is where it can be skipped
        // for an engine that has no such queue without aborting everything asserted above.

        // resume parallel consumer ->
        parallelConsumer.resumeIfPaused();

        // other pending messages should be processed now
        Awaitility
                .waitAtMost(defaultTimeout)
                .alias(numTestRecordsPerSet + " records should be processed")
                .untilAsserted(() -> assertThat(testUserFunction.numProcessedRecords.get()).isEqualTo(numTestRecordsPerSet));

        // overall committed offset should reach the total number of processed records
        awaitForCommit(numTestRecordsPerSet);
    }

    /**
     * The pre-loaded-queue half of what
     * {@link #testThatInFlightWorkIsFinishedSuccessfullyAndOffsetsAreCommitted} used to assert, kept at its
     * original strength and given its own method so that skipping it for an engine that has no such queue does
     * not abort everything that test asserts either side of it.
     * <p>
     * The engine PC ships pushes work into a {@link java.util.concurrent.ThreadPoolExecutor}'s queue ahead of
     * the workers, sized by the dynamic load factor, so by the time a pause lands STRICTLY MORE than
     * {@code maxConcurrency} records are already committed to running and must be seen through. That is a real
     * property of the shipped engine and losing it to accommodate a measurement engine would be a regression
     * in what this class proves.
     * <p>
     * The direct-pull engine ({@code -Dpc.directPull=true}) has no intermediate queue: exactly the in-flight
     * records finish, and the count is exactly {@code maxConcurrency}. A pause there is exact rather than
     * approximate, which is a behaviour difference this assertion cannot express - so it is skipped, visibly,
     * rather than loosened for both. The virtual-thread engine ({@code -Dpc.virtualThreads=true}) is queue-less
     * the same way - every accepted task gets a thread at once - and the {@code Unit Tests (virtual threads)}
     * lane measured the pause landing exactly at {@code maxConcurrency}, so it takes the same visible skip.
     * <p>
     * Not parameterised by commit mode: what is under test is how much work the engine had buffered when the
     * pause landed, which has nothing to do with how offsets are committed.
     */
    @Test
    @SneakyThrows
    void pausingDrainsThePreLoadedExecutorQueueAsWellAsTheInFlightRecords() {
        int degreeOfParallelism = 3;
        int numTestRecordsPerSet = 1_000;

        TestUserFunction testUserFunction = createTestSetup(CommitMode.PERIODIC_CONSUMER_SYNC, degreeOfParallelism);
        testUserFunction.lockProcessing();

        addRecordsWithSetKey(numTestRecordsPerSet);

        Awaitility
                .waitAtMost(defaultTimeout)
                .alias(degreeOfParallelism + " records should be in flight processed")
                .untilAsserted(() -> assertThat(testUserFunction.numInFlightRecords.get()).isEqualTo(degreeOfParallelism));

        parallelConsumer.pauseIfRunning();
        awaitForOneLoopCycle();
        testUserFunction.unlockProcessing();

        Assumptions.assumeFalse(parallelConsumer.getWm().getOptions().isDirectPullEngine(),
                "the direct-pull engine has no pre-loaded executor queue, so a pause is exact: "
                        + "exactly maxConcurrency records finish, never more");
        Assumptions.assumeFalse(parallelConsumer.getWm().getOptions().isUseVirtualThreads(),
                "the virtual-thread pool hands every accepted task a thread at once - no pre-loaded queue to "
                        + "drain, so a pause is exact there too: the lane measured exactly maxConcurrency finishing");

        Awaitility
                .waitAtMost(defaultTimeout)
                .alias("more than the " + degreeOfParallelism + " in-flight records should be processed, "
                        + "because the executor queue was pre-loaded ahead of them")
                .untilAsserted(() -> assertThat(testUserFunction.numProcessedRecords.get()).isGreaterThan(degreeOfParallelism));
    }

}
