package bz.stub.parallelconsumer;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions.ProcessingOrder;
import bz.stub.parallelconsumer.internal.utils.KafkaTestUtils;
import bz.stub.parallelconsumer.internal.utils.ProgressBarUtils;
import bz.stub.parallelconsumer.internal.AbstractParallelEoSStreamProcessor;
import bz.stub.parallelconsumer.internal.RateLimiter;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import me.tongfei.progressbar.ProgressBar;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import pl.tlinkowski.unij.api.UniLists;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static bz.stub.parallelconsumer.internal.utils.StringUtils.msg;
import static bz.stub.parallelconsumer.AbstractParallelEoSStreamProcessorTestBase.defaultTimeout;
import static bz.stub.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.KEY;
import static bz.stub.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.PARTITION;
import static bz.stub.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.UNORDERED;
import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.waitAtMost;

/**
 * Batch test which can be used in the different modules. The need for this is because the batch methods in each module
 * all have different signatures, and return types.
 */
@Slf4j
@RequiredArgsConstructor
public abstract class BatchTestMethods<POLL_RETURN> {

    public static final long FAILURE_TARGET = 5L;
    private final ParallelEoSStreamProcessorTestBase baseTest;

    protected abstract KafkaTestUtils getKtu();


    protected void setupParallelConsumer(int targetBatchSize, int maxConcurrency, ParallelConsumerOptions.ProcessingOrder ordering) {
        //
        ParallelConsumerOptions<Object, Object> options = ParallelConsumerOptions.builder()
                .batchSize(targetBatchSize)
                .ordering(ordering)
                .maxConcurrency(maxConcurrency)
                .build();
        baseTest.setupParallelConsumerInstance(options);

        //
        baseTest.parentParallelConsumer.setTimeBetweenCommits(ofSeconds(5));
    }

    protected abstract AbstractParallelEoSStreamProcessor getPC();

    public void averageBatchSizeTest(int numRecsExpected) {
        final int targetBatchSize = 20;
        int maxConcurrency = 8;

        ProgressBar bar = ProgressBarUtils.getNewMessagesBar(log, numRecsExpected);

        setupParallelConsumer(targetBatchSize, maxConcurrency, UNORDERED);

        //
        getKtu().sendRecords(numRecsExpected);

        //
        var numBatches = new AtomicInteger(0);
        var numRecordsProcessed = new AtomicInteger(0);
        long start = System.currentTimeMillis();
        RateLimiter statusLogger = new RateLimiter(1);

        averageBatchSizeTestPoll(numBatches, numRecordsProcessed, statusLogger);

        //
        waitAtMost(defaultTimeout).alias("expected number of records")
                .failFast(() -> getPC().isClosedOrFailed())
                .untilAsserted(() -> {
                    bar.stepTo(numRecordsProcessed.get());
                    assertThat(numRecordsProcessed.get()).isEqualTo(numRecsExpected);
                });
        bar.close();

        //

        //
        double targetMetThreshold = 999. / 1000.;
        double acceptableAttainedBatchSize = targetBatchSize * targetMetThreshold;
        double averageBatchSize = calcAverage(numRecordsProcessed, numBatches);
        assertThat(averageBatchSize).isGreaterThan(acceptableAttainedBatchSize);

        baseTest.parentParallelConsumer.requestCommitAsap();
        baseTest.awaitForCommit(numRecsExpected);
        var duration = System.currentTimeMillis() - start;
        log.info("Processed {} records in {} ms. Average batch size was: {}. {} records per second.", numRecsExpected, duration, averageBatchSize, numRecsExpected / (duration / 1000.0));
    }

    /**
     * Must call {@link #averageBatchSizeTestPollInner}
     */
    protected abstract void averageBatchSizeTestPoll(AtomicInteger numBatches, AtomicInteger numRecords, RateLimiter statusLogger);

    protected POLL_RETURN averageBatchSizeTestPollInner(AtomicInteger numBatches, AtomicInteger numRecords, RateLimiter statusLogger, PollContext<String, String> pollBatch) {
        int size = (int) pollBatch.size();

        statusLogger.performIfNotLimited(() -> {
            try {
                log.debug(
                        "Processed {} records in {} batches with average size {}",
                        numRecords.get(),
                        numBatches.get(),
                        calcAverage(numRecords, numBatches)
                );
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        });

        try {
            log.trace("Batch size {}", size);
            return averageBatchSizeTestPollStep(pollBatch);
        } finally {
            numBatches.getAndIncrement();
            numRecords.addAndGet(size);
        }
    }

    protected abstract POLL_RETURN averageBatchSizeTestPollStep(PollContext<String, String> recordList);

    private double calcAverage(AtomicInteger numRecords, AtomicInteger numBatches) {
        return numRecords.get() / (0.0 + numBatches.get());
    }


    /**
     * Asserts the EXACT number of batches the records arrive in, which only holds while every record sits on its
     * own shard - hence {@link KafkaTestUtils#sendRecordsWithDistinctKeys(int)} below rather than
     * {@link KafkaTestUtils#sendRecords(int)}.
     * <p>
     * {@code sendRecords} draws its keys with replacement. Under {@link ProcessingOrder#KEY} two records drawn onto
     * one key share a shard, a shard yields at most one record per work-retrieval round, and the same five records
     * then arrive in four batches instead of three - against an {@code expectedNumOfBatches} computed from the
     * record count alone. It is not a timing failure and no re-run makes it less likely: it is decided entirely by
     * a draw the test never inspects. Keeping the exact-count assertion therefore means removing the randomness
     * from the input, not loosening the assertion.
     * <p>
     * The KEY-ordering behaviour this can no longer exercise - records on one key never sharing a batch - is
     * covered by {@link #keyOrderNeverBatchesTwoRecordsOfOneKey()}.
     * <p>
     * Measured and written up in
     * {@code docs/solutions/test-flakiness/a-randomised-key-draw-decided-a-batch-count-the-test-computed-from-the-record-count-2026-09-08.md}.
     */
    @SneakyThrows
    public void simpleBatchTest(ParallelConsumerOptions.ProcessingOrder order) {
        int batchSizeSetting = 2;
        int numRecsExpected = 5;

        getPC().setTimeBetweenCommits(ofSeconds(1));

        setupParallelConsumer(batchSizeSetting, ParallelConsumerOptions.DEFAULT_MAX_CONCURRENCY, order);

        var recs = getKtu().sendRecordsWithDistinctKeys(numRecsExpected);
        List<PollContext<String, String>> batchesReceived = new CopyOnWriteArrayList<>();

        //
        simpleBatchTestPoll(batchesReceived);

        //
        int expectedNumOfBatches = (order == PARTITION) ?
                numRecsExpected : // partition ordering restricts the batch sizes to a single element as all records are in a single partition
                (int) Math.ceil(numRecsExpected / (double) batchSizeSetting);

        waitAtMost(defaultTimeout).alias("expected number of batches")
                .failFast(() -> getPC().isClosedOrFailed())
                .untilAsserted(() -> {
                    assertThat(batchesReceived).hasSize(expectedNumOfBatches);
                });

        assertThat(batchesReceived)
                .as("batch size")
                .allSatisfy(receivedBatchEntry -> assertThat(receivedBatchEntry).hasSizeLessThanOrEqualTo(batchSizeSetting))
                .as("all messages processed")
                .flatExtracting(PollContext::getConsumerRecordsFlattened).hasSameElementsAs(recs);

        assertThat(getPC().isClosedOrFailed()).isFalse();

        baseTest.awaitForCommit(numRecsExpected);
        getPC().closeDrainFirst();
    }

    public abstract void simpleBatchTestPoll(List<PollContext<String, String>> batchesReceived);

    /**
     * Two records that share a key never share a batch under {@link ProcessingOrder#KEY} - whatever the batch size.
     * <p>
     * A shard is per key and an order-restricted shard hands out at most one record per work-retrieval round
     * ({@code ProcessingShard.getWorkIfAvailable}, grep {@code isOrderRestricted}), so three records on one key
     * need three batches to themselves and the run takes at least four batches to deliver five records at a batch
     * size of two. That is correct behaviour and it is why {@code ceil(records / batchSize)} is not a valid
     * expectation for an input whose key distribution the test does not control - see {@link #simpleBatchTest}.
     * <p>
     * <b>Core only, deliberately.</b> The batching and shard arithmetic under test is the core engine's and is
     * identical whichever module drives it; what the wrapper modules add is their own poll signature, which
     * {@link #simpleBatchTest} already covers in each of them.
     */
    @SneakyThrows
    public void keyOrderNeverBatchesTwoRecordsOfOneKey() {
        final int batchSizeSetting = 2;
        final String collidingKey = "one-key-three-records";
        final List<String> keys = UniLists.of(collidingKey, collidingKey, collidingKey, "second-key", "third-key");

        setupParallelConsumer(batchSizeSetting, ParallelConsumerOptions.DEFAULT_MAX_CONCURRENCY, KEY);
        getPC().setTimeBetweenCommits(ofSeconds(1));

        var recs = getKtu().sendRecordsWithKeys(keys);
        List<PollContext<String, String>> batchesReceived = new CopyOnWriteArrayList<>();

        //
        simpleBatchTestPoll(batchesReceived);

        // wait on the RECORDS, not on a batch count - the batch count is what this test is measuring
        waitAtMost(defaultTimeout).alias("every record delivered")
                .failFast(() -> getPC().isClosedOrFailed())
                .untilAsserted(() -> assertThat(batchesReceived.stream().mapToLong(PollContext::size).sum())
                        .isEqualTo(keys.size()));

        assertThat(batchesReceived)
                .as("batch size")
                .allSatisfy(receivedBatchEntry -> assertThat(receivedBatchEntry).hasSizeLessThanOrEqualTo(batchSizeSetting))
                .as("all messages processed")
                .flatExtracting(PollContext::getConsumerRecordsFlattened).hasSameElementsAs(recs);

        for (var batch : batchesReceived) {
            var keysInBatch = batch.getConsumerRecordsFlattened().stream()
                    .map(ConsumerRecord::key)
                    .collect(Collectors.toList());
            assertThat(keysInBatch)
                    .as("no batch may hold two records of one key under KEY ordering - batch %s", keysInBatch)
                    .doesNotHaveDuplicates();
        }

        assertThat(batchesReceived)
                .as("%s records on one key need %s batches to themselves, so ceil(%s / %s) cannot be the expectation",
                        3, 3, keys.size(), batchSizeSetting)
                .hasSizeGreaterThanOrEqualTo(3);

        assertThat(getPC().isClosedOrFailed()).isFalse();

        baseTest.awaitForCommit(keys.size());
        getPC().closeDrainFirst();
    }

    @SneakyThrows
    public void batchFailureTest(ParallelConsumerOptions.ProcessingOrder order) {
        int batchSize = 5;
        int expectedNumOfMessages = 20;

        setupParallelConsumer(batchSize, ParallelConsumerOptions.DEFAULT_MAX_CONCURRENCY, order);

        var recs = getKtu().sendRecords(expectedNumOfMessages);
        List<PollContext<String, String>> receivedBatches = Collections.synchronizedList(new ArrayList<>());

        //
        batchFailPoll(receivedBatches);

        //
        baseTest.awaitForCommit(expectedNumOfMessages);

        //
        int expectedNumOfBatches = (int) Math.ceil(expectedNumOfMessages / (double) batchSize);

        // due to the failure, might get one extra batch
        assertThat(receivedBatches).hasSizeGreaterThanOrEqualTo(expectedNumOfBatches);

        assertThat(receivedBatches)
                .as("batch size")
                .allSatisfy(receivedBatch ->
                        assertThat(receivedBatch).hasSizeLessThanOrEqualTo(batchSize))
                .as("all messages processed")
                .flatExtracting(PollContext::getConsumerRecordsFlattened).hasSameElementsAs(recs);

        //
        assertThat(getPC().isClosedOrFailed()).isFalse();
    }

    /**
     * Must call {@link #batchFailPollInner}
     */
    protected abstract void batchFailPoll(List<PollContext<String, String>> receivedBatches);

    protected POLL_RETURN batchFailPollInner(PollContext<String, String> batchPollContext) {
        List<Long> offsets = batchPollContext.getOffsetsFlattened();

        boolean contains = offsets.contains(FAILURE_TARGET);
        if (contains) {
            var target = batchPollContext.stream().filter(x -> x.offset() == FAILURE_TARGET).findFirst().get();
            int numberOfFailedAttempts = target.getNumberOfFailedAttempts();
            int targetAttempts = 3;
            if (numberOfFailedAttempts < targetAttempts) {
                log.debug("Failing batch containing target offset {}", FAILURE_TARGET);
                throw new FakeRuntimeException(msg("Testing failure processing a batch - pretend attempt #{}", numberOfFailedAttempts));
            } else {
                log.debug("Failing target {} now completing as has has reached target attempts {}", offsets, targetAttempts);
            }
        }
        log.debug("Completing batch {}", offsets);
        return null;
    }

}
