package bz.stub.parallelconsumer;

/*-
 * Copyright (C) 2020-2025 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.metrics.PCMetricsDef;
import io.micrometer.core.instrument.*;
import io.micrometer.core.instrument.simple.SimpleConfig;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.assertj.core.data.Offset;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import pl.tlinkowski.unij.api.UniLists;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static bz.stub.parallelconsumer.internal.State.PAUSED;
import static bz.stub.parallelconsumer.internal.State.RUNNING;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

@Slf4j
class PCMetricsTest extends ParallelEoSStreamProcessorTestBase {
    private SimpleMeterRegistry registry;
    private final List<Tag> commonTags = UniLists.of(Tag.of("tag1", "pc1"));

    /**
     * The freeze plan for {@link #metricsRegisterBinding()} - see that test for what the two latched ranges are
     * for. Class level rather than local because {@link #getOptions()} runs before the test body and has to size
     * the worker pool against them: {@link #GAP_WIDTH} workers per partition are parked forever on the gap alone,
     * so a pool no wider than that could never dispatch anything above it and the test would hang.
     */
    private static final int GAP_START_OFFSET = 50;
    private static final int GAP_WIDTH = 2;
    private static final int FREEZE_FROM_OFFSET = 250;
    private static final int MAX_CONCURRENCY = 16;

    @Test
    @SneakyThrows
    void metricsRegisterBinding() {
        // The pool has to outlast the workers the gap parks forever, on both partitions, with something left over.
        // Asserted rather than trusted to stay in step, because getting it wrong hangs rather than fails.
        assertThat(MAX_CONCURRENCY).isGreaterThan(GAP_WIDTH * 2);

        final int quantityP0 = 1000;
        final int quantityP1 = 500;
        final int p1StartingOffset = quantityP0;

        // Freeze the run by gating on each record's OWN offset, never on a shared completion counter. Two ranges
        // are latched open per partition and stay open until the end of the test:
        //
        //   - a GAP_WIDTH-offset hole at GAP_START_OFFSET, so the completed set is deliberately NOT contiguous;
        //   - everything from FREEZE_FROM_OFFSET up, which is what eventually parks the whole worker pool.
        //
        // Shards hand work out in ascending offset order (ProcessingShard's `entries` is a ConcurrentSkipListMap,
        // iterated in `getWorkIfAvailable`), so each partition's completed set is a PREFIX with one known hole in
        // it - [0, GAP_START_OFFSET) u [GAP_START_OFFSET + GAP_WIDTH, wherever it stopped) - whatever the
        // scheduler does. That shape, not any particular count, is what every expectation below rests on.
        //
        // Where it stops is deliberately NOT asserted. Both partitions share one worker pool, so whichever
        // reaches FREEZE_FROM_OFFSET first parks the pool and can leave the other tens of records short. Measured
        // over 30 runs, half of them against a deliberately saturated box: the leading partition always stops at
        // 248, the lagging one anywhere from 161 to 248. An expectation pinned to a particular count would be a
        // flake - which is also why GAP_START_OFFSET sits well below that range, since the shape below only holds
        // for a partition that got past the gap.
        //
        // Gating on a counter - `counter.get() >= numberToBlockAt`, read BEFORE the increment - was the defect
        // this replaces, and it broke the prefix shape rather than the count. Workers evaluate it while holding
        // different offsets, so whichever read a value past the threshold parked without ever completing, and a
        // parked worker could hold a LOWER offset than a completed one. That left holes nobody chose, below a
        // completion count the expectations were then derived from as though there were none.
        //
        // The one hole there is now is chosen, and it is the point: without it
        // PARTITION_HIGHEST_COMPLETED_OFFSET, PARTITION_HIGHEST_SEQUENTIAL_SUCCEEDED_OFFSET and
        // PARTITION_LAST_COMMITTED_OFFSET all collapse onto the same number, and asserting them proves nothing
        // about which is which. With it, the last two are pinned to constants the completion count cannot reach.
        AtomicBoolean blockingEnabled = new AtomicBoolean(true);

        ktu.send(consumerSpy, ktu.generateRecords(0, quantityP0));
        ktu.send(consumerSpy, ktu.generateRecords(1, quantityP1));
        CountDownLatch latchPartition0 = new CountDownLatch(1);
        CountDownLatch latchPartition1 = new CountDownLatch(1);
        AtomicInteger counterP0 = new AtomicInteger();
        AtomicInteger counterP1 = new AtomicInteger();
        AtomicBoolean failedRecordDone = new AtomicBoolean(false);
        parallelConsumer.poll(recordContexts -> {
            recordContexts.forEach(recordContext -> {
                log.trace("Processing: {}", recordContext);
                try {
                    AtomicInteger counter;
                    CountDownLatch latch;
                    long partitionStartingOffset;
                    if (recordContext.partition() == 0) {
                        counter = counterP0;
                        latch = latchPartition0;
                        partitionStartingOffset = 0;
                    } else {
                        counter = counterP1;
                        latch = latchPartition1;
                        partitionStartingOffset = p1StartingOffset;
                    }
                    //towards end of records in Partition 0 - throw RTE to get failed record to verify meter
                    if (recordContext.partition() == 0 && counter.get() > (quantityP0 - 300)) {
                        if (!failedRecordDone.getAndSet(true)) {
                            throw new RuntimeException("Failed a record to verify failed meter");
                        }
                    }
                    long relativeOffset = recordContext.offset() - partitionStartingOffset;
                    boolean inGap = relativeOffset >= GAP_START_OFFSET && relativeOffset < GAP_START_OFFSET + GAP_WIDTH;
                    boolean atOrPastFreezePoint = relativeOffset >= FREEZE_FROM_OFFSET;
                    if (blockingEnabled.get() && (inGap || atOrPastFreezePoint)) {
                        latch.await();
                    } else {
                        Thread.sleep(5);
                    }
                    counter.incrementAndGet();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });
        });

        log.info(registry.getMetersAsString());
        // metrics have some data
        await().atMost(Duration.ofSeconds(300)).pollInterval(Duration.ofSeconds(2)).untilAsserted(() -> {
            assertFalse(registry.getMeters().isEmpty());
            assertEquals(RUNNING.getValue(), registeredGaugeValueFor(PCMetricsDef.PC_STATUS));
            assertEquals(2, registeredGaugeValueFor(PCMetricsDef.NUMBER_OF_SHARDS));
            assertEquals(2, registeredGaugeValueFor(PCMetricsDef.NUMBER_OF_PARTITIONS));
        });

        // metrics show processing is complete
        // 120s budget (was default 10s) - matches the atMost budgets elsewhere in this method,
        // and gives headroom under PIT's instrumented JVM processing 1500 records.
        await().atMost(Duration.ofSeconds(120)).untilAsserted(() -> {
            log.info("counterP0: {}, counterP1: {}", counterP0.get(), counterP1.get());
            log.info(registry.getMetersAsString());
            assertThat(registeredGaugeValueFor(PCMetricsDef.NUM_PAUSED_PARTITIONS)).isEqualTo(2);
        });

        int highestSequentialSucceededOffsetP0 = GAP_START_OFFSET - 1;
        int highestSequentialSucceededOffsetP1 = GAP_START_OFFSET - 1 + p1StartingOffset;
        int highestSeenOffsetP0 = quantityP0 - 1;
        int highestSeenOffsetP1 = quantityP1 + p1StartingOffset - 1;

        // The completion counters stop when the pool is fully parked, but the meters trail them, and by different
        // amounts: a record's success reaches partition state after the user function returns
        // (PartitionState.onSuccess removes from incompleteOffsets and only then raises offsetHighestSucceeded),
        // and the commit that publishes LAST_COMMITTED_OFFSET lands a cycle later still. So no single meter's
        // arrival implies the rest - wait for the whole snapshot at once, rather than sleeping and hoping, or
        // converging on one meter and then reading the others racily.
        //
        // Each attempt reads the counters ONCE and asserts the meters against that reading, so an attempt taken
        // while records are still completing simply disagrees and is retried. When it finally agrees, no
        // completion is in flight - and none can start, because every remaining record is parked behind a latch.
        //
        // Two of these are pinned to constants the completion count cannot reach, and that is the assertion the
        // old test got wrong: the gap fixes where the contiguous watermark stops, no matter how much completed
        // above it.
        await().atMost(Duration.ofSeconds(120)).untilAsserted(() -> {
            int completedP0 = counterP0.get();
            int completedP1 = counterP1.get();
            log.info("counterP0: {}, counterP1: {}", completedP0, completedP1);

            // The run has to have crossed the gap for the shape below to hold, and cannot have passed the freeze
            // point. Checked, not assumed - a partition starved short of the gap would otherwise report a
            // confusing offset mismatch instead of saying what actually went wrong.
            assertThat(completedP0).isGreaterThan(GAP_START_OFFSET).isLessThanOrEqualTo(FREEZE_FROM_OFFSET - GAP_WIDTH);
            assertThat(completedP1).isGreaterThan(GAP_START_OFFSET).isLessThanOrEqualTo(FREEZE_FROM_OFFSET - GAP_WIDTH);

            //Assert record counts, offset counts specific to Partition 0
            // completedP0 records over a prefix with a GAP_WIDTH hole in it, so the highest one reached is that
            // much further up than the count alone would suggest.
            assertThat(registeredGaugeValueFor(PCMetricsDef.PARTITION_HIGHEST_COMPLETED_OFFSET, 0))
                    .isEqualTo(completedP0 - 1 + GAP_WIDTH);
            assertThat(registeredGaugeValueFor(PCMetricsDef.PARTITION_HIGHEST_SEEN_OFFSET, 0))
                    .isEqualTo(highestSeenOffsetP0);
            // Deliberately NOT the highest completed offset, and deliberately not a function of the count: the
            // gap sits below both, and a contiguous watermark cannot step over a hole.
            assertThat(registeredGaugeValueFor(PCMetricsDef.PARTITION_HIGHEST_SEQUENTIAL_SUCCEEDED_OFFSET, 0))
                    .isEqualTo(highestSequentialSucceededOffsetP0);
            assertThat(registeredGaugeValueFor(PCMetricsDef.PARTITION_INCOMPLETE_OFFSETS, 0))
                    .isEqualTo(quantityP0 - completedP0);
            assertThat(registeredGaugeValueFor(PCMetricsDef.PARTITION_LAST_COMMITTED_OFFSET, 0))
                    .isEqualTo(highestSequentialSucceededOffsetP0 + 1);
            assertThat(registeredCounterValueFor(PCMetricsDef.PROCESSED_RECORDS,
                    "topic", topicPartition.topic(), "partition", String.valueOf(0)))
                    .isEqualTo(completedP0);

            //Assert same as above for Partition 1
            assertThat(registeredGaugeValueFor(PCMetricsDef.PARTITION_HIGHEST_COMPLETED_OFFSET, 1))
                    .isEqualTo(completedP1 - 1 + GAP_WIDTH + p1StartingOffset);
            assertThat(registeredGaugeValueFor(PCMetricsDef.PARTITION_HIGHEST_SEEN_OFFSET, 1))
                    .isEqualTo(highestSeenOffsetP1);
            assertThat(registeredGaugeValueFor(PCMetricsDef.PARTITION_HIGHEST_SEQUENTIAL_SUCCEEDED_OFFSET, 1))
                    .isEqualTo(highestSequentialSucceededOffsetP1);
            assertThat(registeredGaugeValueFor(PCMetricsDef.PARTITION_INCOMPLETE_OFFSETS, 1))
                    .isEqualTo(quantityP1 - completedP1);
            assertThat(registeredGaugeValueFor(PCMetricsDef.PARTITION_LAST_COMMITTED_OFFSET, 1))
                    .isEqualTo(highestSequentialSucceededOffsetP1 + 1);
            assertThat(registeredCounterValueFor(PCMetricsDef.PROCESSED_RECORDS,
                    "topic", topicPartition.topic(), "partition", String.valueOf(1)))
                    .isEqualTo(completedP1);

            assertThat(registeredGaugeValueFor(PCMetricsDef.SHARDS_SIZE))
                    .isEqualTo((quantityP0 - completedP0) + (quantityP1 - completedP1));
            // non partition specific metrics
            assertThat(registeredGaugeValueFor(PCMetricsDef.INCOMPLETE_OFFSETS_TOTAL))
                    .isEqualTo((quantityP0 - completedP0) + (quantityP1 - completedP1));
        });
        log.info(registry.getMetersAsString());

        // Safe to read the counters outside a wait now: the assertions above only agreed because nothing was
        // completing, and nothing can start again until the latches drop at the end of this method.
        int remainingP0 = quantityP0 - counterP0.get();
        int remainingP1 = quantityP1 - counterP1.get();

        assertThat(registeredGaugeValueFor(PCMetricsDef.INFLIGHT_RECORDS))
                .isGreaterThan(0); // I think it is CPU number bound as it defaults to some multiplier of available CPUs - so can't assume number based on my machine...

        assertThat(registeredDistributionSummaryFor(PCMetricsDef.METADATA_SPACE_USED))
                .isGreaterThan(0);

        assertThat(registeredTimerFor(PCMetricsDef.OFFSETS_ENCODING_TIME))
                .isGreaterThan(0);

        assertThat(registeredCounterValueFor(PCMetricsDef.OFFSETS_ENCODING_USAGE))
                .isGreaterThan(0);

        assertThat(registeredGaugeValueFor(PCMetricsDef.NUMBER_OF_PARTITIONS))
                .isEqualTo(2);
        assertThat(registeredGaugeValueFor(PCMetricsDef.NUM_PAUSED_PARTITIONS))
                .isEqualTo(2);

        assertThat(registeredDistributionSummaryFor(PCMetricsDef.PAYLOAD_RATIO_USED))
                .isGreaterThan(-1.0); // cant really check for actual value as it may be either 0 or >0 depending on timing of commit / encoding execution.

        assertThat(registeredGaugeValueFor(PCMetricsDef.NUMBER_OF_SHARDS))
                .isEqualTo(2);
        assertThat(registeredGaugeValueFor(PCMetricsDef.PC_STATUS))
                .isEqualTo(RUNNING.getValue());

        // it would be remaining - inFlight, but because inFlight number depends on load factor which in turn depends on CPU core number - adding allowable offset.
        assertThat(registeredGaugeValueFor(PCMetricsDef.WAITING_RECORDS))
                .isCloseTo((remainingP0 + remainingP1), Offset.offset(100.0));

        assertThat(registeredTimerFor(PCMetricsDef.USER_FUNCTION_PROCESSING_TIME)).isGreaterThan(0);

        // Release both latched ranges - the gap as well as the freeze point - so the run can finish.
        blockingEnabled.set(false);
        latchPartition0.countDown();
        latchPartition1.countDown();
        await().atMost(Duration.ofSeconds(120)).untilAsserted(() -> {
            assertThat(counterP0.get()).isEqualTo(quantityP0);
        });

        await().atMost(Duration.ofSeconds(120)).until(() -> counterP0.get() == quantityP0 &&
                registeredGaugeValueFor(PCMetricsDef.WAITING_RECORDS) == 0
        );

        await().atMost(Duration.ofSeconds(120)).pollInterval(Duration.ofSeconds(5)).untilAsserted(() -> {
            log.info(registry.getMetersAsString());
            assertThat(registeredCounterValueFor(PCMetricsDef.FAILED_RECORDS, "topic", topicPartition.topic(), "partition", String.valueOf(0)))
                    .isEqualTo(1);
        });
    }


    @Test
    @SneakyThrows
    void pcStatusMetricUpdatesOnChange() {
        final int quantity = 1000;

        ktu.send(consumerSpy, ktu.generateRecords(0, quantity));

        parallelConsumer.poll(recordContexts -> {
            recordContexts.forEach(recordContext -> {
                log.trace("Processing: {}", recordContext);
                try {
                    Thread.sleep(5);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });
        });

        log.info(registry.getMetersAsString());
        // metrics have some data
        await().atMost(Duration.ofSeconds(20)).pollInterval(Duration.ofSeconds(1)).untilAsserted(() -> {
            assertFalse(registry.getMeters().isEmpty());
            assertEquals(RUNNING.getValue(),
                    registeredGaugeValueFor(PCMetricsDef.PC_STATUS));
        });

        parallelConsumer.pauseIfRunning();
        ktu.send(consumerSpy, ktu.generateRecords(0, 100));
        await().atMost(Duration.ofSeconds(20)).pollInterval(Duration.ofSeconds(1)).untilAsserted(() -> {
            assertEquals(PAUSED.getValue(),
                    registeredGaugeValueFor(PCMetricsDef.PC_STATUS));
        });
        parallelConsumer.resumeIfPaused();
        await().atMost(Duration.ofSeconds(20)).pollInterval(Duration.ofSeconds(1)).untilAsserted(() -> {
            assertEquals(RUNNING.getValue(),
                    registeredGaugeValueFor(PCMetricsDef.PC_STATUS));
        });
    }


    private double registeredGaugeValueFor(PCMetricsDef metricsDef, String... filterTags) {
        return Optional.ofNullable(registry.find(metricsDef.getName()).tags(filterTags).gauge()).map(Gauge::value).orElse(-1.0);
    }

    private double registeredGaugeValueFor(PCMetricsDef metricsDef, int partition) {
        String[] filterTags = new String[]{"topic", topicPartition.topic(), "partition", String.valueOf(partition)};
        return registeredGaugeValueFor(metricsDef, filterTags);
    }

    private double registeredCounterValueFor(PCMetricsDef metricsDef, String... filterTags) {
        return Optional.ofNullable(registry.find(metricsDef.getName()).tags(filterTags).counter())
                .map(Counter::count).orElse(-1.0);
    }

    private double registeredTimerFor(PCMetricsDef metricsDef, String... tags) {
        return Optional.ofNullable(registry.find(metricsDef.getName()).tags(tags).timer())
                .map(timer -> timer.mean(TimeUnit.MILLISECONDS)).orElse(-1.0);
    }

    private double registeredDistributionSummaryFor(PCMetricsDef metricsDef, String... tags) {
        return Optional.ofNullable(registry.find(metricsDef.getName()).tags(tags).summary())
                .map(DistributionSummary::mean).orElse(-1.0);
    }

    @Override
    protected ParallelConsumerOptions<Object, Object> getOptions() {
        registry = new SimpleMeterRegistry(new SimpleConfig() {
            @Override
            public String get(final String key) {
                return null;
            }

            @Override
            public @NotNull Duration step() {
                return Duration.ofSeconds(10);
            }
        }, Clock.SYSTEM);
        ParallelConsumerOptions<Object, Object> options = getDefaultOptions()
                .meterRegistry(registry)
                .metricsTags(commonTags)
                // Pinned, not inherited from the default: metricsRegisterBinding's freeze plan parks workers
                // permanently and needs a pool it can count on. Same value the default happens to carry today.
                .maxConcurrency(MAX_CONCURRENCY)
                .build();

        return options;
    }
}

