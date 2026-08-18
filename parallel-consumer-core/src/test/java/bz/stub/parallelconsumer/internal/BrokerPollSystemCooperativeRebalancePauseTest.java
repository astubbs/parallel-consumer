package bz.stub.parallelconsumer.internal;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.ParallelEoSStreamProcessorTestBase;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static bz.stub.parallelconsumer.internal.utils.LatchTestUtils.awaitLatch;
import static com.google.common.truth.Truth.assertWithMessage;
import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static org.awaitility.Awaitility.await;

/**
 * Regression test for the cooperative-rebalance / back-pressure pause interaction of
 * confluentinc#857: partitions paused for back pressure must be resumed after a rebalance that
 * <b>retains</b> them, or consumption stops silently.
 * <p>
 * <b>The defect this guards against:</b> {@link BrokerPollSystem} once mirrored Kafka's pause
 * state in a {@code boolean} flag ({@code pausedForThrottling} /
 * {@code subscriptionsPausedForBackPressure}), and reset that flag on every partition assignment,
 * reasoning that "Kafka clears its internal pause state on reassignment". That reasoning holds
 * only for the EAGER rebalance protocol. Under COOPERATIVE,
 * {@code ConsumerCoordinator.onJoinPrepare} passes the retained partitions through
 * {@code SubscriptionState.assignFromSubscribed}, which <b>reuses the existing
 * {@code TopicPartitionState}</b> - including its {@code paused} flag. So retained partitions stay
 * paused inside Kafka while the mirror flag was cleared; {@code resumeIfPaused()} - gated on the
 * mirror - became a no-op, and the paused partitions never resumed: consumption stopped with no
 * error. The fix derives the answer from {@code consumer.paused()} instead, which is
 * self-correcting under either protocol.
 * <p>
 * <b>How the rebalance is driven:</b> through the processor's own public
 * {@link org.apache.kafka.clients.consumer.ConsumerRebalanceListener} callback
 * ({@code AbstractParallelEoSStreamProcessor#onPartitionsAssigned(java.util.Collection)}), which
 * is exactly what Kafka itself invokes - NOT through any internals of {@link BrokerPollSystem},
 * so this test compiles and runs unchanged whether or not the defect is present. A cooperative
 * rebalance that retains every partition invokes the listener with the (empty) set of NEWLY added
 * partitions, and leaves the retained partitions' pause state in place - the
 * {@code MockConsumer}'s pause state being untouched models precisely that retention.
 * <p>
 * <b>Why the timing is deterministic:</b>
 * <ul>
 *     <li>Back-pressure engagement is forced, not raced: the user function parks on a latch, so
 *     the in-flight + queued count can only grow past the (pinned, see {@link #getOptions()})
 *     threshold, and cannot shrink until the latch opens. Pausing is therefore guaranteed, and
 *     the paused-with-mirror-flag-set state is stable for as long as the test needs it.</li>
 *     <li>On the defect arm the cleared mirror flag could only be "healed" by a re-pause, and
 *     re-pausing is rate-limited to once per second ({@code BrokerPollSystem#pauseLimiter}) - the
 *     latch is released immediately after the rebalance callback, so the pipeline drains (a
 *     handful of records through an instant no-op function, well under 100ms) long before the
 *     limiter would permit a re-pause. After the drain the throttle is off, so no re-pause can
 *     ever happen: the stall is permanent, and the resume assertion fails every run.</li>
 *     <li>On the fixed arm, resume needs no luck at all: every broker-poll loop iteration
 *     (bounded by the {@code 500ms} test long-poll) re-derives pause state from the consumer, so
 *     the 10s assertion window is ~20x the worst-case resume latency.</li>
 * </ul>
 */
@Timeout(60)
@Slf4j
class BrokerPollSystemCooperativeRebalancePauseTest extends ParallelEoSStreamProcessorTestBase {

    private static final int BACK_PRESSURE_THRESHOLD = 2;

    /**
     * Pin the back-pressure threshold to a small constant so pause engagement is deterministic:
     * {@code shouldThrottle()} compares the pipeline depth against
     * {@code maxConcurrency * batchSize * loadingFactor}; pinning the dynamic load factor to 1
     * (initial == maximum, so it can never step) makes that exactly
     * {@value #BACK_PRESSURE_THRESHOLD}, with no time-dependent drift.
     */
    @Override
    protected ParallelConsumerOptions<Object, Object> getOptions() {
        return getDefaultOptions()
                .maxConcurrency(BACK_PRESSURE_THRESHOLD)
                .initialLoadFactor(1)
                .maximumLoadFactor(1)
                .build();
    }

    @Test
    void partitionsPausedForBackPressureResumeAfterCooperativeRebalanceRetainsThem() {
        var releaseWork = new CountDownLatch(1);
        var processedCount = new AtomicInteger();

        parallelConsumer.poll(recordContexts -> {
            awaitLatch(releaseWork);
            processedCount.incrementAndGet();
        });

        // comfortably above the pinned threshold of 2, small enough to drain in tens of ms
        int recordCount = 12;
        for (int i = 0; i < recordCount; i++) {
            consumerSpy.addRecord(ktu.makeRecord("key-" + i, "value-" + i));
        }

        // 1. Back pressure engages: with the user function parked, pipeline depth can only grow
        //    past the threshold, so the poll system pauses the assignment. Read the pause state
        //    from the test's own MockConsumer reference - consumer methods must not be called
        //    through PC from the test thread (ThreadConfinedConsumer).
        //    The tight poll interval keeps detection latency small, so the whole
        //    callback-then-drain sequence below completes well inside the pause rate limiter's
        //    1s window (see the class javadoc for why that makes the defect arm deterministic).
        await().pollInterval(ofMillis(5)).atMost(ofSeconds(10))
                .untilAsserted(() -> assertWithMessage("back pressure should pause the assignment")
                        .that(consumerSpy.paused()).isNotEmpty());

        // 2. A cooperative rebalance retains every partition: Kafka invokes the listener with the
        //    empty set of newly-added partitions, and keeps the retained partitions paused
        //    (SubscriptionState.assignFromSubscribed reuses each TopicPartitionState). The
        //    MockConsumer's pause state is deliberately left untouched - that IS the cooperative
        //    retention behaviour this test exists to model.
        parallelConsumer.onPartitionsAssigned(Collections.emptyList());

        // 3. The pipeline drains: the user function unblocks and every queued record completes
        //    near-instantly, taking the throttle off.
        releaseWork.countDown();

        // 4. THE regression assertion: once back pressure clears, the poll system must resume the
        //    partitions Kafka still holds paused. With the mirror-flag defect present, the flag
        //    was cleared in step 2 while Kafka kept the partitions paused, resumeIfPaused() is a
        //    permanent no-op, and this times out: consumption has stopped with no error.
        await().atMost(ofSeconds(10))
                .untilAsserted(() -> assertWithMessage(
                        "partitions must be resumed once back pressure clears - a timeout here means the "
                                + "poll system lost track of Kafka's pause state across a rebalance that "
                                + "retained the partitions (cooperative protocol), and consumption has "
                                + "stopped silently: confluentinc#857")
                        .that(consumerSpy.paused()).isEmpty());

        // 5. And records genuinely flow again end-to-end: everything already fed is processed
        //    (some of it only pollable after the resume), and so is a fresh record.
        await().atMost(ofSeconds(10))
                .untilAsserted(() -> assertWithMessage("all fed records should complete processing")
                        .that(processedCount.get()).isEqualTo(recordCount));

        consumerSpy.addRecord(ktu.makeRecord("key-after-resume", "value-after-resume"));
        await().atMost(ofSeconds(10))
                .untilAsserted(() -> assertWithMessage("a record fed after the resume should be processed")
                        .that(processedCount.get()).isEqualTo(recordCount + 1));
    }
}
