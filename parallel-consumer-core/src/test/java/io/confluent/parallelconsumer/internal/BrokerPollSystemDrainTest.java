package io.confluent.parallelconsumer.internal;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import io.confluent.parallelconsumer.ParallelEoSStreamProcessorTestBase;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.CountDownLatch;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static io.confluent.csid.utils.LatchTestUtils.awaitLatch;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.mockingDetails;

/**
 * Covers the broker-poll system's behaviour while the PC is draining for close.
 * <p>
 * <b>The drain-path defect this guards against</b> (see
 * {@code docs/solutions/test-flakiness/pc-silent-stall-under-contention-2026-07-29.md}):
 * {@code BrokerPollSystem.drain()} used to call {@code ConsumerManager.signalStop()} <i>before</i> entering
 * {@code DRAINING}, and {@code ConsumerManager.poll()} guarded the real {@code consumer.poll()} call with a
 * private {@code while (!shutdownRequested)} flag — so once draining started, <b>{@code consumer.poll()}
 * was never invoked again</b>. Two consequences:
 * <ol>
 *     <li><b>Busy-spin:</b> the poll loop's intended sleep is the paused 2s long poll (see the comment in
 *     {@link BrokerPollSystem}{@code #handlePoll()}: "if draining - subs will be paused, so use this to just
 *     sleep") — with the short-circuit it never blocks, spinning at ~10k iterations/s (measured).</li>
 *     <li><b>Zombie group member:</b> rebalance participation (rejoin / revoke-ack) happens inside
 *     {@code consumer.poll()}; a draining consumer that never polls cannot respond to rebalances while its
 *     background heartbeat thread keeps it a live member — it holds its full partition assignment (for up to
 *     {@code max.poll.interval.ms}) while consuming nothing, starving same-group siblings. Maps onto
 *     upstream issue #857 "paused consumption after rebalance".</li>
 * </ol>
 * <b>Desired behaviour</b> (the fix): during {@code DRAINING} the poller keeps invoking
 * {@code consumer.poll()} — the paused long poll is the loop's sleep AND keeps the member
 * rebalance-responsive.
 */
@Timeout(60)
@Slf4j
class BrokerPollSystemDrainTest extends ParallelEoSStreamProcessorTestBase {

    /**
     * Guards the fix: while draining, the poller must keep invoking {@code consumer.poll()} at long-poll
     * cadence — bounded above (no busy-spin) and at least once per window (stays rebalance-responsive).
     * <p>
     * History: committed first as a characterisation of the defect (asserting zero polls during drain,
     * as {@code drainStopsInvokingConsumerPoll_characterisesZombieDrainDefect}); flipped to the desired
     * property by the fix commit that collapsed {@code ConsumerManager.shutdownRequested} into the poll
     * system's lifecycle.
     */
    @Test
    void drainKeepsPollingConsumer_staysRebalanceResponsiveWithoutSpinning() throws InterruptedException {
        var workStarted = new CountDownLatch(1);
        var releaseWork = new CountDownLatch(1);

        // in-flight work that parks, so close(DRAIN) blocks in its await-workers phase - holding the PC in
        // the draining window we want to observe
        parallelConsumer.poll(recordContexts -> {
            workStarted.countDown();
            awaitLatch(releaseWork);
        });
        primeFirstRecord();

        awaitLatch(workStarted);

        // sanity: while RUNNING, the poller is invoking consumer.poll() (proves the counting observable)
        long pollsWhileRunning = consumerPollInvocationCount();
        await().until(() -> consumerPollInvocationCount() > pollsWhileRunning);

        // start close(DRAIN) on another thread - it blocks until the parked work completes
        Thread closer = new Thread(parallelConsumer::closeDrainFirst, "test-drain-closer");
        try {
            closer.start();

            // drain has begun once the subscriptions get paused
            await().untilAsserted(() -> assertThat(consumerSpy.paused()).isNotEmpty());

            // observe poll activity across three long-poll periods of the drain window
            long pollsAtDrainStart = consumerPollInvocationCount();
            io.confluent.csid.utils.ThreadUtils.sleepQuietly(3 * DEFAULT_BROKER_POLL_FREQUENCY_MS);
            long pollsDuringDrainWindow = consumerPollInvocationCount() - pollsAtDrainStart;

            // The drain-window polling property:
            // >= 1: the drainer still polls, so it can ack revokes / rejoin - not a zombie group member.
            // <= 30: polls happen at long-poll cadence (nominal ~3 in this window), not a busy-spin
            //        (the defect's spin ran at ~10k iterations/s - orders of magnitude past this bound).
            assertWithMessage("consumer.poll() invocations during the drain window")
                    .that(pollsDuringDrainWindow)
                    .isAtLeast(1);
            assertWithMessage("consumer.poll() cadence during drain should be the long poll, not a spin")
                    .that(pollsDuringDrainWindow)
                    .isAtMost(30);
        } finally {
            // always release the parked work so close can complete, even if assertions fail
            releaseWork.countDown();
        }

        closer.join(defaultTimeoutMs);
        assertWithMessage("close(DRAIN) should complete once in-flight work finishes")
                .that(closer.isAlive())
                .isFalse();
        assertThat(parallelConsumer.isClosedOrFailed()).isTrue();
        assertWithMessage("close should complete cleanly, not via failure")
                .that(parallelConsumer.getFailureCause())
                .isNull();
    }

    private long consumerPollInvocationCount() {
        return mockingDetails(consumerSpy).getInvocations().stream()
                .filter(invocation -> invocation.getMethod().getName().equals("poll"))
                .count();
    }
}
