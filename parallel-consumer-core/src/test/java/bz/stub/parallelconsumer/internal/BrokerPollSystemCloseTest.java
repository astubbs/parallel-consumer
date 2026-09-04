package bz.stub.parallelconsumer.internal;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelEoSStreamProcessorTestBase;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;

import static com.google.common.truth.Truth.assertWithMessage;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mockingDetails;
import static org.mockito.Mockito.verify;
import static org.awaitility.Awaitility.await;

/**
 * Covers the broker-poll system's behaviour while the PC is CLOSING - the state a bare
 * {@code close()} reaches directly, without passing through {@code DRAINING}.
 * <p>
 * <b>The defect this guards against.</b> {@code handlePoll()} used to poll only in
 * {@code RUNNING} and {@code DRAINING}, so an instance stopped calling {@code consumer.poll()} the
 * moment it entered {@code CLOSING} - while its consumer was still an open group member. Rejoin and
 * revoke-ack happen inside {@code poll()}, so with a JoinGroup already in flight the member could no
 * longer discharge it, and {@code doClose()} then handed that same request to
 * {@code consumer.close()}, whose
 * {@code AbstractCoordinator.close -> ConsumerNetworkClient.awaitPendingRequests} waits for it. The
 * coordinator will not answer a JoinGroup until every member has joined - including the members now
 * stuck in that wait - so each closing instance waited on a coordinator that was waiting on it, for
 * the whole {@code DEFAULT_TIMEOUT} budget, as a silent group member.
 * <p>
 * This is {@link BrokerPollSystemDrainTest}'s defect one lifecycle state along: astubbs/parallel-consumer#80
 * fixed it for {@code DRAINING} and {@code CLOSING} did not get the fix, which mattered more than it
 * sounds because {@code closeDontDrainFirst()} - what a bare {@code close()} calls - skips
 * {@code DRAINING} altogether. Measured at 2 failures in 30 runs of
 * {@code MultiInstanceRebalanceTest.largeNumberOfInstances}, ten of twelve instances parked in that
 * stack; evidence in
 * {@code docs/inflight/test-largenumberofinstances-residual-failures-measured-not-explained.md}.
 * <p>
 * <b>Desired behaviour</b> (the fix): an instance that has not yet left the group keeps polling it.
 * <p>
 * <b>What this test can and cannot prove locally, stated so nobody reads more into a green run.</b>
 * {@code transitionToClosing()} runs on the caller's thread and wakes the consumer, so {@code CLOSING}
 * lands either between control-loop iterations or midway through a long poll. Against a
 * {@code MockConsumer} whose {@code poll} returns immediately, the local loop spins fast enough that
 * the transition essentially always lands BETWEEN iterations - so a fix that only polls from
 * {@code handlePoll()}'s {@code CLOSING} branch passes here and is still broken, because the
 * mid-poll case skips that branch entirely (the woken poll finishes the {@code RUNNING} iteration and
 * the switch below it closes at once). That is not hypothetical: it is what the first version of this
 * fix did, and CI caught it - Unit Tests in run 33838043495, this test, zero closing polls. Attempting
 * to force the window locally with a sleep was tried and did NOT reproduce it, so no such sleep is
 * kept here.
 * <p>
 * What makes the shipped fix safe is therefore structural rather than observed: the discharge poll is
 * an unconditional call on the close path ({@code doClose()}), not a branch that a given interleaving
 * may skip. Read {@code BrokerPollSystem#dischargeCoordinatorBeforeClose} before moving it back into
 * {@code handlePoll()} - that move is the regression this paragraph exists to prevent.
 */
@Timeout(60)
@Slf4j
class BrokerPollSystemCloseTest extends ParallelEoSStreamProcessorTestBase {

    /**
     * The 1ms poll is what makes this observable, and it is not a coincidence to be tidied away:
     * {@code pollBrokerForRecords()} gives {@code RUNNING} and {@code DRAINING} the long-poll
     * timeout and every other state 1ms, and {@code CLOSING} is the only other state that reaches
     * {@code handlePoll()}. So a {@code poll(1ms)} invocation is, uniquely, a poll made while
     * closing - which is why this asserts on the argument rather than on a count. A count would
     * race the loop's ordinary long polls and could pass with the defect present.
     */
    @Test
    void closingKeepsPollingUntilTheConsumerActuallyLeavesTheGroup() {
        parallelConsumer.poll(recordContexts -> {
            // no-op: this test is about the close path, not about work
        });
        primeFirstRecord();

        // sanity: the poller is running and polling at the long-poll timeout, so the spy is observing
        // (a vacuous version of this test would pass on a poller that never started at all)
        await().untilAsserted(() -> assertWithMessage("poller should be polling while RUNNING")
                .that(consumerPollInvocationCount())
                .isGreaterThan(0L));

        // bare close(): DONT_DRAIN, so it transitions straight to CLOSING
        parallelConsumer.close();

        assertWithMessage("a CLOSING instance is still a group member, so it must keep polling - "
                + "a poll(1ms) is uniquely a poll made while CLOSING")
                .that(closingPollInvocationCount())
                .isAtLeast(1L);

        // and it must still have actually closed the consumer, not merely polled forever
        verify(consumerSpy, atLeastOnce()).close(any(Duration.class));
    }

    private long consumerPollInvocationCount() {
        return mockingDetails(consumerSpy).getInvocations().stream()
                .filter(invocation -> invocation.getMethod().getName().equals("poll"))
                .count();
    }

    /** Polls made with the 1ms timeout - see the test's javadoc for why that identifies CLOSING. */
    private long closingPollInvocationCount() {
        return mockingDetails(consumerSpy).getInvocations().stream()
                .filter(invocation -> invocation.getMethod().getName().equals("poll"))
                .filter(invocation -> invocation.getArguments().length == 1
                        && Duration.ofMillis(1).equals(invocation.getArguments()[0]))
                .count();
    }
}
