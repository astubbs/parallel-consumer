package bz.stub.parallelconsumer.internal;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.ParallelConsumerOptions.CommitMode;
import bz.stub.parallelconsumer.ParallelEoSStreamProcessorTestBase;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.KafkaException;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mockito;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static bz.stub.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.UNORDERED;
import static com.google.common.truth.Truth.assertWithMessage;
import static org.awaitility.Awaitility.await;

/**
 * When the broker-poll thread dies, the close it triggers must still close the Kafka consumer - <b>in every
 * commit mode</b>, not only the transactional one.
 * <p>
 * <b>The defect this pins.</b> The consumer close was gated on "am I the component that commits?"
 * ({@code isResponsibleForCommits}) at both ends of the seam: {@code BrokerPollSystem} closes it in the
 * consumer-commit modes, {@code AbstractParallelEoSStreamProcessor} closes it in
 * {@link CommitMode#PERIODIC_TRANSACTIONAL_PRODUCER}. That is an XOR over commit mode, and it assumed the
 * responsible thread is alive at close time. When the poll thread has died it is not, and in the
 * consumer-commit modes - including {@link CommitMode#PERIODIC_CONSUMER_ASYNCHRONOUS}, the shipped default -
 * the XOR left nobody: no {@code consumer.close()}, so no LeaveGroup.
 * <p>
 * <b>What that costs, and which timeout ends it.</b> Not {@code session.timeout.ms}. The consumer object is
 * still open, so its heartbeat thread keeps the session alive; what eventually evicts the member is the
 * heartbeat thread's own poll-interval check ({@code AbstractCoordinator.handlePollTimeoutExpiry}, reached
 * from {@code heartbeat.pollTimeoutExpired}), which fires after <b>{@code max.poll.interval.ms}</b> - five
 * minutes by default. Until then the dead member holds its partitions and nothing consumes them.
 * <p>
 * <b>Why the trigger here is a bare poll failure.</b> Anything escaping {@code BrokerPollSystem.controlLoop}
 * kills that thread; the specific throwable is not load-bearing, so the fixture uses the most general shape
 * rather than one reporter's. Two concrete producers of it are already on the record and both reach this
 * same exit: a user rebalance listener that throws out of {@code consumer.poll()}
 * ({@code PartitionStateManagerRevokeAfterFailedAssignmentTest} drives that one), and a user-supplied
 * {@code MeterRegistry} that throws during revoke
 * ({@code docs/solutions/runtime-errors/a-throwing-meter-registry-kills-the-poll-thread-and-strands-close.md}).
 * {@code ConsumerManager.poll} catches only {@code SaslAuthenticationException} and {@code WakeupException},
 * so a plain {@link KafkaException} propagates exactly as those do.
 * <p>
 * <b>The transactional arm is the control, and it is what makes a red consumer-commit arm mean something.</b>
 * It runs the identical fixture and was GREEN before the fix, because there the control thread was always the
 * designated closer. One term changes between arms - the commit mode - and the outcome flipped. A failure in
 * the transactional arm would therefore be the fixture's fault (the poll thread did not die, or PC did not
 * close), not the defect's.
 *
 * @author Antony Stubbs
 */
@Slf4j
class PollerDeathClosesTheConsumerTest extends ParallelEoSStreamProcessorTestBase {

    /** Marks the simulated failure so the assertions can prove it, and not some other close, ran. */
    private static final String SIMULATED = "simulated: poll failed and killed the broker-poll thread";

    /**
     * Armed <b>before</b> the instance starts, so the first poll dies and there is no timing to lose. The
     * metrics-teardown test arms late for the opposite reason - there the death path is what got in the way
     * of the path under test; here it IS the path under test.
     */
    private final AtomicBoolean pollFails = new AtomicBoolean(false);

    /** Non-vacuity: a run in which the consumer was never polled proves nothing about a dead poller. */
    private final AtomicInteger pollAttempts = new AtomicInteger();

    @ParameterizedTest
    @EnumSource(CommitMode.class)
    @Timeout(120)
    void aDeadPollThreadStillLeavesTheConsumerGroup(CommitMode commitMode) {
        setupParallelConsumerInstance(ParallelConsumerOptions.<String, String>builder()
                .commitMode(commitMode)
                .ordering(UNORDERED)
                .build());

        // Stubbed BEFORE the instance starts: arming a Mockito spy while the poll thread is inside the very
        // method being stubbed is a race, and the gate below needs none - the flag does the arming.
        Mockito.doAnswer(invocation -> {
            pollAttempts.incrementAndGet();
            if (pollFails.get()) {
                throw new KafkaException(SIMULATED);
            }
            return invocation.callRealMethod();
        }).when(consumerSpy).poll(Mockito.any(Duration.class));

        pollFails.set(true);

        parallelConsumer.poll(ignored -> {
            // the close path is the subject, not the processing
        });

        await().atMost(defaultTimeout)
                .until(() -> parallelConsumer.isClosedOrFailed());

        assertWithMessage("the consumer was never polled, so the poll thread cannot have died the way this "
                + "test claims - the fixture, not the engine, is what failed")
                .that(pollAttempts.get()).isGreaterThan(0);

        assertWithMessage("PC shut down for some reason other than the simulated poll failure, so this run "
                + "says nothing about a dead poller. Failure cause: %s", parallelConsumer.getFailureCause())
                .that(describeCauseChain(parallelConsumer.getFailureCause())).contains(SIMULATED);

        // THE PROPERTY. An open consumer keeps heartbeating, so the coordinator cannot tell this member is
        // dead and its partitions stay assigned to it until max.poll.interval.ms expires.
        await().atMost(defaultTimeout)
                .untilAsserted(() -> assertWithMessage(
                        "the broker-poll thread died and nothing closed the consumer, so no LeaveGroup was "
                                + "sent and this member holds its partitions until max.poll.interval.ms "
                                + "expires (commit mode: %s)", commitMode)
                        .that(consumerSpy.closed()).isTrue());
    }

    /**
     * Renders the whole cause chain, because the simulated failure arrives wrapped - the poll thread's
     * exception reaches the control thread through a {@code Future}, and is wrapped again on the way to
     * {@code failureReason}. Bounded by depth and identity for the same reason the test base's own walk is:
     * a cause chain can be cyclic.
     */
    private static String describeCauseChain(Throwable throwable) {
        StringBuilder rendered = new StringBuilder();
        var seen = java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<Throwable, Boolean>());
        for (Throwable t = throwable; t != null && seen.add(t) && seen.size() < 100; t = t.getCause()) {
            rendered.append(t).append(" | ");
        }
        return rendered.toString();
    }
}
