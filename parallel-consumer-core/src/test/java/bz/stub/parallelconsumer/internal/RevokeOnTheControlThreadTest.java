package bz.stub.parallelconsumer.internal;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelEoSStreamProcessorTestBase;
import bz.stub.parallelconsumer.internal.utils.LogCapture;
import ch.qos.logback.classic.Level;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static bz.stub.parallelconsumer.ParallelConsumerOptions.CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER;
import static com.google.common.truth.Truth.assertWithMessage;
import static java.time.Duration.ofSeconds;
import static org.awaitility.Awaitility.await;

/**
 * The close path fires the rebalance callbacks on the control thread itself ({@code maybeCloseConsumer}), and a
 * thread cannot wait on itself - so in transactional mode a revocation that arrives ON the control thread must take
 * the inline branch of {@code commitOnRevokeViaTheControlThread}: a tryLock commit of what the close has already
 * drained, and back without posting a request it would then wait on. The hand-driven harness never starts a control thread, so this runs a
 * real one and fires the revocation from inside a loop-end hook, which the control thread runs.
 * <p>
 * The log line is the witness that the inline branch ran rather than the request path: both return promptly on an
 * idle instance, and only the branch itself says which it was.
 */
class RevokeOnTheControlThreadTest extends ParallelEoSStreamProcessorTestBase {

    @Test
    @Timeout(60)
    void aRevocationOnTheControlThreadItselfCommitsInlineWithoutWaiting() {
        setupParallelConsumerInstance(getDefaultOptions()
                .commitMode(PERIODIC_TRANSACTIONAL_PRODUCER)
                .commitLockAcquisitionTimeout(ofSeconds(30))
                .build());
        List<TopicPartition> assigned = new ArrayList<>(consumerSpy.assignment());
        var fired = new AtomicBoolean();
        var revokeTook = new AtomicReference<Duration>();

        try (var log = LogCapture.of(AbstractParallelEoSStreamProcessor.class, Level.INFO)) {
            parallelConsumer.addLoopEndCallBack(() -> {
                if (fired.compareAndSet(false, true)) {
                    long started = System.nanoTime();
                    parallelConsumer.onPartitionsRevoked(assigned);
                    revokeTook.set(Duration.ofNanos(System.nanoTime() - started));
                }
            });
            parallelConsumer.poll(ignored -> {
                // the revocation is the subject, not the processing
            });

            await("the loop-end hook has revoked on the control thread and returned")
                    .atMost(ofSeconds(30))
                    .until(() -> revokeTook.get() != null);

            assertWithMessage("on the control thread the revocation must not wait for a pass that is itself - it "
                    + "commits inline; a wait here would be the callback waiting on its own thread")
                    .that(revokeTook.get().compareTo(ofSeconds(5)) < 0)
                    .isTrue();
            assertWithMessage("the inline branch announced itself, so this was not the request path returning "
                    + "quickly on an idle instance")
                    .that(log.messagesAt(Level.INFO).stream()
                            .anyMatch(m -> m.contains("reached the control thread itself")))
                    .isTrue();
        }
        assertWithMessage("the instance is still healthy after revoking on its own thread")
                .that(parallelConsumer.isClosedOrFailed())
                .isFalse();
    }
}
