package bz.stub.parallelconsumer.internal;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelEoSStreamProcessorTestBase;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static java.time.Duration.ofSeconds;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * The runtime half of {@code AbstractParallelEoSStreamProcessor#CONTROL_THREAD}: once a control thread exists, a
 * drain of the work mailbox from any other thread is refused, naming both threads.
 * <p>
 * The declaration exists because the revoke path in transactional mode used to commit inline on the broker-poll
 * thread, and the fix for what that omitted - the drain - was to hand the commit to the control thread rather than
 * drain from the poll thread ({@code commitOnRevokeViaTheControlThread}). RacerD reads the declaration and never
 * checks it, so this is what fails if the drain ever grows a second caller thread again - the pattern and the rule
 * are {@code RetryQueue.RetryQueueIterator} and this package's {@code AGENTS.md}, "Declare thread confinement".
 * <p>
 * Awaited rather than sampled: the control thread is started by {@code poll} asynchronously, and until it has
 * recorded itself the guard has nothing to compare against and lets the call through - by design, for the
 * hand-driven harness that never starts one. So the assertion is that the refusal <em>arrives</em>, not that it is
 * immediate.
 */
class MailboxDrainConfinementTest extends ParallelEoSStreamProcessorTestBase {

    @Test
    @Timeout(60)
    void drainingTheMailboxFromAForeignThreadIsRefusedOnceTheControlThreadExists() {
        parallelConsumer.poll(ignored -> {
            // the control thread is the subject, not the processing
        });

        await("the control thread has started and the guard has something to compare against")
                .atMost(ofSeconds(20))
                .untilAsserted(() -> {
                    var refusal = assertThrows(IllegalStateException.class,
                            () -> parallelConsumer.processWorkCompleteMailBox(Duration.ZERO));
                    assertWithMessage("the refusal names the action, the confining thread and the caller, so the "
                            + "reader of a stack trace can tell which side crossed the line")
                            .that(refusal.getMessage())
                            .contains("draining the work mailbox");
                    assertThat(refusal.getMessage()).contains("pc-control");
                    assertThat(refusal.getMessage()).contains(Thread.currentThread().getName());
                });

        assertWithMessage("a refused foreign drain must not have failed the instance - the guard protects the "
                + "control thread's state, it does not tear the instance down")
                .that(parallelConsumer.isClosedOrFailed())
                .isFalse();
    }
}
