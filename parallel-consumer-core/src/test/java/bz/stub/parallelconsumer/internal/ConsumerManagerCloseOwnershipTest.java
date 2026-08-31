package bz.stub.parallelconsumer.internal;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * {@link ConsumerManager#close(Duration)} under a live consumer owner - the SHUTDOWN PATH, one level
 * above {@link ThreadConfinedConsumerTest}.
 * <p>
 * That test covers the guard primitive: {@code tryClaimOwnership} refuses to steal, and a guarded
 * {@code close()} then throws without closing the delegate. This one covers the caller that actually
 * runs at shutdown, because the primitive being correct does not establish that the close SEQUENCE
 * uses it correctly - {@code ConsumerManager.close} waits out pending requests, then claims, then
 * closes, and the claim's result governs whether that last step can legally happen.
 * <p>
 * The case is not hypothetical. It is reached whenever {@code brokerPollSubsystem.closeAndWait()}
 * times out or throws and the close sequence proceeds anyway: teardown runs in a {@code finally} that
 * executes even though the poll thread was never joined.
 * <p>
 * The write-up naming this the two-threads-one-consumer commit seam arrives with the
 * confluentinc#857 work rather than with this change - cited by topic rather than by path, because
 * a path that does not resolve on the branch it is read from is worse than no citation.
 * <p>
 * <b>What must hold, and why each matters:</b>
 * <ol>
 *   <li><b>The delegate consumer is NOT closed</b> while another thread owns it. This is the safety
 *   property; everything else is reporting. Closing a consumer another thread is polling is the data
 *   race the ownership guard exists to prevent.</li>
 *   <li><b>The refusal is loud.</b> It throws rather than returning quietly, because
 *   {@code doClose} catches it and that catch is where the user is told the consequence - no
 *   LeaveGroup, so the group's next rebalance waits out {@code session.timeout.ms}. A silent skip
 *   would leave the member gone with nobody told why the group stalled.</li>
 *   <li><b>An unowned consumer still closes normally</b> - the guard must not break the happy path,
 *   which is the ordinary shutdown every user hits.</li>
 * </ol>
 */
class ConsumerManagerCloseOwnershipTest extends ThreadConfinedConsumerTestBase {

    private static final Duration TIMEOUT = Duration.ofSeconds(1);

    private ConsumerManager<String, String> manager;

    @BeforeEach
    void wrapTheConfinedConsumerInAManager() {
        manager = new ConsumerManager<>(confined, TIMEOUT, TIMEOUT, TIMEOUT);
    }

    /**
     * The poll thread is still alive and owns the consumer - the closeAndWait-timed-out case. The
     * manager must refuse, and above all must leave the delegate open.
     */
    @Test
    void closeRefusesAndLeavesTheConsumerOpenWhenAnotherThreadStillOwnsIt() throws Exception {
        onOtherThread(() -> confined.claimOwnership());

        assertThrows(IllegalStateException.class, () -> manager.close(TIMEOUT));

        // the property that matters: the live owner's consumer survived the attempt
        assertThat(delegate.closed()).isFalse();
    }

    /**
     * The ordinary shutdown: the poll loop has released (or never claimed), so the closing thread
     * takes over and the consumer closes. Without this, the test above would pass just as well
     * against a guard that refused everything.
     */
    @Test
    void closeSucceedsWhenNobodyOwnsTheConsumer() {
        assertDoesNotThrow(() -> manager.close(TIMEOUT));

        assertThat(delegate.closed()).isTrue();
    }

    /**
     * A released owner is the normal handoff at shutdown - the poll loop exited and released in its
     * {@code finally}, and the closing thread claims what it left behind.
     */
    @Test
    void closeSucceedsAfterTheOwnerReleases() throws Exception {
        onOtherThread(() -> {
            confined.claimOwnership();
            confined.releaseOwnership();
        });

        assertDoesNotThrow(() -> manager.close(TIMEOUT));

        assertThat(delegate.closed()).isTrue();
    }
}
