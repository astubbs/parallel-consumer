package bz.stub.parallelconsumer.internal;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * The ownership lifecycle of {@link ThreadConfinedConsumer}: claim, guarded use, release, and the
 * non-stealing takeover the closing thread uses.
 * <p>
 * The load-bearing properties (see confluentinc#857 and the close-path incident this fixes -
 * pc-control's {@code Consumer.close()} was rejected against a pc-broker-poll owner whose loop had
 * already exited, so the consumer never closed and no LeaveGroup was sent):
 * <ol>
 *   <li>a foreign thread is rejected while the owner holds the claim - the guard's whole point</li>
 *   <li>after the owner releases, another thread can take over and close - the sequential handoff
 *       at shutdown is legal</li>
 *   <li>{@link ThreadConfinedConsumer#tryClaimOwnership()} never steals from a live claim - a
 *       close that races a still-running poll loop must still be rejected</li>
 *   <li>a non-owner cannot release someone else's claim - releasing on another thread's behalf
 *       would silently disarm the guard</li>
 * </ol>
 */
class ThreadConfinedConsumerTest {

    ThreadConfinedConsumer<String, String> confined;

    MockConsumer<String, String> delegate;

    ExecutorService otherThread;

    @BeforeEach
    void setup() {
        delegate = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        confined = new ThreadConfinedConsumer<>(delegate);
        otherThread = Executors.newSingleThreadExecutor(runnable -> {
            Thread thread = new Thread(runnable, "test-foreign-thread");
            thread.setDaemon(true);
            return thread;
        });
    }

    @AfterEach
    void teardown() {
        otherThread.shutdownNow();
    }

    /** Runs the action on the foreign thread and rethrows anything it threw. */
    private void onOtherThread(Runnable action) throws Exception {
        try {
            otherThread.submit(action).get(10, TimeUnit.SECONDS);
        } catch (java.util.concurrent.ExecutionException e) {
            if (e.getCause() instanceof Exception cause) throw cause;
            throw e;
        }
    }

    @Test
    void unclaimedConsumerAllowsAnyThread() throws Exception {
        // init-time calls (subscribe etc.) happen before any claim, from arbitrary threads
        assertDoesNotThrow(() -> confined.assignment());
        onOtherThread(() -> assertDoesNotThrow(() -> confined.assignment()));
    }

    @Test
    void foreignThreadRejectedWhileClaimHeld() throws Exception {
        confined.claimOwnership();

        onOtherThread(() -> {
            var thrown = assertThrows(IllegalStateException.class, () -> confined.close());
            assertThat(thrown).hasMessageThat().contains("close");
            assertThat(thrown).hasMessageThat().contains("test-foreign-thread");
        });

        // owner is unaffected
        assertDoesNotThrow(() -> confined.assignment());
    }

    @Test
    void closingThreadCanTakeOverAfterOwnerReleases() throws Exception {
        // simulate the poll thread's life on the foreign thread: claim, use, release on loop exit
        onOtherThread(() -> {
            confined.claimOwnership();
            confined.assignment();
            confined.releaseOwnership();
        });

        // the closing thread takes over and closes - the shutdown handoff that was being rejected
        assertThat(confined.tryClaimOwnership()).isTrue();
        assertDoesNotThrow(() -> confined.close(Duration.ofSeconds(1)));
        assertThat(delegate.closed()).isTrue();

        // and the takeover re-arms the guard for the new owner
        onOtherThread(() ->
                assertThrows(IllegalStateException.class, () -> confined.assignment()));
    }

    @Test
    void tryClaimNeverStealsFromLiveOwner() throws Exception {
        onOtherThread(() -> confined.claimOwnership());

        // the closeAndWait-timed-out case: poll loop still holds the claim - close must NOT proceed
        assertThat(confined.tryClaimOwnership()).isFalse();
        assertThrows(IllegalStateException.class, () -> confined.close());
        assertThat(delegate.closed()).isFalse();
    }

    @Test
    void tryClaimIsIdempotentForCurrentOwner() {
        confined.claimOwnership();
        assertThat(confined.tryClaimOwnership()).isTrue();
        assertDoesNotThrow(() -> confined.assignment());
    }

    @Test
    void nonOwnerCannotReleaseSomeoneElsesClaim() throws Exception {
        confined.claimOwnership();

        onOtherThread(() -> {
            confined.releaseOwnership(); // must be a warning no-op, not a disarm
            assertThrows(IllegalStateException.class, () -> confined.assignment());
        });
    }

    @Test
    void wakeupAllowedFromAnyThread() throws Exception {
        confined.claimOwnership();
        // the one documented thread-safe Consumer method must stay reachable cross-thread
        onOtherThread(() -> assertDoesNotThrow(() -> confined.wakeup()));
    }
}
