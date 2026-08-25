package bz.stub.parallelconsumer.proxy.engine;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

/**
 * The protection window's state machine (R42, R44): it holds, it expires exactly once, and a reconnect takes
 * it out of the picture entirely.
 *
 * @author Antony Stubbs
 */
class ReconnectWindowTest {

    private static final Duration WINDOW = Duration.ofSeconds(30);

    private final EngineFixture.TestClock clock = new EngineFixture.TestClock();

    private final ReconnectWindow window = new ReconnectWindow(new LivenessSettings(true,
            Duration.ofSeconds(60), Duration.ofSeconds(20), WINDOW, clock));

    @Test
    void aConnectedSessionIsNotHoldingAnything() {
        assertThat(window.isHolding()).isFalse();
        assertThat(window.expireIfDue()).isFalse();
    }

    @Test
    void theWindowHoldsUntilItsDeadline() {
        window.open();

        clock.advance(WINDOW.minusSeconds(1));

        assertThat(window.isHolding()).isTrue();
        assertWithMessage("the records are held until the window is actually up")
                .that(window.expireIfDue()).isFalse();
    }

    /**
     * The expiry is one-shot on purpose: a client that never comes back must not leave the engine sweeping
     * every control loop pass, dispatching and abandoning the same records forever.
     */
    @Test
    void theWindowExpiresExactlyOnce() {
        window.open();
        clock.advance(WINDOW);

        assertThat(window.expireIfDue()).isTrue();

        clock.advance(WINDOW.multipliedBy(10));
        assertThat(window.expireIfDue()).isFalse();
        assertThat(window.isHolding()).isFalse();
    }

    @Test
    void aReconnectTakesTheWindowOutOfThePicture() {
        window.open();
        clock.advance(WINDOW.minusSeconds(1));

        window.close();
        clock.advance(WINDOW.multipliedBy(10));

        assertWithMessage("a window closed by a reconnect must never fire afterwards")
                .that(window.expireIfDue()).isFalse();
        assertThat(window.isHolding()).isFalse();
    }

    /** Repeated loss notifications (a torn-down stream reporting twice) must not extend the deadline. */
    @Test
    void openingAnAlreadyOpenWindowDoesNotExtendIt() {
        window.open();
        clock.advance(WINDOW.dividedBy(2));
        window.open();
        clock.advance(WINDOW.dividedBy(2));

        assertThat(window.expireIfDue()).isTrue();
    }
}
