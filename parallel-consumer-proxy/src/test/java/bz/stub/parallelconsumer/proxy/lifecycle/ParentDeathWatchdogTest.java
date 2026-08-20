package bz.stub.parallelconsumer.proxy.lifecycle;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.io.InputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

/**
 * KTD19's two signals, and the case that must NOT fire.
 * <p>
 * No wall-clock sleeps anywhere: every wait is a bounded latch await, so a slow machine makes the test
 * slower rather than red, and a watchdog that never fires fails on the bound instead of hanging the suite.
 */
class ParentDeathWatchdogTest {

    /** Long enough to be reached only by a genuine failure, short enough that a hang is not a coffee break. */
    private static final Duration GENEROUS = Duration.ofSeconds(10);

    /** The poll must be brisk, or the second signal costs more than the first is worth. */
    private static final Duration POLL = Duration.ofMillis(20);

    /**
     * The primary signal. The parent holds the write end and never writes; when it dies the kernel closes
     * the last write end and the read returns -1.
     */
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void eofOnTheLifelineIsParentDeath() throws Exception {
        var writeEnd = new PipedOutputStream();
        var lifeline = new PipedInputStream(writeEnd);

        try (var watchdog = ParentDeathWatchdog.watching(lifeline, () -> true, POLL)) {
            watchdog.start();

            assertWithMessage("a live parent that simply has nothing to say is not death")
                    .that(watchdog.awaitDeath(Duration.ofMillis(200)))
                    .isFalse();

            writeEnd.close(); // the parent dies: its write end goes with it

            assertThat(watchdog.awaitDeath(GENEROUS)).isTrue();
            assertThat(watchdog.cause()).isEqualTo(ParentDeathWatchdog.Cause.LIFELINE_CLOSED);
        }
    }

    /**
     * The second signal, and the reason it exists: a wrapper process - a shell between the client and the
     * sidecar - inherits the write end and holds it open after the real parent is gone, so EOF never
     * arrives. The pid poll is what notices. This is why the client must launch the proxy directly.
     */
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void thePidPollCatchesDeathWhenAWrapperHoldsThePipeOpen() throws Exception {
        var writeEnd = new PipedOutputStream();
        var lifeline = new PipedInputStream(writeEnd); // deliberately never closed: the wrapper still holds it
        var parentAlive = new AtomicBoolean(true);

        try (var watchdog = ParentDeathWatchdog.watching(lifeline, parentAlive::get, POLL)) {
            watchdog.start();

            assertWithMessage("the pipe is open and the parent is alive - nothing has happened yet")
                    .that(watchdog.awaitDeath(Duration.ofMillis(200)))
                    .isFalse();

            parentAlive.set(false);

            assertThat(watchdog.awaitDeath(GENEROUS)).isTrue();
            assertThat(watchdog.cause()).isEqualTo(ParentDeathWatchdog.Cause.PARENT_PROCESS_GONE);
        }
    }

    /**
     * The negative control. A watchdog that reports death when neither signal fired would pass both tests
     * above and kill every healthy sidecar in production - so "does not fire" is asserted, not assumed.
     */
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void aLivingParentOnAnOpenPipeIsNeverDeath() throws Exception {
        var writeEnd = new PipedOutputStream();
        var lifeline = new PipedInputStream(writeEnd);

        try (var watchdog = ParentDeathWatchdog.watching(lifeline, () -> true, POLL)) {
            watchdog.start();

            // many poll intervals, so a spurious fire has every chance to happen
            assertThat(watchdog.awaitDeath(POLL.multipliedBy(25))).isFalse();
            assertThat(watchdog.cause()).isNull();
        } finally {
            writeEnd.close();
        }
    }

    /**
     * An IOException on the lifeline reads the same as EOF - the parent is gone either way, and a watchdog
     * that propagated it instead would leave the sidecar running with no supervisor.
     * <p>
     * <b>Not a {@link PipedInputStream}</b>, deliberately. Closing one from another thread does not wake a
     * reader already blocked in {@code read()}: that wait loop only re-checks whether the WRITE side died,
     * and {@code writeSide} is never set here because the parent never writes. The stub below can actually
     * raise the condition being asserted, where the pipe would only ever time out and look like a defect in
     * this class.
     */
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void aBrokenLifelineReadsAsDeathRatherThanAnError() throws Exception {
        var breakNow = new CountDownLatch(1);
        var lifeline = new InputStream() {
            @Override
            public int read() throws IOException {
                try {
                    breakNow.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException("interrupted", e);
                }
                throw new IOException("Broken pipe");
            }
        };

        try (var watchdog = ParentDeathWatchdog.watching(lifeline, () -> true, POLL)) {
            watchdog.start();

            assertWithMessage("the read is still blocked - nothing has gone wrong yet")
                    .that(watchdog.awaitDeath(Duration.ofMillis(200)))
                    .isFalse();

            breakNow.countDown();

            assertThat(watchdog.awaitDeath(GENEROUS)).isTrue();
            assertThat(watchdog.cause()).isEqualTo(ParentDeathWatchdog.Cause.LIFELINE_CLOSED);
        }
    }
}
