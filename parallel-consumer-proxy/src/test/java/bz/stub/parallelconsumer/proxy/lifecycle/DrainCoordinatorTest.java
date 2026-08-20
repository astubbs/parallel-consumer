package bz.stub.parallelconsumer.proxy.lifecycle;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

/**
 * KTD17: core's drain does not cover foreign in-flight work, so the proxy supplies its own wait.
 * <p>
 * The load-bearing test here is {@link #withoutTheProxysOwnWaitTheEngineClosesOnATruncatedRun}, the negative
 * control - it removes only the wait, holds everything else identical, and shows the close happens with a
 * record still out. Without it, the happy-path test would pass just as well against a coordinator that did
 * nothing at all and got lucky on timing.
 */
class DrainCoordinatorTest {

    private static final Duration POLL = Duration.ofMillis(10);

    private static final Duration GENEROUS = Duration.ofSeconds(10);

    /** Records what the drain did to the world, and in what order - the ordering is a requirement, not a detail. */
    private static final class FakeTarget implements DrainCoordinator.DrainTarget {

        final List<String> calls = new ArrayList<>();

        final AtomicInteger inFlight = new AtomicInteger();

        /** What the registry read at the moment the engine was closed - the drain's whole point. */
        volatile int inFlightWhenClosed = -1;

        @Override
        public void stopAcceptingNewWork() {
            calls.add("stopAcceptingNewWork");
        }

        @Override
        public void tellClientToShutDown() {
            calls.add("tellClientToShutDown");
        }

        @Override
        public int foreignRecordsInFlight() {
            return inFlight.get();
        }

        @Override
        public void closeEngineDrainingFirst() {
            inFlightWhenClosed = inFlight.get();
            calls.add("closeEngineDrainingFirst");
        }
    }

    /**
     * The client is slow: one record stays out, then resolves. The drain must still be waiting when it
     * resolves, and must close only afterwards.
     */
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void drainWaitsForASlowClientAndDoesNotCloseEarly() throws Exception {
        var target = new FakeTarget();
        target.inFlight.set(1);

        var coordinator = DrainCoordinator.of(target, GENEROUS, POLL);
        var drain = new Thread(coordinator::drain, "drain-under-test");
        drain.start();

        // The record is still out. Give the drain many poll intervals to get it wrong.
        Thread.sleep(POLL.toMillis() * 10);
        assertWithMessage("closed while a record was still in a foreign process")
                .that(target.calls).doesNotContain("closeEngineDrainingFirst");

        target.inFlight.set(0); // the slow client finally reports

        drain.join(GENEROUS.toMillis());
        assertThat(drain.isAlive()).isFalse();
        assertThat(target.calls).contains("closeEngineDrainingFirst");
        assertWithMessage("the engine was closed with foreign work still outstanding")
                .that(target.inFlightWhenClosed).isEqualTo(0);
    }

    /**
     * THE NEGATIVE CONTROL. Same target, same record still out - only the proxy's own wait is removed, by
     * giving the drain no time to perform it. The engine is then closed with the record still in a foreign
     * process, which is precisely the state KTD17 says core's own drain leaves behind.
     * <p>
     * Same magnitude, different position: if this passed AND the test above passed, the wait would be proven
     * to be the thing making the difference. If this one closed at zero in flight too, the wait would be
     * decoration.
     */
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void withoutTheProxysOwnWaitTheEngineClosesOnATruncatedRun() {
        var target = new FakeTarget();
        target.inFlight.set(1);

        var outcome = DrainCoordinator.of(target, Duration.ZERO, POLL).drain();

        assertThat(outcome).isEqualTo(DrainCoordinator.Outcome.TIMED_OUT);
        assertWithMessage("with no wait, the close must be the one that sees outstanding foreign work")
                .that(target.inFlightWhenClosed).isEqualTo(1);
    }

    /**
     * Covers the shutdown half of AE14. A record that never comes back must not hold the sidecar open forever,
     * and must not have an outcome invented for it: the drain commits what resolved and leaves the rest to
     * redelivery, which means it reports a timeout and closes - nothing more.
     */
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void aRecordThatNeverComesBackTimesOutAndIsLeftForRedelivery() {
        var target = new FakeTarget();
        target.inFlight.set(2);

        var outcome = DrainCoordinator.of(target, Duration.ofMillis(150), POLL).drain();

        assertThat(outcome).isEqualTo(DrainCoordinator.Outcome.TIMED_OUT);
        assertThat(target.calls).contains("closeEngineDrainingFirst");
        assertWithMessage("the drain resolved a record it never heard about - an invented outcome")
                .that(target.inFlight.get()).isEqualTo(2);
    }

    /**
     * Order is a requirement: new work stops first, then the client is told to shut down so it can stop
     * handing records to workers and report what it already holds, and only then does the wait begin. Telling
     * the client after the wait would mean waiting for reports it had no reason to send yet.
     */
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void newWorkStopsAndTheClientIsToldBeforeTheWaitBegins() {
        var target = new FakeTarget();
        target.inFlight.set(0);

        var outcome = DrainCoordinator.of(target, GENEROUS, POLL).drain();

        assertThat(outcome).isEqualTo(DrainCoordinator.Outcome.DRAINED);
        assertThat(target.calls)
                .containsExactly("stopAcceptingNewWork", "tellClientToShutDown", "closeEngineDrainingFirst")
                .inOrder();
    }

    /** A drain that is asked to run twice must not close the engine twice. */
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void drainingTwiceClosesOnce() {
        var target = new FakeTarget();
        var coordinator = DrainCoordinator.of(target, GENEROUS, POLL);

        assertThat(coordinator.drain()).isEqualTo(DrainCoordinator.Outcome.DRAINED);
        assertThat(coordinator.drain()).isEqualTo(DrainCoordinator.Outcome.DRAINED);

        assertThat(target.calls.stream().filter("closeEngineDrainingFirst"::equals).count()).isEqualTo(1);
    }
}
