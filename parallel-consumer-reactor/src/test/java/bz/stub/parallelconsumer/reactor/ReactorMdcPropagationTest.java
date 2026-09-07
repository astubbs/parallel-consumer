package bz.stub.parallelconsumer.reactor;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.MdcBoundaryProbe;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

import static com.google.common.truth.Truth.assertWithMessage;
import static org.awaitility.Awaitility.await;

/**
 * The Reactor engine is a second thread boundary: {@code Mono.fromCallable(..).subscribeOn(scheduler)} means the user's
 * function runs on a Reactor scheduler thread, not on the PC worker thread that the core fix covers.
 *
 * @author Antony Stubbs
 * @see bz.stub.parallelconsumer.internal.MdcPropagation
 */
@Slf4j
class ReactorMdcPropagationTest extends ReactorUnitTestBase {

    private final MdcBoundaryProbe probe = new MdcBoundaryProbe();

    @AfterEach
    void clearCallersContext() {
        probe.clearCallersContext();
    }

    @Test
    void callersContextReachesTheUserFunctionOnTheReactorScheduler() {
        primeFirstRecord();
        primeFirstRecord();
        primeFirstRecord();

        probe.establishCallersContext();

        reactorPC.react(rec -> {
            probe.observeCurrentThread();
            return Mono.just("done: " + rec.offset());
        });

        await().atMost(defaultTimeout).untilAsserted(() -> {
            // three primed above; ReactorUnitTestBase, unlike ReactorPCTest, primes none of its own
            assertWithMessage("records processed").that(probe.observations()).hasSize(3);

            // asserted positively (the scheduler PC configures by default), which is stronger than merely "not a PC
            // worker thread" - if this ever stops being true, the test has stopped covering the boundary it exists for
            probe.assertObservedOnlyOn("Reactor scheduler", thread -> thread.startsWith("boundedElastic"));

            probe.assertCallersContextWasVisible("Reactor scheduler thread");
        });
    }

}
