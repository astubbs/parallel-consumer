package bz.stub.parallelconsumer.mutiny;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.MdcBoundaryProbe;
import io.smallrye.mutiny.Uni;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static com.google.common.truth.Truth.assertWithMessage;
import static org.awaitility.Awaitility.await;

/**
 * The Mutiny engine is a thread boundary of the same shape as Reactor's: {@code runSubscriptionOn(executor)} means the
 * user's function runs on a Mutiny worker, not on the PC worker thread that the core fix covers.
 *
 * @author Antony Stubbs
 * @see bz.stub.parallelconsumer.internal.MdcPropagation
 */
@Slf4j
class MutinyMdcPropagationTest extends MutinyUnitTestBase {

    private final MdcBoundaryProbe probe = new MdcBoundaryProbe();

    @AfterEach
    void clearCallersContext() {
        probe.clearCallersContext();
    }

    @Test
    void callersContextReachesTheUserFunctionOnTheMutinyExecutor() {
        primeFirstRecord();
        primeFirstRecord();
        primeFirstRecord();

        probe.establishCallersContext();

        mutinyPC.onRecord(ctx -> {
            probe.observeCurrentThread();
            return Uni.createFrom().item("done: " + ctx.getSingleConsumerRecord().offset());
        });

        await().atMost(defaultTimeout).untilAsserted(() -> {
            assertWithMessage("records processed").that(probe.observations()).hasSize(3);

            // the executor is supplied by the caller, so there is no fixed name to assert positively - what must hold
            // is that it is not a PC worker thread, or the test has stopped covering the boundary it exists for
            probe.assertObservedOnlyOn("Mutiny executor", thread -> !thread.startsWith("pc-"));

            probe.assertCallersContextWasVisible("Mutiny executor thread");
        });
    }

}
