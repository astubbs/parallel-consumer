package io.confluent.parallelconsumer.mutiny;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import io.smallrye.mutiny.Uni;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;

import java.util.concurrent.ConcurrentLinkedQueue;

import static com.google.common.truth.Truth.assertWithMessage;
import static org.awaitility.Awaitility.await;

/**
 * The Mutiny engine is a thread boundary of the same shape as Reactor's: {@code runSubscriptionOn(executor)} means the
 * user's function runs on a Mutiny worker, not on the PC worker thread that the core fix covers.
 *
 * @author Antony Stubbs
 * @see io.confluent.parallelconsumer.internal.MdcPropagation
 */
@Slf4j
class MutinyMdcPropagationTest extends MutinyUnitTestBase {

    private static final String CALLER_KEY = "trace_id";
    private static final String CALLER_VALUE = "caller-trace-abc";

    private final ConcurrentLinkedQueue<String> threadsUsed = new ConcurrentLinkedQueue<>();
    private final ConcurrentLinkedQueue<String> contextSeen = new ConcurrentLinkedQueue<>();

    @AfterEach
    void clearCallersContext() {
        MDC.clear();
    }

    @Test
    void callersContextReachesTheUserFunctionOnTheMutinyExecutor() {
        primeFirstRecord();
        primeFirstRecord();
        primeFirstRecord();

        MDC.put(CALLER_KEY, CALLER_VALUE);

        mutinyPC.onRecord(ctx -> {
            threadsUsed.add(Thread.currentThread().getName());
            contextSeen.add(String.valueOf(MDC.get(CALLER_KEY)));
            return Uni.createFrom().item("done: " + ctx.getSingleConsumerRecord().offset());
        });

        await().atMost(defaultTimeout).untilAsserted(() -> {
            assertWithMessage("records processed").that(contextSeen).hasSize(3);

            // if this ever stops being true, the test has stopped covering the boundary it exists to cover
            assertWithMessage("the user function must run on the Mutiny executor, not the PC worker thread")
                    .that(threadsUsed.stream().noneMatch(thread -> thread.startsWith("pc-")))
                    .isTrue();

            assertWithMessage("the caller's diagnostic context must be visible on the Mutiny executor thread")
                    .that(contextSeen.stream().allMatch(CALLER_VALUE::equals))
                    .isTrue();
        });
    }

}
