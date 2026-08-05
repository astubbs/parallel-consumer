package io.confluent.parallelconsumer.reactor;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;
import reactor.core.publisher.Mono;

import java.util.concurrent.ConcurrentLinkedQueue;

import static com.google.common.truth.Truth.assertWithMessage;
import static org.awaitility.Awaitility.await;

/**
 * The Reactor engine is a second thread boundary: {@code Mono.fromCallable(..).subscribeOn(scheduler)} means the user's
 * function runs on a Reactor scheduler thread, not on the PC worker thread that the core fix covers.
 *
 * @author Antony Stubbs
 * @see io.confluent.parallelconsumer.internal.MdcPropagation
 */
@Slf4j
class ReactorMdcPropagationTest extends ReactorUnitTestBase {

    private static final String CALLER_KEY = "trace_id";
    private static final String CALLER_VALUE = "caller-trace-abc";

    private final ConcurrentLinkedQueue<String> threadsUsed = new ConcurrentLinkedQueue<>();
    private final ConcurrentLinkedQueue<String> contextSeen = new ConcurrentLinkedQueue<>();

    @AfterEach
    void clearCallersContext() {
        MDC.clear();
    }

    @Test
    void callersContextReachesTheUserFunctionOnTheReactorScheduler() {
        primeFirstRecord();
        primeFirstRecord();
        primeFirstRecord();

        MDC.put(CALLER_KEY, CALLER_VALUE);

        reactorPC.react(rec -> {
            threadsUsed.add(Thread.currentThread().getName());
            contextSeen.add(String.valueOf(MDC.get(CALLER_KEY)));
            return Mono.just("done: " + rec.offset());
        });

        await().atMost(defaultTimeout).untilAsserted(() -> {
            // three primed above; ReactorUnitTestBase, unlike ReactorPCTest, primes none of its own
            assertWithMessage("records processed").that(contextSeen).hasSize(3);

            // if this ever stops being true, the test has stopped covering the boundary it exists to cover
            assertWithMessage("the user function must run on the Reactor scheduler, not the PC worker thread")
                    .that(threadsUsed.stream().allMatch(thread -> thread.startsWith("boundedElastic")))
                    .isTrue();

            assertWithMessage("the caller's diagnostic context must be visible on the scheduler thread")
                    .that(contextSeen.stream().allMatch(CALLER_VALUE::equals))
                    .isTrue();
        });
    }

}
