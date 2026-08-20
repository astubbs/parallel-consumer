package bz.stub.parallelconsumer.internal;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.ParallelEoSStreamProcessorTestBase;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.truth.Truth.assertWithMessage;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

/**
 * The metrics registry is the USER'S, so metrics teardown is third-party code running inside PC's
 * close - and it runs in {@code doClose}'s {@code finally}. This pins the rule that it cannot break
 * shutting down.
 * <p>
 * <b>Why a guard was needed at all, which is the part worth not re-deriving.</b> An exception thrown
 * from a {@code finally} block REPLACES the one already in flight. Unguarded, a registry that threw
 * would therefore:
 * <ol>
 *   <li>destroy the real shutdown failure, substituting a metrics error for the actual cause;</li>
 *   <li>skip the remaining teardown, so the meters it was cleaning up leak anyway;</li>
 *   <li>never reach {@code state = CLOSED}. That does NOT strand a caller polling
 *   {@code isClosedOrFailed()} - it also returns true once the control thread's future completes,
 *   which an escape from the {@code finally} does, exceptionally. The harm is quieter: callers get a
 *   premature true that means "the control thread finished, somehow", not "closed cleanly", and
 *   nothing distinguishes them. The chaos harness gates instance restarts on exactly that method, so
 *   it proceeds against a half-torn-down predecessor immediately rather than waiting.</li>
 * </ol>
 * Each is worse than the metrics problem that triggered it, and all three are silent.
 * <p>
 * A push-style registry that flushes on {@code close()} is the realistic trigger - a reporting
 * backend being unreachable at shutdown is ordinary, not exotic.
 */
@Slf4j
class MetricsTeardownCannotBreakCloseTest extends ParallelEoSStreamProcessorTestBase {

    /** Counts what the registry was asked to do, so a passing run cannot be one that never called it. */
    private static final AtomicInteger removeAttempts = new AtomicInteger();

    /**
     * Armed only for the close, because a registry that throws from the START kills the instance
     * before it can be closed - meter removal also runs on the REVOKE path, and an exception there
     * takes out the broker-poll thread. That is a real and separate exposure (third-party registry
     * code on a critical path, unguarded), recorded in the PR note; it is not what this test is
     * about, and leaving it armed throughout meant the close path was never reached.
     */
    private static final AtomicBoolean armed = new AtomicBoolean(false);

    /**
     * Throws from every teardown entry point PC uses: {@code remove} backs
     * {@code deregisterMeters()} and {@code PCMetrics.close()}'s meter cleanup.
     */
    static class ExplodingRegistry extends SimpleMeterRegistry {
        @Override
        public Meter remove(Meter.Id mappedId) {
            if (!armed.get()) {
                return super.remove(mappedId);
            }
            removeAttempts.incrementAndGet();
            throw new IllegalStateException("metrics backend unreachable at shutdown (simulated)");
        }
    }

    @Override
    protected ParallelConsumerOptions<Object, Object> getOptions() {
        return getDefaultOptions()
                .meterRegistry(new ExplodingRegistry())
                .build();
    }

    @Test
    @Timeout(60)
    void aThrowingMetricsRegistryCannotFailTheCloseOrStrandTheState() {
        parallelConsumer.poll(ignored -> {
            // no-op: the close path is the subject, not the processing
        });

        armed.set(true);
        assertDoesNotThrow(() -> parallelConsumer.closeDrainFirst(),
                "a failing metrics registry must not propagate out of close()");

        assertWithMessage("close must still reach a terminal state, or callers polling "
                + "isClosedOrFailed() wait forever")
                .that(parallelConsumer.isClosedOrFailed()).isTrue();

        // Non-vacuity: if the registry was never asked to remove anything, the guard was never
        // exercised and this test would pass against no guard at all.
        assertWithMessage("the exploding registry was never called, so nothing was guarded - "
                + "this test would pass without the fix")
                .that(removeAttempts.get()).isGreaterThan(0);
    }
}
