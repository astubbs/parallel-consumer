package bz.stub.parallelconsumer.examples.support;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.examples.support.SimulatedService.SimulatedFailureException;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SimulatedServiceTest {

    private static final Duration NO_LATENCY = Duration.ZERO;

    @Test
    void failuresAreDeterministicNotRandom() {
        SimulatedService service = new SimulatedService("fraud scorer", NO_LATENCY, 0.25d);

        List<Integer> failedCalls = new ArrayList<>();
        for (int call = 1; call <= 8; call++) {
            try {
                service.call(() -> "scored");
            } catch (SimulatedFailureException e) {
                failedCalls.add(call);
            }
        }

        assertThat(failedCalls)
                .as("a quarter of calls fail, and always the same ones - a seeded random would still move "
                        + "under reordering")
                .containsExactly(4, 8);
        assertThat(service.getFailureCount()).hasValue(2);
        assertThat(service.getCallCount()).hasValue(8);
    }

    @Test
    void aServiceWithNoFailureFractionNeverFails() {
        SimulatedService service = new SimulatedService("pricing service", NO_LATENCY);

        for (int i = 0; i < 20; i++) {
            assertThat(service.call(() -> "priced")).isEqualTo("priced");
        }

        assertThat(service.getFailureCount()).hasValue(0);
    }

    @Test
    void theCallersWorkDoesNotRunWhenTheServiceFails() {
        SimulatedService service = new SimulatedService("carrier API", NO_LATENCY, 0.5d);
        AtomicBoolean workRan = new AtomicBoolean();

        service.run(() -> workRan.set(true)); // call 1 succeeds
        assertThat(workRan).isTrue();

        workRan.set(false);
        assertThatThrownBy(() -> service.run(() -> workRan.set(true))) // call 2 is the 1-in-2 failure
                .isInstanceOf(SimulatedFailureException.class)
                .hasMessageContaining("carrier API");
        assertThat(workRan)
                .as("a failed dependency call never reaches the work behind it")
                .isFalse();
    }

    @Test
    void theLatencyIsActuallyWaitedOut() {
        Duration latency = Duration.ofMillis(20);
        SimulatedService service = new SimulatedService("inventory lookup", latency);

        long before = System.nanoTime();
        service.call(() -> "looked up");
        long elapsedNanos = System.nanoTime() - before;

        // lower bound only - a sleep can overshoot arbitrarily under load, but never undershoot
        assertThat(elapsedNanos).isGreaterThanOrEqualTo(latency.toNanos());
    }

    @Test
    void nonsensicalConfigurationIsRejected() {
        assertThatThrownBy(() -> new SimulatedService(" ", NO_LATENCY))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("name");
        assertThatThrownBy(() -> new SimulatedService("x", Duration.ofMillis(-1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("latency");
        assertThatThrownBy(() -> new SimulatedService("x", NO_LATENCY, 1d))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("failureFraction");
        assertThatThrownBy(() -> new SimulatedService("x", NO_LATENCY, -0.1d))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("failureFraction");
    }
}
