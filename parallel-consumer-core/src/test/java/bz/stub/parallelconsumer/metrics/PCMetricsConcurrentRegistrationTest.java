package bz.stub.parallelconsumer.metrics;

/*-
 * Copyright (C) 2020-2026 Antony Stubbs and contributors
 */

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import lombok.SneakyThrows;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import pl.tlinkowski.unij.api.UniLists;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Closing the metrics subsystem while another thread is still registering meters must neither throw nor leak.
 * <p>
 * The two ends of {@code registeredMeters} run on different threads and always have. {@link PCMetrics#close()} walks
 * it to unregister each meter, reached on the control thread via {@code AbstractParallelEoSStreamProcessor.doClose}.
 * The registration methods append from the broker-poll thread on every partition assignment. {@code close()} is
 * {@code synchronized} and none of the adders is, so the monitor never excluded the writes that matter.
 * <p>
 * <b>Why a crash here is not a metrics problem.</b> A {@link java.util.ConcurrentModificationException} out of
 * {@code close()} propagates into {@code doClose}'s {@code finally} and skips the {@code state = CLOSED} transition on
 * the next line - the transition that block exists to guarantee. The consumer is then stuck short of CLOSED and the
 * group waits out its session timeout instead of departing promptly. A quiet cleanup failure surfaces as a stall in
 * an unrelated subsystem.
 * <p>
 * The two tests here cover the two halves, and the second is the one worth reading: not throwing is not the same as
 * not losing anything.
 *
 * @author Antony Stubbs
 */
class PCMetricsConcurrentRegistrationTest {

    /**
     * Enough pre-registered meters that {@code close()} spends a meaningful window inside its walk, so a registration
     * lands during it rather than needing luck.
     */
    private static final int PRE_REGISTERED = 2_000;

    /**
     * Registration racing {@code close()} must not throw.
     * <p>
     * Repeated because this is a race: one attempt reproduces today, but that is the environment's choice.
     */
    @RepeatedTest(20)
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    @SneakyThrows
    void closingWhileMetersAreStillBeingRegisteredDoesNotThrow() {
        var registry = new SimpleMeterRegistry();
        var metrics = new PCMetrics(registry, UniLists.of(Tag.of("tag1", "pc1")), null);

        for (int i = 0; i < PRE_REGISTERED; i++) {
            registerCounter(metrics, i);
        }

        var keepRegistering = new AtomicBoolean(true);
        var registrar = new Thread(() -> {
            for (int i = PRE_REGISTERED; keepRegistering.get(); i++) {
                registerCounter(metrics, i);
            }
        }, "meter-registrar");
        registrar.setDaemon(true);
        registrar.start();

        try {
            assertThatCode(metrics::close)
                    .as("closing while another thread registers")
                    .doesNotThrowAnyException();
        } finally {
            keepRegistering.set(false);
            registrar.join(TimeUnit.SECONDS.toMillis(10));
            registry.close();
        }
    }

    /**
     * A meter that arrives <em>while {@code close()} is walking</em> must still be unregistered.
     * <p>
     * This is the half that making the collection concurrent does not buy. Any walk sees the collection as it was
     * when the walk began, so a {@code clear()} afterwards discards whatever arrived during it - without ever
     * unregistering those meters. They stay in the caller's registry, tagged to a dead consumer, and nothing reports
     * it. That is strictly worse than the exception it replaced, because the exception at least fired.
     * <p>
     * Driven deterministically rather than by threads and hope: the registry registers one extra meter the first time
     * it is asked to remove one, which is precisely a rebalance landing mid-walk, and happens on exactly the edge
     * that matters. The {@code registeredDuringClose} assertion exists so the test cannot quietly stop exercising the
     * scenario and keep passing.
     */
    @Test
    void aMeterArrivingWhileCloseIsWalkingIsStillUnregistered() {
        var self = new AtomicReference<PCMetrics>();
        var registeredDuringClose = new AtomicBoolean(false);

        var registry = new SimpleMeterRegistry() {
            @Override
            public Meter remove(Meter.Id id) {
                if (registeredDuringClose.compareAndSet(false, true)) {
                    registerCounter(self.get(), Integer.MAX_VALUE);
                }
                return super.remove(id);
            }
        };

        var metrics = new PCMetrics(registry, UniLists.of(Tag.of("tag1", "pc1")), null);
        self.set(metrics);
        for (int i = 0; i < 5; i++) {
            registerCounter(metrics, i);
        }

        metrics.close();

        assertThat(registeredDuringClose)
                .as("the scenario under test actually happened - a meter was registered during the walk")
                .isTrue();
        assertThat(registry.getMeters())
                .as("meters left behind in the caller's registry after close")
                .isEmpty();

        registry.close();
    }

    private static void registerCounter(PCMetrics metrics, int i) {
        metrics.getCounterFromMetricDef(PCMetricsDef.PROCESSED_RECORDS,
                Tag.of("topic", "input"),
                Tag.of("partition", String.valueOf(i)));
    }
}
