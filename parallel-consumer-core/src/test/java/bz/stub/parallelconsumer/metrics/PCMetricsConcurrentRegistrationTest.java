package bz.stub.parallelconsumer.metrics;

/*-
 * Copyright (C) 2020-2026 Antony Stubbs and contributors
 */

import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import pl.tlinkowski.unij.api.UniLists;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Closing the metrics subsystem while another thread is still registering meters must not throw.
 * <p>
 * The two ends of {@code registeredMeters} run on different threads and always have.
 * {@link PCMetrics#close()} walks the list to unregister each meter, and reaches it on the control thread via
 * {@code AbstractParallelEoSStreamProcessor.doClose}. The four registration methods append to that same list from the
 * broker-poll thread, on every partition assignment. {@code close()} is {@code synchronized} and none of the four
 * adders is, so the monitor never excluded the writes that matter - it only made the class read as though it handled
 * concurrency.
 * <p>
 * <b>Why this is worth a test rather than a one-line type change.</b> A {@link java.util.ConcurrentModificationException}
 * thrown out of {@code close()} does not merely lose some metrics: it propagates into {@code doClose}'s {@code finally}
 * block and skips the {@code state = CLOSED} transition on the next line - the very transition that block exists to
 * guarantee. The consumer is then stuck short of CLOSED, and the Kafka group waits out its session timeout instead of
 * getting a prompt departure. A quiet cleanup failure becomes a visible stall in an unrelated subsystem.
 *
 * @author Antony Stubbs
 */
@Slf4j
class PCMetricsConcurrentRegistrationTest {

    /**
     * Enough pre-registered meters that {@code close()} spends a meaningful window inside its walk, so a registration
     * lands during it rather than needing luck.
     */
    private static final int PRE_REGISTERED = 2_000;

    /**
     * Repeated because this is a race, not a deterministic sequence: the interleaving is the environment's to choose.
     * Measured against the plain {@code ArrayList} this replaced, a single attempt is already enough - but a race that
     * reproduces on the first attempt today can become one that needs twenty after an unrelated timing change, and a
     * regression test that quietly stops reproducing is worse than none.
     */
    private static final int ATTEMPTS = 20;

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    @SneakyThrows
    void closingWhileMetersAreStillBeingRegisteredDoesNotThrow() {
        for (int attempt = 0; attempt < ATTEMPTS; attempt++) {
            var registry = new SimpleMeterRegistry();
            var metrics = new PCMetrics(registry, UniLists.of(Tag.of("tag1", "pc1")), "instance-" + attempt);

            for (int i = 0; i < PRE_REGISTERED; i++) {
                registerCounter(metrics, i);
            }

            var keepRegistering = new AtomicBoolean(true);
            var registrarFailure = new AtomicReference<Throwable>();
            var registrarStarted = new CountDownLatch(1);

            var registrar = new Thread(() -> {
                registrarStarted.countDown();
                try {
                    for (int i = PRE_REGISTERED; keepRegistering.get(); i++) {
                        registerCounter(metrics, i);
                    }
                } catch (Throwable t) {
                    // the writer's own view of the same race - a plain list can fail here too, on a concurrent resize
                    registrarFailure.set(t);
                }
            }, "meter-registrar-" + attempt);
            registrar.setDaemon(true);
            registrar.start();
            registrarStarted.await(10, TimeUnit.SECONDS);

            try {
                metrics.close();
            } finally {
                keepRegistering.set(false);
                registrar.join(TimeUnit.SECONDS.toMillis(10));
                registry.close();
            }

            assertThat(registrarFailure.get())
                    .as("registering a meter on attempt %s while close() walked the list", attempt)
                    .isNull();
        }
    }

    private static void registerCounter(PCMetrics metrics, int i) {
        metrics.getCounterFromMetricDef(PCMetricsDef.PROCESSED_RECORDS,
                Tag.of("topic", "input"),
                Tag.of("partition", String.valueOf(i)));
    }
}
