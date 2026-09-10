package bz.stub.parallelconsumer.fluent;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelStreamProcessor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.annotation.InterfaceStability;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A running definition: what {@link ParallelConsumerDefinition#start()} hands back.
 * <p>
 * It is {@link AutoCloseable} so a definition reads as a try-with-resources block, and its close <b>drains</b> the
 * work already in flight - which is where it differs from the classic API, whose plain close does not (R17).
 * <p>
 * <b>This is the first cut.</b> Close and await are here because a definition cannot be started without them; the
 * parked-set query, the stop path's three await exits, the outcome meters and the handle's consumer operations are
 * later units of the same milestone and grow this type in place.
 */
@Slf4j
@InterfaceStability.Unstable
public class ConsumerHandle implements AutoCloseable {

    private final ParallelStreamProcessor<byte[], byte[]> processor;

    private final CountDownLatch shutdown = new CountDownLatch(1);

    private final AtomicBoolean closing = new AtomicBoolean();

    ConsumerHandle(ParallelStreamProcessor<byte[], byte[]> processor) {
        this.processor = processor;
    }

    /**
     * Drains the work already in flight, bounded by the options' drain timeout, then closes. Idempotent: a
     * second call returns once the first has finished.
     */
    @Override
    public void close() {
        if (closing.compareAndSet(false, true)) {
            try {
                processor.closeDrainFirst();
            } finally {
                shutdown.countDown();
            }
        } else {
            awaitShutdown();
        }
    }

    /**
     * Blocks until this instance has shut down.
     */
    public void awaitShutdown() {
        try {
            shutdown.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.debug("Interrupted while awaiting shutdown", e);
        }
    }

    /**
     * @return true when the instance shut down within the bound
     */
    public boolean awaitShutdown(Duration timeout) {
        try {
            return shutdown.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    /**
     * The engine underneath, for the units that grow this handle. Not part of the fluent surface.
     */
    ParallelStreamProcessor<byte[], byte[]> processor() {
        return processor;
    }

}
