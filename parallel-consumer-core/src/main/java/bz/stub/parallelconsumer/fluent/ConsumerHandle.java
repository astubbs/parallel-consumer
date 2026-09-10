package bz.stub.parallelconsumer.fluent;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelStreamProcessor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.annotation.InterfaceStability;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

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
public class ConsumerHandle implements AutoCloseable, InstanceControl {

    private final ParallelStreamProcessor<byte[], byte[]> processor;

    private final CountDownLatch shutdown = new CountDownLatch(1);

    private final AtomicBoolean closing = new AtomicBoolean();

    /**
     * The definition fault that ended this instance, if one did. First writer wins: the fault is what stopped the
     * instance, and every record after it would report the same thing.
     */
    private final AtomicReference<Throwable> fault = new AtomicReference<>();

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
     * Blocks until this instance has shut down, and <b>rethrows the definition fault that stopped it</b> if one
     * did. A definition fault is not something a caller can be left to discover from silence: the instance is gone
     * and no record was processed (KTD3, R24).
     */
    public void awaitShutdown() {
        try {
            shutdown.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.debug("Interrupted while awaiting shutdown", e);
        }
        surfaceAnyFault();
    }

    /**
     * @return true when the instance shut down within the bound
     * @throws RuntimeException the definition fault that stopped the instance, if one did and it shut down within
     *                          the bound
     */
    public boolean awaitShutdown(Duration timeout) {
        boolean shutDown;
        try {
            shutDown = shutdown.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
        if (shutDown) {
            surfaceAnyFault();
        }
        return shutDown;
    }

    /**
     * @return the definition fault that stopped this instance, if one did
     */
    public Optional<Throwable> failureCause() {
        return Optional.ofNullable(fault.get());
    }

    // ---------------------------------------------------------------- InstanceControl

    /**
     * <b>Closes from a thread of its own, and that is the whole point.</b> This is called from a worker thread,
     * inside the user function's failure path, and the engine's close awaits the worker pool - so a worker that
     * closed inline would be waiting for itself until the shutdown timeout expired (KTD6).
     * <p>
     * <b>Seam for the lifecycle unit</b>, which routes a fault through the same stop path the stop outcome uses:
     * mark, pause, fence, throw, close. What is here is the part a definition fault cannot do without - the
     * instance ends rather than retrying a record that can only fail the same way, and whoever is awaiting
     * shutdown is told why.
     */
    @Override
    public void fatal(Throwable definitionFault) {
        if (!fault.compareAndSet(null, definitionFault)) {
            return;
        }
        log.error("Stopping this instance: a definition fault reached a record, and every record would meet it",
                definitionFault);
        Thread closer = new Thread(this::closeAfterFault, "pc-fluent-fault-closer");
        closer.setDaemon(true);
        closer.start();
    }

    /**
     * <b>Seam for the lifecycle unit</b> (R24, KTD6). The wrapper has already handed the record back incomplete, so
     * a restart delivers it again; what is missing is the instance actually stopping - marking, pausing so no
     * further record is dispatched, and closing on the declared close path.
     */
    @Override
    public void stopRequested(ConsumerRecord<byte[], byte[]> record, String reason) {
        log.warn("A route asked the instance to stop at {}-{}@{}: {}. The stop outcome does not stop the instance "
                        + "in this release - the record is handed back incomplete and the instance keeps running.",
                record.topic(), record.partition(), record.offset(), reason);
    }

    private void closeAfterFault() {
        if (closing.compareAndSet(false, true)) {
            try {
                // Not a drain: there is nothing worth draining, and every record in flight meets the same fault.
                processor.closeDontDrainFirst();
            } catch (RuntimeException closeFailed) {
                log.error("Closing after a definition fault failed", closeFailed);
            } finally {
                shutdown.countDown();
            }
        }
    }

    private void surfaceAnyFault() {
        Throwable cause = fault.get();
        if (cause instanceof RuntimeException) {
            throw (RuntimeException) cause;
        }
        if (cause instanceof Error) {
            throw (Error) cause;
        }
        if (cause != null) {
            throw new IllegalStateException("This instance stopped because of a definition fault", cause);
        }
    }

    /**
     * The engine underneath, for the units that grow this handle. Not part of the fluent surface.
     */
    ParallelStreamProcessor<byte[], byte[]> processor() {
        return processor;
    }

}
