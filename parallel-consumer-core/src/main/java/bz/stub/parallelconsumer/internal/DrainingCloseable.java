package bz.stub.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2023 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import java.io.Closeable;
import java.time.Duration;

import bz.stub.parallelconsumer.ParallelConsumerOptions;

public interface DrainingCloseable extends Closeable {

    enum DrainingMode {
        /**
         * Stop downloading more messages from the Broker, but finish processing what has already been queued.
         */
        DRAIN,
        /**
         * Stop downloading more messages, and stop processing more messages in the queue, but finish processing
         * messages already being processed locally.
         */
        DONT_DRAIN
    }

    /**
     * Close the consumer <b>WITHOUT draining</b> - this is {@link #closeDontDrainFirst()}, not
     * {@link #closeDrainFirst()}.
     * <p>
     * Note that this is also what try-with-resources and any other {@link Closeable} handling will call. Records that
     * have been downloaded but not yet started are therefore dropped (never committed, so redelivered after the
     * rebalance), and only the records already in flight are waited for. If you want the queued backlog processed
     * before closing, call {@link #closeDrainFirst()} explicitly.
     * <p>
     * Uses the timeout specified through {@link ParallelConsumerOptions#shutdownTimeout}.
     *
     * @see DrainingMode#DONT_DRAIN
     * @see #closeDrainFirst()
     * @see ParallelConsumerOptions#shutdownTimeout
     * @see #close(Duration, DrainingMode)
     */
    default void close() {
        closeDontDrainFirst();
    }

    /**
     * @see DrainingMode#DRAIN
     */
    default void closeDrainFirst() {
        close(DrainingMode.DRAIN);
    }

    /**
     * @see DrainingMode#DONT_DRAIN
     */
    default void closeDontDrainFirst() {
        close(DrainingMode.DONT_DRAIN);
    }

    /**
     * @see DrainingMode#DRAIN
     */
    default void closeDrainFirst(Duration timeout) {
        close(timeout, DrainingMode.DRAIN);
    }

    /**
     * @see DrainingMode#DONT_DRAIN
     */
    default void closeDontDrainFirst(Duration timeout) {
        close(timeout, DrainingMode.DONT_DRAIN);
    }

    /**
     * Close the consumer.
     *
     * @param timeout      how long to wait for the records already in flight to finish - overrides
     *                     {@link ParallelConsumerOptions#shutdownTimeout} only. {@link
     *                     ParallelConsumerOptions#drainTimeout} is NOT overridden, and still contributes its share of
     *                     the overall budget for a {@link DrainingMode#DRAIN} close. Note it is a term in that budget
     *                     rather than a deadline on draining itself - the drain loop runs until the backlog is empty.
     * @param drainingMode specify if PC should wait for messages already consumed from the broker to be processed before closing
     */
    void close(Duration timeout, DrainingMode drainingMode);

    /**
     * Close the consumer using timeout specified in ParallelConsumerOptions
     *
     * @param drainingMode wait for messages already consumed from the broker to be processed before closing
     */
    void close(DrainingMode drainingMode);

    /**
     * Of the records consumed from the broker, how many do we have remaining in our local queues
     *
     * @return the number of consumed but outstanding records to process
     */
    long workRemaining();

}
