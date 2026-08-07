package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020-2023 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.Getter;

/**
 * The run state of the controller.
 * <p>
 * Exposed publicly through {@link PCHealth#getControllerState()}, so that a health check can distinguish a running
 * instance from one that is shutting down without casting to a concrete implementation.
 *
 * @see PCHealth
 * @see ParallelConsumer#getHealth()
 */
public enum State {
    UNUSED(0),
    RUNNING(1),
    /**
     * When paused, the system will stop submitting work to the processing pool. Polling for new work however may
     * continue until internal buffers have been filled sufficiently and the auto-throttling takes effect. In flight
     * work will not be affected by transitioning to this state (i.e. processing will finish without any interrupts
     * being sent).
     */
    PAUSED(2),
    /**
     * When draining, the system will stop polling for more records, but will attempt to process all already downloaded
     * records. Note that if you choose to close without draining, records already processed will still be committed
     * first before closing.
     */
    DRAINING(3),
    CLOSING(4),
    CLOSED(5);

    // Enum value used for metrics - deterministic as opposed to ordinal to prevent change on adding / removing enum constants
    @Getter
    private int value;

    State(int value) {
        this.value = value;
    }

    /**
     * Whether this state means the controller is up - that is, not shutting down.
     * <p>
     * True for {@link #UNUSED}, {@link #RUNNING} and {@link #PAUSED}; false for {@link #DRAINING}, {@link #CLOSING} and
     * {@link #CLOSED}. {@link #PAUSED} counts as up because pausing is a deliberate user action, not a fault.
     * <p>
     * <strong>This says nothing about whether work is progressing.</strong> An instance that is making no progress at
     * all still reports {@link #RUNNING}. Deliberately named after what it tests rather than "healthy", so that the
     * claim is not made in call sites and log lines where this documentation does not travel. Observe progress through
     * the {@code pc.*} Micrometer meters instead.
     *
     * @return true if the controller is not shutting down
     */
    public boolean isRunningOrPaused() {
        switch (this) {
            case UNUSED:
            case RUNNING:
            case PAUSED:
                return true;
            case DRAINING:
            case CLOSING:
            case CLOSED:
                return false;
            default:
                throw new IllegalStateException("Unmapped state: " + this);
        }
    }
}
