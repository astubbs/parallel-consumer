package bz.stub.parallelconsumer.fluent;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.internal.DrainingCloseable.DrainingMode;
import org.apache.kafka.common.annotation.InterfaceStability;

/**
 * How this instance shuts down: whether the records already fetched are processed first (R17, R24).
 * <p>
 * It is declared once per instance, as data, because it is a property of the deployment rather than of a record -
 * and because the stop outcome closes the instance from a thread of its own, with nobody there to pass an argument
 * (KTD6). The same choice governs {@link ConsumerHandle#close()}, so an instance has one answer to "what happens to
 * the backlog", whoever asked it to stop.
 *
 * @see ParallelConsumerDefinition#closePath(ClosePath)
 */
@InterfaceStability.Unstable
public enum ClosePath {

    /**
     * Process the records already fetched, then close - the default, and what makes the handle a graceful shutdown
     * (R17). Bounded by the options' drain timeout, with the shutdown timeout bounding the close that follows.
     * <p>
     * <b>A drain still dispatches the buffered backlog</b>, so "nothing new starts" is not what this gives you; that
     * is {@link #DONT_DRAIN_FIRST}. What a drain does guarantee is that a record already fetched is either processed
     * or left incomplete, never dropped silently mid-flight.
     */
    DRAIN_FIRST(DrainingMode.DRAIN),

    /**
     * Let the records already inside the processing function finish, and start nothing further - the fetched
     * backlog is left uncommitted and delivered again after a restart (R24, AE14).
     */
    DONT_DRAIN_FIRST(DrainingMode.DONT_DRAIN);

    private final DrainingMode drainingMode;

    ClosePath(DrainingMode drainingMode) {
        this.drainingMode = drainingMode;
    }

    /**
     * The engine's own spelling of this choice.
     */
    DrainingMode drainingMode() {
        return drainingMode;
    }
}
