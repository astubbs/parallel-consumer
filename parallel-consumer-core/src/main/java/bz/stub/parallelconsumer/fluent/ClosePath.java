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
 * (KTD6). The same choice governs {@link ParallelConsumerInstance#close()}, so an instance has one answer to
 * "what happens to
 * the backlog", whoever asked it to stop.
 *
 * <h2>It maps one-to-one onto the engine's {@link DrainingMode}, and adds nothing</h2>
 * Two constants, two engine behaviours, and the mapping is data on the constant rather than a {@code switch} at the
 * point of use. So the honest question is why a facade type exists at all rather than the setter simply taking
 * {@code DrainingMode}, and the answer is a <b>package boundary, not a name</b>: {@code DrainingMode} is nested in
 * {@code bz.stub.parallelconsumer.internal.DrainingCloseable}, and the premise of this package
 * ({@code package-info}) is that a user of the facade never has to name an {@code internal} type. The two packages
 * also carry different stability promises - the fluent package is excluded from the API-compatibility gate and
 * {@code internal} is not - so taking the engine's enum on a public setter would publish a signature this package
 * cannot hold still. Nothing stops a user naming {@code DrainingMode}; this is about what the facade's own surface
 * obliges them to name.
 * <p>
 * The names are the second, smaller reason: {@code DRAIN_FIRST} and {@code DONT_DRAIN_FIRST} match the classic
 * API's own method names, {@code closeDrainFirst()} and {@code closeDontDrainFirst()}, which is what a reader
 * arriving from that API already has in their hands.
 *
 * @see ParallelConsumerDefinition#withClosePath(ClosePath)
 */
@InterfaceStability.Unstable
public enum ClosePath {

    /**
     * Process the records already fetched, then close - the default, and what makes the instance a graceful
     * shutdown
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

    /**
     * The engine's own spelling of this choice, bound to the constant rather than switched on at the point of use, so
     * the mapping between the two vocabularies lives in one place.
     */
    private final DrainingMode drainingMode;

    /**
     * Binds a path to the engine's draining mode. The two constants are the whole set, and a third is not what this
     * enum is for - it exists to keep an {@code internal} type off the facade's published surface, which the class
     * javadoc above states in full. Room for a third value is a latent benefit this deliberately does not claim.
     */
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
