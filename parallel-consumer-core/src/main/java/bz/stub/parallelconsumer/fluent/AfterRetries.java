package bz.stub.parallelconsumer.fluent;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.state.PartitionStateManager;
import org.apache.kafka.common.annotation.InterfaceStability;

import java.time.Duration;
import java.util.Objects;
import java.util.OptionalInt;

import static bz.stub.parallelconsumer.internal.utils.StringUtils.msg;

/**
 * What happens to a record once it has exhausted its retries (R27): it {@link #park()}s in place, or it
 * {@link #stop()}s the instance.
 *
 * <h2>Park is the dead-letter of first resort</h2>
 * The offset map already commits past an incomplete record, so an exhausted record can stay where it is, holding no
 * worker, with the source topic as its store and the map as its index. Copying it to a topic - which this API spells
 * <em>dlq</em>, as a verb - is only needed when the map's capacity or the topic's retention forces it, so every
 * {@code dlq} call below is optional and a definition that makes none of them is complete.
 *
 * <h2>Park is also where scheduled retry lives</h2>
 * A policy may grant an exhausted record more attempts, spaced out: {@link #thenRetryAfter(Duration)} says how long
 * to wait and {@link #forCycles(int)} says how many such waits there are, after which the record parks with no
 * delay at all. That is scheduled retry (astubbs#234) expressed as park rather than as a second mechanism - the
 * record holds no worker while it waits either way.
 * <p>
 * The policy is data: no callbacks, nothing a wire contract could not carry (R18). It is a per-route setting with an
 * instance default, so each route takes a {@link #copy()} of the default unless it declares its own (R6).
 */
@InterfaceStability.Unstable
public final class AfterRetries {

    /**
     * The highest export percentage a definition may declare, and the default.
     * <p>
     * The engine stops a partition taking work at
     * {@link PartitionStateManager#USED_PAYLOAD_THRESHOLD_MULTIPLIER_DEFAULT} of the commit-metadata cap, so a
     * percentage at or above that is never reached and would read as a setting that silently does nothing. Five
     * points below it is the margin the owner chose, which lands on seventy today. Both numbers are provisional on
     * the current encoding: exact continuous offset encoding (astubbs#237,
     * confluentinc#53) makes the payload size precise and they are revisited when it lands.
     */
    public static final int MAX_PAYLOAD_PERCENTAGE =
            (int) (PartitionStateManager.USED_PAYLOAD_THRESHOLD_MULTIPLIER_DEFAULT * 100) - 5;

    /**
     * Which of the two reactions this policy asks for.
     */
    @InterfaceStability.Unstable
    public enum Reaction {
        /**
         * The record stays incomplete in the offset map, holds no worker, and waits for a trigger, an operator, or a
         * restart.
         */
        PARK,
        /**
         * The instance stops. A record that has run out of attempts on this route is treated as the definition's
         * author saying the instance should not carry on without someone looking (R24, R27).
         */
        STOP
    }

    private final Reaction reaction;

    private String destination;

    private boolean immediately;

    private Duration olderThan;

    private Integer payloadPercentage;

    private Duration parkDelay;

    private Integer parkCycles;

    private AfterRetries(Reaction reaction) {
        this.reaction = reaction;
    }

    /**
     * Park in place, with no export trigger. Add one of the {@code dlq} calls below when the offset map's capacity or
     * the topic's retention forces a copy out.
     */
    public static AfterRetries park() {
        return new AfterRetries(Reaction.PARK);
    }

    /**
     * Stop the instance instead of parking, for a route whose exhausted record means something is wrong with the
     * deployment rather than with the record. The stopping record is left incomplete, so a restart delivers it
     * again and the instance will stop again - the definition's author owns breaking that loop (R24).
     */
    public static AfterRetries stop() {
        return new AfterRetries(Reaction.STOP);
    }

    /**
     * The classic dead-letter queue: export on the dispatch after exhaustion, one retry delay later (R27, AE19).
     */
    public static AfterRetries dlqImmediately(String destination) {
        return park().dlqTo(destination).dlqImmediately();
    }

    /**
     * Wait this long, then attempt the record once more - which is what scheduled retry is (astubbs#234). Declared
     * together with {@link #forCycles(int)}: the delay says how long each cycle waits, the cycle count says how
     * many of them there are, and one without the other is refused at definition time.
     * <p>
     * After the last cycle the record parks with no delay at all, meaning until it is resumed or exported (R27). A
     * permanent decode failure never takes this path - there is nothing a wait could change about a payload that
     * can never be read (R12).
     */
    public AfterRetries thenRetryAfter(Duration delay) {
        requireParking("thenRetryAfter");
        Objects.requireNonNull(delay, "A park delay must be supplied");
        if (delay.isNegative() || delay.isZero()) {
            throw new IllegalArgumentException(msg("thenRetryAfter ({}) must be positive - it is how long a parked "
                    + "record waits before its next attempt", delay));
        }
        this.parkDelay = delay;
        return this;
    }

    /**
     * How many times {@link #thenRetryAfter(Duration)} grants the record another attempt before it parks for good
     * (R27).
     */
    public AfterRetries forCycles(int cycles) {
        requireParking("forCycles");
        if (cycles < 1) {
            throw new IllegalArgumentException(msg("forCycles ({}) must be at least one - it counts the attempts a "
                    + "park delay grants; leave it out for a record that parks as soon as its retries run out",
                    cycles));
        }
        this.parkCycles = cycles;
        return this;
    }

    /**
     * Where exported records go. A destination shared by several routes carries records from all of them, so its
     * consumer reads raw bytes and dispatches on the source-topic provenance header (R13).
     */
    public AfterRetries dlqTo(String destination) {
        requireParking("dlqTo");
        this.destination = Objects.requireNonNull(destination, "A dead-letter destination topic must be supplied");
        return this;
    }

    /**
     * @see #dlqImmediately(String)
     */
    public AfterRetries dlqImmediately() {
        requireParking("dlqImmediately");
        this.immediately = true;
        return this;
    }

    /**
     * Export a parked record before the source topic's retention could delete it. Without this - and without a
     * destination - a parked record survives only while the instance runs: the engine holds it in memory, so it can
     * still be resumed or exported, but a restart cannot re-poll it (R27).
     */
    public AfterRetries dlqOlderThan(Duration age) {
        requireParking("dlqOlderThan");
        Objects.requireNonNull(age, "An age bound must be supplied");
        if (age.isNegative() || age.isZero()) {
            throw new IllegalArgumentException(msg("dlqOlderThan ({}) must be positive - it is how old a parked "
                    + "record may get before it is exported", age));
        }
        this.olderThan = age;
        return this;
    }

    /**
     * Export the oldest parked records on a partition once its offset-map payload reaches this whole percentage of
     * Kafka's commit-metadata cap, until the payload is back below it (R27).
     *
     * @param percentage a whole percentage, at most {@link #MAX_PAYLOAD_PERCENTAGE}
     */
    public AfterRetries dlqWhenOffsetPayloadReaches(int percentage) {
        requireParking("dlqWhenOffsetPayloadReaches");
        this.payloadPercentage = percentage;
        return this;
    }

    /**
     * A stopping policy has nothing to export: the two reactions are alternatives, and silently keeping a
     * destination on a policy that never parks would leave a setting that can never fire.
     */
    private void requireParking(String setting) {
        if (reaction != Reaction.PARK) {
            throw new IllegalArgumentException(msg("{} cannot be declared on afterRetries(stop()) - stopping the "
                    + "instance and exporting the record are alternatives; declare park() to export", setting));
        }
    }

    public Reaction reaction() {
        return reaction;
    }

    public String destination() {
        return destination;
    }

    public boolean isDlqImmediately() {
        return immediately;
    }

    public Duration ageBound() {
        return olderThan;
    }

    /**
     * @return how long each park cycle waits, or null when no cycles were declared
     */
    public Duration parkDelay() {
        return parkDelay;
    }

    /**
     * @return how many park cycles this policy grants, zero when none were declared
     */
    public int parkCycles() {
        return parkCycles == null ? 0 : parkCycles;
    }

    /**
     * Whether a delay or a cycle count was declared at all - the two must be declared together, and this is what
     * lets the definition say so rather than silently ignoring the half that arrived (R27).
     */
    boolean declaresAnyParkCycle() {
        return parkDelay != null || parkCycles != null;
    }

    /**
     * @return the declared percentage, or empty when none was declared and the instance default applies
     */
    public OptionalInt payloadPercentage() {
        return payloadPercentage == null ? OptionalInt.empty() : OptionalInt.of(payloadPercentage);
    }

    /**
     * Whether any export trigger was declared. A destination with no trigger exports nothing, which is why the
     * definition refuses one (KTD5).
     */
    public boolean hasExportTrigger() {
        return immediately || olderThan != null || payloadPercentage != null;
    }

    /**
     * An independent copy, so a route that takes the instance default and then overrides part of it does not edit
     * the default every other route shares (R6).
     */
    public AfterRetries copy() {
        AfterRetries copy = new AfterRetries(reaction);
        copy.destination = destination;
        copy.immediately = immediately;
        copy.olderThan = olderThan;
        copy.payloadPercentage = payloadPercentage;
        copy.parkDelay = parkDelay;
        copy.parkCycles = parkCycles;
        return copy;
    }

    @Override
    public String toString() {
        return "AfterRetries(" + reaction + ", destination=" + destination + ", immediately=" + immediately
                + ", olderThan=" + olderThan + ", payloadPercentage=" + payloadPercentage
                + ", parkDelay=" + parkDelay + ", parkCycles=" + parkCycles + ")";
    }
}
