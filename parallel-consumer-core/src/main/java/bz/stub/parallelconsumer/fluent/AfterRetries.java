package bz.stub.parallelconsumer.fluent;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.apache.kafka.common.annotation.InterfaceStability;

import java.time.Duration;
import java.util.Objects;

import static bz.stub.parallelconsumer.internal.utils.StringUtils.msg;

/**
 * What happens to a record once it has exhausted its retries (R27): it {@link #park()}s in place, or it
 * {@link #stop()}s the instance.
 *
 * <h2>Park is the whole of the answer in this release</h2>
 * The offset map already commits past an incomplete record, so an exhausted record can stay where it is, holding no
 * worker, with the source topic as its store and the map as its index. Nothing is copied anywhere, and a definition
 * that declares no reaction at all gets this one.
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
     * Which of the two reactions this policy asks for.
     */
    @InterfaceStability.Unstable
    public enum Reaction {
        /**
         * The record stays incomplete in the offset map, holds no worker, and waits for an operator or a
         * restart.
         */
        PARK,
        /**
         * The instance stops. A record that has run out of attempts on this route is treated as the definition's
         * author saying the instance should not carry on without someone looking (R24, R27).
         */
        STOP
    }

    /**
     * Which of the two reactions this policy asks for, fixed by the factory that made it and never changed after.
     * That it cannot change is what lets {@link #requireParking(String)} refuse a park setting outright, rather
     * than storing one that could never fire.
     */
    private final Reaction reaction;

    /**
     * How long each park cycle waits before the next attempt. Null when none was declared. It is declared together
     * with {@link #parkCycles}, and {@link #declaresAnyParkCycle()} is what lets the definition refuse half of that
     * pair rather than silently ignoring the half that arrived (R27).
     */
    private Duration parkDelay;

    /**
     * How many attempts {@link #parkDelay} grants before the record parks for good. Boxed so that "not declared"
     * stays distinguishable from a declared value: undeclared and declared are different answers here, and
     * {@link #parkCycles()} may only collapse them to zero once the pair has been validated.
     */
    private Integer parkCycles;

    /**
     * Private, so a policy can only be born through {@link #park()} or {@link #stop()} and therefore always names
     * its reaction. {@link #copy()} is the only other caller, and it carries the remaining fields across by hand
     * for the same reason: there is no constructor that takes them.
     */
    private AfterRetries(Reaction reaction) {
        this.reaction = reaction;
    }

    /**
     * Park in place. It is the default on every route, so declaring it states the intent rather than changing
     * anything; {@link #thenRetryAfter(Duration)} below is what qualifies it.
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
     * Wait this long, then attempt the record once more - which is what scheduled retry is (astubbs#234). Declared
     * together with {@link #forCycles(int)}: the delay says how long each cycle waits, the cycle count says how
     * many of them there are, and one without the other is refused at definition time.
     * <p>
     * After the last cycle the record parks with no delay at all, meaning until a rebalance or a restart delivers
     * it again (R27). A permanent decode failure never takes this path - there is nothing a wait could change
     * about a payload that can never be read (R12).
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
     * A stopping policy has no park to qualify: the two reactions are alternatives, and silently keeping a park
     * delay on a policy that never parks would leave a setting that can never fire.
     */
    private void requireParking(String setting) {
        if (reaction != Reaction.PARK) {
            throw new IllegalArgumentException(msg("{} cannot be declared on afterRetries(stop()) - stopping the "
                    + "instance and parking the record are alternatives; declare park() to schedule a retry",
                    setting));
        }
    }

    /**
     * The one thing a reader of this policy must branch on: park the record, or stop the instance. Everything else
     * here only qualifies the parking case (R27).
     */
    public Reaction reaction() {
        return reaction;
    }

    /**
     * How long a cycling record waits before its next attempt. The dispatch wrapper turns it into the delay a
     * retriable failure carries, so it is the only place the schedule in "scheduled retry" comes from; validation
     * reads the same null to refuse a cycle count declared without one (R27, astubbs#234).
     *
     * @return how long each park cycle waits, or null when no cycles were declared
     */
    public Duration parkDelay() {
        return parkDelay;
    }

    /**
     * How many extra attempts the park delay grants, collapsed to zero when none were declared. Zero is the answer
     * that makes the dispatch wrapper's comparison against the cycles already spent park a record on the first
     * look, so the undeclared case needs no branch of its own. It cannot tell undeclared from declared - that is
     * {@link #declaresAnyParkCycle()}'s job, and it exists because validation must.
     *
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
     * An independent copy, so a route that takes the instance default and then overrides part of it does not edit
     * the default every other route shares (R6).
     */
    public AfterRetries copy() {
        AfterRetries copy = new AfterRetries(reaction);
        copy.parkDelay = parkDelay;
        copy.parkCycles = parkCycles;
        return copy;
    }

    /**
     * Every setting, nulls included, because this is read inside a validation refusal that must show what the
     * policy actually carries - an omitted null would make an undeclared setting look like a declared one.
     */
    @Override
    public String toString() {
        return "AfterRetries(" + reaction + ", parkDelay=" + parkDelay + ", parkCycles=" + parkCycles + ")";
    }
}
