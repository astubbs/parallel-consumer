package bz.stub.parallelconsumer.fluent;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.Percent;
import bz.stub.parallelconsumer.state.PartitionStateManager;
import org.apache.kafka.common.annotation.InterfaceStability;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

import static bz.stub.parallelconsumer.Percent.percentOf;
import static bz.stub.parallelconsumer.internal.utils.StringUtils.msg;

/**
 * What happens to a record once it has exhausted its retries (R27): it {@link #park()}s in place, it is
 * {@link #dlq(String) dlq}'d to another topic, or it {@link #stop()}s the instance.
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
     * The engine's own pause threshold as a percentage: where a partition stops taking work, which is the quantity
     * the ceiling below is measured down from. Read off
     * {@link PartitionStateManager#USED_PAYLOAD_THRESHOLD_MULTIPLIER_DEFAULT} rather than written down again, so a
     * change to the threshold moves the ceiling with it.
     */
    public static final Percent PAUSE_THRESHOLD_PERCENTAGE =
            percentOf(PartitionStateManager.USED_PAYLOAD_THRESHOLD_MULTIPLIER_DEFAULT * 100);

    /**
     * The highest export percentage a definition may declare, and the default.
     * <p>
     * The engine stops a partition taking work at {@link #PAUSE_THRESHOLD_PERCENTAGE} of the commit-metadata cap, so
     * a percentage at or above that is never reached and would read as a setting that silently does nothing. Five
     * points below it is the margin the owner chose. Both numbers are provisional on the current encoding: exact
     * continuous offset encoding (astubbs#237, confluentinc#53) makes the payload size precise and they are
     * revisited when it lands.
     * <p>
     * It is a {@link Percent} rather than a bare number for the same reason the setting it bounds takes one: the
     * ceiling is the same kind of quantity as the value it is compared against, and a bare number here would put
     * the unit back in the reader's head.
     */
    public static final Percent MAX_PAYLOAD_PERCENTAGE =
            percentOf(PAUSE_THRESHOLD_PERCENTAGE.percentage() - 5);

    /**
     * Which of the three reactions this policy asks for. They answer three different questions: park is about
     * <em>this record</em>, dead-letter is about <em>capacity</em>, and stop is about <em>the deployment</em>.
     */
    @InterfaceStability.Unstable
    public enum Reaction {
        /**
         * The record stays incomplete in the offset map, holds no worker, and waits for a trigger, an operator, or a
         * restart.
         */
        PARK,
        /**
         * The record is copied to a dead-letter topic on the dispatch that exhausts it and then completed, so it
         * never enters the parked view and the committed offset moves straight past it.
         * <p>
         * <b>Nothing reads this yet.</b> A definition carrying it is refused at start, so no dispatch ever sees it -
         * see {@link AfterRetries#dlq(String)} for what it waits on.
         */
        DLQ,
        /**
         * The instance stops. A record that has run out of attempts on this route is treated as the definition's
         * author saying the instance should not carry on without someone looking (R24, R27).
         */
        STOP
    }

    /**
     * Which of the two reactions this policy asks for, fixed by the factory that made it and never changed after.
     * That it cannot change is what lets {@link #requireParking(String)} refuse an export setting outright, rather
     * than storing one that could never fire.
     */
    private final Reaction reaction;

    /**
     * The topic exported records are copied to, or null for the complete default: park in place, with the source
     * topic as the store and the offset map as the index, and nothing copied anywhere.
     */
    private String destination;

    /**
     * The first of the three export triggers: export on the dispatch after exhaustion, which is the classic
     * dead-letter queue. A destination declared with no trigger at all would export nothing, which is why the
     * definition refuses that pairing rather than accepting it (KTD5).
     */
    private boolean immediately;

    /**
     * The second export trigger: the age a parked record may reach before it is copied out, ahead of the source
     * topic's retention deleting it. Null when no age bound was declared.
     */
    private Duration olderThan;

    /**
     * The third export trigger. Null when not declared, which stays distinguishable from a declared value - a
     * distinction {@link #payloadPercentage()} hands on as an {@link Optional} rather than as a sentinel. Held as a
     * {@link Percent} so the value carries its unit from the call site all the way to the refusal that quotes it.
     */
    private Percent payloadPercentage;

    /**
     * How long each park cycle waits before the next attempt. Null when none was declared. It is declared together
     * with {@link #parkCycles}, and {@link #declaresAnyParkCycle()} is what lets the definition refuse half of that
     * pair rather than silently ignoring the half that arrived (R27).
     */
    private Duration parkDelay;

    /**
     * How many attempts {@link #parkDelay} grants before the record parks for good. Boxed for the same reason as
     * {@link #payloadPercentage}: undeclared and declared are different answers here, and {@link #parkCycles()} may
     * only collapse them to zero once the pair has been validated.
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
     * Dead-letter the record instead of parking it: on the dispatch that exhausts its retries it is copied to
     * {@code destination} with its provenance headers (R13) and then completed, so the committed offset moves past
     * it and the offset map carries nothing for it. It is the reaction to reach for when a record has to leave the
     * topic rather than sit in it - a capacity or a retention question, not a severity one.
     *
     * <h2>This reaction does not work yet</h2>
     * A definition that declares a dead-letter destination is <b>refused at start, naming the topic</b>, and that
     * refusal covers this factory: nothing in the engine copies a record on. Export is a re-dispatch rather than a
     * send from the failure path, and the commands it needs - producing from the parked set within the
     * transaction's budget, oldest-first once a partition's offset payload reaches a fraction of the
     * commit-metadata cap - are Milestone C. Until they land the honest answer is the refusal: a reaction accepted
     * and silently parking instead would be the one outcome worse than not offering it.
     * <p>
     * It is spelled out here rather than left out because the reaction is what a user picks between, and a policy
     * that could name only two of the three read as though the third did not exist.
     *
     * <h2>Against {@link #dlqImmediately(String)}</h2>
     * That one parks the record and makes exhaustion its export trigger, so the copy happens on the <em>next</em>
     * dispatch, one retry delay later, and the record is in the parked view until it does. This one never parks.
     * Which of the two survives the milestone that builds export is open; both are recorded so the choice is made
     * against a written shape rather than from memory.
     *
     * @param destination the topic exhausted records are copied to. This definition may not route it itself - an
     *                    instance that consumed its own exports would loop them (R13).
     */
    public static AfterRetries dlq(String destination) {
        AfterRetries policy = new AfterRetries(Reaction.DLQ);
        // Set here rather than through dlqTo, which is a qualifier on a parking policy: this reaction carries its
        // destination as part of what it is, and has nothing left for a trigger to qualify.
        policy.destination = Objects.requireNonNull(destination, "A dead-letter destination topic must be supplied");
        return policy;
    }

    /**
     * The classic dead-letter queue: export on the dispatch after exhaustion, one retry delay later (R27, AE19).
     * The record parks first and exhaustion is its export trigger, which is what distinguishes it from
     * {@link #dlq(String)}.
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
     * Makes exhaustion itself the export trigger, for a destination named separately by {@link #dlqTo(String)}.
     * It is the trigger half of {@link #dlqImmediately(String)} on its own, so a policy assembled setting by
     * setting can say what the one-call factory says; on its own it is refused, because a trigger with nowhere to
     * send to would export nothing (R27, AE7).
     *
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
     * Export the oldest parked records on a partition once its offset-map payload reaches this percentage of Kafka's
     * commit-metadata cap, until the payload is back below it (R27).
     * <p>
     * Declaring one is refused in this version: the engine has no accessor for a partition's encoded payload length,
     * so the trigger would never fire (KTD5). The value is still recorded here rather than rejected on the spot, so
     * that the definition's refusal can quote the percentage that was asked for.
     *
     * @param percentage a percentage of the cap - {@code percentOf(70)} is seventy percent of it - at most
     *                   {@link #MAX_PAYLOAD_PERCENTAGE}, which is also what applies when none is declared
     */
    public AfterRetries dlqWhenOffsetPayloadReaches(Percent percentage) {
        requireParking("dlqWhenOffsetPayloadReaches");
        this.payloadPercentage = Objects.requireNonNull(percentage, "An export percentage must be supplied");
        return this;
    }

    /**
     * The same trigger for a caller who would rather write the number than the type: {@code 70} is seventy percent
     * of the cap, the unit {@link Percent} spells out. It builds one, so a value that is not a percentage is refused
     * here and now rather than being stored and explained later as something else.
     *
     * @param percentage a percentage of the cap out of a hundred, not a fraction of one
     * @see #dlqWhenOffsetPayloadReaches(Percent)
     */
    public AfterRetries dlqWhenOffsetPayloadReaches(double percentage) {
        return dlqWhenOffsetPayloadReaches(percentOf(percentage));
    }

    /**
     * Everything below {@link #park()} qualifies a record that parks, so a policy with another reaction has nothing
     * for it to qualify - and silently keeping the setting would leave one that can never fire. The two other
     * reactions turn it away for different reasons, so the refusal says which one it is looking at rather than
     * naming stop() for both.
     */
    private void requireParking(String setting) {
        if (reaction == Reaction.STOP) {
            throw new IllegalArgumentException(msg("{} cannot be declared on afterRetries(stop()) - stopping the "
                    + "instance and exporting the record are alternatives; declare park() to export", setting));
        }
        if (reaction == Reaction.DLQ) {
            throw new IllegalArgumentException(msg("{} cannot be declared on afterRetries(dlq(...)) - that reaction "
                    + "already copies the record out on the dispatch that exhausts it, so there is no park for this "
                    + "setting to qualify. Declare park() beside it to park first and export on a trigger.",
                    setting));
        }
    }

    /**
     * The one thing a reader of this policy must branch on: park the record, dead-letter it, or stop the instance.
     * Everything else here only qualifies the parking case (R27).
     */
    public Reaction reaction() {
        return reaction;
    }

    /**
     * Where this policy's exported records are copied. Validation reads it together with
     * {@link #hasExportTrigger()}, which is what lets it refuse a destination with no trigger and a trigger with no
     * destination in the same pass; it also refuses a destination this same definition routes, because an instance
     * that consumed its own exports would loop them (R13).
     *
     * @return the topic exported records are copied to, or null when this policy parks in place and copies nothing -
     * which is a complete policy, not an unfinished one
     */
    public String destination() {
        return destination;
    }

    /**
     * Whether exhaustion itself is the export trigger, rather than an age or a payload bound. Named
     * {@code isDlqImmediately} to match the {@code dlqImmediately} a definition's author actually wrote, so a
     * refusal quoting one reads the same as the code quoting the other.
     */
    public boolean isDlqImmediately() {
        return immediately;
    }

    /**
     * The age export trigger as it was declared. Null is the only way this policy can say the trigger is absent -
     * there is no duration that means "never" - which is why validation tests it against null rather than against a
     * sentinel, and why it is one of the three triggers {@link #hasExportTrigger()} counts.
     *
     * @return how old a parked record may get before it is exported, or null when no age bound was declared
     */
    public Duration ageBound() {
        return olderThan;
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
     * The offset-payload export trigger as it was declared. Empty rather than a number because an undeclared
     * percentage would resolve to the instance-wide default, and only the definition holds that; in this release
     * validation refuses a present value outright, since the trigger needs an engine accessor that arrives with
     * Milestone C (KTD5).
     *
     * @return the declared percentage, or empty when none was declared and the instance default applies
     */
    public Optional<Percent> payloadPercentage() {
        return Optional.ofNullable(payloadPercentage);
    }

    /**
     * Whether anything would ever send to {@link #destination()}. A parking policy needs one of the three declared
     * triggers, and a destination with none exports nothing, which is why the definition refuses that pairing
     * (KTD5). {@link Reaction#DLQ} needs no trigger because exhaustion <em>is</em> its trigger - the reaction and
     * the destination are one statement - and answering false for it would have validation refuse the reaction as
     * untriggered.
     */
    public boolean hasExportTrigger() {
        return reaction == Reaction.DLQ || immediately || olderThan != null || payloadPercentage != null;
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

    /**
     * Every setting, nulls included, because this is read inside a validation refusal that must show what the
     * policy actually carries - an omitted null would make an undeclared setting look like a declared one.
     */
    @Override
    public String toString() {
        return "AfterRetries(" + reaction + ", destination=" + destination + ", immediately=" + immediately
                + ", olderThan=" + olderThan + ", payloadPercentage=" + payloadPercentage
                + ", parkDelay=" + parkDelay + ", parkCycles=" + parkCycles + ")";
    }
}
