package io.confluent.parallelconsumer.streams;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import java.util.Collections;

/**
 * The Kafka Streams constructs that do not work on the Parallel Consumer dispatch path, and the one place
 * that says so.
 * <p>
 * <b>These are not merely unimplemented - they produce silently wrong answers.</b> Stream time advances in
 * {@code PartitionGroup.nextRecord()}, which the PC path bypasses entirely (KTD8 - one record path,
 * switched, never both), so it never moves. Window close, join emission and suppression emission are all
 * gated on it. Worse, the {@code observedStreamTime} fields those operators keep are plain non-volatile
 * {@code long}s doing read-modify-write: under concurrent dispatch they are corrupted, not merely
 * reordered. Nothing throws and nothing logs - the topology runs and the numbers are wrong. Refusing the
 * construct is the only honest behaviour available until the semantics are fixed.
 * <p>
 * <b>Refusal is conditional on the seam.</b> {@link #refuse()} is a no-op when
 * {@link PcDispatchSwitch#isEnabled()} is false, so a seam-off run stays behaviourally identical to stock
 * Kafka Streams - which is what makes the module's behaviour-preservation claim (Apache Kafka's own 419
 * tests, unmodified, zero failures) still true after this class exists.
 * <p>
 * <b>Reinstatement is evidence-gated, not judgement-gated.</b> A construct comes back off this list when
 * Kafka's own test suite exercises it with the seam <b>on</b> and passes - not when someone reads the code
 * and concludes it looks fine.
 *
 * @author Antony Stubbs
 * @see PcSupportedEnvelope
 * @see PcDispatchSwitch
 */
public enum PcUnsupportedConstruct {

    KSTREAM_KSTREAM_JOIN(
            "KStream-KStream join",
            "join emission is gated on stream time, which never advances on the PC path, and "
                    + "KStreamKStreamJoin.sharedTimeTracker is mutated from both join sides without synchronisation"),

    KSTREAM_KTABLE_JOIN(
            "KStream-KTable join",
            "the table side is read at the record's stream time, which never advances on the PC path"),

    KSTREAM_GLOBALKTABLE_JOIN(
            "KStream-GlobalKTable join",
            "the global table is read at the record's stream time, which never advances on the PC path"),

    KTABLE_KTABLE_JOIN(
            "KTable-KTable join",
            "join results are emitted per update in arrival order, which concurrent dispatch does not preserve"),

    KTABLE_KTABLE_FOREIGN_KEY_JOIN(
            "KTable-KTable foreign-key join",
            "the subscription store is updated and read across the join's two halves without ordering guarantees "
                    + "that concurrent dispatch can honour"),

    WINDOWED_AGGREGATION(
            "windowed aggregation (windowedBy)",
            "windowCloseTime is derived from observedStreamTime, which never advances on the PC path, so which "
                    + "records count as late changes with dispatch order - and observedStreamTime is a non-volatile "
                    + "long updated read-modify-write from every worker"),

    WINDOWED_COGROUPED_AGGREGATION(
            "windowed cogrouped aggregation (windowedBy)",
            "same as windowed aggregation - window close is stream-time driven and stream time does not advance "
                    + "on the PC path"),

    SUPPRESSION(
            "suppression (suppress)",
            "\"only the final result per window\" is a statement about stream time, which never advances on the "
                    + "PC path, so suppressed updates would never be emitted"),

    WINDOW_STORE(
            "a WindowStore",
            "the store keeps its own non-volatile observedStreamTime and uses it to decide which records are too "
                    + "late to retain - so under concurrent dispatch it drops records based on a value that is being "
                    + "corrupted by read-modify-write from several workers at once"),

    SESSION_STORE(
            "a SessionStore",
            "session merging is driven by stream time and by record arrival order, neither of which the PC path "
                    + "preserves"),

    SUPPRESSION_BUFFER(
            "a suppression buffer",
            "the buffer emits on stream time, which never advances on the PC path"),

    VERSIONED_KEY_VALUE_STORE(
            "a versioned key-value store",
            "the store keeps a non-volatile observedStreamTime and silently DROPS any put older than "
                    + "observedStreamTime minus the grace period, so concurrent dispatch loses writes rather than "
                    + "merely reordering them - and reads outside history retention are rejected off the same field"),

    EXACTLY_ONCE(
            "exactly-once processing (processing.guarantee)",
            "under exactly_once_v2 the Kafka Streams transaction is per-StreamThread rather than per-task, so a "
                    + "worker's send joins a transaction covering every task on that thread and one task's work "
                    + "cannot be committed without committing every other worker's in-flight work; the older "
                    + "per-task exactly_once is no better placed, because StreamsProducer.transactionInFlight is a "
                    + "non-volatile check-then-act (KTD7: this module is at-least-once)");

    /**
     * How this construct is named back to the user. Deliberately the name they would recognise from their own
     * topology, not the internal class that implements it.
     */
    private final String displayName;

    /**
     * Why it is refused. Carried with the construct rather than written at the throw site, so the twelve
     * {@link #refuse()} call sites in the generated Kafka sources stay one line each and cannot drift apart.
     */
    private final String reason;

    PcUnsupportedConstruct(final String displayName, final String reason) {
        this.displayName = displayName;
        this.reason = reason;
    }

    public String getDisplayName() {
        return displayName;
    }

    public String getReason() {
        return reason;
    }

    /**
     * Refuse this construct if - and only if - the PC dispatch seam is on.
     * <p>
     * The guard is the whole point. Unconditional refusal would break Apache Kafka's own test suite, which this
     * module runs unmodified with the seam <b>off</b> as its behaviour-preservation evidence, and several of
     * those tests build exactly the constructs listed here.
     *
     * @throws UnsupportedOperationException if the seam is on
     */
    public void refuse() {
        if (!PcDispatchSwitch.isEnabled()) {
            return;
        }
        throw new UnsupportedOperationException(describe());
    }

    /**
     * The refusal message for this construct on its own: what was refused, why, and how to get stock Kafka
     * Streams dispatch back.
     */
    public String describe() {
        return PcRefusalMessage.forConstructs(Collections.singletonList(this));
    }
}
