package bz.stub.parallelconsumer.fluent;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.apache.kafka.common.annotation.InterfaceStability;

/**
 * Told once when a record parks (R16): after its last attempt, before its offset commits.
 *
 * <h2>It is sugar, and that is the point</h2>
 * Parking is already an outcome - it is counted, and the parked set is queryable (R19, R28) - so nothing here is
 * needed for correct operation. This is the Java binding's shortcut for the common case, one log line or one alert
 * at the moment a record gives up, and it is the only construct in the fluent API that is a second callback (KD3,
 * R18). A foreign client of the language proxy reads the parked set instead.
 *
 * <h2>What arrives, and what does not</h2>
 * <b>Exactly once per record per assignment.</b> A rebalance resets the facade's per-record state (R10), so a
 * record parked, reassigned and parked again is observed once in each assignment; within one assignment a second
 * park of the same record fires nothing.
 * <p>
 * <b>Typed values only when decoding succeeded.</b> A record parked by a permanent decode failure (R12) has no
 * value to hand over, so {@link TypedRecordContext#key()} and {@link TypedRecordContext#value()} are null and
 * {@link TypedRecordContext#raw()} carries the original bytes and headers - which is also what an export would carry.
 * Its attempt count is zero, because a payload that can never decode spends no attempts.
 * <p>
 * <b>A throw from here changes nothing.</b> It is caught and logged; the record parks either way. An observer is a
 * report, and a report that could alter the outcome would make parking depend on the reporting.
 *
 * @param <K> the route's consumed key type
 * @param <V> the route's consumed value type
 */
@FunctionalInterface
@InterfaceStability.Unstable
public interface ParkObserver<K, V> {

    /**
     * Told that this record has parked: its last attempt has run, and its offset will not commit past it until it
     * is resumed or exported. It returns nothing because it is a report and not a decision - the record parks
     * whatever this method does - and the type-level contract above is the whole of what it may assume.
     *
     * @param record   the parked record, decoded when it could be decoded and raw when it could not
     * @param failure  the last failure, or null when the function asked for the park itself with
     *                 {@link Outcome#park(String)} - nothing failed in that case
     * @param attempts how many times this route's function ran for this record, zero for a permanent decode failure
     */
    void onParked(TypedRecordContext<K, V> record, Throwable failure, int attempts);
}
