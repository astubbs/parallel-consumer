package bz.stub.parallelconsumer.fluent;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.apache.kafka.common.annotation.InterfaceStability;

/**
 * A route's one processing function - the only callback the fluent API requires, and the only construct on it that a
 * wire contract could not carry as data (KD3, R18).
 * <p>
 * Return an {@link Outcome} to report a terminal outcome; <b>throw to retry</b> (R9). Any exception is a retry - the
 * engine's own retriable exception keeps its meaning and differs only in that it is not logged at error level. A
 * checked exception is allowed precisely so the clients your function calls need no wrapping.
 *
 * @param <K>  the route's consumed key type
 * @param <V>  the route's consumed value type
 * @param <PK> the route's produced key type, {@link Void} on a route that declares no {@link Produced} types
 * @param <PV> the route's produced value type, {@link Void} on a route that declares no {@link Produced} types
 */
@FunctionalInterface
@InterfaceStability.Unstable
public interface ProcessFunction<K, V, PK, PV> {

    Outcome<PK, PV> process(ProcessContext<K, V> context) throws Exception;
}
