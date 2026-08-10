package io.confluent.parallelconsumer.streams;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.VersionedKeyValueStore;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.internals.TimeOrderedKeyValueBuffer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * The backstop: what this module supports, checked once, at task construction.
 * <p>
 * <b>Why a second line of defence exists at all.</b> The DSL refusals in
 * {@link PcUnsupportedConstruct#refuse()} only fire for someone who called {@code windowedBy}, {@code join}
 * or {@code suppress}. The Processor API reaches the same machinery without touching {@code KStream} at
 * all - {@code topology.addStateStore(Stores.windowStoreBuilder(...))} builds a window store and connects
 * it to a plain {@code Processor}, and no DSL method is ever called. That route would sail straight past
 * layers 1 and 2. And exactly-once is not a topology shape at all: it is one configuration key.
 * <p>
 * <b>Why here.</b> {@code StreamTask}'s constructor holds both the {@code ProcessorTopology} and the task
 * config, and {@code StreamTask} is already patched - so this check costs no additional patched class,
 * which is the objection that sinks most "just add a check" proposals in this module.
 * <p>
 * <b>Classification is by interface, never by class name.</b> The stores that reach
 * {@code ProcessorTopology.stateStores()} are wrapped several layers deep - {@code MeteredWindowStore} over
 * {@code ChangeLoggingWindowBytesStore} over the bytes store - and every wrapper implements the interface it
 * wraps. {@code instanceof} sees through the whole stack; a name match sees only the outermost wrapper and
 * breaks the first time Kafka adds one.
 *
 * @author Antony Stubbs
 * @see PcUnsupportedConstruct
 */
public final class PcSupportedEnvelope {

    private PcSupportedEnvelope() {
    }

    /**
     * Refuse a task whose topology or configuration reaches outside the supported envelope.
     * <p>
     * A no-op when the seam is off, for the same reason {@link PcUnsupportedConstruct#refuse()} is: Apache
     * Kafka's own {@code StreamTaskTest} builds EOS-enabled tasks and window stores, and this module runs that
     * suite unmodified with the seam off as its behaviour-preservation evidence. An unguarded check here turns
     * those cases into constructor errors and voids the claim.
     * <p>
     * Called <b>before</b> the task's PC dispatcher is created, so a refused task never allocates a worker pool
     * that nothing will shut down.
     *
     * <b>Global stores are deliberately out of scope.</b> {@code ProcessorTopology.globalStateStores()} is not
     * inspected, because a global store is read and written by {@code GlobalStreamThread} - one thread, its own
     * task type, never the PC dispatch path. Refusing one here would refuse a construct this module does not
     * actually break.
     *
     * @param taskId      named in the message, because a user with several tasks needs to know which topology
     *                    is at fault
     * @param stateStores the task topology's state stores, already constructed
     * @param eosEnabled  the task config's exactly-once flag - configuration, not topology shape, which is why
     *                    it cannot be inferred from the stores
     * @throws UnsupportedOperationException if the seam is on and anything here is outside the envelope
     */
    public static void checkTask(final String taskId,
                                 final Collection<StateStore> stateStores,
                                 final boolean eosEnabled) {
        if (!PcDispatchSwitch.isEnabled()) {
            return;
        }

        final List<PcUnsupportedConstruct> found = findUnsupported(stateStores, eosEnabled);
        if (found.isEmpty()) {
            return;
        }

        // The recurrence sentence is not padding. This throw leaves StreamTask's constructor, is caught by
        // nothing on the way out, and reaches KafkaStreams' uncaught-exception handler - which under
        // StreamsUncaughtExceptionHandlerResponse.REPLACE_THREAD replaces the thread unconditionally, with no
        // backoff and no attempt counter. The refusal is a property of the topology and the switch, so the
        // replacement thread is refused too, and the application rebalances in a loop. Whoever reads this
        // message is looking at that loop; the message has to tell them it will not stop on its own.
        throw new UnsupportedOperationException("Task " + taskId + ": " + PcRefusalMessage.forConstructs(found)
                + " This is a property of the topology and the switch, not a transient failure, so every task "
                + "creation refuses again: a REPLACE_THREAD uncaught-exception handler will rebalance in a loop "
                + "until the topology changes or the seam is turned off.");
    }

    /**
     * Every unsupported construct in one pass, de-duplicated and in a stable order.
     * <p>
     * Package-private rather than private so the unit tests can assert the classification directly, without
     * standing up a {@code StreamTask} to observe it through a thrown message.
     */
    static List<PcUnsupportedConstruct> findUnsupported(final Collection<StateStore> stateStores,
                                                        final boolean eosEnabled) {
        // A LinkedHashSet because both properties are load-bearing: de-duplicated, because three window stores
        // in one topology is one problem rather than three lines of message; insertion-ordered, because the
        // message should read in the order the constructs were found. An EnumSet would silently reorder them
        // into declaration order.
        final Set<PcUnsupportedConstruct> found = new LinkedHashSet<>();

        if (eosEnabled) {
            found.add(PcUnsupportedConstruct.EXACTLY_ONCE);
        }

        if (stateStores != null) {
            for (final StateStore store : stateStores) {
                final PcUnsupportedConstruct construct = classify(store);
                if (construct != null) {
                    found.add(construct);
                }
            }
        }

        return new ArrayList<>(found);
    }

    /**
     * @return the construct this store implies, or {@code null} when the store is inside the envelope. A plain
     *         key-value store is the supported stateful case (KTD3 - stateless first, then one non-windowed
     *         aggregation) and must not be refused, or the module's own stateful proof stops running.
     */
    private static PcUnsupportedConstruct classify(final StateStore store) {
        // The four store interfaces are disjoint in Kafka 3.9.2, so this order does not currently disambiguate
        // anything. It is written most-specific-first anyway, so that a future Kafka in which one of them
        // starts extending another reports the narrower construct rather than the vaguer one.
        if (store instanceof TimeOrderedKeyValueBuffer) {
            return PcUnsupportedConstruct.SUPPRESSION_BUFFER;
        }
        if (store instanceof SessionStore) {
            return PcUnsupportedConstruct.SESSION_STORE;
        }
        if (store instanceof WindowStore) {
            return PcUnsupportedConstruct.WINDOW_STORE;
        }
        // Easy to miss, and the most dangerous of the four: VersionedKeyValueStore extends StateStore directly
        // rather than WindowStore, so it looks like an ordinary key-value store and is reachable with no refused
        // DSL call anywhere - Materialized.as(Stores.persistentVersionedKeyValueStore(...)) is enough. Its
        // observedStreamTime does not merely mis-order under concurrency, it silently discards puts.
        if (store instanceof VersionedKeyValueStore) {
            return PcUnsupportedConstruct.VERSIONED_KEY_VALUE_STORE;
        }
        return null;
    }
}
