package io.confluent.parallelconsumer.streams;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.internals.InMemoryTimeOrderedKeyValueChangeBuffer;

import java.time.Duration;

/**
 * The state stores the refusal tests are built on, in one place.
 * <p>
 * {@link PcSupportedEnvelopeTest} classifies the built stores directly; {@link ProcessorApiBackstopTest}
 * attaches the builders to a Processor API topology. Same stores, two levels - so they belong here rather
 * than cloned into both, which is exactly the shape the repo's duplicate-code check flags.
 * <p>
 * <b>These are real Kafka stores, deliberately.</b> A hand-rolled stub implementing {@code WindowStore}
 * would satisfy the {@code instanceof} chain while telling us nothing about the
 * {@code MeteredWindowStore}-over-{@code ChangeLogging...}-over-bytes-store stack that
 * {@code ProcessorTopology.stateStores()} actually hands the backstop.
 * <p>
 * In-memory wherever Kafka offers it, so the unit suite never opens RocksDB. Versioned stores have no
 * in-memory variant in Kafka 3.9, but building a store is not opening one - RocksDB is touched at
 * {@code init}, and nothing here initialises.
 *
 * @author Antony Stubbs
 */
final class RefusedStoreFixtures {

    private static final Duration RETENTION = Duration.ofMinutes(10);
    private static final Duration WINDOW_SIZE = Duration.ofMinutes(1);

    private RefusedStoreFixtures() {
    }

    static StoreBuilder<?> windowStoreBuilder() {
        return Stores.windowStoreBuilder(
                Stores.inMemoryWindowStore("windows", RETENTION, WINDOW_SIZE, false),
                Serdes.String(), Serdes.String());
    }

    static StoreBuilder<?> sessionStoreBuilder() {
        return Stores.sessionStoreBuilder(
                Stores.inMemorySessionStore("sessions", RETENTION),
                Serdes.String(), Serdes.String());
    }

    static StoreBuilder<?> suppressionBufferBuilder() {
        return new InMemoryTimeOrderedKeyValueChangeBuffer.Builder<>(
                "suppression", Serdes.String(), Serdes.String());
    }

    /**
     * The one that looks harmless. {@code VersionedKeyValueStore} extends {@code StateStore} directly rather
     * than {@code WindowStore}, so it is reachable through {@code Materialized} with no refused DSL call
     * anywhere - and it silently drops puts older than its observed stream time.
     */
    static StoreBuilder<?> versionedKeyValueStoreBuilder() {
        return Stores.versionedKeyValueStoreBuilder(
                Stores.persistentVersionedKeyValueStore("versioned", Duration.ofDays(1)),
                Serdes.String(), Serdes.String());
    }

    /**
     * The supported one. Non-windowed stateful aggregation is inside the envelope (KTD3), and every refusal
     * test needs it as the positive control.
     */
    static StoreBuilder<?> keyValueStoreBuilder() {
        return Stores.keyValueStoreBuilder(
                Stores.inMemoryKeyValueStore("counts"), Serdes.String(), Serdes.String());
    }

    static StateStore build(final StoreBuilder<?> builder) {
        return (StateStore) builder.build();
    }
}
