package bz.stub.parallelconsumer.client.conformance;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.client.ClientOptions;
import bz.stub.parallelconsumer.client.ParallelConsumerClient;
import bz.stub.parallelconsumer.client.RecordProcessor;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.OptionalLong;

/**
 * The transport-binding seam of the shared conformance suite: everything {@link SpikeConformanceTest} needs
 * from an engine-plus-transport fixture, expressed without naming either transport. Each transport module's
 * test tree supplies its own implementation - the gRPC one boots the proxy's {@code ProxyHarness} engine lane
 * (from the proxy test-jar) and connects the wire client to it; the direct one drives core's mock Kafka
 * clients in-process. The suite itself cannot tell which it is running against, which is the point (KTD20):
 * the test class is the constant and the transport is the only variable.
 * <p>
 * <b>This interface is transport-neutral by construction, and must stay so.</b> It lives in the API module's
 * test-jar, whose classpath has no protobuf, no gRPC and no core - so a transport type appearing here fails
 * the build of the direct module, whose {@code bannedDependencies} rule is the enforcement (AE26).
 *
 * @author Antony Stubbs
 * @see SpikeConformanceTest
 */
public interface SpikeFixture extends AutoCloseable {

    /**
     * Builds the transport's client from the given options, starts it with the given processor, and completes
     * whatever engine-side arrangement the transport needs (partition assignment, seeding). After this
     * returns, the fixture's seeded records are on their way to the processor.
     */
    void start(ClientOptions options, RecordProcessor processor);

    /**
     * The engine's most recently committed offset for the fixture's single topic-partition - the next offset
     * to be read, i.e. one past the last record completed. Empty until the first commit.
     */
    OptionalLong committedOffset();

    /** Everything the engine's producer has produced, in produce order, decoded as UTF-8. */
    List<ProducedRecord> produced();

    /**
     * How many records the engine currently counts as out for processing - the standing leak check: after a
     * converged run this returns to zero, under every transport.
     */
    long recordsOutForProcessing();

    /** Tears down the client and the engine, asserting neither ended with an unexpected failure. */
    @Override
    void close();

    /** One record to seed into the fixture's topic before the client connects. Offsets follow list order. */
    final class Seed {
        private final String key;
        private final String value;

        public Seed(String key, String value) {
            this.key = key;
            this.value = value;
        }

        public String key() {
            return key;
        }

        public String value() {
            return value;
        }
    }

    /** One record the engine produced, decoded as UTF-8 for assertion; key and value may be {@code null}. */
    final class ProducedRecord {
        private final String topic;
        private final String key;
        private final String value;

        public ProducedRecord(String topic, String key, String value) {
            this.topic = topic;
            this.key = key;
            this.value = value;
        }

        /** Decodes a transport's raw produced record, preserving {@code null} key and value (a tombstone). */
        public static ProducedRecord decodeUtf8(String topic, byte[] key, byte[] value) {
            return new ProducedRecord(topic,
                    key == null ? null : new String(key, StandardCharsets.UTF_8),
                    value == null ? null : new String(value, StandardCharsets.UTF_8));
        }

        public String topic() {
            return topic;
        }

        public String key() {
            return key;
        }

        public String value() {
            return value;
        }
    }
}
