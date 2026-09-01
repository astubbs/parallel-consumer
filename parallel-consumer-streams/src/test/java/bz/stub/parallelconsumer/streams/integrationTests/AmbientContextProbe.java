package bz.stub.parallelconsumer.streams.integrationTests;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A {@code transformValues} node that reads the per-task record context <em>ambiently</em> for every record,
 * while N worker threads are inside the same task, and reports every read that described somebody else's
 * record.
 * <p>
 * <b>Why the deprecated {@code ProcessorContext} and not the modern {@code api.Processor}.</b> On the modern
 * API the timestamp and headers arrive on the {@code Record} argument, so a probe written against it would
 * pass whether or not the shared context slot is confined - it would be testing nothing. The deprecated
 * {@code ProcessorContext} resolves {@code topic()}, {@code partition()}, {@code offset()},
 * {@code timestamp()} and {@code headers()} through the single per-task slot that U4 thread-confined, which
 * is exactly the thing under test.
 * <p>
 * <b>Every comparison has an anchor outside the record context.</b> The expected topic, offset, timestamp and
 * header are all derived from the record's <em>value</em>, which arrives as a method argument. Comparing two
 * ambient reads against each other would agree perfectly on the wrong record.
 * <p>
 * <b>The optional store write is the R9 upgrade.</b> A stateless topology's only ambient reader is this class
 * itself, which proves the confinement works for a reader written here. Pass a store name and the probe
 * also writes through a real Kafka Streams store stack -
 * {@code MeteredKeyValueStore -> ChangeLoggingKeyValueBytesStore -> RocksDBStore} - whose changelog record
 * timestamp comes from {@code context.timestamp()} inside {@code ChangeLoggingKeyValueBytesStore.put}. That
 * is Kafka's own ambient read of the confined field, observable from outside the JVM as a record timestamp on
 * the changelog topic. The write happens <b>after</b> the processing delay, deliberately: a slot that is
 * correct on entry and belongs to a sibling by the time it is used is the interesting failure, and a write on
 * entry would miss it.
 * <p>
 * Violations are collected, not thrown. Throwing here would kill the StreamThread and shut the client down,
 * and the test would then fail on a timeout that says nothing about which read was wrong.
 *
 * @author Antony Stubbs
 */
@Slf4j
final class AmbientContextProbe {

    private final String probeHeaderName;

    private final long processingCostMs;

    /**
     * Null for the stateless arm. Non-null names a state store this probe writes one entry to per record -
     * see the class javadoc.
     */
    private final String storeName;

    private volatile String expectedTopic;

    private final Map<String, Long> expectedOffsetByValue = new ConcurrentHashMap<>();

    private final AtomicInteger inFlight = new AtomicInteger();
    private final AtomicInteger peakConcurrency = new AtomicInteger();
    private final AtomicInteger observations = new AtomicInteger();
    private final Map<String, Boolean> valuesSeen = new ConcurrentHashMap<>();
    private final Map<String, Boolean> valuesSeenTwice = new ConcurrentHashMap<>();
    private final ConcurrentLinkedQueue<String> violations = new ConcurrentLinkedQueue<>();

    AmbientContextProbe(final String probeHeaderName, final long processingCostMs, final String storeName) {
        this.probeHeaderName = probeHeaderName;
        this.processingCostMs = processingCostMs;
        this.storeName = storeName;
    }

    void reset() {
        expectedTopic = null;
        expectedOffsetByValue.clear();
        inFlight.set(0);
        peakConcurrency.set(0);
        observations.set(0);
        valuesSeen.clear();
        valuesSeenTwice.clear();
        violations.clear();
    }

    void setExpectedTopic(final String topic) {
        this.expectedTopic = topic;
    }

    /**
     * The probe's only anchor for {@code context.offset()} that does not itself come from the record context.
     * Recorded by the test as each record's send is acknowledged.
     */
    void recordProducedOffset(final String value, final long offset) {
        expectedOffsetByValue.put(value, offset);
    }

    int getPeakConcurrency() {
        return peakConcurrency.get();
    }

    int getObservationCount() {
        return observations.get();
    }

    ConcurrentLinkedQueue<String> getViolations() {
        return violations;
    }

    Map<String, Boolean> getValuesSeenTwice() {
        return valuesSeenTwice;
    }

    /**
     * Kafka Streams' {@code ApiUtils.checkSupplier} rejects a supplier that hands back the same instance
     * twice, so state that must survive the run cannot live on the transformer.
     */
    ValueTransformerWithKey<String, String, String> newTransformer() {
        return new ProbeTransformer(this);
    }

    private String observe(final ProcessorContext context,
                           final String readOnlyKey,
                           final String value,
                           final KeyValueStore<String, String> store) {
        // Duplicate detection keyed on the value, which arrives as a method argument - deliberately not on
        // context.offset(), which is the very thing under suspicion.
        if (valuesSeen.put(value, Boolean.TRUE) != null) {
            valuesSeenTwice.put(value, Boolean.TRUE);
        }
        int concurrent = inFlight.incrementAndGet();
        peakConcurrency.accumulateAndGet(concurrent, Math::max);
        try {
            inspectAmbientContext(context, readOnlyKey, value);
            Thread.sleep(processingCostMs);
            // Read a second time, AFTER the window in which siblings were dispatched. The first read could
            // be right by luck on a slot that is later overwritten; the interesting failure is a context
            // that was correct on entry and belongs to someone else by the time it is used.
            inspectAmbientContext(context, readOnlyKey, value);
            if (store != null) {
                // Kafka's own ambient read, through Kafka's own store stack. The key is the record's value,
                // so the changelog record this produces is self-describing: its key says which record wrote
                // it and its timestamp says which record Kafka Streams THOUGHT was writing it.
                store.put(value, value);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            inFlight.decrementAndGet();
        }
        observations.incrementAndGet();
        return value;
    }

    private void inspectAmbientContext(final ProcessorContext context, final String key, final String value) {
        if (!expectedTopic.equals(context.topic())) {
            violations.add("topic for " + key + "/" + value + ": expected " + expectedTopic
                    + " but the ambient context said " + context.topic());
        }
        if (context.partition() != 0) {
            violations.add("partition for " + key + "/" + value + ": expected 0 but the ambient context said "
                    + context.partition());
        }

        Long expectedOffset = expectedOffsetByValue.get(value);
        if (expectedOffset == null) {
            violations.add("no produced offset recorded for value " + value + " - the probe saw a record "
                    + "the test never sent");
        } else if (context.offset() != expectedOffset) {
            violations.add("offset for " + key + "/" + value + ": expected " + expectedOffset
                    + " but the ambient context said " + context.offset());
        }

        // The expected timestamp is carried INSIDE the value, so this compares an ambient read against an
        // argument rather than against another ambient read.
        long expectedTimestamp = expectedTimestampOf(value);
        if (context.timestamp() != expectedTimestamp) {
            violations.add("timestamp for " + key + "/" + value + ": expected " + expectedTimestamp
                    + " but the ambient context said " + context.timestamp());
        }

        Header probeHeader = context.headers().lastHeader(probeHeaderName);
        if (probeHeader == null) {
            violations.add("no " + probeHeaderName + " header in the ambient context for " + key + "/" + value);
        } else {
            String carried = new String(probeHeader.value(), StandardCharsets.UTF_8);
            if (!value.equals(carried)) {
                violations.add("headers for " + key + "/" + value + ": the ambient context carried the "
                        + "header of " + carried);
            }
        }
    }

    /**
     * Every fixture record value ends in {@code -t<timestamp>}, put there by the generator precisely so that
     * an ambient timestamp read has something outside the record context to be checked against.
     */
    static long expectedTimestampOf(final String value) {
        return Long.parseLong(value.substring(value.lastIndexOf("-t") + 2));
    }

    /**
     * The per-instance half: it owns nothing but the {@code ProcessorContext} it was initialised with and,
     * where the topology has one, the state store it writes through.
     */
    private static final class ProbeTransformer implements ValueTransformerWithKey<String, String, String> {

        private final AmbientContextProbe probe;

        private ProcessorContext context;

        private KeyValueStore<String, String> store;

        private ProbeTransformer(final AmbientContextProbe probe) {
            this.probe = probe;
        }

        @Override
        @SuppressWarnings("unchecked")
        public void init(final ProcessorContext processorContext) {
            this.context = processorContext;
            if (probe.storeName != null) {
                this.store = (KeyValueStore<String, String>) processorContext.getStateStore(probe.storeName);
            }
        }

        @Override
        public String transform(final String readOnlyKey, final String value) {
            return probe.observe(context, readOnlyKey, value, store);
        }

        @Override
        public void close() {
            // nothing to release
        }
    }
}
