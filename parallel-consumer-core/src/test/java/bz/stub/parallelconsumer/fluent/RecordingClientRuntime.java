package bz.stub.parallelconsumer.fluent;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.internal.utils.LongPollingMockConsumer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

/**
 * A {@link ClientRuntime} that records whether it was asked for anything.
 * <p>
 * It is how the definition tests prove the two claims that cannot be seen from the outside: that a definition which
 * is written and never started constructs no client at all, and that a definition needing no producer never asks for
 * one (AE7, R4). Both are assertions about a call that did <em>not</em> happen, so they need a seam that counts.
 */
class RecordingClientRuntime implements ClientRuntime {

    int consumerCalls;

    int producerCalls;

    final java.util.List<DefinitionView> definitionsSeen = new java.util.ArrayList<>();

    private final Consumer<byte[], byte[]> consumer;

    private final Producer<byte[], byte[]> producer;

    /**
     * Whether this runtime hands back a producer instance, or declines and leaves Parallel Consumer to build one
     * from the definition's properties - the two answers {@link ClientRuntime#producer} distinguishes.
     */
    private final boolean suppliesAProducer;

    RecordingClientRuntime() {
        this(true);
    }

    /**
     * A runtime that counts the producer request and then declines it, as the real Kafka runtime does so that
     * producer recovery stays available (R1, astubbs#410).
     */
    static RecordingClientRuntime decliningToSupplyAProducer() {
        return new RecordingClientRuntime(false);
    }

    private RecordingClientRuntime(boolean suppliesAProducer) {
        this.consumer = new LongPollingMockConsumer<>(OffsetResetStrategy.EARLIEST);
        this.producer = new MockProducer<>(true, new ByteArraySerializer(), new ByteArraySerializer());
        this.suppliesAProducer = suppliesAProducer;
    }

    @Override
    public Consumer<byte[], byte[]> consumer(DefinitionView definition) {
        consumerCalls++;
        definitionsSeen.add(definition);
        return consumer;
    }

    @Override
    public java.util.Optional<Producer<byte[], byte[]>> producer(DefinitionView definition) {
        producerCalls++;
        definitionsSeen.add(definition);
        // Empty is the seam's way of saying "build your own, and keep producer recovery" (R1, astubbs#410).
        return suppliesAProducer ? java.util.Optional.of(producer) : java.util.Optional.empty();
    }

    boolean builtNothing() {
        return consumerCalls == 0 && producerCalls == 0;
    }
}
