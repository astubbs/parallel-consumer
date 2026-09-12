package bz.stub.parallelconsumer.sandbox;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumer;
import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.ParallelConsumerOptions.ProcessingOrder;
import bz.stub.parallelconsumer.ParallelEoSStreamProcessor;
import bz.stub.parallelconsumer.PollContext;
import bz.stub.parallelconsumer.fluent.Outcome;
import bz.stub.parallelconsumer.fluent.ParallelConsumerDefinition;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.function.Consumer;
import java.util.function.LongFunction;

/**
 * The setup every sandbox run-level test repeats: an empty definition, a route that always succeeds, and the
 * committed offsets read straight out of the mock consumer's commit history.
 * <p>
 * It exists because the duplicate-code check found the first three lines of every such test to be the same three
 * lines, and because {@link #highestCommittedOffset} was written twice with the same walk. Reading the commit
 * history rather than {@link SandboxConsumer#highestCommittedOffsets()} is deliberate and is the reason this
 * helper does not simply delegate: it keeps a test's evidence independent of the ledger the bound's own wait
 * consults.
 */
final class SandboxFixtures {

    private SandboxFixtures() {
    }

    /**
     * An empty definition with no broker properties - the sandbox replaces every client, so nothing here is read.
     */
    static ParallelConsumerDefinition definition() {
        return ParallelConsumer.connect(new Properties());
    }

    /**
     * Adds a String route whose function always succeeds, which is what a test that is about the plumbing - the
     * driver, the bound, the wait - wants its records to do.
     * <p>
     * A String route rather than a typed one because these tests are not about what is in a record: this module's
     * driver is fed by functions the caller writes, and {@link #countedValues(String)} is the smallest honest one.
     */
    static ParallelConsumerDefinition succeedingStringRoute(ParallelConsumerDefinition definition, String topic) {
        definition.string(topic).process(context -> Outcome.succeeded());
        return definition;
    }

    /**
     * The value function a test uses when it does not care what is in the record, only that there is one: the
     * topic and the record's index, which is enough to tell two records apart in a failure message.
     */
    static LongFunction<Object> countedValues(String topic) {
        return index -> topic + "-" + index;
    }

    /**
     * The options every classic sandbox test that is not about the options themselves builds: this sandbox's
     * consumer, and partition ordering because a classic test asserting on what arrived wants the order a broker
     * would have given it.
     */
    static <K, V> ParallelConsumerOptions<K, V> partitionOrdered(ClassicSandbox<K, V> classic) {
        return ParallelConsumerOptions.<K, V>builder()
                .consumer(classic.consumer())
                .ordering(ProcessingOrder.PARTITION)
                .build();
    }

    /**
     * Builds a classic instance over a sandbox and starts driving into it - the four lines every classic test
     * repeated, which the duplicate-code check flagged as an eighteen-line clone.
     * <p>
     * The options are the caller's, because what a classic test varies is exactly the options; the poll function
     * is the caller's, because that is where the test's evidence is collected; the key and value functions are
     * the caller's, because this module's driver has no opinion about what a record contains; and the instance is
     * returned rather than closed here, because a bound closes it and a test that reaches its bound still calls
     * {@code closeDrainFirst()} afterwards to cover the run that did not.
     */
    static <K, V> ParallelEoSStreamProcessor<K, V> startClassic(ClassicSandbox<K, V> classic,
                                                                ParallelConsumerOptions<K, V> options,
                                                                Consumer<PollContext<K, V>> onPoll,
                                                                LongFunction<K> keys,
                                                                LongFunction<V> values) {
        ParallelEoSStreamProcessor<K, V> pc = new ParallelEoSStreamProcessor<>(options);
        pc.subscribe(classic.topics());
        pc.poll(onPoll);
        classic.startDriving(pc, keys, values);
        return pc;
    }

    /**
     * The keys a classic test uses when it is not about keys: a pool of strings, the same shape the fluent path
     * gives a topic that declared none.
     */
    static LongFunction<String> pooledKeys(int cardinality) {
        return index -> "key-" + Math.floorMod(index, cardinality);
    }

    /**
     * A sandbox consumer with one topic, already subscribed and assigned, for a test that drives the consumer
     * directly rather than through a run.
     * <p>
     * {@code SeededOffsetsTest} deliberately does not use this and spells the same two calls out inline: the
     * order of seeding and assignment is that file's subject, so a reader has to see it.
     */
    static <K, V> SandboxConsumer<K, V> assignedConsumer(String topic, int partitions) {
        SandboxConsumer<K, V> consumer = new SandboxConsumer<>(Collections.singletonList(topic), partitions);
        // MockConsumer#rebalance is the DYNAMIC assignment path and refuses to run before something has
        // subscribed - which in a real run is the engine, in ClientRuntime#started.
        consumer.subscribe(Collections.singletonList(topic));
        consumer.assignAfterSeeding();
        return consumer;
    }

    /**
     * The highest offset committed for one partition, read from the raw commit history: later commits supersede
     * earlier ones for the same partition, but the histories are per-call, so this walks them and keeps the
     * highest.
     */
    static long highestCommittedOffset(Sandbox sandbox, TopicPartition partition) {
        long highest = 0;
        for (Map<TopicPartition, OffsetAndMetadata> commit : sandbox.consumer().getCommitHistoryInt()) {
            OffsetAndMetadata offset = commit.get(partition);
            if (offset != null) {
                highest = Math.max(highest, offset.offset());
            }
        }
        return highest;
    }
}
