package bz.stub.parallelconsumer.sandbox;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumer;
import bz.stub.parallelconsumer.fluent.Outcome;
import bz.stub.parallelconsumer.fluent.ParallelConsumerDefinition;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;
import java.util.Properties;

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
     * Adds a JSON route whose function always succeeds, which is what a test that is about the plumbing - the
     * generator, the bound, the wait - wants its records to do.
     */
    static <V> ParallelConsumerDefinition succeedingJsonRoute(ParallelConsumerDefinition definition,
                                                              String topic,
                                                              Class<V> valueType) {
        definition.json(topic, valueType).process(context -> Outcome.succeeded());
        return definition;
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
