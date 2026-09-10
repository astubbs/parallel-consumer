package bz.stub.parallelconsumer.sandbox;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumer;
import bz.stub.parallelconsumer.fluent.ConsumerHandle;
import bz.stub.parallelconsumer.fluent.Outcome;
import bz.stub.parallelconsumer.fluent.ParallelConsumerDefinition;
import bz.stub.parallelconsumer.sandbox.demo.Order;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.Map;
import java.util.Properties;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * The bound: a run that ends itself, and ends by draining rather than by stopping.
 * <p>
 * <b>Draining is the part worth testing.</b> A close that merely stops would leave the last records generated but
 * never dispatched and their offsets never committed, and the state a test read afterwards would be the middle of
 * the run rather than its end - which is the difference between a broker-free test kit and a race. The evidence
 * is that <em>every</em> generated offset has committed by the time the handle's close returns, with nothing
 * awaiting or polling in between.
 */
@Timeout(60)
class RecordCountBoundTest {

    private static final long RECORDS = 50;

    @Test
    void aRecordCountBoundEndsTheRunAndDrainsBeforeClosing() {
        ParallelConsumerDefinition definition = ParallelConsumer.connect(new Properties());
        definition.json("orders", Order.class)
                .process(context -> Outcome.succeeded());

        Sandbox sandbox = Sandbox.builder()
                .perSecond(2000)
                .bound(Bound.afterRecords(RECORDS))
                .build();

        ConsumerHandle handle = definition.start(sandbox);
        assertThat(sandbox.awaitBound(Duration.ofSeconds(30))).isTrue();
        // The bound closes the handle itself, on the generator's thread; this waits for that close to finish
        // rather than performing it.
        handle.awaitShutdown();

        assertWithMessage("a count bound counts records, not ticks, so it must stop exactly on the number")
                .that(sandbox.generatedRecords()).isEqualTo(RECORDS);
        assertThat(sandbox.consumer().publishedCounts().get(new TopicPartition("orders", 0))).isEqualTo(RECORDS);

        // No awaiting, no polling: whatever had not committed by the time close() returned never will.
        assertWithMessage("the close drains first, so every record generated before the bound has committed by "
                + "the time it returns")
                .that(highestCommittedOffset(sandbox, new TopicPartition("orders", 0))).isEqualTo(RECORDS);

        handle.close();
    }

    @Test
    void anUnboundedSandboxSaysSoRatherThanWaitingForever() {
        ParallelConsumerDefinition definition = ParallelConsumer.connect(new Properties());
        definition.json("orders", Order.class)
                .process(context -> Outcome.succeeded());

        Sandbox sandbox = Sandbox.builder().perSecond(100).build();
        try (ConsumerHandle handle = definition.start(sandbox)) {
            IllegalStateException refusal = assertThrows(IllegalStateException.class,
                    () -> sandbox.awaitBound(Duration.ofSeconds(1)));
            assertThat(refusal).hasMessageThat().contains("unbounded");
            handle.close();
        }
    }

    private static long highestCommittedOffset(Sandbox sandbox, TopicPartition partition) {
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
