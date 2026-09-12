package bz.stub.parallelconsumer.sandbox;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.fluent.ParallelConsumerInstance;
import bz.stub.parallelconsumer.fluent.ParallelConsumerDefinition;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * The bound: a run that ends itself, and ends with every record it generated accounted for.
 * <p>
 * <b>The accounting is the part worth testing.</b> A bound that merely stopped would leave the last records
 * generated but never dispatched and their offsets never committed, and the state a test read afterwards would be
 * the middle of the run rather than its end - which is the difference between a broker-free test kit and a race.
 * The evidence is that <em>every</em> generated offset has committed by the time the bound has finished, with
 * nothing awaiting or polling in between. Nothing here parks - the route always succeeds - so a commit is the
 * whole of what the wait can count; {@link ParkedRunBoundTest} is the other half.
 * <p>
 * <b>Draining is not what provides that</b>, which is the whole of the defect this test used to carry
 * (astubbs#504): a drain-first close transitions to closing once nothing is awaiting selection, while the worker
 * pool may still hold queued tasks, and the close then clears that queue - so on a loaded runner this assertion
 * read 40 where it expected 50. The bound now waits until the instance has accounted for every published record
 * before it closes ({@link SandboxConsumer#awaitEveryPublishedRecordCommitted()}), and this is the assertion that
 * guards it.
 */
@Timeout(60)
class RecordCountBoundTest {

    private static final long RECORDS = 50;

    @Test
    void aRecordCountBoundEndsTheRunAndDrainsBeforeClosing() {
        ParallelConsumerDefinition definition =
                SandboxFixtures.succeedingStringRoute(SandboxFixtures.definition(), "orders");

        Sandbox sandbox = Sandbox.builder()
                .perSecond(2000)
                .bound(Bound.afterRecords(RECORDS))
                .feeding("orders", SandboxFixtures.countedValues("orders"))
                .build();

        ParallelConsumerInstance instance = definition.start(sandbox);
        // The bound does all three things on the driver's own thread - stop publishing, wait for every
        // published record's offset to commit, close the instance - and awaitBound covers all three, so this
        // returning true is already the end of the run.
        assertThat(sandbox.awaitBound(Duration.ofSeconds(30))).isTrue();
        // Idempotent, and here to say that nothing more is pending rather than to make anything happen.
        instance.awaitShutdown();

        assertWithMessage("a count bound counts records, not ticks, so it must stop exactly on the number")
                .that(sandbox.generatedRecords()).isEqualTo(RECORDS);
        assertThat(sandbox.consumer().publishedCounts().get(new TopicPartition("orders", 0))).isEqualTo(RECORDS);

        // No awaiting, no polling here: whatever had not committed by the time the bound finished never will.
        // Read from the raw commit history rather than through SandboxConsumer#highestCommittedOffsets, so this
        // is evidence independent of the ledger the bound's own wait consults.
        assertWithMessage("the bound waits for the offsets, so every record generated before it has committed by "
                + "the time the bound has finished")
                .that(SandboxFixtures.highestCommittedOffset(sandbox, new TopicPartition("orders", 0)))
                .isEqualTo(RECORDS);

        instance.close();
    }

    @Test
    void anUnboundedSandboxSaysSoRatherThanWaitingForever() {
        ParallelConsumerDefinition definition =
                SandboxFixtures.succeedingStringRoute(SandboxFixtures.definition(), "orders");

        Sandbox sandbox = Sandbox.builder()
                .perSecond(100)
                .feeding("orders", SandboxFixtures.countedValues("orders"))
                .build();
        try (ParallelConsumerInstance instance = definition.start(sandbox)) {
            IllegalStateException refusal = assertThrows(IllegalStateException.class,
                    () -> sandbox.awaitBound(Duration.ofSeconds(1)));
            assertThat(refusal).hasMessageThat().contains("unbounded");
            instance.close();
        }
    }
}
