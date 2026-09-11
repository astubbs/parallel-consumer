package bz.stub.parallelconsumer.sandbox;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.ParallelConsumerOptions.CommitMode;
import bz.stub.parallelconsumer.ParallelConsumerOptions.ProcessingOrder;
import bz.stub.parallelconsumer.ParallelEoSStreamProcessor;
import bz.stub.parallelconsumer.internal.ExternalEngine;
import bz.stub.parallelconsumer.sandbox.demo.Order;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * The classic API in the sandbox (R33, AE26): an existing options-builder application runs broker-free, with its
 * start call changed and nothing else.
 * <p>
 * Note what this path does <b>not</b> exercise: a mock consumer holds records of the instance's own types, so the
 * generated orders go in as {@link Order} objects and the instance's deserialisers never run. That is a real
 * difference from the fluent path and from a broker, and it is stated here so that a green run is not read as
 * covering deserialisation.
 */
@Slf4j
@Timeout(60)
class ClassicSandboxTest {

    private static final int RECORDS = 20;

    /**
     * A serialiser for the produce side only - the consume side needs none, which is the point above.
     */
    private static final Serializer<Order> ORDER_WRITER =
            (topic, order) -> order == null ? null : order.toString().getBytes(StandardCharsets.UTF_8);

    @Test
    void aClassicDefinitionConsumesGeneratedRecordsOfItsOwnDeclaredTypes() {
        ConcurrentLinkedQueue<Order> seen = new ConcurrentLinkedQueue<>();

        Sandbox sandbox = Sandbox.builder()
                .perSecond(1000)
                .bound(Bound.afterRecords(RECORDS))
                .build();

        try (ClassicSandbox<String, Order> classic = sandbox.classic(String.class, Order.class, "orders")) {
            // SandboxFixtures.partitionOrdered is the ordinary options builder with one line changed - the
            // consumer is this sandbox's - which is the whole of what a classic application alters.
            ParallelEoSStreamProcessor<String, Order> pc = SandboxFixtures.startClassic(classic,
                    SandboxFixtures.partitionOrdered(classic),
                    context -> seen.add(context.getSingleRecord().value()));

            assertThat(classic.awaitBound(Duration.ofSeconds(30))).isTrue();
            pc.closeDrainFirst();
        }

        assertThat(seen).hasSize(RECORDS);
        Order first = seen.peek();
        assertWithMessage("the classic path hands over the generated objects themselves, filled the same way "
                + "the fluent path fills them").that(first.getEmail()).contains("@");
        assertThat(first.getCustomerName()).isNotEmpty();
    }

    /**
     * The transactional arm. {@code MockProducer} can act as either, and Parallel Consumer's producer wrapper
     * reads the commit mode to decide, so {@code transactionInitialized()} is the honest evidence that the
     * instance started transactional rather than merely being configured to.
     */
    @Test
    void aTransactionalClassicDefinitionStartsWithATransactionalMockProducer() {
        Sandbox sandbox = Sandbox.builder()
                .perSecond(1000)
                .bound(Bound.afterRecords(RECORDS))
                .build();

        try (ClassicSandbox<String, Order> classic = sandbox.classic(String.class, Order.class, "orders")) {
            var producer = classic.producer(new StringSerializer(), ORDER_WRITER);
            ParallelEoSStreamProcessor<String, Order> pc = SandboxFixtures.startClassic(classic,
                    ParallelConsumerOptions.<String, Order>builder()
                            .consumer(classic.consumer())
                            .producer(producer)
                            .commitMode(CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER)
                            .ordering(ProcessingOrder.PARTITION)
                            .build(),
                    context -> log.debug("{}", context.getSingleRecord().value()));

            // Read after the instance was constructed, which is when the producer wrapper decides: the flag is a
            // latch, so asking once the run has started says the same thing as asking a moment earlier.
            assertWithMessage("the producer wrapper initialises transactions when the commit mode asks for them")
                    .that(producer.transactionInitialized()).isTrue();

            assertThat(classic.awaitBound(Duration.ofSeconds(30))).isTrue();
            pc.closeDrainFirst();

            List<?> commits = producer.consumerGroupOffsetsHistory();
            assertWithMessage("under the transactional commit mode the offsets are committed through the "
                    + "producer's transaction, not through the consumer").that(commits).isNotEmpty();
        }
    }

    /**
     * The other half of the transactional claim: an external engine - Vert.x, Reactor, Mutiny - refuses the
     * transactional commit mode at construction, so the sandbox cannot be used to run a combination a broker
     * would reject.
     *
     * <h2>What this covers, and what it does not</h2>
     * <b>It gives the sandbox no coverage, and the name no longer pretends otherwise.</b> The refusal lives in
     * core's {@code ExternalEngine} constructor and fires on the commit mode alone, before any client is read -
     * so {@code classic.consumer()} and {@code classic.producer(...)} below are inert, and this test would pass
     * with any clients at all. What it does cover is that refusal itself, which <b>nothing else in the tree
     * tests</b>; it is kept here for that reason rather than deleted, and the gap is core's to own properly.
     * <p>
     * <b>Through a minimal subclass rather than through the Vert.x module</b>, deliberately: the refusal is not in
     * any of the three engines, so pulling Vert.x and Netty onto this module's test classpath would reach core's
     * code by the heaviest possible route.
     */
    @Test
    void anExternalEngineRefusesTheTransactionalCommitModeBeforeItReadsAnyClient() {
        Sandbox sandbox = Sandbox.builder().build();
        try (ClassicSandbox<String, Order> classic = sandbox.classic(String.class, Order.class, "orders")) {
            ParallelConsumerOptions<String, Order> transactional = ParallelConsumerOptions
                    .<String, Order>builder()
                    .consumer(classic.consumer())
                    .producer(classic.producer(new StringSerializer(), ORDER_WRITER))
                    .commitMode(CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER)
                    .build();

            IllegalStateException refusal = assertThrows(IllegalStateException.class,
                    () -> new StubExternalEngine<>(transactional));

            assertThat(refusal).hasMessageThat().contains("do not support transactions");
        }
    }

    /**
     * The smallest thing that is an external engine: one abstract method, answered the way a synchronous one
     * would answer it. It exists only to reach {@code ExternalEngine}'s constructor.
     */
    private static final class StubExternalEngine<K, V> extends ExternalEngine<K, V> {

        private StubExternalEngine(ParallelConsumerOptions<K, V> options) {
            super(options);
        }

        @Override
        protected boolean isAsyncFutureWork(List<?> resultsFromUserFunction) {
            return false;
        }
    }
}
