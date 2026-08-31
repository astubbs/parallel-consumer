package bz.stub.parallelconsumer.client.direct;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.client.ClientOptions;
import bz.stub.parallelconsumer.client.InboundRecord;
import bz.stub.parallelconsumer.client.OutboundRecord;
import bz.stub.parallelconsumer.client.Outcome;
import bz.stub.parallelconsumer.internal.utils.LongPollingMockConsumer;
import bz.stub.parallelconsumer.model.CommitHistory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * The direct transport carrying a record end to end, with no wire under it: core's mock Kafka clients in,
 * the user's function run, the outcome applied, the offset advanced, the produce payload on the producer.
 * <p>
 * <b>Why the offset and the produced record are the assertions, rather than "the processor was called".</b>
 * The processor being invoked proves the hand-out; it proves nothing about whether the {@link Outcome} was
 * applied, and a transport that ran the function and dropped the verdict is exactly the failure that looks
 * identical from the processor's side. The committed offset is core's own statement that it accepted the
 * success, and the producer's history is core's statement that it produced what the outcome asked for.
 * <p>
 * <b>No broker and no Docker.</b> It is core's mock-clients pattern, which is what keeps the Java client's
 * primary evidence in the ordinary surefire lane rather than behind a profile.
 * <p>
 * The cross-language conformance suite drives this transport as well, under its own name, and arrives with the
 * conformance rung stacked above this one. This class is the transport's own suite: what has to hold for
 * <em>this</em> module regardless of what the shared scenarios later say.
 *
 * @author Antony Stubbs
 */
@Timeout(value = 120, unit = TimeUnit.SECONDS)
class DirectParallelConsumerClientTest {

    private static final String TOPIC = "orders";
    private static final TopicPartition PARTITION = new TopicPartition(TOPIC, 0);
    private static final Duration OBSERVE_BUDGET = Duration.ofSeconds(30);

    private final LongPollingMockConsumer<byte[], byte[]> consumer =
            new LongPollingMockConsumer<>(OffsetResetStrategy.EARLIEST);
    private final MockProducer<byte[], byte[]> producer =
            new MockProducer<>(true, new ByteArraySerializer(), new ByteArraySerializer());

    /**
     * Every client a test builds, closed once the test ends. A list rather than a single nullable field so
     * there is no "not started yet" state to reason about, and so a test that closes its own client - which
     * is what {@link #theSessionEndCompletesWhenTheClientIsClosedAndNotBefore} is about - needs no flag to
     * stop this closing it twice. {@code close()} is idempotent, which the API promises and this relies on.
     */
    private final List<DirectParallelConsumerClient> clients = new ArrayList<>();

    @AfterEach
    void closeClients() {
        clients.forEach(DirectParallelConsumerClient::close);
    }

    @Test
    void aSuccessfulOutcomeAdvancesTheOffsetAndProducesWhatItAskedFor() {
        var seen = new ConcurrentLinkedQueue<InboundRecord>();

        var client = start();
        client.poll(record -> {
            seen.add(record);
            // a null key is the API's keyless record, not a missing value - see OutboundRecord#of
            return Outcome.success(Collections.singletonList(
                    OutboundRecord.of("responses", null, "handled".getBytes(StandardCharsets.UTF_8))));
        });
        seed(client, "a-key", "a-value");

        Awaitility.await("the record reaches the processor as the API's own type")
                .atMost(OBSERVE_BUDGET).untilAsserted(() -> assertThat(seen).hasSize(1));
        var delivered = seen.peek();
        assertThat(delivered.topic()).isEqualTo(TOPIC);
        assertThat(delivered.offset()).isEqualTo(0);
        assertThat(new String(delivered.value(), StandardCharsets.UTF_8)).isEqualTo("a-value");
        assertWithMessage("first delivery, so no failure state and attempt 1")
                .that(delivered.attempt()).isEqualTo(1);
        assertThat(delivered.lastFailureReason()).isEmpty();

        Awaitility.await("core produced the outcome's payload with its own producer, before the offset moved")
                .atMost(OBSERVE_BUDGET).untilAsserted(() -> {
                    assertThat(producer.history()).hasSize(1);
                    assertThat(producer.history().get(0).topic()).isEqualTo("responses");
                });
        Awaitility.await("the success is applied: core commits past the record")
                .atMost(OBSERVE_BUDGET).untilAsserted(() -> assertThat(highestCommit()).isEqualTo(1L));
    }

    /**
     * A failure {@link Outcome} is core's retry path, not an exception the client swallowed: the record comes
     * back, and it comes back carrying the reason the previous attempt gave - which is the product data
     * {@link InboundRecord#lastFailureReason()} promises and the one place the two transports must agree.
     */
    @Test
    void aFailureOutcomeIsRedeliveredCarryingItsReason() {
        var attempts = new ConcurrentLinkedQueue<InboundRecord>();

        var client = start(ClientOptions.builder()
                .topics(Collections.singletonList(TOPIC))
                .maxConcurrency(1)
                .defaultMessageRetryDelay(Duration.ofMillis(1)));
        client.poll(record -> {
            attempts.add(record);
            return attempts.size() == 1 ? Outcome.failure("the database said no") : Outcome.success();
        });
        seed(client, "a-key", "a-value");

        Awaitility.await("the failed record is handed back for another attempt")
                .atMost(OBSERVE_BUDGET).untilAsserted(() -> assertThat(attempts).hasSize(2));
        // the module compiles to Java 8 bytecode, so no List.copyOf here
        var redelivery = new ArrayList<>(attempts).get(1);
        assertWithMessage("second delivery of the same record").that(redelivery.attempt()).isEqualTo(2);
        assertWithMessage("the reason travels with the redelivery, unwrapped from core's user-function wrapper")
                .that(redelivery.lastFailureReason()).hasValue("the database said no");
        assertThat(redelivery.lastFailureAt()).isPresent();

        Awaitility.await("only the eventual success advances the offset")
                .atMost(OBSERVE_BUDGET).untilAsserted(() -> assertThat(highestCommit()).isEqualTo(1L));
    }

    /**
     * {@code sessionEnd()} is the surface an application watches to learn consumption stopped, and the two
     * transports must not disagree about it. In-process the clean end is {@code close()}, which is what this
     * pins: the stage was not already complete while consuming, and it completes normally once the session is
     * over.
     */
    @Test
    void theSessionEndCompletesWhenTheClientIsClosedAndNotBefore() {
        var running = start();
        running.poll(record -> Outcome.success());
        seed(running, "a-key", "a-value");

        CompletableFuture<Void> end = running.sessionEnd().toCompletableFuture();
        Awaitility.await("consumption is under way")
                .atMost(OBSERVE_BUDGET).untilAsserted(() -> assertThat(highestCommit()).isEqualTo(1L));
        assertWithMessage("a session that is still consuming has not ended").that(end.isDone()).isFalse();

        running.close();

        assertThat(end.isDone()).isTrue();
        assertWithMessage("an ordinary close is a clean end, not a failure").that(end.isCompletedExceptionally())
                .isFalse();
    }

    /**
     * The API says poll may be called at most once per client, and the two transports enforce it identically -
     * a second call on a running session would start a second engine over the same consumer.
     */
    @Test
    void pollMayBeCalledAtMostOnce() {
        var running = start();
        running.poll(record -> Outcome.success());

        var thrown = assertThrows(IllegalStateException.class, () -> running.poll(record -> Outcome.success()));
        assertThat(thrown).hasMessageThat().contains("at most once");
    }

    private DirectParallelConsumerClient start() {
        return start(ClientOptions.builder().topics(Collections.singletonList(TOPIC)).maxConcurrency(1));
    }

    private DirectParallelConsumerClient start(ClientOptions.Builder options) {
        var built = DirectParallelConsumerClient.builder()
                .options(options.build())
                .consumer(consumer)
                .producer(producer)
                .build();
        clients.add(built);
        return built;
    }

    /**
     * The manual rebalance dance a {@code MockConsumer} needs: {@code rebalance} assigns the partition but
     * fires no listener, so the client - a rebalance listener by delegation to core - is told separately, and
     * records may only be added once the partition is assigned.
     */
    private void seed(DirectParallelConsumerClient client, String key, String value) {
        consumer.subscribeWithRebalanceAndAssignment(Collections.singletonList(TOPIC), 1);
        client.onPartitionsAssigned(Collections.singletonList(PARTITION));
        consumer.addRecord(new ConsumerRecord<>(TOPIC, PARTITION.partition(), 0,
                key.getBytes(StandardCharsets.UTF_8), value.getBytes(StandardCharsets.UTF_8)));
    }

    /** The highest offset core has committed for the partition, or -1 when it has committed nothing. */
    private long highestCommit() {
        return CommitHistory.forPartition(consumer.getCommitHistoryInt(), PARTITION).highestCommit().orElse(-1L);
    }
}
