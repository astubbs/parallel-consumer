package bz.stub.parallelconsumer.client.conformance;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.client.ClientOptions;
import bz.stub.parallelconsumer.client.Outcome;
import bz.stub.parallelconsumer.client.OutboundRecord;
import bz.stub.parallelconsumer.client.ProcessingOrder;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

/**
 * The spike of the language-proxy plan (U29, astubbs#242): one record all the way through - seeded,
 * dispatched, transported, handed to the user's {@code RecordProcessor}, its outcome applied, its response
 * produced, its offset committed - written once and run unmodified against both Java transports. Each
 * transport module's test tree supplies a subclass whose only content is a {@link SpikeFixture}; every
 * {@code @Test} here runs under both with <b>no transport-specific branch</b>, which is what makes the control
 * experiment structural (KTD20, KTD33): a behavioural difference between the two runs is the transport's bug,
 * never the suite's ambiguity.
 * <p>
 * <b>Scope is the thinnest possible vertical slice</b>: success outcomes, the produce payload (R6), the
 * committed offset, and KTD39's client-side dispatch queue exercised by seeding one more record than the
 * transport runs executors. Deliberately excluded, all additive later (U20 grows this suite to the full
 * conformance set): multiple executors, failure and retry, worker death, fencing behaviour, waves, drain.
 * <p>
 * Everything runs in the surefire lane: mock Kafka clients on the engine side, no broker, no Docker.
 *
 * @author Antony Stubbs
 * @see SpikeFixture
 */
@Timeout(120)
public abstract class SpikeConformanceTest {

    /** Mirrors the proxy harness's budget: sized for slow shared CI hardware; a healthy run converges fast. */
    protected static final Duration CONVERGENCE_BUDGET = Duration.ofSeconds(30);

    /** Far below the engine's 5s default, so committed-offset awaits converge fast. */
    protected static final Duration COMMIT_INTERVAL = Duration.ofMillis(100);

    /**
     * The transport under test, bound to an engine over the given topic with the given records seeded. The
     * suite owns everything else.
     */
    protected abstract SpikeFixture fixture(String topic, List<SpikeFixture.Seed> seeds);

    private ClientOptions options(String topic) {
        return ClientOptions.builder()
                .topics(Collections.singletonList(topic))
                .maxConcurrency(1)
                .ordering(ProcessingOrder.KEY)
                .commitInterval(COMMIT_INTERVAL)
                .build();
    }

    /**
     * The whole vertical path, one record: the processor runs exactly once, its response record is produced by
     * the engine (R6 - the only sanctioned route for worker output, KTD7), and the committed offset advances
     * past the record. The round trip is conceptual: the produce is asserted at the engine's producer, not
     * read back from a broker (that is U16's, broker-backed).
     */
    @Test
    void oneRecordTravelsTheFullPathAndItsResponseIsProduced() {
        var topic = "spike-one-record-full-path";
        var invocations = new CopyOnWriteArrayList<String>();
        try (var fixture = fixture(topic, Collections.singletonList(new SpikeFixture.Seed("lone-key", "hello")))) {

            fixture.start(options(topic), record -> {
                invocations.add(new String(record.value(), StandardCharsets.UTF_8));
                return Outcome.success(Collections.singletonList(OutboundRecord.of(topic + "-responses",
                        record.key(), "world".getBytes(StandardCharsets.UTF_8))));
            });

            awaitCommittedOffset(fixture, 1);

            assertWithMessage("the user function ran exactly once")
                    .that(invocations).containsExactly("hello");

            assertWithMessage("the success outcome's produce payload reached the engine's producer")
                    .that(fixture.produced()).hasSize(1);
            var produced = fixture.produced().get(0);
            assertThat(produced.topic()).isEqualTo(topic + "-responses");
            assertThat(produced.key()).isEqualTo("lone-key");
            assertThat(produced.value()).isEqualTo("world");

            awaitNoRecordsOutForProcessing(fixture);
        }
    }

    /** The produce payload is optional (R6): a bare success commits the offset and produces nothing. */
    @Test
    void aSuccessWithoutProducePayloadCommitsAndProducesNothing() {
        var topic = "spike-success-without-produce";
        var invocations = new CopyOnWriteArrayList<String>();
        try (var fixture = fixture(topic, Collections.singletonList(new SpikeFixture.Seed("lone-key", "hello")))) {

            fixture.start(options(topic), record -> {
                invocations.add(new String(record.value(), StandardCharsets.UTF_8));
                return Outcome.success();
            });

            awaitCommittedOffset(fixture, 1);

            assertThat(invocations).containsExactly("hello");
            assertWithMessage("a success with no produce payload produces nothing")
                    .that(fixture.produced()).isEmpty();

            awaitNoRecordsOutForProcessing(fixture);
        }
    }

    /**
     * KTD39's client-side dispatch queue, exercised from the first unit: one more record is seeded than the
     * transport runs executors - two records, one executor - so the second record traverses the queue between
     * dispatch and executor rather than going straight to a free thread. Hand-out is FIFO by arrival (KTD39
     * rule 3), so the processor sees the records in dispatch order, each exactly once, and both offsets
     * commit. The records share a key under KEY ordering, so the engine's own shard rule - not the suite's
     * luck - fixes the dispatch order the queue must preserve.
     */
    @Test
    void twoRecordsThroughOneExecutorAreHandedOutInDispatchOrder() {
        var topic = "spike-two-records-one-executor";
        var invocations = new CopyOnWriteArrayList<String>();
        try (var fixture = fixture(topic, Arrays.asList(
                new SpikeFixture.Seed("shared-key", "first"),
                new SpikeFixture.Seed("shared-key", "second")))) {

            fixture.start(options(topic), record -> {
                invocations.add(new String(record.value(), StandardCharsets.UTF_8));
                return Outcome.success();
            });

            awaitCommittedOffset(fixture, 2);

            assertWithMessage("hand-out is FIFO: dispatch order, each record exactly once (KTD39)")
                    .that(invocations).containsExactly("first", "second").inOrder();

            awaitNoRecordsOutForProcessing(fixture);
        }
    }

    /**
     * The same vertical path with the outcome arriving as a {@link java.util.concurrent.CompletionStage}
     * rather than a return - the form every wrapping language builds its client on, so it is held to the
     * identical contract rather than to a lighter one of its own.
     * <p>
     * <b>The stage is deliberately completed on a different thread</b> from the one the transport called the
     * processor on. That is the property under test: the transport must report a verdict it did not compute
     * itself, arriving after it moved on, with the record's token still matched to it. A stage completed
     * inline would pass while proving only what the synchronous form already proves.
     */
    @Test
    void anAsynchronousProcessorTravelsTheSamePathAndItsResponseIsProduced() {
        var topic = "spike-async-one-record-full-path";
        var invocations = new CopyOnWriteArrayList<String>();
        var answeringThreads = new CopyOnWriteArrayList<String>();
        try (var fixture = fixture(topic, Collections.singletonList(new SpikeFixture.Seed("lone-key", "hello")))) {

            fixture.startAsync(options(topic), record -> {
                var callingThread = Thread.currentThread().getName();
                var payload = new String(record.value(), StandardCharsets.UTF_8);
                return CompletableFuture.supplyAsync(() -> {
                    invocations.add(payload);
                    answeringThreads.add(callingThread + " -> " + Thread.currentThread().getName());
                    return Outcome.success(Collections.singletonList(OutboundRecord.of(topic + "-responses",
                            record.key(), "world".getBytes(StandardCharsets.UTF_8))));
                });
            });

            awaitCommittedOffset(fixture, 1);

            assertWithMessage("the user function ran exactly once")
                    .that(invocations).containsExactly("hello");

            assertWithMessage("the outcome was produced on a thread the transport does not own")
                    .that(answeringThreads).hasSize(1);
            var handoff = answeringThreads.get(0).split(" -> ");
            assertWithMessage("the answering thread differs from the calling one: " + answeringThreads.get(0))
                    .that(handoff[1]).isNotEqualTo(handoff[0]);

            assertWithMessage("the success outcome's produce payload reached the engine's producer")
                    .that(fixture.produced()).hasSize(1);
            var produced = fixture.produced().get(0);
            assertThat(produced.topic()).isEqualTo(topic + "-responses");
            assertThat(produced.key()).isEqualTo("lone-key");
            assertThat(produced.value()).isEqualTo("world");

            awaitNoRecordsOutForProcessing(fixture);
        }
    }

    /**
     * <b>Both transports answer "has the session ended?" the same way</b>, which is the one API they share and
     * therefore the one place a divergence would cost every mirroring language. The stage is silent while the
     * session is alive and completes normally once the client is closed - the clean end. Its exceptional half
     * (a stream that died under a live session) is transport-specific to drive and is proved in the gRPC
     * module's own {@code SessionEndTest}; there is no in-process equivalent to break.
     * <p>
     * Before this existed, {@code poll} had returned and nothing on the surface could tell an application its
     * session had stopped consuming - the parked P0 (astubbs#242).
     */
    @Test
    void theEndOfTheSessionIsObservableAndFiresOnlyWhenItEnds() {
        var topic = "spike-session-end-observable";
        java.util.concurrent.CompletionStage<Void> sessionEnd;
        try (var fixture = fixture(topic, Collections.singletonList(new SpikeFixture.Seed("lone-key", "hello")))) {

            fixture.start(options(topic), record -> Outcome.success());
            awaitCommittedOffset(fixture, 1);

            sessionEnd = fixture.sessionEnd();
            assertWithMessage("the session end does not fire while the session is still consuming")
                    .that(sessionEnd.toCompletableFuture().isDone()).isFalse();
        }

        // the fixture closed the client on the way out of the block
        Awaitility.await().atMost(CONVERGENCE_BUDGET).untilAsserted(() ->
                assertWithMessage("closing the client completes the session end")
                        .that(sessionEnd.toCompletableFuture().isDone()).isTrue());
        assertWithMessage("a session the application ended is not a failure")
                .that(sessionEnd.toCompletableFuture().isCompletedExceptionally()).isFalse();
    }

    private static void awaitCommittedOffset(SpikeFixture fixture, long expectedOffset) {
        Awaitility.await().atMost(CONVERGENCE_BUDGET).untilAsserted(() ->
                assertWithMessage("the committed offset advances past the completed records")
                        .that(fixture.committedOffset()).isEqualTo(OptionalLong.of(expectedOffset)));
    }

    /** The standing leak check: after any converged run, the engine counts nothing as out for processing. */
    private static void awaitNoRecordsOutForProcessing(SpikeFixture fixture) {
        Awaitility.await().atMost(CONVERGENCE_BUDGET).untilAsserted(() ->
                assertWithMessage("records out for processing returns to zero after the run")
                        .that(fixture.recordsOutForProcessing()).isEqualTo(0));
    }
}
