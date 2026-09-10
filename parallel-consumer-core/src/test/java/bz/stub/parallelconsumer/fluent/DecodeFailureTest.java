package bz.stub.parallelconsumer.fluent;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.truth.Truth.assertThat;

/**
 * A payload a route cannot read never ends the poll thread, and which of the three decode results it was decides
 * what happens to it (R12, F4, AE6's decode half).
 * <p>
 * <b>Transient by default</b>, because a stock deserialiser that throws cannot tell a corrupt payload from a
 * registry outage - so the default is the one that keeps trying. A route that can tell says so with
 * {@link Formats#classifyDecodeFailures}, and a permanent failure parks at once <em>without</em> spending an
 * attempt: there is nothing to try again.
 */
@Timeout(60)
class DecodeFailureTest {

    private static final String POISON = "poison";

    private final RecordingClientRuntime runtime = new RecordingClientRuntime();

    private ConsumerHandle handle;

    @AfterEach
    void closeTheInstance() {
        if (handle != null) {
            RecordingClientRuntime.closeWithoutDraining(handle);
        }
    }

    private static Properties props() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "decode-failure-test");
        return properties;
    }

    /**
     * A deserialiser that rejects one payload and reads every other one, so a test can put a poison record beside
     * good ones rather than in a topic of its own.
     */
    private static Deserializer<String> rejecting() {
        return (topic, data) -> {
            String value = new String(data, StandardCharsets.UTF_8);
            if (value.equals(POISON)) {
                throw new SerializationException("this payload cannot be read");
            }
            return value;
        };
    }

    /**
     * AE6's transient half: the record follows the ordinary retry path and parks once its attempts run out, while
     * the other route keeps working and the poll thread stays alive throughout - which is the whole point, since
     * on the classic API a deserialisation failure ends that thread.
     */
    @Test
    void aTransientDecodeFailureRetriesAndThenParksWhileTheOtherRouteIsUnaffected() {
        var auditSeen = new AtomicInteger();
        var pc = ParallelConsumer.connect(props());
        pc.topic("orders")
                .consumed(Consumed.with(Serdes.String(), rejecting()))
                .retryLimit(2)
                .retryDelay(Duration.ofMillis(10))
                .process(context -> Outcome.succeeded());
        pc.string("audit").process(context -> {
            auditSeen.incrementAndGet();
            return Outcome.succeeded();
        });

        handle = runtime.startAndAssign(pc, 1);
        runtime.publish("orders", 0, 0, "key-0", POISON);
        runtime.publish("orders", 0, 1, "key-1", "a readable order");

        RouteDispatcher dispatcher = pc.dispatcher();
        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(dispatcher.parkedCount()).isEqualTo(1));

        // A limit of two counts the attempts after the first, so three decode attempts happened and no fourth.
        assertThat(dispatcher.ledger().attempts("orders", 0, 0)).isEqualTo(3);
        // The readable record on the same route and partition still succeeded.
        assertThat(dispatcher.succeededCount()).isAtLeast(1L);

        // The poll thread is alive: records published after the poison one still arrive, on the other route.
        runtime.publish("audit", 0, 0, "key-0", "audited");
        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(auditSeen.get()).isEqualTo(1));
        assertThat(handle.failureCause().isPresent()).isFalse();
    }

    /**
     * AE6's permanent half: parked at once, with <b>no attempt spent</b> - so the facade's count is zero while the
     * engine has counted the hand-back, which is the one place in this milestone where the two disagree.
     */
    @Test
    void aPermanentDecodeFailureIsParkedAtOnceWithoutSpendingAnAttempt() {
        var ran = new AtomicInteger();
        var pc = ParallelConsumer.connect(props());
        pc.topic("orders")
                .consumed(Consumed.with(Serdes.String(),
                        Formats.classifyDecodeFailures(Serdes.serdeFrom(Serdes.String().serializer(), rejecting()),
                                failure -> Decode.permanentFailure(failure))))
                .retryLimit(5)
                .retryDelay(Duration.ofMillis(10))
                .process(context -> {
                    ran.incrementAndGet();
                    return Outcome.succeeded();
                });

        handle = runtime.startAndAssign(pc, 1);
        runtime.publish("orders", 0, 0, "key-0", POISON);

        RouteDispatcher dispatcher = pc.dispatcher();
        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(dispatcher.parkedCount()).isEqualTo(1));

        assertThat(dispatcher.ledger().attempts("orders", 0, 0)).isEqualTo(0);
        assertThat(ran.get()).isEqualTo(0);

        // The engine counted the hand-back as a failure; the facade did not count it as an attempt. Two numbers,
        // on purpose - KTD4's cross-check, seen at the one moment they meet.
        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(dispatcher.attemptCountsAtLastHandBack("orders", 0, 0)).isNotNull());
        int[] counts = dispatcher.attemptCountsAtLastHandBack("orders", 0, 0);
        assertThat(counts[0]).isEqualTo(0);
        assertThat(counts[1]).isEqualTo(1);

        // Parked means parked: no further attempt arrives however long the instance runs.
        Awaitility.await().pollDelay(Duration.ofMillis(300)).atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> assertThat(ran.get()).isEqualTo(0));
        assertThat(dispatcher.parkedCount()).isEqualTo(1);
    }

    /**
     * The same classifier, answering the other way: a failure it calls transient behaves exactly as an ordinary
     * failed attempt, so the distinction really is the classifier's and not the wrapper's.
     */
    @Test
    void aFailureTheClassifierCallsTransientSpendsAttemptsAsUsual() {
        var pc = ParallelConsumer.connect(props());
        pc.topic("orders")
                .consumed(Consumed.with(Serdes.String(),
                        Formats.classifyDecodeFailures(Serdes.serdeFrom(Serdes.String().serializer(), rejecting()),
                                failure -> Decode.transientFailure(failure))))
                .retryLimit(1)
                .retryDelay(Duration.ofMillis(10))
                .process(context -> Outcome.succeeded());

        handle = runtime.startAndAssign(pc, 1);
        runtime.publish("orders", 0, 0, "key-0", POISON);

        RouteDispatcher dispatcher = pc.dispatcher();
        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(dispatcher.parkedCount()).isEqualTo(1));

        assertThat(dispatcher.ledger().attempts("orders", 0, 0)).isEqualTo(2);
    }
}
