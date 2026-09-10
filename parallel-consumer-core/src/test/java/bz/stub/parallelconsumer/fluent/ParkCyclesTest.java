package bz.stub.parallelconsumer.fluent;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.FakeRuntimeException;
import bz.stub.parallelconsumer.ParallelConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
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

import static bz.stub.parallelconsumer.fluent.AfterRetries.park;
import static com.google.common.truth.Truth.assertThat;

/**
 * Park cycles: scheduled retry (astubbs#234) delivered as a park delay rather than as a second mechanism (R27).
 * <p>
 * A policy that declares a delay and a number of cycles gives an exhausted record one more attempt per cycle,
 * spaced by that delay, and parks it for good after the last one. In between it is <b>not parked</b> - it has an
 * attempt coming, so it is not in the parked view and nothing observes it; what it does hold is a worker's worth of
 * nothing, because a record waiting on a retry delay holds no thread either way.
 * <p>
 * The two paths that never take a cycle are the two where another attempt is known to be pointless: a payload that
 * can never be decoded (R12), and a record the function itself declared hopeless (R8).
 */
@Timeout(120)
class ParkCyclesTest {

    private static final String TOPIC = "orders";

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
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "park-cycles-test");
        return properties;
    }

    /**
     * The shape R27 describes: after each delay the record is attempted once more, and after the declared cycles it
     * parks without delay. With a limit of zero the first failure exhausts it, so the two cycles are two extra
     * attempts and nothing else.
     */
    @Test
    void aDelayWithTwoCyclesReAttemptsTwiceThenParksWithoutDelay() {
        var attempts = new AtomicInteger();
        var observedCycles = new AtomicInteger(-1);
        var pc = ParallelConsumer.connect(props());
        pc.string(TOPIC)
                .retryLimit(0)
                .retryDelay(Duration.ofMillis(10))
                .afterRetries(park().thenRetryAfter(Duration.ofMillis(200)).forCycles(2))
                .onParked((record, failure, count) -> observedCycles.set(count))
                .process(context -> {
                    attempts.incrementAndGet();
                    throw new FakeRuntimeException("this record never succeeds");
                });

        handle = runtime.startAndAssign(pc, 1);
        runtime.publish(TOPIC, 0, 0, "key-0", "an order");

        RouteDispatcher dispatcher = pc.dispatcher();
        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(dispatcher.parkedForRoute(TOPIC)).hasSize(1));

        // One attempt to exhaust the limit, then one per cycle. The fourth never happens.
        assertThat(attempts.get()).isEqualTo(3);
        ParkedRecord parked = dispatcher.parkedForRoute(TOPIC).get(0);
        assertThat(parked.cycles()).isEqualTo(2);
        assertThat(parked.attempts()).isEqualTo(3);
        // The observer heard about it once, and only at the end - the cycles were retries, not parks.
        assertThat(observedCycles.get()).isEqualTo(3);
        assertThat(dispatcher.parkedCount()).isEqualTo(1);

        // Parked without delay means parked: no further attempt, however many cycle delays go by.
        Awaitility.await().pollDelay(Duration.ofSeconds(1)).atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(attempts.get()).isEqualTo(3));
    }

    /**
     * While the cycles are running the record is not parked: it is between attempts, which is what makes the parked
     * view an honest list of what an operator can act on.
     */
    @Test
    void aRecordBetweenCyclesIsNotInTheParkedView() {
        var attempts = new AtomicInteger();
        var pc = ParallelConsumer.connect(props());
        pc.string(TOPIC)
                .retryLimit(0)
                .retryDelay(Duration.ofMillis(10))
                .afterRetries(park().thenRetryAfter(Duration.ofSeconds(3)).forCycles(1))
                .process(context -> {
                    attempts.incrementAndGet();
                    throw new FakeRuntimeException("this record never succeeds");
                });

        handle = runtime.startAndAssign(pc, 1);
        runtime.publish(TOPIC, 0, 0, "key-0", "an order");

        RouteDispatcher dispatcher = pc.dispatcher();
        // The first attempt has exhausted the limit and the record is waiting out its one cycle.
        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(attempts.get()).isEqualTo(1));
        assertThat(dispatcher.parkedForRoute(TOPIC)).isEmpty();
        assertThat(dispatcher.parkedCount()).isEqualTo(0);
        assertThat(dispatcher.parkedRecords().cyclesUsed(recordAt(0))).isEqualTo(1);

        // ...and once the cycle's delay elapses it is attempted again and then parks.
        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            assertThat(attempts.get()).isEqualTo(2);
            assertThat(dispatcher.parkedForRoute(TOPIC)).hasSize(1);
        });
    }

    /**
     * A record that succeeds on a cycle attempt completes like any other, and takes its cycle count with it - the
     * next time this offset is seen it starts from zero (R10).
     */
    @Test
    void aRecordThatSucceedsOnACycleAttemptCompletesAndIsForgotten() {
        var attempts = new AtomicInteger();
        var pc = ParallelConsumer.connect(props());
        pc.string(TOPIC)
                .retryLimit(0)
                .retryDelay(Duration.ofMillis(10))
                .afterRetries(park().thenRetryAfter(Duration.ofMillis(100)).forCycles(3))
                .process(context -> {
                    if (attempts.incrementAndGet() < 3) {
                        throw new FakeRuntimeException("not yet");
                    }
                    return Outcome.succeeded();
                });

        handle = runtime.startAndAssign(pc, 1);
        runtime.publish(TOPIC, 0, 0, "key-0", "an order that comes good");

        RouteDispatcher dispatcher = pc.dispatcher();
        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(dispatcher.succeededCount()).isEqualTo(1));

        assertThat(attempts.get()).isEqualTo(3);
        assertThat(dispatcher.parkedForRoute(TOPIC)).isEmpty();
        assertThat(dispatcher.parkedRecords().cyclesUsed(recordAt(0))).isEqualTo(0);
        assertThat(dispatcher.ledger().attempts(TOPIC, 0, 0)).isEqualTo(0);
    }

    /**
     * AE6's park half, against a policy that would otherwise grant cycles: a payload that can never be decoded
     * parks at once, spends no attempt, and takes no cycle - a wait changes nothing about bytes that will never
     * read (R12, R27).
     */
    @Test
    void aPermanentDecodeFailureNeverTakesTheDelayPath() {
        var ran = new AtomicInteger();
        var pc = ParallelConsumer.connect(props());
        pc.topic(TOPIC)
                .consumed(Consumed.with(Serdes.String(),
                        Formats.classifyDecodeFailures(Serdes.serdeFrom(Serdes.String().serializer(), rejecting()),
                                Decode::permanentFailure)))
                .retryLimit(5)
                .retryDelay(Duration.ofMillis(10))
                .afterRetries(park().thenRetryAfter(Duration.ofSeconds(30)).forCycles(5))
                .process(context -> {
                    ran.incrementAndGet();
                    return Outcome.succeeded();
                });

        handle = runtime.startAndAssign(pc, 1);
        runtime.publish(TOPIC, 0, 0, "key-0", "poison");

        RouteDispatcher dispatcher = pc.dispatcher();
        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(dispatcher.parkedForRoute(TOPIC)).hasSize(1));

        ParkedRecord parked = dispatcher.parkedForRoute(TOPIC).get(0);
        assertThat(parked.attempts()).isEqualTo(0);
        assertThat(parked.cycles()).isEqualTo(0);
        assertThat(parked.reason()).contains("can never be decoded");
        assertThat(ran.get()).isEqualTo(0);
    }

    /**
     * A record the function declared hopeless skips its cycles for the same reason it skips its retries: they are
     * more attempts, and the function has said what another attempt would find (R8).
     */
    @Test
    void aParkOutcomeSkipsTheCyclesToo() {
        var attempts = new AtomicInteger();
        var pc = ParallelConsumer.connect(props());
        pc.string(TOPIC)
                .retryLimit(5)
                .retryDelay(Duration.ofMillis(10))
                .afterRetries(park().thenRetryAfter(Duration.ofMillis(100)).forCycles(5))
                .process(context -> {
                    attempts.incrementAndGet();
                    return Outcome.park("the account was closed");
                });

        handle = runtime.startAndAssign(pc, 1);
        runtime.publish(TOPIC, 0, 0, "key-0", "an order");

        RouteDispatcher dispatcher = pc.dispatcher();
        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(dispatcher.parkedForRoute(TOPIC)).hasSize(1));

        assertThat(dispatcher.parkedForRoute(TOPIC).get(0).cycles()).isEqualTo(0);
        assertThat(dispatcher.parkedForRoute(TOPIC).get(0).reason()).isEqualTo("the account was closed");
        Awaitility.await().pollDelay(Duration.ofMillis(500)).atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> assertThat(attempts.get()).isEqualTo(1));
    }

    /**
     * A stand-in for the record at an offset, purely to ask the cycle map about it by coordinates.
     */
    private static ConsumerRecord<byte[], byte[]> recordAt(long offset) {
        return new ConsumerRecord<>(TOPIC, 0, offset, new byte[0], new byte[0]);
    }

    private static Deserializer<String> rejecting() {
        return (topic, data) -> {
            String value = new String(data, StandardCharsets.UTF_8);
            if (value.equals("poison")) {
                throw new SerializationException("this payload cannot be read");
            }
            return value;
        };
    }
}
