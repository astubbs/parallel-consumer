package bz.stub.parallelconsumer.fluent;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Properties;

import static com.google.common.truth.Truth.assertThat;

/**
 * The intent hook (KTD4): how a facade throw tells the engine's retry-delay provider what it meant by it.
 * <p>
 * The engine has no hand-back path other than failing a record, so every facade throw - a record at its retry limit,
 * a permanent decode failure - looks to the engine exactly like a user function that failed. The only thing that
 * distinguishes them is the delay the provider hands back when the engine asks, <em>synchronously, inside the failure
 * path, on the thread that threw</em>. So the wrapper writes its intent into a thread-local before it throws, and the
 * provider reads and clears it.
 *
 * <h2>Why the ordering has a control arm</h2>
 * "Write the intent before the throw" is the whole mechanism, and getting it backwards does not fail: the provider
 * simply finds nothing and answers with the route's ordinary retry delay, so a park silently becomes a one-second
 * retry loop against whatever was already failing. The arm below writes the intent <em>after</em> the provider has
 * been asked and pins that exact outcome, so the mechanism's dependence on the ordering is stated by a test rather
 * than by a comment.
 *
 * @see EngineRetryDelayProviderContractTest for the engine-side details this hook relies on
 */
class RetryIntentHookTest {

    private static final String TOPIC = "orders";

    @AfterEach
    void clearAnyIntentsThisTestLeft() {
        RetryIntents.clearThread();
    }

    private static Properties props() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "retry-intent-hook-test");
        return properties;
    }

    /**
     * A started-but-for-the-clients definition, which is all the provider needs: it reads the route table for the
     * fallback delay and the thread-local for the intent.
     */
    private static RouteDispatcher dispatcherWithRouteDelay(Duration routeDelay) {
        var pc = ParallelConsumer.define(props());
        var route = pc.string(TOPIC);
        if (routeDelay != null) {
            route.retryDelay(routeDelay);
        }
        route.process(context -> Outcome.succeeded());
        pc.buildOptions(new RecordingClientRuntime());
        return pc.dispatcher();
    }

    private static ConsumerRecord<byte[], byte[]> record(String topic, int partition, long offset) {
        return new ConsumerRecord<>(topic, partition, offset, new byte[0], new byte[0]);
    }

    // ------------------------------------------------------------------ the two control arms

    @Test
    void anIntentWrittenBeforeTheThrowIsWhatTheProviderAnswers() {
        RouteDispatcher dispatcher = dispatcherWithRouteDelay(null);
        RetryIntents.park(record(TOPIC, 0, 5), RouteDispatcher.PARKED_UNTIL_RESUMED);

        Duration answered = dispatcher.retryDelayFor(TOPIC, 0, 5);

        assertThat(answered).isEqualTo(RouteDispatcher.PARKED_UNTIL_RESUMED);
    }

    /**
     * The control arm. Same record, same intent, written one step too late - and the answer is the ordinary retry
     * delay, which on a route that declares none is the engine's own default of one second.
     */
    @Test
    void anIntentWrittenAfterTheThrowIsNotThereWhenTheProviderAsksAndTheRecordRetriesInASecond() {
        RouteDispatcher dispatcher = dispatcherWithRouteDelay(null);

        Duration answered = dispatcher.retryDelayFor(TOPIC, 0, 5);
        RetryIntents.park(record(TOPIC, 0, 5), RouteDispatcher.PARKED_UNTIL_RESUMED);

        assertThat(answered).isEqualTo(Duration.ofSeconds(1));
        assertThat(answered).isNotEqualTo(RouteDispatcher.PARKED_UNTIL_RESUMED);
    }

    @Test
    void withNoIntentTheProviderAnswersWithTheRoutesOwnRetryDelay() {
        RouteDispatcher dispatcher = dispatcherWithRouteDelay(Duration.ofSeconds(30));

        assertThat(dispatcher.retryDelayFor(TOPIC, 0, 5)).isEqualTo(Duration.ofSeconds(30));
    }

    /**
     * A topic with no route cannot reach the wrapper, but the provider is instance-wide and the engine may ask about
     * a record from a partition being revoked. It answers rather than throwing: a provider that throws is replaced by
     * the engine's default delay, silently.
     */
    @Test
    void theProviderAnswersForATopicItHasNoRouteFor() {
        RouteDispatcher dispatcher = dispatcherWithRouteDelay(Duration.ofSeconds(30));

        assertThat(dispatcher.retryDelayFor("a-topic-no-route-claims", 0, 5)).isEqualTo(Duration.ofSeconds(1));
    }

    // ------------------------------------------------------------------ the intent is per record, not per thread

    /**
     * A failed batch calls the provider once per container on one thread, and two partitions carry the same offset
     * number - so a thread-local holding one intent would answer the second record with the first record's delay.
     */
    @Test
    void twoPartitionsSharingAnOffsetNumberKeepTheirOwnIntentsOnOneThread() {
        RouteDispatcher dispatcher = dispatcherWithRouteDelay(Duration.ofSeconds(30));

        RetryIntents.park(record(TOPIC, 0, 7), RouteDispatcher.PARKED_UNTIL_RESUMED);
        RetryIntents.retry(record(TOPIC, 1, 7), Duration.ofSeconds(30));

        assertThat(dispatcher.retryDelayFor(TOPIC, 0, 7)).isEqualTo(RouteDispatcher.PARKED_UNTIL_RESUMED);
        assertThat(dispatcher.retryDelayFor(TOPIC, 1, 7)).isEqualTo(Duration.ofSeconds(30));
    }

    @Test
    void twoTopicsSharingAPartitionAndOffsetKeepTheirOwnIntents() {
        RouteDispatcher dispatcher = dispatcherWithRouteDelay(Duration.ofSeconds(30));

        RetryIntents.park(record(TOPIC, 0, 7), RouteDispatcher.PARKED_UNTIL_RESUMED);

        assertThat(dispatcher.retryDelayFor("audit", 0, 7)).isEqualTo(Duration.ofSeconds(1));
        assertThat(dispatcher.retryDelayFor(TOPIC, 0, 7)).isEqualTo(RouteDispatcher.PARKED_UNTIL_RESUMED);
    }

    /**
     * Read once and gone. The engine asks for one record once per failure, and an intent left behind would be read by
     * the record's <em>next</em> failure, turning one park into a permanent one.
     */
    @Test
    void takingAnIntentClearsIt() {
        RouteDispatcher dispatcher = dispatcherWithRouteDelay(Duration.ofSeconds(30));
        RetryIntents.park(record(TOPIC, 0, 5), RouteDispatcher.PARKED_UNTIL_RESUMED);

        assertThat(dispatcher.retryDelayFor(TOPIC, 0, 5)).isEqualTo(RouteDispatcher.PARKED_UNTIL_RESUMED);
        assertThat(dispatcher.retryDelayFor(TOPIC, 0, 5)).isEqualTo(Duration.ofSeconds(30));
    }

    // ------------------------------------------------------------------ what the facade's provider never does

    /**
     * The engine replaces a provider that throws, returns null, returns a negative delay or returns one that cannot
     * be added to an {@link Instant} with its own default, and warns - so a facade fault of any of those four shapes
     * would turn every park into a one-second retry with nothing failing.
     * {@link EngineRetryDelayProviderContractTest} pins that engine behaviour; this pins that the facade's own
     * provider never presents any of the four, for every intent it can write and for the no-intent fallback.
     */
    @Test
    void theFacadesProviderNeverThrowsNeverAnswersNullNegativeOrUnrepresentable() {
        RouteDispatcher dispatcher = dispatcherWithRouteDelay(Duration.ofSeconds(30));
        RetryIntents.park(record(TOPIC, 0, 1), RouteDispatcher.PARKED_UNTIL_RESUMED);
        RetryIntents.retry(record(TOPIC, 0, 2), Duration.ofSeconds(30));

        for (long offset : new long[]{1, 2, 3}) {
            Duration answered = dispatcher.retryDelayFor(TOPIC, 0, offset);

            assertThat(answered).isNotNull();
            assertThat(answered.isNegative()).isFalse();
            // representable: the engine catches the overflow and silently substitutes its own default
            Instant unused = Instant.now().plus(answered);
            assertThat(unused).isNotNull();
        }
    }

    /**
     * The park delay is a hundred years, not a forever: {@code ChronoUnit.FOREVER} and anything of that magnitude
     * overflows {@link Instant#plus}, which the engine catches and replaces with its default - a park that becomes a
     * one-second retry.
     */
    @Test
    void theParkDelayIsFarFutureAndStillRepresentable() {
        Instant parkedUntil = Instant.now().plus(RouteDispatcher.PARKED_UNTIL_RESUMED);

        assertThat(parkedUntil).isGreaterThan(Instant.now().plus(Duration.ofDays(36499)));
    }
}
