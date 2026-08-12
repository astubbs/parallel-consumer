package io.confluent.parallelconsumer.streams.integrationTests;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import io.confluent.parallelconsumer.integrationTests.BrokerIntegrationTest;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse;
import pl.tlinkowski.unij.api.UniSets;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static org.awaitility.Awaitility.await;

/**
 * The base every broker-backed Kafka Streams arm in this module extends: how a topology is pointed at the test
 * broker, and how it is started and waited for.
 * <p>
 * <b>Only what the arms must agree on lives here.</b> The module's whole method is comparing arms that differ
 * in one term, so the terms an experiment varies - thread count, commit interval, the dispatch switch, the
 * topology itself - stay at the call site where they can be read next to the assertions they support. What is
 * shared is the part where disagreement would silently make two arms incomparable: the same broker, the same
 * serdes, and the same starting position in the topic.
 *
 * @author Antony Stubbs
 */
@Slf4j
abstract class BrokerStreamsIntegrationTest extends BrokerIntegrationTest<String, String> {

    /**
     * Generous, because a cold broker plus the first rebalance is not quick - but bounded, so a topology that
     * never reaches RUNNING fails here with its last state rather than timing out much later in some drain.
     */
    private static final Duration STARTUP_TIMEOUT = Duration.ofSeconds(60);

    /**
     * For the arms that ask for it: log the stack trace and take the client down.
     * <p>
     * A StreamThread that dies without this leaves nothing but a test timeout to diagnose from, and an arm
     * that carried on with one thread fewer would be measuring a topology that is no longer the one it
     * described.
     */
    static final StreamsUncaughtExceptionHandler LOG_AND_SHUT_DOWN_CLIENT = throwable -> {
        log.error("Streams thread died", throwable);
        return StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
    };

    /**
     * The props every arm shares, and nothing else. No thread count and no commit interval: an arm that
     * inherited either without saying so would be measuring something other than what it claims to.
     */
    Properties baseStreamsProps(final String applicationId) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        // Start from the beginning so the test is not racing the topology's startup - several arms produce
        // everything before starting, and every one of those records would otherwise be skipped.
        props.put(StreamsConfig.consumerPrefix("auto.offset.reset"), "earliest");
        return props;
    }

    KafkaStreams startAndAwaitRunning(final StreamsBuilder builder, final Properties props) {
        return startAndAwaitRunning(new KafkaStreams(builder.build(), props));
    }

    /**
     * @param uncaughtHandler installed before {@code start()}, which is the only point at which it can be -
     *                        see {@link #LOG_AND_SHUT_DOWN_CLIENT}
     */
    KafkaStreams startAndAwaitRunning(final StreamsBuilder builder,
                                      final Properties props,
                                      final StreamsUncaughtExceptionHandler uncaughtHandler) {
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.setUncaughtExceptionHandler(uncaughtHandler);
        return startAndAwaitRunning(streams);
    }

    /**
     * The application's own committed offset for one input partition, or {@code null} when the group has
     * never committed.
     * <p>
     * The reader carries the app's group id but <b>never subscribes</b>, so it performs an OffsetFetch
     * without joining the group and cannot rebalance the topology under test. Shared because two arms of
     * this module need it and a second copy of that subtlety is how one of them silently starts
     * rebalancing what it is measuring.
     */
    OffsetAndMetadata committedOffsetOrNull(final String applicationId, final TopicPartition partition) {
        try (KafkaConsumer<String, String> groupReader = getKcu().createNewConsumer(applicationId)) {
            return groupReader.committed(UniSets.of(partition)).get(partition);
        }
    }

    /**
     * Sleeps for the full duration, restoring the interrupt flag and failing loudly if interrupted.
     *
     * @param what what the sleep is for, used in the failure message - a bare "interrupted" tells the
     *             reader nothing about which wait died
     */
    static void sleepThrough(final Duration duration, final String what) {
        try {
            Thread.sleep(duration.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while " + what, e);
        }
    }

    private static KafkaStreams startAndAwaitRunning(final KafkaStreams streams) {
        streams.start();

        AtomicInteger polls = new AtomicInteger();
        await().atMost(STARTUP_TIMEOUT).until(() -> {
            KafkaStreams.State state = streams.state();
            // Every tenth poll, not every one: a slow start would otherwise bury the run's own logging under
            // a line per Awaitility interval, and the state only matters when it stops changing.
            if (polls.getAndIncrement() % 10 == 0) {
                log.info("Waiting for Streams to run, state={}", state);
            }
            return state == KafkaStreams.State.RUNNING;
        });
        return streams;
    }
}
