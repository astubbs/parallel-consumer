package io.confluent.parallelconsumer.streamsspike.integrationTests;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import io.confluent.parallelconsumer.integrationTests.BrokerIntegrationTest;
import io.confluent.parallelconsumer.integrationTests.utils.KafkaClientUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.junit.jupiter.api.Test;
import pl.tlinkowski.unij.api.UniLists;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * The control arm (U3). Runs a stateless topology through <em>stock</em> Kafka Streams while our generated -
 * but as yet <em>unpatched</em> - copies of {@code StreamTask}, {@code AbstractProcessorContext},
 * {@code ProcessorContextImpl} and {@code RecordCollectorImpl} shadow the jar's.
 * <p>
 * The patch file is empty at this point by design, so those classes are byte-for-byte the released 3.9.2
 * sources. Any behavioural difference here is therefore the generate-and-patch harness's fault and nothing
 * else's. That is the whole point: without this arm, a failure at U5 could not be attributed between the
 * technique and the change, and the spike's verdict would be worthless.
 * <p>
 * If this test goes red, the plan says STOP - write up the verdict and re-plan rather than proceeding to U4.
 *
 * @see io.confluent.parallelconsumer.streamsspike.ShadowedClassLoadingTest
 */
@Slf4j
class ShadowedStreamsControlTest extends BrokerIntegrationTest<String, String> {

    private static final int KEYS = 5;
    private static final int RECORDS_PER_KEY = 20;
    private static final int TOTAL = KEYS * RECORDS_PER_KEY;

    private static final String SUFFIX = "-processed";

    @Test
    void statelessTopologyBehavesIdenticallyUnderShadowedClasses() {
        String inputTopic = setupTopic("control-in");
        String outputTopic = setupTopic("control-out");
        ensureTopic(inputTopic, 1);
        ensureTopic(outputTopic, 1);

        KafkaStreams streams = startStatelessTopology(inputTopic, outputTopic);
        try {
            List<ProducerRecord<String, String>> sent = produce(inputTopic);
            Map<String, List<String>> received = consumeByKey(outputTopic);

            assertThat(flatten(received))
                    .as("every input record must produce exactly one output record - no drops, no duplicates")
                    .hasSize(TOTAL);

            assertThat(received.keySet())
                    .as("every key that went in must come out")
                    .hasSize(KEYS);

            assertPerKeyOrderPreserved(sent, received);
        } finally {
            streams.close(Duration.ofSeconds(30));
        }
    }

    private KafkaStreams startStatelessTopology(String inputTopic, String outputTopic) {
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> stream = builder.stream(inputTopic);
        stream.mapValues(value -> value + SUFFIX).to(outputTopic);

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "control-arm-" + System.nanoTime());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        // Start from the beginning so the test is not racing the topology's startup.
        props.put(StreamsConfig.consumerPrefix("auto.offset.reset"), "earliest");

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        AtomicInteger polls = new AtomicInteger();
        await().atMost(Duration.ofSeconds(60)).until(() -> {
            KafkaStreams.State state = streams.state();
            if (polls.getAndIncrement() % 10 == 0) {
                log.info("Waiting for Streams to run, state={}", state);
            }
            return state == KafkaStreams.State.RUNNING;
        });
        return streams;
    }

    private List<ProducerRecord<String, String>> produce(String inputTopic) {
        List<ProducerRecord<String, String>> sent = new ArrayList<>();
        try (KafkaProducer<String, String> producer = getKcu().createNewProducer(KafkaClientUtils.ProducerMode.NOT_TRANSACTIONAL)) {
            for (int i = 0; i < RECORDS_PER_KEY; i++) {
                for (int k = 0; k < KEYS; k++) {
                    var record = new ProducerRecord<>(inputTopic, "key-" + k, "v" + i);
                    producer.send(record);
                    sent.add(record);
                }
            }
            producer.flush();
        }
        log.info("Produced {} records across {} keys", sent.size(), KEYS);
        return sent;
    }

    /**
     * Reads the output topic until {@link #TOTAL} records have arrived or we give up, preserving arrival order
     * per key so ordering can be asserted.
     */
    private Map<String, List<String>> consumeByKey(String outputTopic) {
        Map<String, List<String>> byKey = new LinkedHashMap<>();
        try (KafkaConsumer<String, String> consumer = getKcu().createNewConsumer(KafkaClientUtils.GroupOption.NEW_GROUP)) {
            // UniLists, not List.of - this module inherits the project's --release 8 API surface, so the
            // Java 9+ collection factories are unavailable even though Jabel allows the newer syntax.
            consumer.subscribe(UniLists.of(outputTopic));

            await().atMost(Duration.ofSeconds(90)).until(() -> {
                ConsumerRecords<String, String> polled = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> record : polled) {
                    byKey.computeIfAbsent(record.key(), k -> new ArrayList<>()).add(record.value());
                }
                int total = flatten(byKey).size();
                log.debug("Consumed {}/{} so far", total, TOTAL);
                return total >= TOTAL;
            });
        }
        return byKey;
    }

    /**
     * A stateless single-partition topology must preserve per-key ordering exactly. This is the property U6
     * will re-assert once PC is dispatching, where it stops being free - so establishing it here, unpatched,
     * is what makes that later assertion meaningful.
     */
    private void assertPerKeyOrderPreserved(List<ProducerRecord<String, String>> sent, Map<String, List<String>> received) {
        Map<String, List<String>> expected = new LinkedHashMap<>();
        for (ProducerRecord<String, String> record : sent) {
            expected.computeIfAbsent(record.key(), k -> new ArrayList<>()).add(record.value() + SUFFIX);
        }

        for (Map.Entry<String, List<String>> entry : expected.entrySet()) {
            assertThat(received.get(entry.getKey()))
                    .as("key %s must come out in the order it went in, with the topology's transform applied",
                            entry.getKey())
                    .containsExactlyElementsOf(entry.getValue());
        }
    }

    private static List<String> flatten(Map<String, List<String>> byKey) {
        List<String> all = new ArrayList<>();
        byKey.values().forEach(all::addAll);
        return all;
    }
}
