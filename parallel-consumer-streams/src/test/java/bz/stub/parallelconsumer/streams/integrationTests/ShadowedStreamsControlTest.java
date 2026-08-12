package io.confluent.parallelconsumer.streams.integrationTests;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import io.confluent.parallelconsumer.integrationTests.utils.KafkaClientUtils;
import io.confluent.parallelconsumer.streams.PcDispatchSwitch;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;
import pl.tlinkowski.unij.api.UniLists;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * The control arm. Runs a stateless topology through stock, single-threaded Kafka Streams while our generated
 * copies of {@code StreamTask}, {@code AbstractProcessorContext}, {@code ProcessorContextImpl} and
 * {@code RecordCollectorImpl} shadow the jar's.
 * <p>
 * It was written at U3 against an <em>empty</em> patch, which made it an attribution test for the
 * generate-and-patch harness itself: byte-for-byte the released 3.9.2 sources, so any behavioural difference
 * had to be the harness's fault and nothing else's. Without that baseline, a failure later could not be
 * attributed between the technique and the change, and the module's verdict would be worthless.
 * <p>
 * From U4 on it earns its keep a second way: the patch is no longer empty, so this arm is the standing
 * assertion that the confinement change is <em>behaviour-preserving</em> when only one thread is running.
 * Concurrency fixes that quietly break the serial path are not fixes.
 * <p>
 * If this test goes red, the plan says STOP - write up the verdict and re-plan.
 * <p>
 * <b>The stock path is asked for, not assumed.</b> {@link PcDispatchSwitch} defaults <em>on</em> - depending
 * on this artifact is the opt-in - so this arm turns it off itself, in {@link #useStockDispatch()}. This test
 * exists to say what the patched classes do under serial, single-threaded Streams, and it would silently stop
 * saying that if it ran on the PC path.
 *
 * @see io.confluent.parallelconsumer.streams.ShadowedClassLoadingTest
 */
@Slf4j
// The switch it depends on is process-wide, so an arm that turns it on in another class would otherwise be
// able to do so underneath this one - converting the control arm into a second PC arm, silently.
@Isolated
class ShadowedStreamsControlTest extends BrokerStreamsIntegrationTest {

    private static final int KEYS = 5;
    private static final int RECORDS_PER_KEY = 20;
    private static final int TOTAL = KEYS * RECORDS_PER_KEY;

    private static final String SUFFIX = "-processed";

    /**
     * Explicit, because the default is now the other way. Without this the topology below would run on the PC
     * worker pool and this class would quietly become a second parallel arm rather than the baseline the whole
     * module is measured against.
     */
    @BeforeEach
    void useStockDispatch() {
        PcDispatchSwitch.disable();
        assertThat(PcDispatchSwitch.isEnabled())
                .as("the control arm must run stock, serial Kafka Streams dispatch")
                .isFalse();
    }

    /** Hand the JVM back at the artifact's default, so the next test states its own requirement. */
    @AfterEach
    void restoreDefaultDispatch() {
        PcDispatchSwitch.resetToDefault();
    }

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

        // No thread count: stock Kafka Streams' own default is what this arm is the baseline for.
        return startAndAwaitRunning(builder, baseStreamsProps("control-arm-" + System.nanoTime()));
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
