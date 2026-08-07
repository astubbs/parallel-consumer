package io.confluent.parallelconsumer.examples.streams.integrationTests;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import io.confluent.parallelconsumer.integrationTests.utils.KafkaClientUtils;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.kstream.KStream;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Generates - and, on every subsequent run, re-verifies - the <b>stateless</b> stock Kafka Streams baseline
 * that {@code PcDrivenStreamsProofTest} in {@code parallel-consumer-streams-spike} asserts against (U6).
 * <p>
 * See {@link StockBaselineFixtureSupport} for why a generator for the spike lives in an examples module, and
 * for the fixture format.
 *
 * @author Antony Stubbs
 * @see StockStatefulBaselineFixtureTest
 */
@Slf4j
class StockBaselineFixtureTest extends StockBaselineFixtureSupport {

    static final String SUFFIX = "-processed";

    static final String TOPOLOGY = "stream -> mapValues((key, value) -> value + \"" + SUFFIX + "\") -> to";

    static final String FIXTURE_RELATIVE_PATH =
            SPIKE_MODULE + "/src/test/resources/stock-baseline-fixture.tsv";

    /**
     * The load-bearing assertion of this class, and the reason it is not simply a second arm inside the spike
     * module. Everything else here is only as meaningful as this is.
     */
    @Test
    void theBaselineIsGeneratedByGenuinelyStockKafkaStreams() throws Exception {
        assertClasspathIsStock();
    }

    @SneakyThrows
    @Test
    void stockStreamsRunProducesTheTrackedBaselineFixture() {
        assertClasspathIsStock();

        List<Row> inputs = buildInputs();

        String inputTopic = setupTopic("stock-baseline-in");
        String outputTopic = setupTopic("stock-baseline-out");
        ensureTopic(inputTopic, 1);
        ensureTopic(outputTopic, 1);

        KafkaStreams streams = startTopology("stock-baseline", builder -> {
            KStream<String, String> stream = builder.stream(inputTopic);
            stream.mapValues((key, value) -> value + SUFFIX).to(outputTopic);
        });

        List<Row> outputs;
        try {
            produce(inputTopic, inputs);
            outputs = consume(outputTopic);
        } finally {
            streams.close(Duration.ofSeconds(60));
        }

        assertBaselineIsSane(inputs, outputs);

        verifyOrRegenerate(
                render(getClass(), FIXTURE_RELATIVE_PATH, TOPOLOGY, inputs, outputs, new String[]{
                        "The stateless arm: one output record per input, value suffixed, timestamp carried",
                        "through unchanged.",
                }),
                FIXTURE_RELATIVE_PATH);
    }

    // ---------------------------------------------------------------------------------------------------

    private List<Row> consume(final String outputTopic) {
        List<Row> outputs = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = getKcu().createNewConsumer(KafkaClientUtils.GroupOption.NEW_GROUP)) {
            consumer.subscribe(Collections.singletonList(outputTopic));

            await().atMost(Duration.ofSeconds(120)).until(() -> {
                ConsumerRecords<String, String> polled = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> record : polled) {
                    outputs.add(new Row(record.key(), record.value(), record.timestamp()));
                }
                return outputs.size() >= TOTAL;
            });
        }
        return outputs;
    }

    private static void assertBaselineIsSane(final List<Row> inputs, final List<Row> outputs) {
        assertThat(inputs).as("the run shape must produce %s input records", TOTAL).hasSize(TOTAL);
        assertThat(outputs)
                .as("stock Kafka Streams must emit exactly one output per input - a baseline recorded from a "
                        + "run that dropped or duplicated records would be worse than no baseline")
                .hasSize(TOTAL);

        Map<String, List<String>> expectedByKey = new LinkedHashMap<>();
        for (Row in : inputs) {
            expectedByKey.computeIfAbsent(in.key, k -> new ArrayList<>()).add(in.value + SUFFIX);
        }
        Map<String, List<String>> actualByKey = new LinkedHashMap<>();
        for (Row out : outputs) {
            actualByKey.computeIfAbsent(out.key, k -> new ArrayList<>()).add(out.value);
        }
        assertThat(actualByKey.keySet()).as("every key that went in must come out").isEqualTo(expectedByKey.keySet());
        for (Map.Entry<String, List<String>> entry : expectedByKey.entrySet()) {
            assertThat(actualByKey.get(entry.getKey()))
                    .as("key %s must come out in the order it went in", entry.getKey())
                    .containsExactlyElementsOf(entry.getValue());
        }

        Map<String, Long> inputTimestamps = new LinkedHashMap<>();
        for (Row in : inputs) {
            inputTimestamps.put(in.value + SUFFIX, in.timestamp);
        }
        for (Row out : outputs) {
            assertThat(out.timestamp)
                    .as("stock Kafka Streams carries the input record's timestamp to the sink for %s - the "
                            + "spike asserts that too, and it is the property that breaks first if the "
                            + "per-record context leaks between worker threads", out.value)
                    .isEqualTo(inputTimestamps.get(out.value));
        }
    }
}
