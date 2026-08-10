package io.confluent.parallelconsumer.examples.streams.integrationTests;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Generates - and, on every subsequent run, re-verifies - the <b>stateful</b> stock Kafka Streams baseline
 * that {@code PcDrivenStatefulProofTest} in {@code parallel-consumer-streams-spike} asserts against (U7).
 * <p>
 * See {@link StockBaselineFixtureSupport} for why a generator for the spike lives in an examples module, and
 * for the fixture format.
 * <p>
 * <b>Why a non-windowed count, and nothing more ambitious.</b> The spike's thread-confinement change is only
 * load-bearing where something reads the per-task record context <em>ambiently</em>, and in Kafka Streams
 * that is the state-store stack: {@code MeteredKeyValueStore}, {@code ChangeLoggingKeyValueBytesStore} and
 * {@code StoreQueryUtils}. A stateless topology instantiates none of them. Windowed operators, joins and
 * suppression would instantiate them too, but they also change their own semantics under out-of-order
 * processing - so a failure there could not be attributed to the confinement, which is the only thing the
 * spike is trying to measure. A non-windowed aggregation is the smallest topology that makes the store stack
 * real while keeping the expected output a fixed, order-independent fact.
 * <p>
 * <b>Caching is disabled deliberately, and it is not free.</b> With the record cache on, a KTable emits only
 * on flush and the downstream sees one record per key per commit; with it off, every update is forwarded, so
 * this baseline has exactly one output per input and the counts appear as {@code 1, 2, 3, 4, 5}. That is a
 * DSL emission-semantics change, not an implementation detail - the spike's write-up prices it.
 *
 * @author Antony Stubbs
 * @see StockBaselineFixtureTest
 */
@Slf4j
class StockStatefulBaselineFixtureTest extends StockBaselineFixtureSupport {

    static final String COUNT_STORE = "pc-spike-counts";

    static final String TOPOLOGY =
            "stream -> groupByKey -> count(Materialized.as(\"" + COUNT_STORE + "\").withCachingDisabled()) -> toStream -> to";

    static final String FIXTURE_RELATIVE_PATH =
            SPIKE_MODULE + "/src/test/resources/stock-stateful-baseline-fixture.tsv";

    /**
     * Same premise as the stateless generator's, asserted again rather than assumed to still hold: this
     * class could be run on its own.
     */
    @Test
    void theStatefulBaselineIsGeneratedByGenuinelyStockKafkaStreams() throws Exception {
        assertClasspathIsStock();
    }

    @SneakyThrows
    @Test
    void stockStatefulRunProducesTheTrackedBaselineFixture() {
        assertClasspathIsStock();

        List<Row> inputs = buildInputs();

        String inputTopic = setupTopic("stock-stateful-in");
        String outputTopic = setupTopic("stock-stateful-out");
        ensureTopic(inputTopic, 1);
        ensureTopic(outputTopic, 1);

        KafkaStreams streams = startTopology("stock-stateful", builder -> {
            KStream<String, String> stream = builder.stream(inputTopic);
            stream.groupByKey()
                    .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as(COUNT_STORE)
                            .withCachingDisabled())
                    .toStream()
                    .to(outputTopic, Produced.with(Serdes.String(), Serdes.Long()));
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
                        "The stateful arm: a non-windowed count over a caching-disabled KV store. 'out' values",
                        "are the running count for the key, rendered as a decimal string; the timestamp is the",
                        "timestamp of the input record that produced that count.",
                }),
                FIXTURE_RELATIVE_PATH);
    }

    // ---------------------------------------------------------------------------------------------------

    private List<Row> consume(final String outputTopic) {
        List<Row> outputs = new ArrayList<>();
        try (KafkaConsumer<String, Long> consumer = getKcu().createNewConsumer(true, longValueConsumerProps())) {
            consumer.subscribe(Collections.singletonList(outputTopic));

            await().atMost(Duration.ofSeconds(120)).until(() -> {
                ConsumerRecords<String, Long> polled = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, Long> record : polled) {
                    outputs.add(new Row(record.key(), Long.toString(record.value()), record.timestamp()));
                }
                return outputs.size() >= TOTAL;
            });
        }
        return outputs;
    }

    /**
     * The count topology emits {@code Long} values, so the shared consumer factory's String deserializer has
     * to be overridden. Deserialising them as strings and comparing the mojibake would "work" and would stop
     * being a comparison of what the topology actually emitted.
     */
    private static Properties longValueConsumerProps() {
        Properties props = new Properties();
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        return props;
    }

    /**
     * The baseline is only worth recording if stock itself got the aggregation right: every key must count
     * 1..5 in order, and each count must carry the timestamp of the record that produced it.
     */
    private static void assertBaselineIsSane(final List<Row> inputs, final List<Row> outputs) {
        assertThat(inputs).as("the run shape must produce %s input records", TOTAL).hasSize(TOTAL);
        assertThat(outputs)
                .as("with caching disabled stock Kafka Streams forwards every update, so there must be "
                        + "exactly one output per input")
                .hasSize(TOTAL);

        Map<String, List<String>> countsByKey = new LinkedHashMap<>();
        for (Row out : outputs) {
            countsByKey.computeIfAbsent(out.key, k -> new ArrayList<>()).add(out.value);
        }
        assertThat(countsByKey).as("every key that went in must come out").hasSize(KEYS);
        List<String> expectedSequence = new ArrayList<>();
        for (int n = 1; n <= RECORDS_PER_KEY; n++) {
            expectedSequence.add(Long.toString(n));
        }
        for (Map.Entry<String, List<String>> entry : countsByKey.entrySet()) {
            assertThat(entry.getValue())
                    .as("key %s must count 1..%s in order - a baseline recorded from a run that lost an "
                            + "update would make the spike's headline assertion meaningless", entry.getKey(),
                            RECORDS_PER_KEY)
                    .containsExactlyElementsOf(expectedSequence);
        }

        // The nth output for a key must carry the timestamp of that key's nth input. This is the property
        // that breaks first if a per-record context leaks between worker threads, so the baseline has to
        // pin it down here where nothing is concurrent.
        Map<String, List<Long>> inputTimestampsByKey = new LinkedHashMap<>();
        for (Row in : inputs) {
            inputTimestampsByKey.computeIfAbsent(in.key, k -> new ArrayList<>()).add(in.timestamp);
        }
        Map<String, List<Long>> outputTimestampsByKey = new LinkedHashMap<>();
        for (Row out : outputs) {
            outputTimestampsByKey.computeIfAbsent(out.key, k -> new ArrayList<>()).add(out.timestamp);
        }
        for (Map.Entry<String, List<Long>> entry : inputTimestampsByKey.entrySet()) {
            assertThat(outputTimestampsByKey.get(entry.getKey()))
                    .as("the nth count for key %s must carry the timestamp of that key's nth input record",
                            entry.getKey())
                    .containsExactlyElementsOf(entry.getValue());
        }
    }
}
