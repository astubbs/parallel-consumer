package bz.stub.parallelconsumer.integrationTests;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelEoSStreamProcessor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.record.CompressionType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static bz.stub.parallelconsumer.integrationTests.utils.KafkaClientUtils.ProducerMode.NOT_TRANSACTIONAL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Every record batch a producer compresses is decompressed on the consumer side by the codec library on THIS
 * classpath, not the broker's - so a bump to {@code zstd-jni}, {@code snappy-java} or {@code lz4-java} is only
 * proven compatible with the {@code kafka-clients} we ship once a batch produced with that codec has come back
 * through the consumer intact. Nothing else in the suite sets {@code compression.type}; the default producer is
 * uncompressed, which exercises none of the three native libraries.
 * <p>
 * One case per codec kafka-clients ships, each producing a payload large enough that the codec does real work
 * (a compressible run plus a unique tail, so a batch that came back short or scrambled fails on content, not
 * just on count). The zstd case additionally covers the library PC's own offset-map codec uses.
 * <p>
 * Written for the {@code zstd-jni 1.5.7-12 -> 1.5.7-17} bump (three advisories fixed in 1.5.7-14), and kept so
 * that the next codec bump has a test to point at rather than an argument.
 */
@Slf4j
class CompressionCodecsIT extends BrokerIntegrationTest<String, String> {

    private static final int RECORDS = 200;

    @ParameterizedTest
    @EnumSource(value = CompressionType.class, names = {"GZIP", "SNAPPY", "LZ4", "ZSTD"})
    void batchesProducedWithCodecAreConsumedIntact(CompressionType codec) {
        ParallelEoSStreamProcessor<String, String> pc = startPcOnNewTopic(options -> options);

        // release 8 target: no String.repeat
        String payload = String.join("", Collections.nCopies(2_000, "x")) + UUID.randomUUID();
        Properties compression = new Properties();
        compression.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, codec.name);
        try (KafkaProducer<String, String> producer = getKcu().createNewProducer(NOT_TRANSACTIONAL, compression)) {
            for (int i = 0; i < RECORDS; i++) {
                producer.send(new ProducerRecord<>(getTopic(), "key-" + i, i + ":" + payload));
            }
            producer.flush();
        }

        // key -> value as consumed; assertions run outside the user function so a mismatch fails the test
        // rather than being retried by PC
        Map<String, String> consumed = new ConcurrentHashMap<>();
        try {
            pc.poll(context -> consumed.put(context.key(), context.value()));

            await().atMost(Duration.ofSeconds(30))
                    .untilAsserted(() -> assertThat(consumed).as("records consumed with " + codec).hasSize(RECORDS));
        } finally {
            pc.close();
        }

        for (int i = 0; i < RECORDS; i++) {
            assertThat(consumed.get("key-" + i))
                    .as("value of record %d produced with %s", i, codec)
                    .isEqualTo(i + ":" + payload);
        }
    }
}
