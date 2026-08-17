package bz.stub.parallelconsumer.examples.streams.integrationTests;
/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.examples.streams.StreamsApp;
import bz.stub.parallelconsumer.integrationTests.BrokerIntegrationTest;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.assertj.core.api.Assertions;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

@Slf4j
public class StreamsAppTest extends BrokerIntegrationTest<String, String> {

    @SneakyThrows
    @Test
    public void test() {
        log.info("Test start");
        ensureTopic(StreamsApp.inputTopic, 1);
        ensureTopic(StreamsApp.outputTopicName, 1);

        // Dependencies injected via the constructor - no subclass/override hooks needed.
        StreamsApp coreApp = new StreamsApp(BrokerIntegrationTest.kafkaContainer.getBootstrapServers());

        coreApp.run();

        try (Producer<String, String> kafkaProducer = getKcu().createNewProducer(false)) {

            kafkaProducer.send(new ProducerRecord<>(StreamsApp.inputTopic, "a key 1", "a value"));
            kafkaProducer.send(new ProducerRecord<>(StreamsApp.inputTopic, "a key 2", "a value"));
            kafkaProducer.send(new ProducerRecord<>(StreamsApp.inputTopic, "a key 3", "a value"));

            Awaitility.await().untilAsserted(() -> {
                Assertions.assertThat(coreApp.getMessageCount().get()).isEqualTo(3);
            });

        } finally {
            coreApp.close();
        }
    }
}
