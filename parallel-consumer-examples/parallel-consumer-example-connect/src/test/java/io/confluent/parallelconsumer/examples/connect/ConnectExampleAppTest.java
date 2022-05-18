package io.confluent.parallelconsumer.examples.connect;

/*-
 * Copyright (C) 2020-2021 Confluent, Inc.
 */

import io.confluent.csid.utils.KafkaTestUtils;
import io.confluent.csid.utils.LongPollingMockConsumer;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serdes;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.nio.charset.StandardCharsets;
import java.time.Duration;

import static org.mockito.Mockito.when;
import static pl.tlinkowski.unij.api.UniLists.of;

@Slf4j
class ConnectExampleAppTest {

    @SneakyThrows
    @Test
    void test() {
        log.debug("Test start");
        var connectApp = new ConnectAppUnderTest();
        TopicPartition tp = new TopicPartition(connectApp.inputTopic, 0);

        connectApp.run();

        connectApp.mockConsumer.addRecord(makeRecord(connectApp, "a key 1", 0));
        connectApp.mockConsumer.addRecord(makeRecord(connectApp, "a key 2", 1));
        connectApp.mockConsumer.addRecord(makeRecord(connectApp, "a key 3", 2));

        Awaitility.await().pollInterval(Duration.ofSeconds(1)).untilAsserted(() -> {
            KafkaTestUtils.assertLastCommitIs(connectApp.mockConsumer, 3);
        });

        connectApp.close();
    }

    private ConsumerRecord<byte[], byte[]> makeRecord(final ConnectAppUnderTest connectApp, final String s, final int i) {
        byte[] value = "a value".getBytes(StandardCharsets.UTF_8);
        byte[] key = s.getBytes(StandardCharsets.UTF_8);
        return new ConsumerRecord<>(connectApp.inputTopic, 0, i, key, value);
    }


    static class ConnectAppUnderTest extends ConnectExampleApp {

        LongPollingMockConsumer<byte[], byte[]> mockConsumer = Mockito.spy(new LongPollingMockConsumer<>(OffsetResetStrategy.EARLIEST));
        TopicPartition tp = new TopicPartition(inputTopic, 0);

        public ConnectAppUnderTest() {
            super();
        }

        @Override
        Consumer<byte[], byte[]> getKafkaConsumer() {
            when(mockConsumer.groupMetadata())
                    .thenReturn(new ConsumerGroupMetadata("groupid")); // todo fix AK mock consumer
            return mockConsumer;
        }

        @Override
        Producer<byte[], byte[]> getKafkaProducer() {
            var stringSerializer = Serdes.ByteArray().serializer();
            return new MockProducer<>(true, stringSerializer, stringSerializer);
        }

        @Override
        protected void postSetup() {
            mockConsumer.subscribeWithRebalanceAndAssignment(of(inputTopic), 1);
        }
    }
}
