package io.confluent.parallelconsumer.examples.vertx;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.vertx.VertxParallelEoSStreamProcessor.RequestInfo;
import io.confluent.parallelconsumer.vertx.VertxParallelStreamProcessor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import pl.tlinkowski.unij.api.UniMaps;

import java.util.Map;
import java.util.Properties;

import static pl.tlinkowski.unij.api.UniLists.of;

@Slf4j
public class VertxApp {

    static String inputTopic = "input-topic-" + RandomUtils.nextInt();

    Consumer<String, String> getKafkaConsumer() {
        return new KafkaConsumer<>(new Properties());
    }

    Producer<String, String> getKafkaProducer() {
        return new KafkaProducer<>(new Properties());
    }

    VertxParallelStreamProcessor<String, String> parallelConsumer;


    void run() {
        Consumer<String, String> kafkaConsumer = getKafkaConsumer();
        Producer<String, String> kafkaProducer = getKafkaProducer();
        var options = ParallelConsumerOptions.<String, String>builder()
                .ordering(ParallelConsumerOptions.ProcessingOrder.KEY)
                .consumer(kafkaConsumer)
                .producer(kafkaProducer)
                .build();

        this.parallelConsumer = VertxParallelStreamProcessor.createEosStreamProcessor(options);
        parallelConsumer.subscribe(of(inputTopic));

        postSetup();

        int port = getPort();

        // tag::example[]
        parallelConsumer.vertxHttpReqInfo(context -> {
            var consumerRecord = context.getSingleConsumerRecord();
            log.info("Concurrently constructing and returning RequestInfo from record: {}", consumerRecord);
            Map<String, String> params = UniMaps.of("recordKey", consumerRecord.key(), "payload", consumerRecord.value());
            return new RequestInfo("localhost", port, "/api", params); // <1>
        }, onSend -> {
            // <2>
        }, onComplete -> {
            log.info("Response from the HTTP request: {}", onComplete.result()); // <3>
        });
        // end::example[]

    }

    protected int getPort() {
        return 8080;
    }

    void close() {
        this.parallelConsumer.closeDrainFirst();
    }

    protected void postSetup() {
        // no-op, for testing
    }

}
