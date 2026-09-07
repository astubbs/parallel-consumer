package bz.stub.parallelconsumer.examples.vertx;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.vertx.VertxParallelEoSStreamProcessor.RequestInfo;
import bz.stub.parallelconsumer.vertx.JStreamVertxParallelStreamProcessor;
import lombok.SneakyThrows;
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

    JStreamVertxParallelStreamProcessor<String, String> parallelConsumer;

    Thread resultConsumer;


    void run() {
        Consumer<String, String> kafkaConsumer = getKafkaConsumer();
        Producer<String, String> kafkaProducer = getKafkaProducer();
        var options = ParallelConsumerOptions.<String, String>builder()
                .ordering(ParallelConsumerOptions.ProcessingOrder.KEY)
                .consumer(kafkaConsumer)
                .producer(kafkaProducer)
                .build();

        this.parallelConsumer = JStreamVertxParallelStreamProcessor.createEosStreamProcessor(options);
        parallelConsumer.subscribe(of(inputTopic));

        postSetup();

        int port = getPort();

        // tag::example[]
        var resultStream = parallelConsumer.vertxHttpReqInfoStream(context -> {
            var consumerRecord = context.getSingleConsumerRecord();
            log.info("Concurrently constructing and returning RequestInfo from record: {}", consumerRecord);
            Map<String, String> params = UniMaps.of("recordKey", consumerRecord.key(), "payload", consumerRecord.value());
            return new RequestInfo("localhost", port, "/api", params); // <1>
        });

        resultConsumer = new Thread(() -> // <2>
                resultStream.forEach(result -> log.info("From result stream: {}", result)),
                "vertx-result-stream-consumer");
        resultConsumer.start();
        // end::example[]

    }

    protected int getPort() {
        return 8080;
    }

    @SneakyThrows
    void close() {
        this.parallelConsumer.closeDrainFirst();
        resultConsumer.join(); // the stream ends when the consumer closes, so this returns
    }

    protected void postSetup() {
        // no-op, for testing
    }

}
