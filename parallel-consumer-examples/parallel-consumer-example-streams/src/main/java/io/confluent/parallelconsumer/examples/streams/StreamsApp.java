package io.confluent.parallelconsumer.examples.streams;

/*-
 * Copyright (C) 2020 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */


import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.ParallelStreamProcessor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import pl.tlinkowski.unij.api.UniLists;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class StreamsApp {

    public static String inputTopic = "input-topic-" + RandomUtils.nextInt();

    public static String outputTopicName = "my-output-topic-" + RandomUtils.nextInt();

    /**
     * Where the Kafka broker(s) live. Injected via the constructor so the same app can point at a real
     * cluster or, in tests, a throwaway broker - no subclass-and-override-hook DI needed.
     */
    private final String bootstrapServers;

    KafkaStreams streams;

    ParallelStreamProcessor<String, String> parallelConsumer;

    @Getter
    private final AtomicInteger messageCount = new AtomicInteger();

    public StreamsApp(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    private Consumer<String, String> getKafkaConsumer() {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        p.put(ConsumerConfig.GROUP_ID_CONFIG, StreamsApp.class.getSimpleName() + "-" + RandomUtils.nextInt());
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false); // Parallel Consumer manages commits
        return new KafkaConsumer<>(p);
    }

    private Producer<String, String> getKafkaProducer() {
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer<>(p);
    }

    // tag::example[]
    public void run() {
        preprocess(); // <1>
        concurrentProcess(); // <2>
    }

    void preprocess() {
        StreamsBuilder builder = new StreamsBuilder();
        builder.<String, String>stream(inputTopic)
                .mapValues((key, value) -> {
                    log.info("Streams preprocessing key: {} value: {}", key, value);
                    return String.valueOf(value.length());
                })
                .to(outputTopicName);

        startStreams(builder.build());
    }

    void startStreams(Topology topology) {
        streams = new KafkaStreams(topology, getStreamsProperties());
        streams.start();
    }

    void concurrentProcess() {
        setupParallelConsumer();

        parallelConsumer.poll(record -> {
            log.info("Concurrently processing a record: {}", record);
            messageCount.getAndIncrement();
        });
    }
    // end::example[]

    private void setupParallelConsumer() {
        Consumer<String, String> kafkaConsumer = getKafkaConsumer();
        Producer<String, String> kafkaProducer = getKafkaProducer();
        var options = ParallelConsumerOptions.<String, String>builder()
                .ordering(ParallelConsumerOptions.ProcessingOrder.KEY)
                .consumer(kafkaConsumer)
                .producer(kafkaProducer)
                .build();


        parallelConsumer = ParallelStreamProcessor.createEosStreamProcessor(options);
        parallelConsumer.subscribe(UniLists.of(outputTopicName));
    }

    Properties getStreamsProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, this.getClass().getName());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        return props;
    }

    public void close() {
        streams.close();
        parallelConsumer.close();
    }

}
