package io.confluent.parallelconsumer.examples.connect;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.ParallelEoSStreamProcessor;
import io.confluent.parallelconsumer.ParallelStreamProcessor;
import io.confluent.parallelconsumer.internal.InternalRuntimeError;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import one.util.streamex.StreamEx;
import org.apache.commons.lang3.RandomUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.connect.file.FileStreamSinkConnector;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.sink.SinkConnector;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.storage.Converter;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.UnaryOperator;

import static io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.KEY;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static pl.tlinkowski.unij.api.UniLists.of;

/**
 * Basic core examples
 */
@Slf4j
public class ConnectExampleApp {


    final String inputTopic = "input-topic-" + RandomUtils.nextInt();
    private final String outputTopic = "output-topic-" + RandomUtils.nextInt();
    @NonNull
    private final Converter keyConverter;
    private final BlockingQueue<SinkTask> taskQueue = new LinkedBlockingQueue<>();
    @NonNull
    private final Converter valueConverter;
    private final Plugins plugins = new Plugins(Map.of());

    ParallelStreamProcessor<byte[], byte[]> parallelConsumer;
    private final Duration timeout = Duration.ofSeconds(10);

    public ConnectExampleApp() {
        String stringConverter = "org.apache.kafka.connect.storage.StringConverter";
        Map<String, String> connProps = Map.of(
                ConnectorConfig.NAME_CONFIG, "pc-connect-test",
                ConnectorConfig.CONNECTOR_CLASS_CONFIG, "org.apache.kafka.connect.file.FileStreamSinkConnector",
                WorkerConfig.KEY_CONVERTER_CLASS_CONFIG, stringConverter,
                WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG, stringConverter);
        ConnectorConfig connConfig = new ConnectorConfig(plugins, connProps);
        keyConverter = plugins.newConverter(connConfig, WorkerConfig.KEY_CONVERTER_CLASS_CONFIG,
                Plugins.ClassLoaderUsage.CURRENT_CLASSLOADER);
        valueConverter = plugins.newConverter(connConfig, WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG,
                Plugins.ClassLoaderUsage.CURRENT_CLASSLOADER);
    }

    Consumer<byte[], byte[]> getKafkaConsumer() {
        return new KafkaConsumer<>(new Properties());
    }

    Producer<byte[], byte[]> getKafkaProducer() {
        return new KafkaProducer<>(new Properties());
    }

    @SuppressWarnings("UnqualifiedFieldAccess")
    void run() {
        SinkConnector connector = new FileStreamSinkConnector();
//        MockSinkConnector connector = new MockSinkConnector();


        int concurrency = 3;

        List<Map<String, String>> taskConfigs = connector.taskConfigs(concurrency);

        // todo - rebalance
//        onAssigned(sinkTask);
//        onLost(sinkTask);

        var tasks = StreamEx.of(taskConfigs)
                .map(config -> startTask(connector, config))
                .toList();
        taskQueue.addAll(tasks);

        UnaryOperator<Map<TopicPartition, OffsetAndMetadata>> pcPreCommitHook = (Map<TopicPartition, OffsetAndMetadata> offsetsToCommit) -> {
            var adjustedOffsets = StreamEx.of(taskQueue).map(sinkTask -> sinkTask.preCommit(offsetsToCommit)).toList();
            var collected = new HashMap<TopicPartition, OffsetAndMetadata>();
            adjustedOffsets.forEach(collected::putAll);
            return collected;
        };

        this.parallelConsumer = setupParallelConsumer(concurrency, pcPreCommitHook);
        postSetup();

        var deserializer = Serdes.String().deserializer();

        // tag::example[]
        parallelConsumer.poll(context -> {
                    String topic = context.getSingleRecord().topic();
                    log.info("Concurrently processing a context: k:{} {}", deserializer.deserialize(topic, context.key()), context);

                    SinkTask task = acquireSinkTask();
                    try {
                        var records = context.getConsumerRecordsFlattened();
                        var sinkRecords = StreamEx.of(records)
                                .map(this::convertRecordToSinkRecord)
                                .toList();
                        task.put(sinkRecords);
                    } finally {
                        this.taskQueue.add(task);
                    }
                }
        );
        // end::example[]

//        stop(file);
    }

//    private List<SinkTask> findTasksForPartitions(Map<TopicPartition, OffsetAndMetadata> offsetsToCommit) {
//        return taskQueue.stream().collect(Collectors.toList());
//    }

    private SinkRecord convertRecordToSinkRecord(ConsumerRecord<byte[], byte[]> rec) {
        var key = this.keyConverter.toConnectData(rec.topic(), rec.key());
        var value = this.valueConverter.toConnectData(rec.topic(), rec.value());
        return new SinkRecord(rec.topic(), rec.partition(), key.schema(), key, key.schema(), value, rec.offset());
    }

    private void onLost(final SinkTask sinkTask) {
//        sinkTask.close();
    }

    private void onAssigned(final SinkTask sinkTask) {
//        sinkTask.open();
    }

    private SinkTask acquireSinkTask() {
        try {
            SinkTask poll = this.taskQueue.poll(timeout.toMillis(), MILLISECONDS);
            if (poll == null) {
                throw new InternalRuntimeError("Could not acquire task within timeout {} - possible bug as available tasks should match max concurrent requests for task.", timeout);
            }
            return poll;
        } catch (InterruptedException e) {
            throw new InternalRuntimeError(e);
        }

    }

    private void stop(final FileStreamSinkConnector file) {
        try {
            this.taskQueue.forEach(SinkTask::stop);
        } finally {
            file.stop();
        }
    }

    private SinkTask startTask(SinkConnector sink, Map<String, String> config) {
        Class<? extends org.apache.kafka.connect.connector.Task> taskClass = sink.taskClass();
        var task = plugins.newTask(taskClass);
        if (task instanceof SinkTask sinkTask) {
            sinkTask.initialize(new PCSinkTaskContext());
            sinkTask.start(config);
            return sinkTask;
        } else {
            throw new IllegalStateException("Not a sink task");
        }
    }

    protected void postSetup() {
        // no-op
    }

    @SuppressWarnings({"FeatureEnvy"})
    ParallelStreamProcessor<byte[], byte[]> setupParallelConsumer(int concurrency, UnaryOperator<Map<TopicPartition, OffsetAndMetadata>> abstractOffsetCommitter) {
        // tag::exampleSetup[]
        var kafkaConsumer = getKafkaConsumer(); // <1>
        var kafkaProducer = getKafkaProducer();

        var options = ParallelConsumerOptions.<byte[], byte[]>builder()
                .ordering(KEY) // <2>
                .maxConcurrency(concurrency) // <3>
                .consumer(kafkaConsumer)
                .producer(kafkaProducer)
                .build();


        var eosStreamProcessor = new ParallelEoSStreamProcessor<>(options, abstractOffsetCommitter);

        eosStreamProcessor.subscribe(of(inputTopic)); // <4>

        return eosStreamProcessor;
        // end::exampleSetup[]
    }

    void close() {
        this.parallelConsumer.close();
    }


}
