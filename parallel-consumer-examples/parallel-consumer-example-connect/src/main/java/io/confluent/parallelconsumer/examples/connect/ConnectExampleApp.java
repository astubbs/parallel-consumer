package io.confluent.parallelconsumer.examples.connect;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.ParallelStreamProcessor;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import one.util.streamex.StreamEx;
import org.apache.commons.lang3.RandomUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.connect.file.FileStreamSinkConnector;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.sink.SinkConnector;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.tools.MockSinkConnector;

import java.util.*;

import static io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.KEY;
import static pl.tlinkowski.unij.api.UniLists.of;

/**
 * Basic core examples
 */
@Slf4j
public class ConnectExampleApp {


    private final String inputTopic = "input-topic-" + RandomUtils.nextInt();
    private final String outputTopic = "output-topic-" + RandomUtils.nextInt();
    private final Converter keyConverter;
    private final Deque<SinkTask> taskQueue = new ArrayDeque<>();
    private final Converter valueConverter;
    private final Plugins plugins = new Plugins(Map.of());

    ParallelStreamProcessor<byte[], byte[]> parallelConsumer;

    public ConnectExampleApp() {
        Map<String, String> connProps = Map.of();
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
//        SinkConnector connector = new FileStreamSinkConnector();
        SinkConnector connector = new MockSinkConnector();

        List<Map<String, String>> taskConfigs = connector.taskConfigs(10);

//        onAssigned(sinkTask);
//        onLost(sinkTask);

        //            SinkConnectorConfig sinkConfig = new SinkConnectorConfig(plugins, connConfig.originalsStrings());

        StreamEx.of(taskConfigs)
                .map(config -> startTask(connector, config))
                .forEach(this.taskQueue::offer);

        this.parallelConsumer = setupParallelConsumer();

        // tag::example[]
        parallelConsumer.poll(context -> {
                    log.info("Concurrently processing a context: {}", context);
                    SinkTask task = aquireSinkTask();
                    try {
                        var records = context.getConsumerRecordsFlattened();
                        var sinkRecords = StreamEx.of(records)
                                .map(this::convertRecordToSinkRecord)
                                .toList();
                        task.put(sinkRecords);
                    } finally {
                        this.taskQueue.offer(task);
                    }
                }
        );
        // end::example[]

//        stop(file);
    }

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

    private SinkTask aquireSinkTask() {
        return this.taskQueue.poll();
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

//    protected void postSetup() {
//        // ignore
//    }

    @SuppressWarnings({"FeatureEnvy"})
    ParallelStreamProcessor<byte[], byte[]> setupParallelConsumer() {
        // tag::exampleSetup[]
        var kafkaConsumer = getKafkaConsumer(); // <1>
        var kafkaProducer = getKafkaProducer();

        var options = ParallelConsumerOptions.<byte[], byte[]>builder()
                .ordering(KEY) // <2>
                .maxConcurrency(1000) // <3>
                .consumer(kafkaConsumer)
                .producer(kafkaProducer)
                .build();

        var eosStreamProcessor =
                ParallelStreamProcessor.createEosStreamProcessor(options);

        eosStreamProcessor.subscribe(of(inputTopic)); // <4>

        return eosStreamProcessor;
        // end::exampleSetup[]
    }

    void close() {
        this.parallelConsumer.close();
    }

//    void runPollAndProduce() {
//        this.parallelConsumer = setupParallelConsumer();
//
//        postSetup();
//
//        // tag::exampleProduce[]
//        parallelConsumer.pollAndProduce(context -> {
//                    var consumerRecord = context.getSingleRecord().getConsumerRecord();
//                    var result = processBrokerRecord(consumerRecord);
//                    return new ProducerRecord<>(outputTopic, consumerRecord.key(), result.payload);
//                }, consumeProduceResult -> {
//                    log.debug("Message {} saved to broker at offset {}",
//                            consumeProduceResult.getOut(),
//                            consumeProduceResult.getMeta().offset());
//                }
//        );
//        // end::exampleProduce[]
//    }

//    private Result processBrokerRecord(ConsumerRecord<String, String> consumerRecord) {
//        return new Result("Some payload from " + consumerRecord.value());
//    }

//    void customRetryDelay() {
//        // tag::customRetryDelay[]
//        final double multiplier = 0.5;
//        final int baseDelaySecond = 1;
//
//        ParallelConsumerOptions.<String, String>builder()
//                .retryDelayProvider(recordContext -> {
//                    int numberOfFailedAttempts = recordContext.getNumberOfFailedAttempts();
//                    long delayMillis = (long) (baseDelaySecond * Math.pow(multiplier, numberOfFailedAttempts) * 1000);
//                    return Duration.ofMillis(delayMillis);
//                });
//        // end::customRetryDelay[]
//    }
//
//    void maxRetries() {
//        ParallelStreamProcessor<String, String> pc = ParallelStreamProcessor.createEosStreamProcessor(null);
//        // tag::maxRetries[]
//        final int maxRetries = 10;
//        final Map<ConsumerRecord<String, String>, Long> retriesCount = new ConcurrentHashMap<>();
//
//        pc.poll(context -> {
//            var consumerRecord = context.getSingleRecord().getConsumerRecord();
//            Long retryCount = retriesCount.computeIfAbsent(consumerRecord, ignore -> 0L);
//            if (retryCount < maxRetries) {
//                processRecord(consumerRecord);
//                // no exception, so completed - remove from map
//                retriesCount.remove(consumerRecord);
//            } else {
//                log.warn("Retry count {} exceeded max of {} for record {}", retryCount, maxRetries, consumerRecord);
//                // giving up, remove from map
//                retriesCount.remove(consumerRecord);
//            }
//        });
//        // end::maxRetries[]
//    }
//
//    private void processRecord(final ConsumerRecord<String, String> record) {
//        // no-op
//    }
//
//    void circuitBreaker() {
//        ParallelStreamProcessor<String, String> pc = ParallelStreamProcessor.createEosStreamProcessor(null);
//        // tag::circuitBreaker[]
//        final Map<String, Boolean> upMap = new ConcurrentHashMap<>();
//
//        pc.poll(context -> {
//            var consumerRecord = context.getSingleRecord().getConsumerRecord();
//            String serverId = extractServerId(consumerRecord);
//            boolean up = upMap.computeIfAbsent(serverId, ignore -> true);
//
//            if (!up) {
//                up = updateStatusOfSever(serverId);
//            }
//
//            if (up) {
//                try {
//                    processRecord(consumerRecord);
//                } catch (CircuitBreakingException e) {
//                    log.warn("Server {} is circuitBroken, will retry message when server is up. Record: {}", serverId, consumerRecord);
//                    upMap.put(serverId, false);
//                }
//                // no exception, so set server status UP
//                upMap.put(serverId, true);
//            } else {
//                throw new RuntimeException(msg("Server {} currently down, will retry record latter {}", up, consumerRecord));
//            }
//        });
//        // end::circuitBreaker[]
//    }

//    private boolean updateStatusOfSever(final String serverId) {
//        return false;
//    }
//
//    private String extractServerId(final ConsumerRecord<String, String> consumerRecord) {
//        // no-op
//        return null;
//    }

//    void batching() {
//        // tag::batching[]
//        ParallelStreamProcessor.createEosStreamProcessor(ParallelConsumerOptions.<String, String>builder()
//                .consumer(getKafkaConsumer())
//                .producer(getKafkaProducer())
//                .maxConcurrency(100)
//                .batchSize(5) // <1>
//                .build());
//        parallelConsumer.poll(context -> {
//            // convert the batch into the payload for our processing
//            List<String> payload = context.stream()
//                    .map(this::preparePayload)
//                    .collect(Collectors.toList());
//            // process the entire batch payload at once
//            processBatchPayload(payload);
//        });
//        // end::batching[]
//    }

//    private void processBatchPayload(List<String> batchPayload) {
//        // example
//    }

//    private String preparePayload(RecordContext<String, String> rc) {
//        ConsumerRecord<String, String> consumerRecords = rc.getConsumerRecord();
//        int failureCount = rc.getNumberOfFailedAttempts();
//        return msg("{}, {}", consumerRecords, failureCount);
//    }

    @Value
    static class Result {
        String payload;
    }

}
