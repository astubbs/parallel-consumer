package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020-2021 Confluent, Inc.
 */

import io.confluent.parallelconsumer.internal.BrokerPollSystem;
import io.confluent.parallelconsumer.internal.ConsumerManager;
import io.confluent.parallelconsumer.internal.ProducerManager;
import io.confluent.parallelconsumer.state.WorkManager;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import pl.tlinkowski.unij.api.UniLists;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;

import static io.confluent.parallelconsumer.internal.UserFunctions.carefullyRun;

/**
 * @see ParallelConsumer
 */
@Slf4j
public class ParallelEoSStreamProcessor<K, V> extends AbstractParallelStreamProcessor<K, V> {

    /**
     * Construct the AsyncConsumer by wrapping this passed in conusmer and producer, which can be configured any which
     * way as per normal.
     *
     * @see ParallelConsumerOptions
     */
    public ParallelEoSStreamProcessor(ParallelConsumerOptions newOptions) {
        Objects.requireNonNull(newOptions, "Options must be supplied");

        log.info("Confluent Parallel Consumer initialise... Options: {}", newOptions);

        options = newOptions;
        options.validate();

        this.consumer = options.getConsumer();

        checkGroupIdConfigured(consumer);
        checkNotSubscribed(consumer);
        checkAutoCommitIsDisabled(consumer);

        workerPool = setupWorkerPool(newOptions.getMaxConcurrency());

        this.wm = new WorkManager<K, V>(newOptions, consumer, dynamicExtraLoadFactor);

        ConsumerManager<K, V> consumerMgr = new ConsumerManager<>(consumer);

        this.brokerPollSubsystem = new BrokerPollSystem<>(consumerMgr, wm, this, newOptions);

        if (options.isProducerSupplied()) {
            this.producerManager = Optional.of(new ProducerManager<>(options.getProducer(), consumerMgr, this.wm, options));
            if (options.isUsingTransactionalProducer())
                this.committer = this.producerManager.get();
            else
                this.committer = this.brokerPollSubsystem;
        } else {
            this.producerManager = Optional.empty();
            this.committer = this.brokerPollSubsystem;
        }
    }

    @Override
    public void poll(Consumer<ConsumerRecord<K, V>> usersVoidConsumptionFunction) {
        Function<ConsumerRecord<K, V>, List<Object>> wrappedUserFunc = (record) -> {
            log.trace("asyncPoll - Consumed a record ({}), executing void function...", record.offset());

            carefullyRun(usersVoidConsumptionFunction, record);

            log.trace("asyncPoll - user function finished ok.");
            return UniLists.of(); // user function returns no produce records, so we satisfy our api
        };
        Consumer<Object> voidCallBack = (ignore) -> log.trace("Void callback applied.");
        supervisorLoop(wrappedUserFunc, voidCallBack);
    }

    @Override
    @SneakyThrows
    public void pollAndProduceMany(Function<ConsumerRecord<K, V>, List<ProducerRecord<K, V>>> userFunction,
                                   Consumer<ConsumeProduceResult<K, V, K, V>> callback) {
        // todo refactor out the producer system to a sub class
        if (!options.isProducerSupplied()) {
            throw new IllegalArgumentException("To use the produce flows you must supply a Producer in the options");
        }

        // wrap user func to add produce function
        Function<ConsumerRecord<K, V>, List<ConsumeProduceResult<K, V, K, V>>> wrappedUserFunc = (consumedRecord) -> {

            List<ProducerRecord<K, V>> recordListToProduce = carefullyRun(userFunction, consumedRecord);

            if (recordListToProduce.isEmpty()) {
                log.debug("No result returned from function to send.");
            }
            log.trace("asyncPoll and Stream - Consumed a record ({}), and returning a derivative result record to be produced: {}", consumedRecord, recordListToProduce);

            List<ConsumeProduceResult<K, V, K, V>> results = new ArrayList<>();
            log.trace("Producing {} messages in result...", recordListToProduce.size());
            for (ProducerRecord<K, V> toProduce : recordListToProduce) {
                log.trace("Producing {}", toProduce);
                RecordMetadata produceResultMeta = producerManager.get().produceMessage(toProduce);
                var result = new ConsumeProduceResult<>(consumedRecord, toProduce, produceResultMeta);
                results.add(result);
            }
            return results;
        };

        supervisorLoop(wrappedUserFunc, callback);
    }

    @Override
    @SneakyThrows
    public void pollAndProduceMany(Function<ConsumerRecord<K, V>, List<ProducerRecord<K, V>>> userFunction) {
        pollAndProduceMany(userFunction, (record) -> {
            // no op call back
            log.trace("No-op user callback");
        });
    }

    @Override
    @SneakyThrows
    public void pollAndProduce(Function<ConsumerRecord<K, V>, ProducerRecord<K, V>> userFunction) {
        pollAndProduce(userFunction, (record) -> {
            // no op call back
            log.trace("No-op user callback");
        });
    }

    @Override
    @SneakyThrows
    public void pollAndProduce(Function<ConsumerRecord<K, V>, ProducerRecord<K, V>> userFunction,
                               Consumer<ConsumeProduceResult<K, V, K, V>> callback) {
        pollAndProduceMany((record) -> UniLists.of(userFunction.apply(record)), callback);
    }

    /**
     * Supervisor loop for the main loop.
     *
     * @see #supervisorLoop(Function, Consumer)
     */

}
