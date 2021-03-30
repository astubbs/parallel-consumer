package io.confluent.parallelconsumer;

import lombok.SneakyThrows;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import pl.tlinkowski.unij.api.UniLists;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

import static io.confluent.parallelconsumer.UserFunctions.carefullyRun;

public class ParallelEoSStreamProcessor extends AbstractParallelStreamProcessor {
    /**
     * Construct the AsyncConsumer by wrapping this passed in conusmer and producer, which can be configured any which way
     * as per normal.
     *
     * @param newOptions
     * @see ParallelConsumerOptions
     */
    public ParallelEoSStreamProcessor(final ParallelConsumerOptions newOptions) {
        super(newOptions);
    }

    @Override
    public void poll(Consumer usersVoidConsumptionFunction) {
        Function wrappedUserFunc = (record) -> {
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
    public void pollAndProduceMany(Function userFunction,
                                   Consumer callback) {
        // todo refactor out the producer system to a sub class
        if (!options.isProducerSupplied()) {
            throw new IllegalArgumentException("To use the produce flows you must supply a Producer in the options");
        }

        // wrap user func to add produce function
        Function wrappedUserFunc = (consumedRecord) -> {

            List recordListToProduce = carefullyRun(userFunction, consumedRecord);

            if (recordListToProduce.isEmpty()) {
                log.debug("No result returned from function to send.");
            }
            log.trace("asyncPoll and Stream - Consumed a record ({}), and returning a derivative result record to be produced: {}", consumedRecord, recordListToProduce);

            List results = new ArrayList<>();
            log.trace("Producing {} messages in result...", recordListToProduce.size());
            for (ProducerRecord toProduce : recordListToProduce) {
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
    public void pollAndProduceMany(Function userFunction) {
        pollAndProduceMany(userFunction, (record) -> {
            // no op call back
            log.trace("No-op user callback");
        });
    }

    @Override
    @SneakyThrows
    public void pollAndProduce(Function userFunction) {
        pollAndProduce(userFunction, (record) -> {
            // no op call back
            log.trace("No-op user callback");
        });
    }

    @Override
    @SneakyThrows
    public void pollAndProduce(Function userFunction,
                               Consumer callback) {
        pollAndProduceMany((record) -> UniLists.of(userFunction.apply(record)), callback);
    }
}
