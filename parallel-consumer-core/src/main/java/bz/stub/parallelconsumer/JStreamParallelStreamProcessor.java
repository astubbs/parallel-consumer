package bz.stub.parallelconsumer;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.internal.AbstractParallelEoSStreamProcessor;
import bz.stub.parallelconsumer.internal.DrainingCloseable;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * Streaming counterpart to {@link ParallelStreamProcessor}: results are handed back through a
 * {@link Stream} instead of a callback.
 * <p>
 * The stream is <b>live</b>. It yields each result as it is produced and blocks in between, so consuming it
 * is what keeps the backing queue drained; it ends when this processor closes, after the results already
 * queued have been delivered. Consume it on a thread that can stay with it - a consumer that walks away
 * early leaves the producer with nobody draining, which is
 * <a href="https://github.com/confluentinc/parallel-consumer/issues/912">confluentinc#912</a>.
 *
 * @see <a href="https://github.com/astubbs/parallel-consumer/issues/122">astubbs#122</a>
 */
public interface JStreamParallelStreamProcessor<K, V> extends DrainingCloseable {

    static <K, V> JStreamParallelStreamProcessor<K, V> createJStreamEosStreamProcessor(ParallelConsumerOptions<K, V> options) {
        return new JStreamParallelEoSStreamProcessor<>(options);
    }

    /**
     * Like {@link AbstractParallelEoSStreamProcessor#pollAndProduceMany} but instead of callbacks, streams the results
     * instead, after the produce result is ack'd by Kafka.
     *
     * @return a stream of results of applying the function to the polled records
     */
    Stream<ParallelStreamProcessor.ConsumeProduceResult<K, V, K, V>> pollProduceAndStream(
            Function<PollContext<K, V>,
                    List<ProducerRecord<K, V>>> userFunction);
}
