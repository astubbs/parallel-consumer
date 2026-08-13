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
 * @deprecated Being removed - the JStream interface is not widely used and its unbounded result deque
 * can cause memory leaks if the stream is not actively consumed. Use the callback-based API instead.
 * <p>
 * The deprecation lives on this interface, not only on {@link JStreamParallelEoSStreamProcessor}, because
 * callers reach the implementation through {@link #createJStreamEosStreamProcessor} and hold this type -
 * so a deprecation on the implementation alone warns nobody before the advertised removal.
 * @see <a href="https://github.com/confluentinc/parallel-consumer/issues/912">confluentinc/parallel-consumer#912</a>
 */
@Deprecated
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
