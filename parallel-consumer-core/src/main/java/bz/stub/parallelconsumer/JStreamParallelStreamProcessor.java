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
 * @deprecated Being removed. The deque behind {@link #pollProduceAndStream} is unbounded and drains only as
 * the caller consumes the returned stream, so a slow or absent consumer grows it until the JVM runs out of
 * memory. Take results through {@link ParallelStreamProcessor#pollAndProduceMany} and its callback instead.
 * <p>
 * Closing <b>discards the backlog</b>: every {@code close} entry point empties the deque once shutdown
 * finishes, so anything the caller never read is dropped rather than delivered. {@code closeDrainFirst()}
 * does <b>not</b> rescue it - draining finishes the queued <i>processing</i>, which enqueues further results,
 * and the clear still follows. Consuming the stream before, or concurrently with, shutdown is the only way
 * to keep results.
 * @see <a href="https://github.com/astubbs/parallel-consumer/issues/122">astubbs#122</a>
 * @see <a href="https://github.com/confluentinc/parallel-consumer/issues/912">confluentinc#912</a>
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
