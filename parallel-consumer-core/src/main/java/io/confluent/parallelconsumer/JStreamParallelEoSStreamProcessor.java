package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import io.confluent.parallelconsumer.internal.DrainingCloseable.DrainingMode;
import io.confluent.parallelconsumer.internal.JStreamResultBuffer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Duration;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * @deprecated Superseded by the callback-based API, which does the same job without a result buffer.
 * The buffer here is now bounded and applies backpressure (see {@link JStreamResultBuffer}), so it no
 * longer leaks, but it remains a second way to do what {@link ParallelEoSStreamProcessor} already does.
 * See <a href="https://github.com/confluentinc/parallel-consumer/issues/912">confluentinc/parallel-consumer#912</a>.
 */
@Slf4j
@Deprecated
public class JStreamParallelEoSStreamProcessor<K, V> extends ParallelEoSStreamProcessor<K, V> implements JStreamParallelStreamProcessor<K, V> {

    private final JStreamResultBuffer<ConsumeProduceResult<K, V, K, V>> results;

    public JStreamParallelEoSStreamProcessor(ParallelConsumerOptions<K, V> parallelConsumerOptions) {
        this(parallelConsumerOptions, JStreamResultBuffer.DEFAULT_CAPACITY);
    }

    /**
     * @param resultBufferCapacity how many unconsumed results to hold before the producer is made to wait
     */
    public JStreamParallelEoSStreamProcessor(ParallelConsumerOptions<K, V> parallelConsumerOptions, int resultBufferCapacity) {
        super(parallelConsumerOptions);

        this.results = new JStreamResultBuffer<>(resultBufferCapacity);
    }

    @Override
    public Stream<ConsumeProduceResult<K, V, K, V>> pollProduceAndStream(Function<PollContext<K, V>, List<ProducerRecord<K, V>>> userFunction) {
        super.pollAndProduceMany(userFunction, result -> {
            log.trace("Wrapper callback applied, sending result to stream. Input: {}", result);
            results.add(result);
        });

        return results.getStream();
    }

    /**
     * Ends the result stream once shutdown completes, so a consuming {@code forEach} returns instead of
     * blocking forever.
     * <p>
     * This overrides the {@link DrainingMode}-taking close rather than the no-arg {@code close()}, because
     * that is the single method every other entry point funnels through - {@code close()},
     * {@code closeDrainFirst()}, {@code closeDontDrainFirst()} and the {@link Duration} variants all end up
     * here. Overriding the no-arg version would leave every other shutdown path hanging.
     * <p>
     * The close happens <b>after</b> the shutdown completes, not before: a {@link DrainingMode#DRAIN} close
     * keeps processing in-flight work, and that work produces more results which the consumer should still
     * receive.
     *
     * @see <a href="https://github.com/confluentinc/parallel-consumer/issues/912">confluentinc/parallel-consumer#912</a>
     */
    @Override
    public void close(DrainingMode drainMode) {
        super.close(drainMode);
        results.close();
    }

}
