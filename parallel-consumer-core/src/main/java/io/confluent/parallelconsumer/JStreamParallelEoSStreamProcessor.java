package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import io.confluent.csid.utils.Java8StreamUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.List;
import io.confluent.parallelconsumer.internal.DrainingCloseable.DrainingMode;
import io.confluent.parallelconsumer.internal.JStreamResultDeques;

import java.time.Duration;

import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * @deprecated Being removed — the JStream interface is not widely used and its unbounded result deque
 * can cause memory leaks if the stream is not actively consumed. Use the callback-based API instead.
 * See <a href="https://github.com/confluentinc/parallel-consumer/issues/912">confluentinc/parallel-consumer#912</a>.
 */
@Slf4j
@Deprecated
public class JStreamParallelEoSStreamProcessor<K, V> extends ParallelEoSStreamProcessor<K, V> implements JStreamParallelStreamProcessor<K, V> {

    private final Stream<ConsumeProduceResult<K, V, K, V>> stream;

    private final AtomicLong resultsAdded = new AtomicLong();

    private final ConcurrentLinkedDeque<ConsumeProduceResult<K, V, K, V>> userProcessResultsStream;

    public JStreamParallelEoSStreamProcessor(ParallelConsumerOptions<K, V> parallelConsumerOptions) {
        super(parallelConsumerOptions);

        this.userProcessResultsStream = new ConcurrentLinkedDeque<>();

        this.stream = Java8StreamUtils.setupStreamFromDeque(this.userProcessResultsStream);
    }

    @Override
    public Stream<ConsumeProduceResult<K, V, K, V>> pollProduceAndStream(Function<PollContext<K, V>, List<ProducerRecord<K, V>>> userFunction) {
        super.pollAndProduceMany(userFunction, result -> {
            log.trace("Wrapper callback applied, sending result to stream. Input: {}", result);
            JStreamResultDeques.addAndWarnIfBacklogged(this.userProcessResultsStream, this.resultsAdded, result);
        });

        return this.stream;
    }

    /**
     * Clears any unconsumed results from the deque once shutdown completes, so closing actually releases them.
     * <p>
     * This overrides {@link DrainingMode}-taking close rather than the no-arg {@code close()}, because that is
     * the single method every other entry point funnels through - {@code close()},
     * {@code closeDrainFirst()}, {@code closeDontDrainFirst()} and the {@link Duration} variants all end up
     * here. Overriding the no-arg version would leave every other shutdown path leaking.
     * <p>
     * The clear happens <b>after</b> the shutdown completes, not before: a {@link DrainingMode#DRAIN} close
     * keeps processing in-flight work, and that work enqueues more results.
     *
     * @see <a href="https://github.com/confluentinc/parallel-consumer/issues/912">confluentinc/parallel-consumer#912</a>
     */
    @Override
    public void close(DrainingMode drainMode) {
        super.close(drainMode);
        JStreamResultDeques.clearOnClose(userProcessResultsStream);
    }

}
