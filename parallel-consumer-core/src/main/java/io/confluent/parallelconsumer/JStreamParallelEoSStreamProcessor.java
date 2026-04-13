package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.csid.utils.Java8StreamUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
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
            this.userProcessResultsStream.add(result);
            if (userProcessResultsStream.size() > 10_000 && userProcessResultsStream.size() % 10_000 == 0) {
                log.warn("Result stream backlog: {} items. Unconsumed results accumulate in memory. " +
                        "See https://github.com/confluentinc/parallel-consumer/issues/912", userProcessResultsStream.size());
            }
        });

        return this.stream;
    }

    /**
     * Clears any unconsumed results from the deque on close to prevent memory leaks.
     *
     * @see <a href="https://github.com/confluentinc/parallel-consumer/issues/912">confluentinc/parallel-consumer#912</a>
     */
    @Override
    public void close() {
        int remaining = userProcessResultsStream.size();
        if (remaining > 0) {
            log.info("Clearing {} unconsumed results from stream on close", remaining);
        }
        userProcessResultsStream.clear();
        super.close();
    }

}
