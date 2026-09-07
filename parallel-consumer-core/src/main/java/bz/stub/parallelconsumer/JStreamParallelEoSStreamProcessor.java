package bz.stub.parallelconsumer;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.internal.utils.Java8StreamUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Function;
import java.util.stream.Stream;

@Slf4j
public class JStreamParallelEoSStreamProcessor<K, V> extends ParallelEoSStreamProcessor<K, V> implements JStreamParallelStreamProcessor<K, V> {

    private final Stream<ConsumeProduceResult<K, V, K, V>> stream;

    /**
     * Results waiting for the caller to take them off {@link #stream}.
     * <p>
     * Blocking because the stream waits here rather than ending when it finds nothing; {@link
     * LinkedBlockingQueue} specifically because its {@code size()} is a counter rather than a walk, which a
     * gauge over this buffer would need. Its enqueue takes a lock where the previous
     * {@code ConcurrentLinkedDeque} was lock-free, which is not worth avoiding: each addition follows a Kafka
     * produce that dominates it.
     * <p>
     * It has no capacity, so a consumer that keeps up holds nothing while one that is merely slower than the
     * producer still grows it. What is fixed is the consumer that stops taking at all.
     */
    private final BlockingQueue<ConsumeProduceResult<K, V, K, V>> userProcessResultsStream;

    public JStreamParallelEoSStreamProcessor(ParallelConsumerOptions<K, V> parallelConsumerOptions) {
        super(parallelConsumerOptions);

        this.userProcessResultsStream = new LinkedBlockingQueue<>();

        this.stream = Java8StreamUtils.setupStreamFromQueue(this.userProcessResultsStream, this::isClosedOrFailed);
    }

    @Override
    public Stream<ConsumeProduceResult<K, V, K, V>> pollProduceAndStream(Function<PollContext<K, V>, List<ProducerRecord<K, V>>> userFunction) {
        super.pollAndProduceMany(userFunction, result -> {
            log.trace("Wrapper callback applied, sending result to stream. Input: {}", result);
            this.userProcessResultsStream.add(result);
        });

        return this.stream;
    }

}
