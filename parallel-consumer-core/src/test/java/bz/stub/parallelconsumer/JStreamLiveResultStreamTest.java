package bz.stub.parallelconsumer;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.extern.slf4j.Slf4j;
import org.assertj.core.util.Lists;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Mockito.mock;

/**
 * The result stream is live: it waits for results rather than ending the moment the queue is momentarily
 * empty, and it ends when the processor closes.
 * <p>
 * confluentinc#912 was an OOM, and this is the behaviour behind it. The old bridge returned {@code false}
 * from {@code tryAdvance} on the first empty poll, which a {@link java.util.stream.Spliterator} defines as
 * "no more, ever" - so the caller's terminal operation finished almost immediately and every result produced
 * afterwards piled up with nobody left to drain it.
 *
 * @author Antony Stubbs
 * @see <a href="https://github.com/astubbs/parallel-consumer/issues/122">astubbs#122</a>
 * @see <a href="https://github.com/confluentinc/parallel-consumer/issues/912">confluentinc#912</a>
 */
@Slf4j
class JStreamLiveResultStreamTest extends ParallelEoSStreamProcessorTestBase {

    JStreamParallelEoSStreamProcessor<String, String> streaming;

    @BeforeEach
    public void setupData() {
        super.primeFirstRecord();
    }

    @Override
    protected ParallelEoSStreamProcessor<String, String> initAsyncConsumer(ParallelConsumerOptions<String, String> options) {
        streaming = new JStreamParallelEoSStreamProcessor<>(options);
        return streaming;
    }

    /**
     * The regression: a consumer that starts before any result exists must still receive results produced
     * later, rather than seeing an immediately-finished stream.
     * <p>
     * Against the old bridge this fails with zero results collected - the consumer thread finishes before the
     * first record is even processed.
     */
    @Timeout(30)
    @Test
    void streamDeliversResultsProducedAfterTheConsumerStarts() throws Exception {
        List<Object> received = new CopyOnWriteArrayList<>();
        var consumerReady = new CountDownLatch(1);

        var stream = streaming.pollProduceAndStream(record -> {
            log.debug("Processing {}", record);
            return Lists.list(mock(org.apache.kafka.clients.producer.ProducerRecord.class));
        });

        var consumer = new Thread(() -> {
            consumerReady.countDown();
            stream.forEach(received::add);
        }, "test-result-consumer");
        consumer.start();
        assertThat(consumerReady.await(10, TimeUnit.SECONDS)).isTrue();

        // the record primed in setup is produced after the consumer is already waiting
        awaitUntilTrue(() -> !received.isEmpty());
        assertThat(received).isNotEmpty();

        // and the stream is still open, waiting for more, rather than finished
        assertThat(consumer.isAlive()).isTrue();

        streaming.closeDrainFirst();
        consumer.join(TimeUnit.SECONDS.toMillis(20));
        assertThat(consumer.isAlive()).isFalse();
    }

    /**
     * Closing is what ends the stream, so a consumer parked waiting for the next result has to be released -
     * otherwise the caller's terminal operation never returns.
     */
    @Timeout(30)
    @Test
    void closingEndsAStreamThatIsWaiting() throws Exception {
        var stream = streaming.pollProduceAndStream(record ->
                Lists.list(mock(org.apache.kafka.clients.producer.ProducerRecord.class)));

        var finished = new CountDownLatch(1);
        var consumer = new Thread(() -> {
            stream.forEach(r -> log.debug("Got {}", r));
            finished.countDown();
        }, "test-result-consumer");
        consumer.start();

        // let it settle into waiting, with nothing left to hand it
        Thread.sleep(500);
        assertThat(finished.getCount()).isEqualTo(1);

        streaming.closeDrainFirst();

        assertThat(finished.await(20, TimeUnit.SECONDS)).isTrue();
    }
}
