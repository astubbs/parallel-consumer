package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020-2026 Antony Stubbs and contributors
 */

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.assertj.core.util.Lists;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static com.google.common.truth.Truth.assertThat;
import static io.confluent.csid.utils.LatchTestUtils.awaitLatch;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.mock;

/**
 * Regression tests for astubbs#122 / confluentinc#912, at the processor level.
 * <p>
 * The reported bug was unbounded growth of the result buffer when the returned {@link Stream} is not
 * consumed. The buffer is now bounded and blocking ({@link io.confluent.parallelconsumer.internal.JStreamResultBuffer},
 * which has its own unit tests) - so what matters <em>here</em> is the wiring: that closing the processor
 * ends the stream, through <b>every</b> close entry point.
 * <p>
 * That last part is the trap this class exists to guard. An earlier fix overrode only the no-arg
 * {@code close()}, and {@code closeDrainFirst()} - the shutdown the shipped Vert.x example calls - routes
 * to {@code close(DrainingMode)} and bypassed it entirely. With a blocking buffer the cost of getting that
 * wrong went up: it is no longer a leak, it is a consumer thread that never returns.
 *
 * @see <a href="https://github.com/confluentinc/parallel-consumer/issues/912">confluentinc/parallel-consumer#912</a>
 */
@Slf4j
class JStreamMemoryLeak912Test extends ParallelEoSStreamProcessorTestBase {

    private static final int TIMEOUT_SECONDS = 30;

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
     * {@code close()} must end the stream so a consuming {@code forEach} returns.
     */
    @Test
    @Timeout(TIMEOUT_SECONDS)
    void closeEndsTheStream() throws Exception {
        CountDownLatch consumerFinished = consumeInBackground(produceOneResult());

        streaming.close();

        assertThat(consumerFinished.await(TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();
    }

    /**
     * The funnel guard. {@code closeDrainFirst()} never calls the no-arg {@code close()}, so a fix applied
     * there leaves this path hanging forever. Overriding the wrong method fails this test.
     */
    @Test
    @Timeout(TIMEOUT_SECONDS)
    void closeDrainFirstAlsoEndsTheStream() throws Exception {
        CountDownLatch consumerFinished = consumeInBackground(produceOneResult());

        streaming.closeDrainFirst();

        assertThat(consumerFinished.await(TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();
    }

    /**
     * Closing with nothing ever produced must still terminate a waiting consumer, rather than leaving it
     * parked on a buffer that will never fill.
     */
    @Test
    @Timeout(TIMEOUT_SECONDS)
    void closeEndsTheStreamWhenNothingWasEverProduced() throws Exception {
        Stream<?> stream = streaming.pollProduceAndStream((record) -> Lists.list(mock(ProducerRecord.class)));
        CountDownLatch consumerFinished = consumeInBackground(stream);

        streaming.close();

        assertThat(consumerFinished.await(TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();
    }

    /**
     * Results already buffered when close is called must still reach the consumer - a draining close is
     * supposed to hand over its work, not bin it.
     */
    @Test
    @Timeout(TIMEOUT_SECONDS)
    void resultsProducedBeforeCloseAreStillDelivered() throws Exception {
        Stream<?> stream = produceOneResult();

        var received = new CopyOnWriteArrayList<Object>();
        var consumerFinished = new CountDownLatch(1);
        var consumer = new Thread(() -> {
            stream.forEach(received::add);
            consumerFinished.countDown();
        }, "test-result-consumer");
        consumer.start();

        await().until(() -> !received.isEmpty());

        streaming.closeDrainFirst();

        assertThat(consumerFinished.await(TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();
        assertThat(received).isNotEmpty();
    }

    /**
     * Runs one record through the processor and returns the stream, once the result has actually been
     * produced.
     * <p>
     * The latch fires <em>inside</em> the user function, but the wrapper only enqueues the result after
     * that function returns - so waiting on control-loop cycles instead raced the enqueue and saw an empty
     * buffer under a loaded parallel suite.
     */
    private Stream<?> produceOneResult() {
        var latch = new CountDownLatch(1);
        Stream<?> stream = streaming.pollProduceAndStream((record) -> {
            log.info("Processing record: {}", record);
            myRecordProcessingAction.apply(record.getSingleConsumerRecord());
            latch.countDown();
            return Lists.list(mock(ProducerRecord.class));
        });

        awaitLatch(latch);
        return stream;
    }

    private CountDownLatch consumeInBackground(Stream<?> stream) {
        var consumerFinished = new CountDownLatch(1);
        var consumer = new Thread(() -> {
            stream.forEach(x -> log.debug("Consumed result: {}", x));
            consumerFinished.countDown();
        }, "test-result-consumer");
        consumer.start();
        return consumerFinished;
    }
}
