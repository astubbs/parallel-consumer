package bz.stub.parallelconsumer;

/*-
 * Copyright (C) 2020-2026 Antony Stubbs and contributors
 */

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.assertj.core.util.Lists;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CountDownLatch;

import static com.google.common.truth.Truth.assertThat;
import static bz.stub.parallelconsumer.internal.utils.LatchTestUtils.awaitLatch;
import static org.mockito.Mockito.mock;

/**
 * Regression tests for astubbs#122 / confluentinc#912 - the JStream result deque memory leak.
 * <p>
 * The bug: {@code ConcurrentLinkedDeque<ConsumeProduceResult>} grows without bound when
 * the returned Stream is not actively consumed. On close, the deque was never cleared,
 * leaving all accumulated results in memory until GC.
 *
 * @see <a href="https://github.com/confluentinc/parallel-consumer/issues/912">confluentinc/parallel-consumer#912</a>
 */
@Slf4j
class JStreamMemoryLeak912Test extends ParallelEoSStreamProcessorTestBase {

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
     * Core regression test: after producing a result and closing, the deque must be empty.
     * Before the fix, close() never cleared it.
     */
    @Test
    void closeShouldClearResultDeque() {
        ConcurrentLinkedDeque<?> deque = produceOneResultAndAwaitIt();

        streaming.close();

        assertThat(deque).isEmpty();
    }

    /**
     * The leak survived the first version of this fix because only the no-arg {@code close()} was
     * overridden, while {@code closeDrainFirst()} - the shutdown the shipped Vertx example app uses -
     * routes to {@code close(DrainingMode)} and never reached it. This guards the funnel rather than one
     * entry point, so overriding the wrong method fails here.
     */
    @Test
    void closeDrainFirstShouldAlsoClearResultDeque() {
        ConcurrentLinkedDeque<?> deque = produceOneResultAndAwaitIt();

        streaming.closeDrainFirst();

        assertThat(deque).isEmpty();
    }

    /**
     * Runs one record through the processor without consuming the returned stream, and returns the deque
     * once the result has actually landed in it.
     * <p>
     * The wait is on the deque itself rather than on control-loop cycles: the latch fires <em>inside</em>
     * the user function, but the wrapper only enqueues the result after that function returns, so
     * cycle-counting raced the enqueue and saw an empty deque under a loaded parallel suite. Nothing
     * drains the deque here - the stream is deliberately never consumed - so once non-empty it stays so.
     */
    private ConcurrentLinkedDeque<?> produceOneResultAndAwaitIt() {
        var latch = new CountDownLatch(1);
        streaming.pollProduceAndStream((record) -> {
            log.info("Processing record: {}", record);
            myRecordProcessingAction.apply(record.getSingleConsumerRecord());
            latch.countDown();
            return Lists.list(mock(ProducerRecord.class));
        });

        awaitLatch(latch);

        ConcurrentLinkedDeque<?> deque = getResultDeque();
        awaitUntilTrue(() -> !deque.isEmpty());
        assertThat(deque).isNotEmpty();
        return deque;
    }

    /**
     * Verify that the deque is empty after close even with no results produced.
     */
    @Test
    void closeShouldWorkWithEmptyDeque() {
        ConcurrentLinkedDeque<?> deque = getResultDeque();
        assertThat(deque).isEmpty();

        streaming.close();

        assertThat(deque).isEmpty();
    }

    @SuppressWarnings("unchecked")
    private ConcurrentLinkedDeque<?> getResultDeque() {
        try {
            Field field = JStreamParallelEoSStreamProcessor.class.getDeclaredField("userProcessResultsStream");
            field.setAccessible(true);
            return (ConcurrentLinkedDeque<?>) field.get(streaming);
        } catch (Exception e) {
            throw new RuntimeException("Failed to access userProcessResultsStream field", e);
        }
    }
}
