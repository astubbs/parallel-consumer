package io.confluent.parallelconsumer;

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
import static io.confluent.csid.utils.LatchTestUtils.awaitLatch;
import static org.mockito.Mockito.mock;

/**
 * Regression tests for #912 — JStream result deque memory leak.
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
     * Core regression test: after producing results and closing, the deque must be empty.
     * Before the fix, close() did not clear the deque.
     */
    @Test
    void closeShouldClearResultDeque() {
        var latch = new CountDownLatch(1);
        // Produce a result but don't consume the stream
        streaming.pollProduceAndStream((record) -> {
            log.info("Processing record: {}", record);
            myRecordProcessingAction.apply(record.getSingleConsumerRecord());
            latch.countDown();
            return Lists.list(mock(ProducerRecord.class));
        });

        awaitLatch(latch);

        ConcurrentLinkedDeque<?> deque = getResultDeque();

        // Wait for the result to REACH the deque, rather than for a number of control-loop cycles.
        // The latch fires inside the user function, but the wrapper callback only enqueues the result
        // after that function returns - so cycle-counting raced the enqueue and went empty under a
        // loaded parallel suite. Nothing drains the deque here (the stream is deliberately never
        // consumed), so once non-empty it stays non-empty.
        awaitUntilTrue(() -> !deque.isEmpty());
        assertThat(deque).isNotEmpty();

        // Close should clear the deque
        streaming.close();

        assertThat(deque).isEmpty();
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
