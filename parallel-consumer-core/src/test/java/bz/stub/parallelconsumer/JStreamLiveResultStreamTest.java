package bz.stub.parallelconsumer;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.SneakyThrows;
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
import static java.time.Duration.ofSeconds;
import static org.awaitility.Awaitility.await;
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
     * The empty queue is <b>established, not raced for</b>. The user function is held shut until the consumer
     * thread is parked inside the spliterator with nothing to hand it, so no result can exist before the
     * consumer has met an empty queue. Against the old bridge that state is unreachable - {@code tryAdvance}
     * returned {@code false} on the first empty poll, so the thread ran to completion instead of parking - and
     * the wait for it below is what goes red.
     * <p>
     * An earlier version of this test counted a latch down immediately before {@code forEach}, which does not
     * establish the same thing: the count-down happens before the stream is entered, and the primed record can
     * already have been produced by then, so the old bridge could deliver that one result and exit with every
     * assertion still passing.
     */
    @Timeout(30)
    @Test
    void streamDeliversResultsProducedAfterTheConsumerStarts() {
        List<Object> received = new CopyOnWriteArrayList<>();
        var releaseResult = new CountDownLatch(1);

        var stream = streaming.pollProduceAndStream(record -> {
            log.debug("Processing {}", record);
            // Nothing may reach the result queue until the consumer is demonstrably waiting on an empty one.
            // Bounded, so a mistake here fails the test rather than stranding this worker through teardown.
            assertThat(awaitRelease(releaseResult)).isTrue();
            return Lists.list(mock(org.apache.kafka.clients.producer.ProducerRecord.class));
        });

        var consumer = new Thread(() -> stream.forEach(received::add), "test-result-consumer");
        consumer.start();

        // The assertion, expressed as a wait: reaching this state IS surviving an empty queue.
        await("the result consumer to park on an empty queue")
                .atMost(ofSeconds(10))
                .until(() -> isParked(consumer));
        assertThat(received).isEmpty();

        // only now can a result exist at all - and it still has to be delivered
        releaseResult.countDown();
        awaitUntilTrue(() -> !received.isEmpty());

        // and the stream is still open, waiting for more, rather than finished
        assertThat(consumer.isAlive()).isTrue();

        streaming.closeDrainFirst();
        joinConsumer(consumer);
        assertThat(consumer.isAlive()).isFalse();
    }

    /**
     * Parked in {@code QueueSpliterator.tryAdvance}'s timed poll. {@code forEach} does nothing else that
     * waits, so this state can only be the poll - and a consumer that ended the stream instead is
     * {@code TERMINATED}, which never satisfies it.
     */
    private static boolean isParked(Thread thread) {
        var state = thread.getState();
        return state == Thread.State.WAITING || state == Thread.State.TIMED_WAITING;
    }

    @SneakyThrows
    private static boolean awaitRelease(CountDownLatch latch) {
        return latch.await(20, TimeUnit.SECONDS);
    }

    @SneakyThrows
    private static void joinConsumer(Thread consumer) {
        consumer.join(TimeUnit.SECONDS.toMillis(20));
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
