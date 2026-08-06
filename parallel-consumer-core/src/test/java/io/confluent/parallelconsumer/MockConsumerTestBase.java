package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020-2024 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import io.confluent.csid.utils.LongPollingMockConsumer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.ConcurrentLinkedQueue;

import static pl.tlinkowski.unij.api.UniLists.of;

/**
 * Harness for the tests that drive PC with the plain vanilla {@link MockConsumer}, rather than with
 * {@link LongPollingMockConsumer} and the {@link AbstractParallelEoSStreamProcessorTestBase} machinery built on
 * it.
 * <p>
 * <b>Why these tests do not use the main test base:</b> {@link AbstractParallelEoSStreamProcessorTestBase}
 * wires up a Mockito-spied {@link LongPollingMockConsumer} and a {@link org.apache.kafka.clients.producer.MockProducer
 * MockProducer}. The subject of <em>these</em> tests is what PC does when the consumer misbehaves in ways only a
 * hand-written {@link MockConsumer} subclass can express - {@code poll} or {@code commitSync} throwing, hanging,
 * or recovering on a clock - so they need the raw class, not the well-behaved wrapper.
 * <p>
 * <b>The manual rebalance dance.</b> {@link MockConsumer} is not a correct implementation of the {@code Consumer}
 * contract: subscribing does not produce an assignment, so nothing is ever polled unless the test rebalances the
 * partition in by hand <em>and</em> separately tells PC about it. {@link #setupMockConsumerAndParallelConsumer()}
 * below does exactly that, and it is the reason {@link LongPollingMockConsumer} exists - see
 * {@link LongPollingMockConsumer#revokeAssignment}.
 * <p>
 * <b>What a subclass supplies:</b> the failure behaviour under test ({@link #createMockConsumer()}), any options
 * the scenario needs ({@link #customiseOptions}), and its own assertions. Everything else - the topic, the
 * partition assignment, the PC lifecycle, the record feed and the teardown - is identical across the scenarios
 * and lives here, so that a new rejection or outage scenario is a subclass rather than another copy of the
 * wiring.
 * <p>
 * Assertions are deliberately <em>not</em> hoisted: each scenario keeps its own {@link Awaitility} block, with
 * its own timeout, in its own file. They are the point of the test, and their timeouts are scenario-specific
 * (they have to clear each scenario's simulated outage window).
 *
 * @author Antony Stubbs
 * @see LongPollingMockConsumer
 */
@Slf4j
// 120s of headroom for a suite whose scenarios simulate outages of up to 20s and then wait for recovery.
// @Timeout is @Inherited, so subclasses get this unless they declare their own. NB the value is SECONDS:
// the pre-refactor `@Timeout(60000L)` on three of these classes meant 60000 *seconds*, i.e. no timeout at
// all, which is how a hung MockConsumer test could have wedged CI until the job-level timeout fired.
@Timeout(120)
abstract class MockConsumerTestBase {

    /**
     * Distinct per subclass, so a leaked record from one scenario cannot be polled by another.
     */
    protected final String topic = getClass().getSimpleName();

    protected final TopicPartition topicPartition = new TopicPartition(topic, 0);

    /**
     * Records handed to the user function, in arrival order. Concurrent because PC's worker threads write it.
     */
    protected final ConcurrentLinkedQueue<RecordContext<String, String>> processedRecords = new ConcurrentLinkedQueue<>();

    protected MockConsumer<String, String> mockConsumer;

    protected ParallelEoSStreamProcessor<String, String> parallelConsumer;

    /**
     * The background record feed, if {@link #addRecordsInBackground} was used. Interrupted and joined on teardown.
     */
    private Thread recordAdder;

    /**
     * Override to inject the failure behaviour under test - typically an anonymous subclass overriding
     * {@code poll} and/or {@code commitSync}. Called once, before the options are built.
     */
    protected MockConsumer<String, String> createMockConsumer() {
        return new MockConsumer<>(OffsetResetStrategy.EARLIEST);
    }

    /**
     * Override to add the options the scenario needs (retry budgets, commit mode, commit interval). The consumer
     * is already set on the builder.
     */
    protected void customiseOptions(ParallelConsumerOptions.ParallelConsumerOptionsBuilder<String, String> builder) {
        // no-op by default
    }

    @BeforeEach
    void setupMockConsumerAndParallelConsumer() {
        mockConsumer = createMockConsumer();

        var optionsBuilder = ParallelConsumerOptions.<String, String>builder()
                .consumer(mockConsumer);
        customiseOptions(optionsBuilder);

        parallelConsumer = new ParallelEoSStreamProcessor<>(optionsBuilder.build());
        parallelConsumer.subscribe(of(topic));

        // MockConsumer is not a correct implementation of the Consumer contract - subscribing alone assigns
        // nothing, so the partition must be rebalanced in by hand, and PC told about it separately. This is the
        // difficulty LongPollingMockConsumer exists to remove.
        mockConsumer.rebalance(Collections.singletonList(topicPartition));
        parallelConsumer.onPartitionsAssigned(of(topicPartition));
        mockConsumer.updateBeginningOffsets(Collections.singletonMap(topicPartition, 0L));
    }

    @AfterEach
    void stopRecordFeedAndCloseParallelConsumer() throws InterruptedException {
        // First, and unconditionally: Awaitility's defaults are thread-local global state, so a per-test
        // override must not survive into another test under non-deterministic ordering (PIT surfaces this,
        // surefire's ordering masks it). Done before the close below, which can throw.
        Awaitility.reset();

        // Stop the feed BEFORE closing PC: once the mock consumer is closed, addRecord() throws
        // IllegalStateException, and an uncaught exception on a stray daemon thread gets attributed to
        // whatever test is running next in the same JVM (PIT's minion JVMs make this especially confusing).
        if (recordAdder != null) {
            recordAdder.interrupt();
            // it only ever sleeps in ~1s slices, so this joins promptly; the bound is to fail loudly rather
            // than hang the suite if it ever does not
            recordAdder.join(Duration.ofSeconds(10).toMillis());
            if (recordAdder.isAlive()) {
                log.error("Record adder thread {} did not stop after interrupt", recordAdder.getName());
            }
        }

        // A test may legitimately have closed it itself (that is the subject of the early-close scenario), or
        // PC may have failed - don't re-close, and don't bubble up an expected failure from here.
        //
        // close() is the NON-draining close - see DrainingCloseable#close(), which spells that out. That is
        // what teardown wants: it runs on the failure path too, so it must not be able to hang waiting for
        // in-flight work against a consumer that is still being made to misbehave. A scenario that needs the
        // opposite should call closeDrainFirst() in its own test body, where the wait is visible.
        if (parallelConsumer != null && !parallelConsumer.isClosedOrFailed()) {
            parallelConsumer.close();
        }
    }

    /**
     * Starts PC processing, collecting everything handed to the user function into {@link #processedRecords}.
     */
    protected void startProcessing() {
        parallelConsumer.poll(recordContexts -> recordContexts.forEach(recordContext -> {
            log.debug("Processing: {}", recordContext);
            processedRecords.add(recordContext);
        }));
    }

    /**
     * Publishes {@code count} records to the mock consumer immediately, at offsets {@code 0..count-1}.
     */
    protected void addRecords(int count) {
        for (int offset = 0; offset < count; offset++) {
            mockConsumer.addRecord(recordAt(offset));
        }
    }

    /**
     * Publishes {@code count} records, one every {@code interval}, from a daemon thread - for scenarios that
     * need the backlog to keep arriving while the consumer is misbehaving.
     * <p>
     * The thread is a daemon <em>and</em> is interrupted and joined in {@link
     * #stopRecordFeedAndCloseParallelConsumer()}: it must not survive the test method, or it will wake from its
     * sleep and {@code addRecord()} on a closed mock consumer.
     */
    protected void addRecordsInBackground(int count, Duration interval) {
        if (recordAdder != null) {
            throw new IllegalStateException("a background record feed is already running for this test");
        }
        recordAdder = new Thread(() -> feedRecords(count, interval), topic + "-record-adder");
        recordAdder.setDaemon(true);
        recordAdder.start();
    }

    private void feedRecords(int count, Duration interval) {
        for (int offset = 0; offset < count; offset++) {
            try {
                mockConsumer.addRecord(recordAt(offset));
                Thread.sleep(interval.toMillis());
            } catch (IllegalStateException e) {
                // the mock consumer was closed - the test has ended, stop quietly
                return;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    private ConsumerRecord<String, String> recordAt(int offset) {
        return new ConsumerRecord<>(topic, topicPartition.partition(), offset, "key", "value-" + offset);
    }

}
