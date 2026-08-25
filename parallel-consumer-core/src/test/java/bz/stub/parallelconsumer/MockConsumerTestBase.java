package bz.stub.parallelconsumer;

/*-
 * Copyright (C) 2020-2024 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.internal.utils.LongPollingMockConsumer;
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

import static com.google.common.truth.Truth.assertWithMessage;
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
// all. Fixing those units is all this does; do not read it as wedge protection. JUnit's default
// ThreadMode.SAME_THREAD only converts to a TimeoutException once the invocation RETURNS, so it cannot
// abort a thread blocked on a monitor - and MockConsumer's addRecord/poll/commitSync/close all share one
// monitor, which makes that the wedge this suite can actually produce. Measured: a monitor-blocked test
// under @Timeout(2) ran 15.3s before being reported, where threadMode = SEPARATE_THREAD aborted at 2.0s.
@Timeout(120)
abstract class MockConsumerTestBase {

    /**
     * Distinct per subclass. Not a correctness measure - each test builds its own {@link MockConsumer}, so
     * there is no shared broker and no cross-scenario delivery to prevent - it just keeps logs and any
     * similarity tooling able to tell the scenarios apart.
     */
    protected final String topic = getClass().getSimpleName();

    protected final TopicPartition topicPartition = new TopicPartition(topic, 0);

    /**
     * Records handed to the user function, in arrival order. Concurrent because PC's worker threads write it.
     */
    protected final ConcurrentLinkedQueue<RecordContext<String, String>> processedRecords =
            new ConcurrentLinkedQueue<>();

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
        //
        // The beginning offset is recorded BEFORE the partition is assigned, for the reason
        // LongPollingMockConsumer#subscribeWithRebalanceAndAssignment sets out at length: rebalance() assigns
        // and fires the rebalance listener, so PC's broker-poll thread can poll this partition from inside that
        // call - and a poll of an assigned partition with no beginning offset kills the poll thread with
        // IllegalStateException, taking the engine down. Here the window was two calls wide.
        mockConsumer.updateBeginningOffsets(Collections.singletonMap(topicPartition, 0L));
        mockConsumer.rebalance(Collections.singletonList(topicPartition));
        parallelConsumer.onPartitionsAssigned(of(topicPartition));
    }

    // The class-level @Timeout does NOT cover lifecycle methods - only a @Timeout on the method itself
    // bounds an @AfterEach (measured: a 20s @AfterEach under a class-level @Timeout(2) ran to completion
    // and the test was reported as PASSING). This PR is what moved the PC close out of the test bodies -
    // where the method timeout did bound it - and into here, so without this the only bound on a wedged
    // close would be close()'s own internal budget, and a future scenario that wedges shutdown would hang
    // the fork until the job-level timeout.
    @Timeout(120)
    @AfterEach
    void stopRecordFeedAndCloseParallelConsumer() throws InterruptedException {
        // First, and unconditionally: Awaitility's defaults are static (JVM-wide, `private static volatile`),
        // not per-test, so a per-test override outlives the test that set it. Under this module's default
        // parallel execution that makes it a shared mutable global rather than merely a leak into the next
        // test. Nothing in the tree calls setDefaultTimeout today, so this is defensive. Done before the
        // close below, which can throw.
        Awaitility.reset();

        // Stop the feed BEFORE closing PC: once the mock consumer is closed, addRecord() throws
        // IllegalStateException, and an uncaught exception on a stray daemon thread gets attributed to
        // whatever test is running next in the same JVM (PIT's minion JVMs make this especially confusing).
        String leakedRecordAdder = null;
        if (recordAdder != null) {
            recordAdder.interrupt();
            // interrupt() aborts a sleep of ANY length immediately, so the feed's interval does not matter
            // here. The one way this join can time out is the feed sitting BLOCKED on MockConsumer's monitor
            // inside addRecord(), which interrupt() cannot release - see the warning on
            // #addRecordsInBackground about not holding that monitor.
            recordAdder.join(Duration.ofSeconds(10).toMillis());
            if (recordAdder.isAlive()) {
                leakedRecordAdder = recordAdder.getName();
            }
        }

        try {
            // A test may legitimately have closed it itself (that is the subject of the early-close scenario).
            //
            // close() is the NON-draining close - see DrainingCloseable#close(), which spells that out. That
            // is what teardown wants: it runs on the failure path too, so it must not be able to hang waiting
            // for in-flight work against a consumer that is still being made to misbehave. A scenario that
            // needs the opposite should call closeDrainFirst() in its own test body, where the wait is visible.
            if (parallelConsumer != null && !parallelConsumer.isClosedOrFailed()) {
                parallelConsumer.close();
            }

            // isClosedOrFailed() is true as soon as the control thread's future is DONE - which includes it
            // having died. So the guard above skips close() in exactly the case where close()'s trailing
            // future.get() would have rethrown what the supervisor saw. Assert the cause here instead, so a
            // dead control thread cannot pass silently just because the body's own assertion was already
            // satisfied before it died. A scenario that expects a failure cause should assert it in its body
            // and override this.
            if (parallelConsumer != null) {
                assertWithMessage("PC ended with a failure cause; the scenario did not expect one")
                        .that(parallelConsumer.getFailureCause()).isNull();
            }
        } finally {
            // A hard failure, not a log line. A feed thread that outlived its test goes on to addRecord()
            // against a closed consumer, and the resulting uncaught exception is attributed to whatever runs
            // next - so logging it leaves this suite green while corrupting a later one, which is the
            // "reports success without having checked" failure mode this teardown exists to prevent. JUnit
            // attaches an @AfterEach throw to any primary test failure as a SUPPRESSED exception rather than
            // masking it. In a finally so a throwing close cannot swallow the leak diagnostic, and after the
            // close so that a leaked feed does not also strand a running PC.
            assertWithMessage("a record adder thread did not stop after being interrupted, and would leak into "
                    + "the next test in this JVM")
                    .that(leakedRecordAdder).isNull();
        }
    }

    /**
     * Starts PC processing, collecting everything handed to the user function into {@link #processedRecords}.
     */
    protected void startProcessing() {
        parallelConsumer.poll(recordContexts -> recordContexts.forEach(recordContext -> {
            // info, not debug: logback-test.xml pins this package to info, so a debug line never prints -
            // and for four scenarios whose only assertion is a record count, this trace is the sole evidence
            // distinguishing "stopped delivering" from "merely slow" when one fails on CI.
            log.info("Processing: {}", recordContext);
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
     * <p>
     * <b>A scenario using this must not block while holding the {@link MockConsumer} monitor.</b>
     * {@code addRecord}, {@code poll}, {@code commitSync} and {@code close} all synchronize on the same lock,
     * so a {@code poll}/{@code commitSync} override that sleeps or waits while holding it parks this feed in
     * {@code BLOCKED} - and {@link Thread#interrupt()} cannot release a blocked thread. Teardown's join would
     * then time out and fail the test with nothing actually wrong with it. Throw immediately, or make the
     * delay happen outside the monitor.
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
