package bz.stub.parallelconsumer.fluent;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.FakeRuntimeException;
import bz.stub.parallelconsumer.ParallelConsumer;
import bz.stub.parallelconsumer.internal.utils.LogCapture;
import bz.stub.parallelconsumer.state.ProcessingShard;
import ch.qos.logback.classic.Level;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

import static com.google.common.truth.Truth.assertThat;

/**
 * Pinned, so that removing it is visible: <b>in this release a parked record is counted as slow work by the
 * engine</b>, and named in the periodic warning about records waiting in the queue.
 * <p>
 * The engine's shard scan looks at every record a shard holds. One it cannot take because its retry delay has not
 * elapsed is measured against the slow-work threshold - ten seconds by default - and, past it, counted against its
 * partition's slow-records meter and named in the warning. A parked record's delay never elapses (it is a
 * far-future delay, which is what parking <em>is</em> over today's engine, KTD4), so from about ten seconds after
 * it parks it appears in both, on every pass, for as long as it stays parked.
 * <p>
 * <b>The facade cannot suppress it.</b> The scan is the engine's and it has no way to tell a park from a long
 * backoff. Removing it is the small-tier engine change that makes the scan skip a record whose retry delay has not
 * elapsed (KTD11) - and when that lands, this test goes red rather than the behaviour changing in silence. The
 * figures that mean something meanwhile are the parked count (R19) and the parked view (R28), which this asserts
 * beside the warning so the two readings cannot be confused.
 *
 * <h2>Why it takes ten seconds</h2>
 * That is the engine's default threshold, and the fluent definition deliberately does not expose it - there is no
 * fluent setting to turn down, and inventing one to make a test faster would put a knob on the API for the test's
 * benefit. The wait is the pin.
 */
@Timeout(120)
class ParkedRecordsAreSlowWorkForNowTest {

    /**
     * Unique to this test: {@link LogCapture} reads a logger shared with every other test in this module, so the
     * filter has to be something only this test can produce.
     */
    private static final String TOPIC = "orders-parked-are-slow-work";

    private final RecordingClientRuntime runtime = new RecordingClientRuntime();

    private ConsumerHandle handle;

    @AfterEach
    void closeTheInstance() {
        if (handle != null) {
            RecordingClientRuntime.closeWithoutDraining(handle);
        }
    }

    private static Properties props() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "parked-slow-work-test");
        return properties;
    }

    @Test
    void aParkedRecordIsNamedInTheEnginesSlowWorkWarningUntilTheSmallTierRemovesIt() {
        var pc = ParallelConsumer.connect(props());
        pc.string(TOPIC)
                .retryLimit(0)
                .retryDelay(Duration.ofMillis(10))
                .process(context -> {
                    throw new FakeRuntimeException("this record never succeeds");
                });

        RouteDispatcher dispatcher;
        List<String> warnings;
        try (LogCapture logs = LogCapture.of(ProcessingShard.class, Level.WARN)) {
            handle = runtime.startAndAssign(pc, 1);
            runtime.publish(TOPIC, 0, 0, "key-0", "an order that parks");

            dispatcher = pc.dispatcher();
            Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                    assertThat(dispatcher.parkedForRoute(TOPIC)).hasSize(1));

            // The threshold is ten seconds from when the record was last taken as work, which was its last attempt.
            Awaitility.await().atMost(Duration.ofSeconds(60)).untilAsserted(() ->
                    assertThat(logs.messagesAt(Level.WARN, "waiting longer than", TOPIC)).isNotEmpty());
            warnings = logs.messagesAt(Level.WARN, "waiting longer than", TOPIC);
        }

        assertThat(warnings.get(0)).contains(TOPIC + "-0");
        // Beside it, the readings that do mean something: one record parked, and one park counted.
        assertThat(dispatcher.parkedForRoute(TOPIC)).hasSize(1);
        assertThat(dispatcher.parkedCount()).isEqualTo(1);
    }
}
