package bz.stub.parallelconsumer.fluent;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.FakeRuntimeException;
import bz.stub.parallelconsumer.ParallelConsumer;
import bz.stub.parallelconsumer.internal.utils.LogCapture;
import bz.stub.parallelconsumer.state.ProcessingShard;
import ch.qos.logback.classic.Level;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;

import static bz.stub.parallelconsumer.AbstractParallelEoSStreamProcessorTestBase.defaultTimeout;
import static com.google.common.truth.Truth.assertThat;

/**
 * A parked record is not slow work, and the engine's shard scan says so by skipping it (KTD14).
 *
 * <h2>What "slow work" means, and why a park is not it</h2>
 * The shard scan looks at every record a shard holds and measures how long the ones it cannot take have been
 * waiting. Past the threshold - ten seconds by default - a record is counted against its partition's slow-records
 * meter and its topic is named in the periodic "records in the queue have been waiting longer than" warning. That
 * figure means "work that should have moved and has not". A parked record is work the definition deliberately
 * stopped: it will never become due on its own, so it would be counted on every pass for as long as it stays
 * parked, and an instance doing exactly what it was told would read as an instance in trouble.
 *
 * <h2>The control arm is in the same test</h2>
 * A record that is merely inside a long retry delay is still slow work and must still be warned about - otherwise
 * this change would have silenced the warning rather than narrowed it. Both routes are driven at once, in one
 * instance, so the two answers come from the same scan of the same shard map: the parked topic is absent from the
 * warning and the slow-but-retrying topic is present.
 *
 * <h2>Why it takes ten seconds</h2>
 * That is the engine's default threshold, and the fluent definition deliberately does not expose it - there is no
 * fluent setting to turn down, and inventing one to make a test faster would put a knob on the API for the test's
 * benefit. The wait is the pin.
 */
@Timeout(120)
class ParkedRecordsAreNotSlowWorkTest extends AbstractFluentEngineTest {

    /**
     * Unique to this test: {@link LogCapture} reads a logger shared with every other test in this module, so the
     * filter has to be something only this test can produce.
     */
    private static final String PARKED_TOPIC = "orders-that-park";

    private static final String SLOW_TOPIC = "orders-that-are-merely-slow";

    @Test
    void theWarningNamesTheSlowlyRetryingTopicAndNotTheParkedOne() {
        var pc = ParallelConsumer.connect(props());
        pc.string(PARKED_TOPIC)
                .retryLimit(0)
                .retryDelay(Duration.ofMillis(10))
                .process(context -> {
                    throw new FakeRuntimeException("this record never succeeds, and parks at once");
                });
        // Never exhausted, so it never parks - it just sits in a retry delay far longer than the slow threshold,
        // which is precisely what the warning is for.
        pc.string(SLOW_TOPIC)
                .retryForever()
                .retryDelay(Duration.ofMinutes(30))
                .process(context -> {
                    throw new FakeRuntimeException("this record never succeeds either, but it keeps its deadline");
                });

        RouteDispatcher dispatcher;
        try (LogCapture logs = LogCapture.of(ProcessingShard.class, Level.WARN)) {
            handle = runtime.startAndAssign(pc, 1);
            runtime.publish(PARKED_TOPIC, 0, 0, "key-0", "an order that parks");
            runtime.publish(SLOW_TOPIC, 0, 0, "key-0", "an order that keeps retrying");

            dispatcher = pc.dispatcher();
            Awaitility.await().atMost(defaultTimeout).untilAsserted(() ->
                    assertThat(dispatcher.parkedForRoute(PARKED_TOPIC)).hasSize(1));

            // The threshold is ten seconds from when the record was last taken as work, which was its last attempt.
            Awaitility.await().atMost(Duration.ofSeconds(60)).untilAsserted(() ->
                    assertThat(logs.messagesAt(Level.WARN, "waiting longer than", SLOW_TOPIC)).isNotEmpty());

            // Read after the control arm has fired, so "absent" means "the scan reached this point and skipped it"
            // rather than "the scan has not run yet" - which is what would make this pass for the wrong reason.
            assertThat(logs.messagesAt(Level.WARN, "waiting longer than", PARKED_TOPIC)).isEmpty();
        }

        // Beside it, the readings that do mean something for a parked record: one parked, and one park counted.
        assertThat(dispatcher.parkedForRoute(PARKED_TOPIC)).hasSize(1);
        assertThat(dispatcher.parkedCount()).isEqualTo(1);
    }
}
