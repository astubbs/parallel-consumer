package bz.stub.parallelconsumer.streams.integrationTests;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.integrationTests.utils.KafkaClientUtils;
import bz.stub.parallelconsumer.streams.PcDispatchCounters;
import bz.stub.parallelconsumer.streams.PcDispatchSwitch;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * The channel that tells a user their punctuator behaves differently here, and the gate on the decision
 * that made it the answer to the handover's top-ranked defect (astubbs#255).
 *
 * <h2>Why a test on a log line</h2>
 * A punctuator is the one construct that reaches this seam without passing any of the three refusal layers:
 * it is not a DSL method call, not a state store type, and not a config key, so
 * {@code PcSupportedEnvelope} has nothing structural to inspect. That leaves a warning at registration as
 * the only channel, which makes it load-bearing rather than decorative - and until this class it had no
 * coverage at all, which is recorded as an open hole against U13.
 * <p>
 * <b>What the warning is instead of.</b> The handover ranked "WALL_CLOCK_TIME punctuators fire with no
 * warning and their effects never become commit-covered" first, with
 * {@code hasUncommittedWork() || commitNeeded} as the one-line candidate. The three sibling classes here
 * shrank the second half of that to a bounded idle-window replay cost - {@link PunctuatorEffectSurvivalTest}
 * shows the effects reach the broker regardless of any commit, {@link PostCommitCheckpointGapTest} shows
 * {@code postCommit} runs under load, {@link PunctuatorCommitCoverageTest} shows commit cadence carries no
 * information here at all. So the fix taken is the first half: both punctuation types warn, and the
 * WALL_CLOCK one is new.
 *
 * <h2>Once per task per type</h2>
 * The seam-on arm registers <b>two</b> STREAM_TIME punctuators and one WALL_CLOCK_TIME punctuator on a
 * single-partition, single-thread topology - so exactly one task exists - and asserts one warning of each
 * kind. A per-registration warning would print the same paragraph twice for the same divergence, which is
 * how a genuinely important line gets scrolled past.
 *
 * @author Antony Stubbs
 */
@Slf4j
// PcDispatchSwitch is process-wide, and the appender below is attached to a shared class logger - a
// concurrent test registering a punctuator would land in this class's captured list.
@Isolated
class PunctuatorWarningTest extends BrokerStreamsIntegrationTest {

    private static final int POOL_SIZE = 4;

    private static final int INPUT_RECORDS = 3;

    /**
     * The logger the patched {@code StreamTask} writes through. Named by string rather than by class,
     * because {@code org.apache.kafka.streams.processor.internals.StreamTask} is a generated source: it
     * exists only under {@code target/kafka-patched} and is not importable from a test.
     */
    private static final String STREAM_TASK_LOGGER = "org.apache.kafka.streams.processor.internals.StreamTask";

    /**
     * The distinguishing fragments, chosen to survive a reword of the surrounding paragraph while still
     * failing if the two branches are ever collapsed into one message. Each names the punctuation type,
     * which is the thing the branch is selected on.
     */
    private static final String STREAM_TIME_WARNING = "registered a STREAM_TIME punctuator";

    private static final String WALL_CLOCK_WARNING = "registered a WALL_CLOCK_TIME punctuator";

    private static final Duration PUNCTUATE_INTERVAL = Duration.ofSeconds(30);

    private static final AtomicInteger recordsProcessed = new AtomicInteger();

    @BeforeEach
    void resetCounters() {
        recordsProcessed.set(0);
        PcDispatchCounters.reset();
    }

    @AfterEach
    void resetSwitch() {
        PcDispatchSwitch.resetToDefault();
    }

    @Test
    void bothPunctuationTypesWarnOnceUnderPcDispatch() {
        PcDispatchSwitch.enable(POOL_SIZE);

        ArmResult result = runArm("pc-punctuator-warning");

        assertThat(result.dispatchedToPool)
                .as("premise: this arm went through the PC dispatch seam, so the warning branch was even "
                        + "reachable - it is guarded on pcDispatcher being non-null")
                .isEqualTo(INPUT_RECORDS);

        assertThat(result.streamTimeWarnings)
                .as("TWO STREAM_TIME punctuators were registered on ONE task, and the user gets ONE "
                        + "warning. The divergence is a property of the task, not of the registration; "
                        + "repeating the paragraph per punctuator is how it stops being read.")
                .hasSize(1);

        assertThat(result.wallClockWarnings)
                .as("AND WALL_CLOCK_TIME warns too, which is what this rung changed. maybePunctuateSystemTime "
                        + "is byte-for-byte stock so the FIRING is unaffected - but the punctuator still runs "
                        + "concurrently with records inside the processor chain, which stock never does, and "
                        + "a plain KeyValueStore is supported on this path. That clause is the reason this "
                        + "warning exists and the reason the one-line commitNeeded candidate was not taken "
                        + "instead; if this assertion is ever deleted, the handover's ranked defect 1 goes "
                        + "back to being silent.")
                .hasSize(1);

        assertThat(result.wallClockWarnings.get(0))
                .as("and it says the concurrency clause rather than merely naming the type - the pool size "
                        + "is interpolated, so this also proves the message reached its arguments")
                .contains("CONCURRENTLY with up to " + POOL_SIZE + " records");
    }

    /**
     * The control. Seam off, identical topology: the branch is guarded on the dispatcher existing, so a
     * stock task must say nothing at all. Without this, a warning printed unconditionally would satisfy the
     * arm above and mislead every stock user.
     */
    @Test
    void stockRegistersThroughSilently() {
        PcDispatchSwitch.disable();

        ArmResult result = runArm("stock-punctuator-warning");

        assertThat(result.dispatchedToPool)
                .as("premise: this is the CONTROL and must not have touched the seam - PcDispatchSwitch "
                        + "defaults ON, so seam-off is never ambient")
                .isZero();

        assertThat(result.streamTimeWarnings)
                .as("stock punctuation is stock; a warning here would be a lie told to every user who "
                        + "never turned the seam on")
                .isEmpty();

        assertThat(result.wallClockWarnings)
                .as("and the same for wall clock")
                .isEmpty();
    }

    private ArmResult runArm(final String name) {
        String inputTopic = setupTopic(name + "-in");
        ensureTopic(inputTopic, 1);
        String appId = name + "-" + System.nanoTime();

        produceInput(inputTopic);

        Logger streamTaskLogger = (Logger) LoggerFactory.getLogger(STREAM_TASK_LOGGER);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        streamTaskLogger.addAppender(appender);

        long dispatched;
        List<String> streamTimeWarnings;
        List<String> wallClockWarnings;
        KafkaStreams streams = null;
        try {
            streams = startTopology(appId, inputTopic);
            // The punctuators are registered in init(), which runs before the first record - but waiting
            // for records is what proves the task reached RUNNING with the topology this arm described,
            // rather than reading an empty list from a client that never assigned a task.
            await().atMost(Duration.ofSeconds(60))
                    .until(() -> recordsProcessed.get() >= INPUT_RECORDS);
            dispatched = PcDispatchCounters.getRecordsDispatchedToPool();
            streamTimeWarnings = warningsContaining(appender, STREAM_TIME_WARNING);
            wallClockWarnings = warningsContaining(appender, WALL_CLOCK_WARNING);
            log.info("=== [{}] dispatched={} streamTimeWarnings={} wallClockWarnings={}",
                    name, dispatched, streamTimeWarnings.size(), wallClockWarnings.size());
        } finally {
            // Detached before the close, so a close-path log line cannot land in a list already read - and
            // in a finally, because an appender left attached to a shared class logger outlives this test.
            streamTaskLogger.detachAppender(appender);
            appender.stop();
            if (streams != null) {
                streams.close(Duration.ofSeconds(30));
            }
        }

        return new ArmResult(dispatched, streamTimeWarnings, wallClockWarnings);
    }

    private static List<String> warningsContaining(final ListAppender<ILoggingEvent> appender,
                                                   final String fragment) {
        List<String> matched = new ArrayList<>();
        // Copied rather than streamed over the live list: Kafka's threads keep logging through this logger
        // while the assertions run, and ListAppender's list is not safe to iterate concurrently.
        for (ILoggingEvent event : new ArrayList<>(appender.list)) {
            if (event.getLevel() == Level.WARN && event.getFormattedMessage().contains(fragment)) {
                matched.add(event.getFormattedMessage());
            }
        }
        return matched;
    }

    private void produceInput(final String inputTopic) {
        try (KafkaProducer<String, String> producer =
                     getKcu().createNewProducer(KafkaClientUtils.ProducerMode.NOT_TRANSACTIONAL)) {
            for (int i = 0; i < INPUT_RECORDS; i++) {
                producer.send(new ProducerRecord<>(inputTopic, "key-" + i, "value-" + i));
            }
            producer.flush();
        }
    }

    private KafkaStreams startTopology(final String appId, final String inputTopic) {
        StreamsBuilder builder = new StreamsBuilder();
        builder.<String, String>stream(inputTopic)
                .process((ProcessorSupplier<String, String, Void, Void>) () ->
                        new Processor<String, String, Void, Void>() {
                            @Override
                            public void init(final ProcessorContext<Void, Void> context) {
                                // TWO of the same type, deliberately - the warn-once claim is what
                                // distinguishes this from a per-registration log line.
                                context.schedule(PUNCTUATE_INTERVAL, PunctuationType.STREAM_TIME,
                                        timestamp -> { });
                                context.schedule(PUNCTUATE_INTERVAL, PunctuationType.STREAM_TIME,
                                        timestamp -> { });
                                // The interval is long enough that neither punctuator fires inside this
                                // run: the subject is REGISTRATION, and a firing punctuator would add
                                // timing to a test that has none.
                                context.schedule(PUNCTUATE_INTERVAL, PunctuationType.WALL_CLOCK_TIME,
                                        timestamp -> { });
                            }

                            @Override
                            public void process(final Record<String, String> record) {
                                recordsProcessed.incrementAndGet();
                            }
                        });

        Properties props = baseStreamsProps(appId);
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
        return startAndAwaitRunning(builder, props, LOG_AND_SHUT_DOWN_CLIENT);
    }

    /** What one arm observed. Plain fields: the Java 8 release target rules out a record. */
    private static final class ArmResult {

        private final long dispatchedToPool;

        private final List<String> streamTimeWarnings;

        private final List<String> wallClockWarnings;

        private ArmResult(final long dispatchedToPool,
                          final List<String> streamTimeWarnings,
                          final List<String> wallClockWarnings) {
            this.dispatchedToPool = dispatchedToPool;
            this.streamTimeWarnings = streamTimeWarnings;
            this.wallClockWarnings = wallClockWarnings;
        }
    }
}
