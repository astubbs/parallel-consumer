package io.confluent.parallelconsumer.internal;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.confluent.parallelconsumer.ParallelConsumerOptions;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.api.parallel.Isolated;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.google.common.truth.Truth.assertThat;

/**
 * Covers the reporting of a loading factor that has reached its ceiling - see
 * <a href="https://github.com/astubbs/parallel-consumer/issues/155">astubbs#155</a> (confluentinc#402).
 * <p>
 * The reported symptom was a log filled with {@code Max loading factor steps reached: 100/100}. Two things produced
 * it, and both are exercised here:
 * <ol>
 *     <li>Configuring {@link ParallelConsumerOptions#messageBufferSize} pins the load factor to a single value, so
 *     {@link DynamicLoadFactor#isMaxReached()} is true from startup and the line fired on <em>every</em> control loop
 *     pass, from the first one, for a system that was configured exactly as intended.</li>
 *     <li>A dynamic factor that legitimately steps up to its cap then holds that state indefinitely, and the line was
 *     emitted unrate-limited for as long as it held.</li>
 * </ol>
 * These tests drive {@link AbstractParallelEoSStreamProcessor#checkPipelinePressure()} - the real control-loop pass -
 * many times and assert on what reaches the log.
 * <p>
 * NOTE the log capture below is deliberately minimal and private to this test: a reusable
 * {@code io.confluent.csid.utils.LogCapture} is arriving on the log-verbosity branch, and whichever of the two lands
 * second should delete its own copy and use that one (recorded in {@code docs/inflight/pr-blockers-and-collisions.md}).
 */
// Captures a class-wide logger and changes its level, so nothing else may run while it does - not another class
// (@Isolated), and not its own sibling methods (SAME_THREAD), or their output lands in the capture.
@Isolated
@Execution(ExecutionMode.SAME_THREAD)
class LoadFactorCeilingReportingTest {

    /**
     * Enough passes that an unrate-limited line is unmistakable: before the fix, the static case logged 500 warnings
     * here, and the dynamic case likewise.
     */
    private static final int CONTROL_LOOP_PASSES = 500;

    private static final String OLD_MESSAGE = "Max loading factor steps reached";

    private ParallelConsumerOptions<String, String> options(Consumer<ParallelConsumerOptions.ParallelConsumerOptionsBuilder<String, String>> customiser) {
        var builder = ParallelConsumerOptions.<String, String>builder()
                .consumer(new MockConsumer<String, String>(OffsetResetStrategy.LATEST));
        customiser.accept(builder);
        return builder.build();
    }

    /**
     * The reported noise: a fixed buffer size means the factor starts at its own ceiling, so the ceiling report fired
     * on every pass from startup. There is nothing to warn about - the user asked for exactly this buffer.
     */
    @Test
    void fixedMessageBufferSizeDoesNotWarnOnEveryPass() {
        var options = options(builder -> builder.messageBufferSize(1000));
        var module = new PCModule<>(options);

        var events = runPressureChecks(options, module);

        // the mechanism: pinned at its maximum from construction, so the old code's warn condition is true from pass 1
        assertThat(module.dynamicExtraLoadFactor().isStaticFactor()).isTrue();
        assertThat(module.dynamicExtraLoadFactor().isMaxReached()).isTrue();

        assertThat(messagesAt(events, Level.WARN)).isEmpty();
        assertThat(messagesAt(events, Level.ERROR)).isEmpty();

        // NOTE the two assertions above are negative, so on their own they would pass vacuously if the appender were
        // ever attached to the wrong logger (say the reporting moved class). The positive debug assertion below is
        // what anchors them: it can only hold if the capture is live and pointed at the code under test. Keep them
        // together - see docs/solutions/test-flakiness/vacuous-await-condition-brokerpoller-backpressure-2026-07-31.md
        // for the same shape costing a real regression.

        // it is still observable, just at a level that matches how interesting it is
        var debug = messagesAt(events, Level.DEBUG).stream()
                .filter(message -> message.contains("loading factor is fixed"))
                .collect(Collectors.toList());
        assertThat(debug).isNotEmpty();

        assertReportsTheThresholdItActuallyTested(debug.get(0), options, module);
    }

    /**
     * A dynamic factor which has stepped all the way up to its cap IS worth reporting - it says the in-flight target
     * will not grow any further - but the condition is a steady state, so it must not be repeated every pass.
     */
    @Test
    void dynamicFactorAtCeilingWarnsOnceNotEveryPass() {
        var options = options(builder -> {
            // defaults: a dynamic factor, 2..100
        });
        var atCeiling = new SteppedToCeilingLoadFactor();
        var module = new PCModule<String, String>(options) {
            @Override
            protected DynamicLoadFactor dynamicExtraLoadFactor() {
                return atCeiling;
            }
        };

        var events = runPressureChecks(options, module);

        assertThat(atCeiling.isStaticFactor()).isFalse();

        var warnings = messagesAt(events, Level.WARN);
        assertThat(warnings).hasSize(1);
        // reworded: a saturation signal, and it names what to change - it no longer reads as a failure
        assertThat(warnings.get(0)).contains("saturation signal");
        assertThat(warnings.get(0)).contains("maximumLoadFactor");
        assertThat(warnings.get(0)).doesNotContain(OLD_MESSAGE);

        assertReportsTheThresholdItActuallyTested(warnings.get(0), options, module);
    }

    /**
     * The numbers in the message have to be the numbers the code actually used, or the message is worse than the noise
     * it replaced - a reader comparing them against
     * {@link ParallelConsumerOptions#getTargetAmountOfRecordsInFlight()} would find a mismatch and reasonably suspect
     * a bug.
     * <p>
     * The branch is entered because {@code isPoolQueueLow()} found the pool queue at or below the <em>un-multiplied</em>
     * in-flight target, so that is the threshold the "queued vs" comparison must name. The loaded target (target x
     * factor) is the separate number the factor scales, and is reported as such.
     */
    private void assertReportsTheThresholdItActuallyTested(String message,
                                                           ParallelConsumerOptions<String, String> options,
                                                           PCModule<String, String> module) {
        int poolLoadTarget = options.getTargetAmountOfRecordsInFlight();
        int loadedTarget = poolLoadTarget * module.dynamicExtraLoadFactor().getCurrentFactor();
        // the two must differ, or this assertion cannot tell them apart
        assertThat(loadedTarget).isNotEqualTo(poolLoadTarget);

        assertThat(message).contains("0 queued vs " + poolLoadTarget);
        assertThat(message).contains(loadedTarget + " records");
    }

    @Test
    void fixedFactorNeverSteps() {
        var fixed = DynamicLoadFactor.fixedAt(7);

        assertThat(fixed.isStaticFactor()).isTrue();
        assertThat(fixed.isMaxReached()).isTrue();
        assertThat(fixed.maybeStepUp()).isFalse();
        assertThat(fixed.getCurrentFactor()).isEqualTo(7);
    }

    @Test
    void factorWithHeadroomIsNotStatic() {
        var dynamic = new DynamicLoadFactor(DynamicLoadFactor.DEFAULT_INITIAL_LOADING_FACTOR,
                DynamicLoadFactor.DEFAULT_MAX_LOADING_FACTOR);

        assertThat(dynamic.isStaticFactor()).isFalse();
        assertThat(dynamic.isMaxReached()).isFalse();
    }

    /**
     * Runs {@link #CONTROL_LOOP_PASSES} pressure checks with the queue empty (so it is always below target) and the
     * last work request marked fulfilled - the state which reaches the ceiling report - capturing everything the
     * processor logs while doing so.
     */
    private List<ILoggingEvent> runPressureChecks(ParallelConsumerOptions<String, String> options,
                                                  PCModule<String, String> module) {
        var processorLogger = (Logger) LoggerFactory.getLogger(AbstractParallelEoSStreamProcessor.class);
        var originalLevel = processorLogger.getLevel();
        var appender = new ListAppender<ILoggingEvent>();

        try (var pc = new TestParallelEoSStreamProcessor<>(options, module)) {
            pc.markLastWorkRequestFulfilled();

            processorLogger.setLevel(Level.DEBUG);
            appender.start();
            processorLogger.addAppender(appender);
            try {
                for (int pass = 0; pass < CONTROL_LOOP_PASSES; pass++) {
                    pc.runPipelinePressureCheck();
                }
            } finally {
                processorLogger.detachAppender(appender);
                processorLogger.setLevel(originalLevel);
            }
        }

        return appender.list;
    }

    private List<String> messagesAt(List<ILoggingEvent> events, Level level) {
        return events.stream()
                .filter(event -> event.getLevel() == level)
                .map(ILoggingEvent::getFormattedMessage)
                .collect(Collectors.toList());
    }

    /**
     * A load factor with room to grow (so {@link DynamicLoadFactor#isStaticFactor()} is false) which has already been
     * stepped to its cap.
     * <p>
     * Reaching that state for real costs one {@link DynamicLoadFactor} cool-down period per step - minutes of wall
     * clock for the default 2..100 range - so the end state is asserted directly instead. Only the terminal condition
     * is overridden; the reporting path under test is the real one.
     */
    private static class SteppedToCeilingLoadFactor extends DynamicLoadFactor {

        SteppedToCeilingLoadFactor() {
            super(DEFAULT_INITIAL_LOADING_FACTOR, DEFAULT_MAX_LOADING_FACTOR);
        }

        @Override
        public boolean isMaxReached() {
            return true;
        }
    }
}
