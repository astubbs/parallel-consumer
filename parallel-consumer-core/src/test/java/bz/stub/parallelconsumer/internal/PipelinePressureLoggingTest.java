package bz.stub.parallelconsumer.internal;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static com.google.common.truth.Truth.assertThat;

/**
 * Guards the logging that {@link AbstractParallelEoSStreamProcessor#checkPipelinePressure()} emits when the extra load
 * factor can no longer be stepped up.
 * <p>
 * That method runs once per control-loop pass, so anything it logs unconditionally is logged continuously. Two things
 * are pinned here: the notice is rate limited, and it is not a WARN when the factor was pinned by the user's own
 * {@link ParallelConsumerOptions#messageBufferSize} - in which case "the maximum has been reached" is true from
 * startup and forever after, and describes the configuration rather than the system.
 *
 * @see DynamicLoadFactor#isStatic()
 */
// Counting log events means attaching an appender to the shared AbstractParallelEoSStreamProcessor logger, which any
// concurrently running test would also write to. Observed for real: run in parallel, these tests captured each
// other's output and counted 95 warnings from 50 control-loop passes.
@Isolated("captures a shared class logger")
class PipelinePressureLoggingTest {

    /**
     * Enough passes that an unlimited log statement is unmistakable, while staying well inside the rate limiter's
     * window so that a correctly limited one can only fire once.
     */
    private static final int CONTROL_LOOP_PASSES = 50;

    @Test
    void aPinnedMessageBufferSizeProducesAStaticFactorThatIsAtItsMaximumFromStartup() {
        var factor = new PCModule<>(options().build()).dynamicExtraLoadFactor();
        var pinned = new PCModule<>(options().messageBufferSize(500).build()).dynamicExtraLoadFactor();

        assertThat(factor.isStatic()).isFalse();
        assertThat(factor.isMaxReached()).isFalse();

        // the defect: nothing ever changes about this one, so isMaxReached() is not news
        assertThat(pinned.isStatic()).isTrue();
        assertThat(pinned.isMaxReached()).isTrue();
    }

    @Test
    void aSaturatedDynamicFactorWarnsOnlyOncePerRateLimitWindow() {
        var events = runPipelinePressureChecks(new SaturatedDynamicLoadFactor());

        var warnings = messagesAt(events, Level.WARN);
        assertThat(warnings).hasSize(1);
        assertThat(warnings.get(0)).contains("maximumLoadFactor");
    }

    @Test
    void aStaticFactorNeverWarnsAboutTheUsersOwnConfiguration() {
        // initial == maximum, exactly what PCModule builds for a pinned messageBufferSize
        var events = runPipelinePressureChecks(new DynamicLoadFactor(4, 4));

        assertThat(messagesAt(events, Level.WARN)).isEmpty();

        // demoted, not deleted - it is still the answer to "why isn't PC fetching more?"
        var debug = messagesAt(events, Level.DEBUG).stream()
                .filter(message -> message.contains("messageBufferSize"))
                .collect(Collectors.toList());
        assertThat(debug).isNotEmpty();
    }

    // --- helpers ---

    private static ParallelConsumerOptions.ParallelConsumerOptionsBuilder<String, String> options() {
        return ParallelConsumerOptions.<String, String>builder()
                .consumer(new MockConsumer<>(OffsetResetStrategy.LATEST));
    }

    /**
     * Runs {@link AbstractParallelEoSStreamProcessor#checkPipelinePressure()} {@value #CONTROL_LOOP_PASSES} times over
     * an idle worker pool - the "queue is low, and the last work request was fulfilled" state that reaches the load
     * factor branch - and returns everything the processor logged while doing so.
     */
    private static List<ILoggingEvent> runPipelinePressureChecks(DynamicLoadFactor factor) {
        var opts = options().build();
        var module = new PCModule<String, String>(opts) {
            @Override
            protected DynamicLoadFactor dynamicExtraLoadFactor() {
                return factor;
            }
        };

        var pcLogger = (Logger) LoggerFactory.getLogger(AbstractParallelEoSStreamProcessor.class);
        var appender = new ListAppender<ILoggingEvent>();
        appender.start();
        Level originalLevel = pcLogger.getLevel();
        pcLogger.setLevel(Level.DEBUG);
        pcLogger.addAppender(appender);

        var captured = new ArrayList<ILoggingEvent>();
        try (var pc = new TestParallelEoSStreamProcessor<>(opts, module)) {
            pc.setLastWorkRequestWasFulfilled(true);
            appender.list.clear(); // discard construction-time logging
            for (int pass = 0; pass < CONTROL_LOOP_PASSES; pass++) {
                pc.checkPipelinePressure();
            }
            captured.addAll(appender.list);
        } finally {
            pcLogger.detachAppender(appender);
            pcLogger.setLevel(originalLevel);
        }
        return captured;
    }

    private static List<String> messagesAt(List<ILoggingEvent> events, Level level) {
        return events.stream()
                .filter(event -> event.getLevel() == level)
                .map(ILoggingEvent::getFormattedMessage)
                .collect(Collectors.toList());
    }

    /**
     * A factor that is genuinely dynamic - its initial value is below its maximum, so {@link DynamicLoadFactor#isStatic()}
     * is false - but which has already climbed to its ceiling, the state a hungry pipeline reaches after enough
     * step-ups. Simulated rather than driven for real, because a real climb costs one two-second cool-down per step.
     */
    private static final class SaturatedDynamicLoadFactor extends DynamicLoadFactor {

        private SaturatedDynamicLoadFactor() {
            super(DEFAULT_INITIAL_LOADING_FACTOR, DEFAULT_MAX_LOADING_FACTOR);
        }

        @Override
        public int getCurrentFactor() {
            return getMaxFactor();
        }

        @Override
        public boolean isMaxReached() {
            return true;
        }

        @Override
        public boolean maybeStepUp() {
            return false;
        }
    }
}
