package bz.stub.parallelconsumer.internal;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.ParallelConsumerOptions.AdaptiveConcurrencyMode;
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
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.google.common.truth.Truth.assertThat;

/**
 * The engine-capability half of the adaptive-concurrency contract: a requested mode meeting an engine that cannot
 * serve it must construct fine, WARN once naming why, and leave {@code adaptiveConcurrencyActive} false - the single
 * field downstream components read, so "unsupported" and "disabled" are indistinguishable from then on.
 * <p>
 * The options half - property resolution and seed validation - lives in
 * {@code bz.stub.parallelconsumer.AdaptiveConcurrencyOptionTest}.
 *
 * @see AbstractParallelEoSStreamProcessor#supportsAdaptiveConcurrency()
 * @see AbstractParallelEoSStreamProcessor#isAdaptiveConcurrencyActive()
 */
// Same reasoning as PipelinePressureLoggingTest: counting log events means attaching an appender to the shared
// AbstractParallelEoSStreamProcessor logger, which any concurrently running test would also write to.
@Isolated("captures a shared class logger")
class AdaptiveConcurrencyCapabilityTest {

    /**
     * The supported case: core engine, no direct pull - requesting a mode makes the field true, with nothing to
     * warn about.
     */
    @Test
    void observeOnTheCoreEngineIsActiveWithoutWarning() {
        var captured = captureProcessorLogging(() -> {
            try (var pc = new TestParallelEoSStreamProcessor<>(options().adaptiveConcurrencyMode(AdaptiveConcurrencyMode.OBSERVE).build())) {
                return pc.isAdaptiveConcurrencyActive();
            }
        });

        assertThat(captured.result).isTrue();
        assertThat(adaptiveWarnings(captured.events)).isEmpty();
    }

    /**
     * The default: nothing requested, nothing active, nothing said.
     */
    @Test
    void disabledModeIsInactiveWithoutWarning() {
        var captured = captureProcessorLogging(() -> {
            try (var pc = new TestParallelEoSStreamProcessor<>(options().build())) {
                return pc.isAdaptiveConcurrencyActive();
            }
        });

        assertThat(captured.result).isFalse();
        assertThat(adaptiveWarnings(captured.events)).isEmpty();
    }

    /**
     * Direct pull is an option on the core engine, not a subclass, so the capability answer has to fold it in -
     * an override-only design would report the core class as capable and then have no pool queue to steer.
     */
    @Test
    void observeWithTheDirectPullEngineConstructsButWarnsAndDeactivates() {
        var captured = captureProcessorLogging(() -> {
            try (var pc = new TestParallelEoSStreamProcessor<>(options()
                    .adaptiveConcurrencyMode(AdaptiveConcurrencyMode.OBSERVE)
                    .directPullEngine(true)
                    .build())) {
                return pc.isAdaptiveConcurrencyActive();
            }
        });

        assertThat(captured.result).isFalse();
        var warnings = adaptiveWarnings(captured.events);
        assertThat(warnings).hasSize(1);
        assertThat(warnings.get(0)).contains("direct-pull");
    }

    /**
     * The override contract: {@link ExternalEngine} declines the capability, so a requested mode deactivates with
     * the WARN naming the engine class. Exercised through a minimal test subclass rather than a real Vert.x or
     * Reactor engine - those modules are not on the core test classpath, and what is under test is
     * {@link ExternalEngine}'s own override, which every subclass inherits.
     */
    @Test
    void observeOnAnExternalEngineConstructsButWarnsAndDeactivates() {
        var captured = captureProcessorLogging(() -> {
            try (var pc = new StubExternalEngine<>(options().adaptiveConcurrencyMode(AdaptiveConcurrencyMode.OBSERVE).build())) {
                assertThat(pc.supportsAdaptiveConcurrency()).isFalse();
                return pc.isAdaptiveConcurrencyActive();
            }
        });

        assertThat(captured.result).isFalse();
        var warnings = adaptiveWarnings(captured.events);
        assertThat(warnings).hasSize(1);
        assertThat(warnings.get(0)).contains(StubExternalEngine.class.getSimpleName());
    }

    // --- helpers ---

    private static ParallelConsumerOptions.ParallelConsumerOptionsBuilder<String, String> options() {
        return ParallelConsumerOptions.<String, String>builder()
                .consumer(new MockConsumer<>(OffsetResetStrategy.LATEST));
    }

    /**
     * Runs {@code action} - typically a processor construction - while capturing everything logged to the shared
     * {@link AbstractParallelEoSStreamProcessor} logger.
     */
    private static <T> Captured<T> captureProcessorLogging(Supplier<T> action) {
        var pcLogger = (Logger) LoggerFactory.getLogger(AbstractParallelEoSStreamProcessor.class);
        var appender = new ListAppender<ILoggingEvent>();
        appender.start();
        pcLogger.addAppender(appender);
        try {
            T result = action.get();
            return new Captured<>(result, new ArrayList<>(appender.list));
        } finally {
            pcLogger.detachAppender(appender);
        }
    }

    /**
     * Construction logs other things (and other WARNs are conceivable) - filter to the one this feature owns.
     */
    private static List<String> adaptiveWarnings(List<ILoggingEvent> events) {
        return events.stream()
                .filter(event -> event.getLevel() == Level.WARN)
                .map(ILoggingEvent::getFormattedMessage)
                .filter(message -> message.contains("adaptiveConcurrencyMode"))
                .collect(Collectors.toList());
    }

    private static final class Captured<T> {
        private final T result;
        private final List<ILoggingEvent> events;

        private Captured(T result, List<ILoggingEvent> events) {
            this.result = result;
            this.events = events;
        }
    }

    /**
     * Minimal {@link ExternalEngine} - just enough to construct, so the inherited
     * {@code supportsAdaptiveConcurrency()} override can be exercised.
     */
    private static final class StubExternalEngine<K, V> extends ExternalEngine<K, V> {

        private StubExternalEngine(ParallelConsumerOptions<K, V> newOptions) {
            super(newOptions);
        }

        @Override
        protected boolean isAsyncFutureWork(List<?> resultsFromUserFunction) {
            return false;
        }
    }
}
