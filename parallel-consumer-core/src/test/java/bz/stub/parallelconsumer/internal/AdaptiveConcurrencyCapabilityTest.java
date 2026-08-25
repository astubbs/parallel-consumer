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

import static bz.stub.parallelconsumer.ParallelConsumerOptions.ADAPTIVE_CONCURRENCY_MODE_PROPERTY;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * The engine-capability half of the adaptive-concurrency contract: what happens when a requested mode meets an
 * engine that cannot serve it.
 * <p>
 * The answer depends on WHO asked, and both branches are covered here:
 * <ul>
 * <li><b>Set explicitly on the builder</b> - construction FAILS. The user asked for something that will not
 * happen, and a WARN they never read gets them to "this feature is broken".</li>
 * <li><b>Arrived from the {@value ParallelConsumerOptions#ADAPTIVE_CONCURRENCY_MODE_PROPERTY} system property</b> -
 * construction succeeds, WARNs once naming why, and leaves {@code adaptiveConcurrencyActive} false, so
 * "unsupported" and "disabled" are indistinguishable from then on. The property is JVM-wide and turns measurement
 * on for a whole bench harness or CI matrix; throwing on it would kill every external-engine consumer in the JVM.
 * </li>
 * </ul>
 * The options half - property resolution and seed validation - lives in
 * {@code bz.stub.parallelconsumer.AdaptiveConcurrencyOptionTest}.
 *
 * @see AbstractParallelEoSStreamProcessor#supportsAdaptiveConcurrency()
 * @see AbstractParallelEoSStreamProcessor#isAdaptiveConcurrencyActive()
 * @see ParallelConsumerOptions#isAdaptiveConcurrencyModeExplicit()
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
     * an override-only design would report the core class as capable and then have no pool queue to steer. Asked
     * for explicitly, it is a configuration error.
     */
    @Test
    void explicitObserveWithTheDirectPullEngineFailsConstruction() {
        var options = options()
                .adaptiveConcurrencyMode(AdaptiveConcurrencyMode.OBSERVE)
                .directPullEngine(true)
                .build();

        var thrown = assertThrows(IllegalArgumentException.class,
                () -> new TestParallelEoSStreamProcessor<>(options).close());

        assertThat(thrown).hasMessageThat().contains("adaptiveConcurrencyMode");
        assertThat(thrown).hasMessageThat().contains("direct-pull");
    }

    /**
     * The override contract: {@link ExternalEngine} declines the capability, so an explicitly requested mode is
     * refused with an exception naming the engine class. Exercised through a minimal test subclass rather than a
     * real Vert.x or Reactor engine - those modules are not on the core test classpath, and what is under test is
     * {@link ExternalEngine}'s own override, which every subclass inherits.
     */
    @Test
    void explicitObserveOnAnExternalEngineFailsConstruction() {
        var options = options().adaptiveConcurrencyMode(AdaptiveConcurrencyMode.OBSERVE).build();

        var thrown = assertThrows(IllegalArgumentException.class,
                () -> new StubExternalEngine<>(options).close());

        assertThat(thrown).hasMessageThat().contains("adaptiveConcurrencyMode");
        assertThat(thrown).hasMessageThat().contains(StubExternalEngine.class.getSimpleName());
    }

    /**
     * The other branch, and the reason the throw above is conditional: the same unservable mode arriving from the
     * JVM-wide {@value ParallelConsumerOptions#ADAPTIVE_CONCURRENCY_MODE_PROPERTY} property must NOT throw. That
     * property is how the bench harness and the CI matrix turn measurement on for a whole JVM, so an exception
     * here would fail every Vert.x, Reactor and Mutiny consumer in it - none of whose authors asked for anything.
     * <p>
     * Note what makes this the ambient case rather than the explicit one: the builder is never told the mode, so
     * the value comes from the {@code @Builder.Default} resolver.
     */
    @Test
    void ambientObserveOnAnExternalEngineConstructsButWarnsAndDeactivates() {
        String original = System.getProperty(ADAPTIVE_CONCURRENCY_MODE_PROPERTY);
        System.setProperty(ADAPTIVE_CONCURRENCY_MODE_PROPERTY, "OBSERVE");
        try {
            var captured = captureProcessorLogging(() -> {
                var options = options().build();
                assertThat(options.getAdaptiveConcurrencyMode()).isEqualTo(AdaptiveConcurrencyMode.OBSERVE);
                assertThat(options.isAdaptiveConcurrencyModeExplicit()).isFalse();
                try (var pc = new StubExternalEngine<>(options)) {
                    assertThat(pc.supportsAdaptiveConcurrency()).isFalse();
                    return pc.isAdaptiveConcurrencyActive();
                }
            });

            assertThat(captured.result).isFalse();
            var warnings = adaptiveWarnings(captured.events);
            assertThat(warnings).hasSize(1);
            assertThat(warnings.get(0)).contains(StubExternalEngine.class.getSimpleName());
            assertThat(warnings.get(0)).contains(ADAPTIVE_CONCURRENCY_MODE_PROPERTY);
        } finally {
            if (original == null) {
                System.clearProperty(ADAPTIVE_CONCURRENCY_MODE_PROPERTY);
            } else {
                System.setProperty(ADAPTIVE_CONCURRENCY_MODE_PROPERTY, original);
            }
        }
    }

    /**
     * The same ambient value on an engine that CAN serve it is simply active - the property does its job, and the
     * severity split above never comes into play.
     */
    @Test
    void ambientObserveOnTheCoreEngineIsActiveWithoutWarning() {
        String original = System.getProperty(ADAPTIVE_CONCURRENCY_MODE_PROPERTY);
        System.setProperty(ADAPTIVE_CONCURRENCY_MODE_PROPERTY, "OBSERVE");
        try {
            var captured = captureProcessorLogging(() -> {
                try (var pc = new TestParallelEoSStreamProcessor<>(options().build())) {
                    return pc.isAdaptiveConcurrencyActive();
                }
            });

            assertThat(captured.result).isTrue();
            assertThat(adaptiveWarnings(captured.events)).isEmpty();
        } finally {
            if (original == null) {
                System.clearProperty(ADAPTIVE_CONCURRENCY_MODE_PROPERTY);
            } else {
                System.setProperty(ADAPTIVE_CONCURRENCY_MODE_PROPERTY, original);
            }
        }
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
