package bz.stub.parallelconsumer;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions.AdaptiveConcurrencyMode;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static bz.stub.parallelconsumer.ParallelConsumerOptions.ADAPTIVE_CONCURRENCY_MODE_PROPERTY;
import static com.google.common.truth.Truth.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The options half of the adaptive-concurrency contract: the {@value ParallelConsumerOptions#ADAPTIVE_CONCURRENCY_MODE_PROPERTY}
 * system property's resolution rules (including that it can never select {@link AdaptiveConcurrencyMode#ENFORCE}),
 * and the seed's validation against the mode and the ceiling.
 * <p>
 * The engine-capability half - what happens when a mode meets an engine that cannot serve it - lives in
 * {@code bz.stub.parallelconsumer.internal.AdaptiveConcurrencyCapabilityTest}.
 *
 * @see ParallelConsumerOptions#getAdaptiveConcurrencyMode()
 * @see ParallelConsumerOptions#getAdaptiveConcurrencyInitialTarget()
 */
// The system property is JVM-global state that every concurrently-built options object reads through its builder
// default, so these tests cannot share a schedule with anything that builds options - and the ENFORCE test
// additionally captures a shared class logger.
@Isolated("mutates the pc.adaptiveConcurrency system property, which every options build reads")
class AdaptiveConcurrencyOptionTest {

    private String originalPropertyValue;

    @BeforeEach
    void startFromAnUnsetProperty() {
        originalPropertyValue = System.getProperty(ADAPTIVE_CONCURRENCY_MODE_PROPERTY);
        System.clearProperty(ADAPTIVE_CONCURRENCY_MODE_PROPERTY);
    }

    @AfterEach
    void restoreTheProperty() {
        if (originalPropertyValue == null) {
            System.clearProperty(ADAPTIVE_CONCURRENCY_MODE_PROPERTY);
        } else {
            System.setProperty(ADAPTIVE_CONCURRENCY_MODE_PROPERTY, originalPropertyValue);
        }
    }

    // --- mode resolution ---

    /**
     * With the property absent, the options must equal today's: adaptive concurrency off, seed unset, and a valid
     * configuration - the feature's existence changes nothing for anyone not asking for it.
     */
    @Test
    void theModeIsDisabledAndTheSeedUnsetByDefault() {
        var opts = options().build();

        assertThat(opts.getAdaptiveConcurrencyMode()).isEqualTo(AdaptiveConcurrencyMode.DISABLED);
        assertThat(opts.getAdaptiveConcurrencyInitialTarget()).isEqualTo(0);
        assertThatCode(opts::validate).doesNotThrowAnyException();
    }

    /**
     * Lower case on purpose - the parse is documented case-insensitive.
     */
    @Test
    void thePropertyCanSelectObserve() {
        System.setProperty(ADAPTIVE_CONCURRENCY_MODE_PROPERTY, "observe");

        assertThat(options().build().getAdaptiveConcurrencyMode()).isEqualTo(AdaptiveConcurrencyMode.OBSERVE);
    }

    @Test
    void thePropertyCanSelectDisabledExplicitly() {
        System.setProperty(ADAPTIVE_CONCURRENCY_MODE_PROPERTY, "DISABLED");

        assertThat(options().build().getAdaptiveConcurrencyMode()).isEqualTo(AdaptiveConcurrencyMode.DISABLED);
    }

    /**
     * Enforcement changes what a deployment does to its downstream systems, so a launch flag may never select it -
     * the property downgrades to {@link AdaptiveConcurrencyMode#OBSERVE}, and says so at WARN.
     */
    @Test
    void aPropertyValueOfEnforceResolvesToObserveAndWarns() {
        System.setProperty(ADAPTIVE_CONCURRENCY_MODE_PROPERTY, "ENFORCE");

        var built = captureOptionsLogging(() -> options().build());

        assertThat(built.result.getAdaptiveConcurrencyMode()).isEqualTo(AdaptiveConcurrencyMode.OBSERVE);
        var warnings = messagesAt(built.events, Level.WARN);
        assertThat(warnings).hasSize(1);
        assertThat(warnings.get(0)).contains(ADAPTIVE_CONCURRENCY_MODE_PROPERTY);
        assertThat(warnings.get(0)).contains("ENFORCE");
    }

    /**
     * The other side of the downgrade: an explicit builder value is exactly how ENFORCE is meant to be selected.
     */
    @Test
    void theBuilderCanSelectEnforce() {
        var opts = options().adaptiveConcurrencyMode(AdaptiveConcurrencyMode.ENFORCE).build();

        assertThat(opts.getAdaptiveConcurrencyMode()).isEqualTo(AdaptiveConcurrencyMode.ENFORCE);
        assertThatCode(opts::validate).doesNotThrowAnyException();
    }

    /**
     * Same contract as {@code useVirtualThreads}: the property is a default, so a deliberate builder value beats it.
     */
    @Test
    void anExplicitBuilderValueBeatsTheSystemProperty() {
        System.setProperty(ADAPTIVE_CONCURRENCY_MODE_PROPERTY, "OBSERVE");

        assertThat(options().adaptiveConcurrencyMode(AdaptiveConcurrencyMode.DISABLED).build().getAdaptiveConcurrencyMode())
                .isEqualTo(AdaptiveConcurrencyMode.DISABLED);
    }

    /**
     * A typo'd flag must never silently run the default - the repo's fatal-on-unknown convention. The failure fires
     * where the property is read: {@code build()}, where Lombok evaluates the builder default, which is at-or-before
     * {@code validate()} on every construction path.
     */
    @Test
    void anUnrecognisedPropertyValueFailsLoudlyNamingTheField() {
        System.setProperty(ADAPTIVE_CONCURRENCY_MODE_PROPERTY, "bananas");

        assertThatThrownBy(() -> options().build().validate())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("adaptiveConcurrencyMode")
                .hasMessageContaining(ADAPTIVE_CONCURRENCY_MODE_PROPERTY)
                .hasMessageContaining("bananas");
    }

    /**
     * The property resolver must be deterministic while the property is fixed - two builds may never disagree about
     * the mode a deployment runs in.
     */
    @Test
    void twoIdenticallyBuiltOptionsAgreeOnTheResolvedMode() {
        System.setProperty(ADAPTIVE_CONCURRENCY_MODE_PROPERTY, "OBSERVE");

        var first = options().build();
        var second = options().build();

        assertThat(first.getAdaptiveConcurrencyMode()).isEqualTo(second.getAdaptiveConcurrencyMode());
        assertThat(first.getAdaptiveConcurrencyMode()).isEqualTo(AdaptiveConcurrencyMode.OBSERVE);
    }

    // --- seed validation ---

    /**
     * The ceiling the seed is checked against is {@code maxConcurrency} - the options-level effective maximum. (The
     * ENFORCE-mode default-ceiling substitution belongs to the controller, and is deliberately not modelled here.)
     */
    @Test
    void aSeedAboveMaxConcurrencyIsRejectedNamingBothFields() {
        var opts = options()
                .adaptiveConcurrencyMode(AdaptiveConcurrencyMode.OBSERVE)
                .maxConcurrency(16)
                .adaptiveConcurrencyInitialTarget(17)
                .build();

        assertThatThrownBy(opts::validate)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("adaptiveConcurrencyInitialTarget")
                .hasMessageContaining("maxConcurrency")
                .hasMessageContaining("17");
    }

    @Test
    void aNegativeSeedIsRejectedNamingTheField() {
        var opts = options()
                .adaptiveConcurrencyMode(AdaptiveConcurrencyMode.OBSERVE)
                .adaptiveConcurrencyInitialTarget(-3)
                .build();

        assertThatThrownBy(opts::validate)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("adaptiveConcurrencyInitialTarget")
                .hasMessageContaining("-3");
    }

    /**
     * A seed nothing will ever read is a configuration lie, so it fails rather than being ignored.
     */
    @Test
    void aSeedSetWhileTheModeIsDisabledIsRejected() {
        var opts = options()
                .adaptiveConcurrencyInitialTarget(4)
                .build();

        assertThatThrownBy(opts::validate)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("adaptiveConcurrencyInitialTarget")
                .hasMessageContaining("adaptiveConcurrencyMode");
    }

    /**
     * Guards the bound against over-reaching: 1, a middle value, and the ceiling itself are all legal - in both
     * modes that read the seed.
     */
    @Test
    void validSeedsAreAccepted() {
        assertThatCode(() -> options()
                .adaptiveConcurrencyMode(AdaptiveConcurrencyMode.OBSERVE)
                .adaptiveConcurrencyInitialTarget(1)
                .build().validate()).doesNotThrowAnyException();

        assertThatCode(() -> options()
                .adaptiveConcurrencyMode(AdaptiveConcurrencyMode.ENFORCE)
                .adaptiveConcurrencyInitialTarget(8)
                .build().validate()).doesNotThrowAnyException();

        assertThatCode(() -> options()
                .adaptiveConcurrencyMode(AdaptiveConcurrencyMode.OBSERVE)
                .maxConcurrency(16)
                .adaptiveConcurrencyInitialTarget(16)
                .build().validate()).doesNotThrowAnyException();
    }

    // --- helpers ---

    private static ParallelConsumerOptions.ParallelConsumerOptionsBuilder<String, String> options() {
        return ParallelConsumerOptions.<String, String>builder()
                .consumer(new MockConsumer<>(OffsetResetStrategy.EARLIEST));
    }

    private static <T> CapturedBuild<T> captureOptionsLogging(Supplier<T> build) {
        var optionsLogger = (Logger) LoggerFactory.getLogger(ParallelConsumerOptions.class);
        var appender = new ListAppender<ILoggingEvent>();
        appender.start();
        optionsLogger.addAppender(appender);
        try {
            T result = build.get();
            return new CapturedBuild<>(result, appender.list);
        } finally {
            optionsLogger.detachAppender(appender);
        }
    }

    private static List<String> messagesAt(List<ILoggingEvent> events, Level level) {
        return events.stream()
                .filter(event -> event.getLevel() == level)
                .map(ILoggingEvent::getFormattedMessage)
                .collect(Collectors.toList());
    }

    private static final class CapturedBuild<T> {
        private final T result;
        private final List<ILoggingEvent> events;

        private CapturedBuild(T result, List<ILoggingEvent> events) {
            this.result = result;
            this.events = events;
        }
    }
}
