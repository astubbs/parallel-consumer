package bz.stub.parallelconsumer.proxy.config;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.proxy.protocol.WireDurations;
import bz.stub.parallelconsumer.proxy.protocol.v1.CommitMode;
import bz.stub.parallelconsumer.proxy.protocol.v1.Configure;
import bz.stub.parallelconsumer.proxy.protocol.v1.Configured;
import bz.stub.parallelconsumer.proxy.protocol.v1.InvalidOffsetMetadataPolicy;
import bz.stub.parallelconsumer.proxy.engine.LivenessSettings;
import bz.stub.parallelconsumer.proxy.protocol.v1.ProcessingOrder;
import lombok.experimental.UtilityClass;

import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.function.IntUnaryOperator;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * Maps the wire {@code Configure} onto core's {@link ParallelConsumerOptions}, and a built options object back
 * onto the wire {@code Configured} - the effective-values echo, so a client asserts what it got rather than
 * what it asked for. U7 of the language-proxy plan (astubbs#242); requirements R10, R36, R39, R40; decisions
 * KTD5, KTD16, KTD38.
 * <p>
 * <b>Only scalar, enum and {@code Duration} options travel (R39/KTD5).</b> The enumerated set is every wire
 * field this class reads; what does NOT travel is decided, not forgotten:
 * <ul>
 *     <li>the five object-valued options - {@code consumer}, {@code producer}, {@code meterRegistry},
 *     {@code metricsTags}, {@code retryDelayProvider} - per R10. The consumer and producer are constructed
 *     proxy-side from {@code kafka_properties} (R48, {@link KafkaClientFactory}); the meter registry is the
 *     proxy's own; {@code retryDelayProvider} is a user callback that would cost an RPC round trip per retry;</li>
 *     <li>{@code batchSize} - pinned to 1 by the engine (KTD10), so a value the proxy would always refuse has
 *     no business on the wire;</li>
 *     <li>the transactional-only knobs ({@code allowEagerProcessingDuringTransactionCommit},
 *     {@code commitLockAcquisitionTimeout}, {@code produceLockAcquisitionTimeout}) - meaningless while the
 *     transactional commit mode itself is refused (KTD7);</li>
 *     <li>{@code managedExecutorService}/{@code managedThreadFactory} (the sidecar JVM's own environment, not
 *     client configuration), and {@code ignoreReflectiveAccessExceptionsForAutoCommitDisabledCheck} (a
 *     workaround for wrapped consumer classes, and the proxy constructs its own standard one).</li>
 * </ul>
 * <b>Rejection happens here, before any Kafka client is constructed</b> - a refused {@code Configure} must
 * cost nothing and leak nothing. Rejection messages never embed {@code kafka_properties} content.
 *
 * @author Antony Stubbs
 * @see ConfigureHandler
 */
@UtilityClass
public class OptionsMapper {

    /**
     * KTD38: the executor count is a pure function of connect-time configuration - computed once from max
     * concurrency and nothing else, sent once in {@code Configured}, never revised. Nothing the proxy observes
     * about the client may feed it.
     * <p>
     * <b>The formula itself is an open plan question</b> ("KTD38's executor-count function is named but never
     * defined", Deferred / Open Questions in {@code docs/plans/2026-08-14-001-feat-language-proxy-plan.md}):
     * identity means a Python application with max concurrency 500 would spawn 500 worker processes, and the
     * reviewer-recommended {@code min(max concurrency, client-supplied cap)} needs a deliberate yes because ten
     * clients inherit it. Until the plan answers, this is identity - the simplest function with the required
     * no-observed-input property.
     */
    public static final IntUnaryOperator EXECUTOR_COUNT_FUNCTION = IntUnaryOperator.identity();

    /** A {@code Configure} the proxy refuses; the message is the client-facing reason, credential-free. */
    public static class ConfigureRejectedException extends IllegalArgumentException {
        public ConfigureRejectedException(String reason) {
            super(reason);
        }
    }

    /** The KTD38 function applied to built options - the one value {@code Configured.executor_count} carries. */
    public static int executorCountFor(ParallelConsumerOptions<?, ?> options) {
        return EXECUTOR_COUNT_FUNCTION.applyAsInt(options.getMaxConcurrency());
    }

    /**
     * The subscription {@code Configure} names: exactly one of a topic list or a pattern (R36). Fixed for the
     * process lifetime by {@link ConfigureHandler}.
     */
    public static final class Subscription {
        private final List<String> topics;

        /**
         * The compiled pattern, carried rather than re-derived: {@link #subscriptionOf} has to compile it
         * anyway to refuse an invalid one, and {@code Pattern} remembers its own source text, so one field
         * serves both the engine's {@code subscribe} call and the {@code Configured} echo.
         */
        private final Pattern pattern;

        private Subscription(List<String> topics, Pattern pattern) {
            this.topics = topics;
            this.pattern = pattern;
        }

        public boolean isPattern() {
            return pattern != null;
        }

        public List<String> topics() {
            return topics;
        }

        /** The pattern's source text as {@code Configure} sent it, or {@code null} for a topic list. */
        public String pattern() {
            return pattern == null ? null : pattern.pattern();
        }

        /** The pattern the engine subscribes with - the same instance {@link #subscriptionOf} validated. */
        public Pattern compiledPattern() {
            return pattern;
        }
    }

    /**
     * Reads the subscription out of a {@code Configure}, refusing an ambiguous one. A pattern is compiled here,
     * once - and the {@link Subscription} carries that instance to the engine's {@code subscribe} call -
     * precisely so an invalid one is refused <em>before any Kafka client is constructed</em>. Left to a
     * {@code Pattern.compile} at subscribe time, the {@code PatternSyntaxException}
     * would fire after the consumer, producer and engine were already built, leaking all three. The rejection
     * message may embed the pattern: it is subscription data, never {@code kafka_properties}.
     *
     * @throws ConfigureRejectedException when both a topic list and a pattern are given, or neither - or the
     *                                    pattern does not compile
     */
    public static Subscription subscriptionOf(Configure configure) {
        boolean hasTopics = configure.getTopicsCount() > 0;
        boolean hasPattern = configure.hasTopicPattern();
        if (hasTopics == hasPattern) {
            throw new ConfigureRejectedException(hasTopics
                    ? "Configure must name exactly one subscription form: it carries both a topic list and a "
                    + "topic pattern (R36)"
                    : "Configure names no subscription: give either a topic list or a topic pattern (R36)");
        }
        if (hasPattern) {
            Pattern compiled;
            try {
                compiled = Pattern.compile(configure.getTopicPattern());
            } catch (PatternSyntaxException invalid) {
                throw new ConfigureRejectedException(
                        "topic_pattern is not a valid regular expression: " + invalid.getMessage());
            }
            return new Subscription(null, compiled);
        }
        return new Subscription(List.copyOf(configure.getTopicsList()), null);
    }

    /**
     * Maps the wire options onto a core options builder. Unset fields are left untouched, so core's own
     * defaults apply and the {@link #effectiveConfiguration effective echo} reports them.
     * <p>
     * The returned builder still needs the constructed consumer and producer - deliberately, so rejection has
     * already happened by the time anything real is built.
     *
     * @throws ConfigureRejectedException for the transactional commit mode (KTD7's boundary - refused here with
     *                                    a message naming the restriction, rather than letting
     *                                    {@code ExternalEngine}'s constructor throw an opaque
     *                                    {@code IllegalStateException}), or an unrecognized enum value
     */
    public static ParallelConsumerOptions.ParallelConsumerOptionsBuilder<byte[], byte[]> toOptionsBuilder(
            Configure configure) {
        if (configure.getCommitMode() == CommitMode.COMMIT_MODE_PERIODIC_TRANSACTIONAL_PRODUCER) {
            throw new ConfigureRejectedException(
                    "commit mode PERIODIC_TRANSACTIONAL_PRODUCER is not available through the proxy: "
                            + "exactly-once through the proxy is impossible in v1, not merely unbuilt (KTD7 - "
                            + "lifting the restriction is sanctioned post-v6 core work). The proxy is "
                            + "at-least-once; use PERIODIC_CONSUMER_SYNC or PERIODIC_CONSUMER_ASYNCHRONOUS");
        }

        var builder = ParallelConsumerOptions.<byte[], byte[]>builder();
        if (configure.hasMaxConcurrency()) {
            if (configure.getMaxConcurrency() < 1) {
                // refused by name here, not left to the engine constructor's wave-size-cap check - which fires
                // only after the Kafka clients are already built, and names a derived quantity, not the field
                throw new ConfigureRejectedException(
                        "max_concurrency must be at least 1, got " + configure.getMaxConcurrency());
            }
            builder.maxConcurrency(configure.getMaxConcurrency());
        }
        if (configure.getOrdering() != ProcessingOrder.PROCESSING_ORDER_UNSPECIFIED) {
            builder.ordering(toCoreOrdering(configure.getOrdering(), configure.getOrderingValue()));
        }
        if (configure.getCommitMode() != CommitMode.COMMIT_MODE_UNSPECIFIED) {
            builder.commitMode(toCoreCommitMode(configure.getCommitMode(), configure.getCommitModeValue()));
        }
        if (configure.hasCommitInterval()) {
            builder.commitInterval(WireDurations.toJava(configure.getCommitInterval()));
        }
        if (configure.hasDefaultMessageRetryDelay()) {
            builder.defaultMessageRetryDelay(WireDurations.toJava(configure.getDefaultMessageRetryDelay()));
        }
        if (configure.hasSendTimeout()) {
            builder.sendTimeout(WireDurations.toJava(configure.getSendTimeout()));
        }
        if (configure.hasOffsetCommitTimeout()) {
            builder.offsetCommitTimeout(WireDurations.toJava(configure.getOffsetCommitTimeout()));
        }
        if (configure.hasShutdownTimeout()) {
            builder.shutdownTimeout(WireDurations.toJava(configure.getShutdownTimeout()));
        }
        if (configure.hasDrainTimeout()) {
            builder.drainTimeout(WireDurations.toJava(configure.getDrainTimeout()));
        }
        if (configure.hasThresholdForTimeSpendInQueueWarning()) {
            builder.thresholdForTimeSpendInQueueWarning(
                    WireDurations.toJava(configure.getThresholdForTimeSpendInQueueWarning()));
        }
        if (configure.hasSaslAuthenticationRetryTimeout()) {
            builder.saslAuthenticationRetryTimeout(WireDurations.toJava(configure.getSaslAuthenticationRetryTimeout()));
        }
        if (configure.hasSaslAuthenticationExceptionRetryBackoff()) {
            builder.saslAuthenticationExceptionRetryBackoff(
                    WireDurations.toJava(configure.getSaslAuthenticationExceptionRetryBackoff()));
        }
        if (configure.hasMaxFailureHistory()) {
            builder.maxFailureHistory(configure.getMaxFailureHistory());
        }
        if (configure.hasMessageBufferSize()) {
            builder.messageBufferSize(configure.getMessageBufferSize());
        }
        if (configure.hasInitialLoadFactor()) {
            builder.initialLoadFactor(configure.getInitialLoadFactor());
        }
        if (configure.hasMaximumLoadFactor()) {
            builder.maximumLoadFactor(configure.getMaximumLoadFactor());
        }
        if (configure.hasPcInstanceTag()) {
            builder.pcInstanceTag(configure.getPcInstanceTag());
        }
        if (configure.getInvalidOffsetMetadataPolicy()
                != InvalidOffsetMetadataPolicy.INVALID_OFFSET_METADATA_POLICY_UNSPECIFIED) {
            builder.invalidOffsetMetadataPolicy(toCorePolicy(configure.getInvalidOffsetMetadataPolicy(),
                    configure.getInvalidOffsetMetadataPolicyValue()));
        }
        return builder;
    }

    /**
     * The effective-values echo: built entirely from the constructed options object (so defaults are reported
     * as the values they became, not as absences), plus the negotiated capability intersection and the KTD38
     * executor count. Carries no {@code kafka_properties} <b>by construction</b> - the wire {@code Configured}
     * has no such field to fill.
     */
    public static Configured effectiveConfiguration(ParallelConsumerOptions<?, ?> options,
                                                    Subscription subscription,
                                                    List<String> negotiatedCapabilities,
                                                    LivenessSettings liveness) {
        var configured = Configured.newBuilder()
                .setMaxConcurrency(options.getMaxConcurrency())
                .setExecutorCount(executorCountFor(options))
                .addAllCapabilities(negotiatedCapabilities)
                .setOrdering(toWireOrdering(options.getOrdering()))
                .setCommitMode(toWireCommitMode(options.getCommitMode()))
                .setCommitInterval(WireDurations.toWire(options.getCommitInterval()))
                .setDefaultMessageRetryDelay(WireDurations.toWire(options.getDefaultMessageRetryDelay()))
                .setSendTimeout(WireDurations.toWire(options.getSendTimeout()))
                .setOffsetCommitTimeout(WireDurations.toWire(options.getOffsetCommitTimeout()))
                .setShutdownTimeout(WireDurations.toWire(options.getShutdownTimeout()))
                .setDrainTimeout(WireDurations.toWire(options.getDrainTimeout()))
                .setThresholdForTimeSpendInQueueWarning(
                        WireDurations.toWire(options.getThresholdForTimeSpendInQueueWarning()))
                .setSaslAuthenticationRetryTimeout(WireDurations.toWire(options.getSaslAuthenticationRetryTimeout()))
                .setSaslAuthenticationExceptionRetryBackoff(
                        WireDurations.toWire(options.getSaslAuthenticationExceptionRetryBackoff()))
                .setMaxFailureHistory(options.getMaxFailureHistory())
                .setInvalidOffsetMetadataPolicy(toWirePolicy(options.getInvalidOffsetMetadataPolicy()))
                .setMessageBufferSize(options.getMessageBufferSize())
                .setInitialLoadFactor(options.getInitialLoadFactor())
                .setMaximumLoadFactor(options.getMaximumLoadFactor());
        if (options.getPcInstanceTag() != null) {
            configured.setPcInstanceTag(options.getPcInstanceTag());
        }
        if (subscription.isPattern()) {
            configured.setTopicPattern(subscription.pattern());
        } else {
            configured.addAllTopics(subscription.topics());
        }
        // capability-gated, per the specification's carve-out: the numbers are absent, not defaulted, when the
        // client did not negotiate the machinery they configure - a client must never be told to heartbeat on
        // a session where no lease exists
        if (negotiatedCapabilities.contains(ConfigureHandler.CAPABILITY_HEARTBEAT)) {
            configured.setLeaseDuration(WireDurations.toWire(liveness.leaseDuration()))
                    .setHeartbeatInterval(WireDurations.toWire(liveness.heartbeatInterval()));
        }
        if (negotiatedCapabilities.contains(ConfigureHandler.CAPABILITY_MANIFEST)) {
            configured.setReconnectWindow(WireDurations.toWire(liveness.reconnectWindow()));
        }
        return configured.build();
    }

    /**
     * The session's liveness numbers: whatever {@code Configure} named, the defaults for what it did not, and
     * the {@code heartbeat} capability carried through as the switch that decides whether any lease clock runs
     * at all (R46).
     * <p>
     * Rejection happens here for the same reason every other rejection does - before any Kafka client exists.
     * A non-positive duration is meaningless in all three fields, and a heartbeat interval at or above the
     * lease is a configuration that expires a client obeying it exactly.
     *
     * @throws ConfigureRejectedException on a non-positive duration, or an interval the lease cannot survive
     */
    public static LivenessSettings livenessSettingsOf(Configure configure, List<String> negotiatedCapabilities,
                                                      Clock clock) {
        var leaseDuration = durationOr(configure.hasLeaseDuration(), configure.getLeaseDuration(),
                LivenessSettings.DEFAULT_LEASE_DURATION, "lease_duration");
        var heartbeatInterval = durationOr(configure.hasHeartbeatInterval(), configure.getHeartbeatInterval(),
                LivenessSettings.DEFAULT_HEARTBEAT_INTERVAL, "heartbeat_interval");
        var reconnectWindow = durationOr(configure.hasReconnectWindow(), configure.getReconnectWindow(),
                LivenessSettings.DEFAULT_RECONNECT_WINDOW, "reconnect_window");
        if (heartbeatInterval.compareTo(leaseDuration) >= 0) {
            throw new ConfigureRejectedException(
                    "heartbeat_interval (" + heartbeatInterval + ") must be shorter than lease_duration ("
                            + leaseDuration + "): a client heartbeating exactly as instructed would otherwise "
                            + "have its records reclaimed while it is alive and working (R46)");
        }
        return new LivenessSettings(negotiatedCapabilities.contains(ConfigureHandler.CAPABILITY_HEARTBEAT),
                leaseDuration, heartbeatInterval, reconnectWindow, clock);
    }

    private static Duration durationOr(boolean present, com.google.protobuf.Duration wire, Duration fallback,
                                       String fieldName) {
        if (!present) {
            return fallback;
        }
        var duration = WireDurations.toJava(wire);
        if (duration.isZero() || duration.isNegative()) {
            throw new ConfigureRejectedException(fieldName + " must be positive, got " + duration);
        }
        return duration;
    }

    // --- enum and Duration bridges; each unrecognized wire value is a rejection, never a silent default ---
    // Each inbound bridge takes the raw wire int alongside the enum: the unrecognized case it must report is
    // exactly the one where the generated getNumber() THROWS ("Can't get the number of an unknown enum
    // value"), so the number for the rejection message can only come from the get*Value() accessor.

    private static ParallelConsumerOptions.ProcessingOrder toCoreOrdering(ProcessingOrder ordering,
                                                                          int wireValue) {
        switch (ordering) {
            case PROCESSING_ORDER_UNORDERED:
                return ParallelConsumerOptions.ProcessingOrder.UNORDERED;
            case PROCESSING_ORDER_PARTITION:
                return ParallelConsumerOptions.ProcessingOrder.PARTITION;
            case PROCESSING_ORDER_KEY:
                return ParallelConsumerOptions.ProcessingOrder.KEY;
            default:
                throw new ConfigureRejectedException("unrecognized ordering value " + wireValue);
        }
    }

    private static ProcessingOrder toWireOrdering(ParallelConsumerOptions.ProcessingOrder ordering) {
        switch (ordering) {
            case UNORDERED:
                return ProcessingOrder.PROCESSING_ORDER_UNORDERED;
            case PARTITION:
                return ProcessingOrder.PROCESSING_ORDER_PARTITION;
            case KEY:
            default:
                return ProcessingOrder.PROCESSING_ORDER_KEY;
        }
    }

    private static ParallelConsumerOptions.CommitMode toCoreCommitMode(CommitMode commitMode, int wireValue) {
        switch (commitMode) {
            case COMMIT_MODE_PERIODIC_CONSUMER_SYNC:
                return ParallelConsumerOptions.CommitMode.PERIODIC_CONSUMER_SYNC;
            case COMMIT_MODE_PERIODIC_CONSUMER_ASYNCHRONOUS:
                return ParallelConsumerOptions.CommitMode.PERIODIC_CONSUMER_ASYNCHRONOUS;
            default:
                // the transactional mode was already refused by name before this bridge runs
                throw new ConfigureRejectedException("unrecognized commit mode value " + wireValue);
        }
    }

    private static CommitMode toWireCommitMode(ParallelConsumerOptions.CommitMode commitMode) {
        switch (commitMode) {
            case PERIODIC_CONSUMER_SYNC:
                return CommitMode.COMMIT_MODE_PERIODIC_CONSUMER_SYNC;
            case PERIODIC_CONSUMER_ASYNCHRONOUS:
                return CommitMode.COMMIT_MODE_PERIODIC_CONSUMER_ASYNCHRONOUS;
            case PERIODIC_TRANSACTIONAL_PRODUCER:
            default:
                // unreachable while KTD7 holds: a transactional Configure never builds options to echo
                return CommitMode.COMMIT_MODE_PERIODIC_TRANSACTIONAL_PRODUCER;
        }
    }

    private static ParallelConsumerOptions.InvalidOffsetMetadataHandlingPolicy toCorePolicy(
            InvalidOffsetMetadataPolicy policy, int wireValue) {
        switch (policy) {
            case INVALID_OFFSET_METADATA_POLICY_FAIL:
                return ParallelConsumerOptions.InvalidOffsetMetadataHandlingPolicy.FAIL;
            case INVALID_OFFSET_METADATA_POLICY_IGNORE:
                return ParallelConsumerOptions.InvalidOffsetMetadataHandlingPolicy.IGNORE;
            default:
                throw new ConfigureRejectedException(
                        "unrecognized invalid-offset-metadata policy value " + wireValue);
        }
    }

    private static InvalidOffsetMetadataPolicy toWirePolicy(
            ParallelConsumerOptions.InvalidOffsetMetadataHandlingPolicy policy) {
        switch (policy) {
            case IGNORE:
                return InvalidOffsetMetadataPolicy.INVALID_OFFSET_METADATA_POLICY_IGNORE;
            case FAIL:
            default:
                return InvalidOffsetMetadataPolicy.INVALID_OFFSET_METADATA_POLICY_FAIL;
        }
    }
}
