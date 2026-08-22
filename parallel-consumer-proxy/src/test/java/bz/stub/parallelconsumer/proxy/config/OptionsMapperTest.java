package bz.stub.parallelconsumer.proxy.config;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.proxy.engine.LivenessSettings;
import bz.stub.parallelconsumer.proxy.protocol.v1.CommitMode;
import bz.stub.parallelconsumer.proxy.protocol.v1.Configure;
import bz.stub.parallelconsumer.proxy.protocol.v1.Configured;
import bz.stub.parallelconsumer.proxy.protocol.v1.InvalidOffsetMetadataPolicy;
import bz.stub.parallelconsumer.proxy.protocol.v1.ProcessingOrder;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.util.List;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * The option-mapping half of connect-time configuration (the language-proxy plan's U7): every wire option
 * lands on the built {@code ParallelConsumerOptions}, omissions take core's defaults and the effective echo
 * reports them, and the rejects refuse by name before anything real is constructed.
 *
 * @author Antony Stubbs
 */
class OptionsMapperTest {

    /** Every mapped option round-trips: wire value in, same value on the built options object. */
    @Test
    void everyMappedOptionLandsOnTheBuiltOptions() {
        var configure = Configure.newBuilder()
                .addTopics("in")
                .setMaxConcurrency(7)
                .setOrdering(ProcessingOrder.PROCESSING_ORDER_PARTITION)
                .setCommitMode(CommitMode.COMMIT_MODE_PERIODIC_CONSUMER_SYNC)
                .setCommitInterval(wireDuration(Duration.ofMillis(250)))
                .setDefaultMessageRetryDelay(wireDuration(Duration.ofMillis(75)))
                .setSendTimeout(wireDuration(Duration.ofSeconds(3)))
                .setOffsetCommitTimeout(wireDuration(Duration.ofSeconds(4)))
                .setShutdownTimeout(wireDuration(Duration.ofSeconds(5)))
                .setDrainTimeout(wireDuration(Duration.ofSeconds(6)))
                .setThresholdForTimeSpendInQueueWarning(wireDuration(Duration.ofSeconds(7)))
                .setSaslAuthenticationRetryTimeout(wireDuration(Duration.ofSeconds(8)))
                .setSaslAuthenticationExceptionRetryBackoff(wireDuration(Duration.ofSeconds(9)))
                .setMaxFailureHistory(3)
                .setInvalidOffsetMetadataPolicy(InvalidOffsetMetadataPolicy.INVALID_OFFSET_METADATA_POLICY_IGNORE)
                .build();

        var options = OptionsMapper.toOptionsBuilder(configure).build();

        assertThat(options.getMaxConcurrency()).isEqualTo(7);
        assertThat(options.getOrdering()).isEqualTo(ParallelConsumerOptions.ProcessingOrder.PARTITION);
        assertThat(options.getCommitMode())
                .isEqualTo(ParallelConsumerOptions.CommitMode.PERIODIC_CONSUMER_SYNC);
        assertThat(options.getCommitInterval()).isEqualTo(Duration.ofMillis(250));
        assertThat(options.getDefaultMessageRetryDelay()).isEqualTo(Duration.ofMillis(75));
        assertThat(options.getSendTimeout()).isEqualTo(Duration.ofSeconds(3));
        assertThat(options.getOffsetCommitTimeout()).isEqualTo(Duration.ofSeconds(4));
        assertThat(options.getShutdownTimeout()).isEqualTo(Duration.ofSeconds(5));
        assertThat(options.getDrainTimeout()).isEqualTo(Duration.ofSeconds(6));
        assertThat(options.getThresholdForTimeSpendInQueueWarning()).isEqualTo(Duration.ofSeconds(7));
        assertThat(options.getSaslAuthenticationRetryTimeout()).isEqualTo(Duration.ofSeconds(8));
        assertThat(options.getSaslAuthenticationExceptionRetryBackoff()).isEqualTo(Duration.ofSeconds(9));
        assertThat(options.getMaxFailureHistory()).isEqualTo(3);
        assertThat(options.getInvalidOffsetMetadataPolicy())
                .isEqualTo(ParallelConsumerOptions.InvalidOffsetMetadataHandlingPolicy.IGNORE);
    }

    /**
     * Omitted options take core's defaults, and the effective echo reports the DEFAULTED values - what the
     * client got, not what it asked for.
     */
    @Test
    void omittedOptionsTakeCoresDefaultsAndTheEchoReportsThem() {
        var configure = Configure.newBuilder().addTopics("in").build();

        var options = OptionsMapper.toOptionsBuilder(configure).build();
        var configured = OptionsMapper.effectiveConfiguration(options,
                OptionsMapper.subscriptionOf(configure), ConfigureHandler.PROXY_CAPABILITIES,
                OptionsMapper.livenessSettingsOf(configure, ConfigureHandler.PROXY_CAPABILITIES,
                        Clock.systemUTC()));

        assertThat(configured.getTopicsList()).containsExactly("in");
        assertThat(configured.getMaxConcurrency())
                .isEqualTo(ParallelConsumerOptions.DEFAULT_MAX_CONCURRENCY);
        assertThat(configured.getOrdering()).isEqualTo(ProcessingOrder.PROCESSING_ORDER_KEY);
        assertThat(configured.getCommitMode())
                .isEqualTo(CommitMode.COMMIT_MODE_PERIODIC_CONSUMER_ASYNCHRONOUS);
        assertThat(javaDuration(configured.getCommitInterval()))
                .isEqualTo(ParallelConsumerOptions.DEFAULT_COMMIT_INTERVAL);
        assertThat(javaDuration(configured.getDefaultMessageRetryDelay()))
                .isEqualTo(ParallelConsumerOptions.DEFAULT_STATIC_RETRY_DELAY);
        assertThat(javaDuration(configured.getSendTimeout())).isEqualTo(Duration.ofSeconds(10));
        assertThat(javaDuration(configured.getOffsetCommitTimeout())).isEqualTo(Duration.ofSeconds(10));
        assertThat(javaDuration(configured.getShutdownTimeout())).isEqualTo(Duration.ofSeconds(10));
        assertThat(javaDuration(configured.getDrainTimeout())).isEqualTo(Duration.ofSeconds(30));
        assertThat(javaDuration(configured.getThresholdForTimeSpendInQueueWarning()))
                .isEqualTo(Duration.ofSeconds(10));
        assertThat(javaDuration(configured.getSaslAuthenticationRetryTimeout())).isEqualTo(Duration.ZERO);
        assertThat(javaDuration(configured.getSaslAuthenticationExceptionRetryBackoff()))
                .isEqualTo(ParallelConsumerOptions.SASL_AUTHENTICATION_EXCEPTION_RETRY_BACKOFF);
        assertThat(configured.getMaxFailureHistory()).isEqualTo(10);
        assertThat(configured.getInvalidOffsetMetadataPolicy())
                .isEqualTo(InvalidOffsetMetadataPolicy.INVALID_OFFSET_METADATA_POLICY_FAIL);
    }

    /**
     * KTD7's boundary, refused by name: the transactional commit mode gets a message naming the restriction,
     * not {@code ExternalEngine}'s opaque {@code IllegalStateException} - and it must refuse during mapping,
     * before any Kafka client could have been constructed.
     */
    @Test
    void transactionalCommitModeIsRefusedNamingTheRestriction() {
        var configure = Configure.newBuilder()
                .addTopics("in")
                .setCommitMode(CommitMode.COMMIT_MODE_PERIODIC_TRANSACTIONAL_PRODUCER)
                .build();

        var rejection = assertThrows(OptionsMapper.ConfigureRejectedException.class,
                () -> OptionsMapper.toOptionsBuilder(configure));

        assertThat(rejection).hasMessageThat().contains("PERIODIC_TRANSACTIONAL_PRODUCER");
        assertWithMessage("the refusal points at the decision that shut the door")
                .that(rejection).hasMessageThat().contains("KTD7");
    }

    /** R36: the subscription is exactly one of a topic list or a pattern - both and neither are refused. */
    @Test
    void anAmbiguousSubscriptionIsRefused() {
        var both = Configure.newBuilder().addTopics("in").setTopicPattern("in-.*").build();
        var neither = Configure.newBuilder().build();

        assertThrows(OptionsMapper.ConfigureRejectedException.class,
                () -> OptionsMapper.subscriptionOf(both));
        assertThrows(OptionsMapper.ConfigureRejectedException.class,
                () -> OptionsMapper.subscriptionOf(neither));
    }

    /**
     * An invalid {@code topic_pattern} is refused during mapping - compiled here, once, precisely so the
     * refusal happens before any Kafka client is constructed. The message may embed the pattern: it is
     * subscription data, never {@code kafka_properties}.
     */
    @Test
    void anInvalidTopicPatternIsRefusedDuringMapping() {
        var configure = Configure.newBuilder().setTopicPattern("input-[").build();

        var rejection = assertThrows(OptionsMapper.ConfigureRejectedException.class,
                () -> OptionsMapper.subscriptionOf(configure));

        assertThat(rejection).hasMessageThat().contains("topic_pattern");
        assertThat(rejection).hasMessageThat().contains("input-[");
    }

    /** A non-positive {@code max_concurrency} is refused by name, not left to a construction-time cap check. */
    @Test
    void aNonPositiveMaxConcurrencyIsRefusedByName() {
        var configure = Configure.newBuilder().addTopics("in").setMaxConcurrency(0).build();

        var rejection = assertThrows(OptionsMapper.ConfigureRejectedException.class,
                () -> OptionsMapper.toOptionsBuilder(configure));

        assertThat(rejection).hasMessageThat().contains("max_concurrency");
    }

    /**
     * Forward compatibility: an enum wire number this proxy's schema does not know maps to
     * {@code UNRECOGNIZED}, whose generated {@code getNumber()} throws - so each bridge must carry the raw
     * wire int (the generated {@code get*Value()} accessor) into its rejection message, or the clean
     * rejection itself blows up.
     */
    @Test
    void unknownEnumWireNumbersAreRefusedCleanlyNamingTheNumber() {
        var ordering = assertThrows(OptionsMapper.ConfigureRejectedException.class,
                () -> OptionsMapper.toOptionsBuilder(
                        Configure.newBuilder().addTopics("in").setOrderingValue(99).build()));
        assertThat(ordering).hasMessageThat().contains("99");

        var commitMode = assertThrows(OptionsMapper.ConfigureRejectedException.class,
                () -> OptionsMapper.toOptionsBuilder(
                        Configure.newBuilder().addTopics("in").setCommitModeValue(88).build()));
        assertThat(commitMode).hasMessageThat().contains("88");

        var policy = assertThrows(OptionsMapper.ConfigureRejectedException.class,
                () -> OptionsMapper.toOptionsBuilder(
                        Configure.newBuilder().addTopics("in").setInvalidOffsetMetadataPolicyValue(77).build()));
        assertThat(policy).hasMessageThat().contains("77");
    }

    /**
     * KTD38: the executor count is the pure function of max concurrency - identity, until the plan's open
     * question ("KTD38's executor-count function is named but never defined") settles the formula - and it is
     * derived from connect-time configuration only, which this pins by deriving it twice from the same input.
     */
    @Test
    void executorCountIsThePureFunctionOfMaxConcurrency() {
        var options = OptionsMapper.toOptionsBuilder(
                Configure.newBuilder().addTopics("in").setMaxConcurrency(5).build()).build();

        assertThat(OptionsMapper.executorCountFor(options)).isEqualTo(5);
        assertWithMessage("the function is pure: same configuration, same count, no observed input")
                .that(OptionsMapper.EXECUTOR_COUNT_FUNCTION.applyAsInt(5))
                .isEqualTo(OptionsMapper.executorCountFor(options));
    }

    /** The liveness numbers default when the client names none, and travel in the echo it reads back. */
    @Test
    void theLivenessNumbersDefaultAndAreEchoed() {
        var configure = Configure.newBuilder().addTopics("in").build();

        var liveness = OptionsMapper.livenessSettingsOf(configure, ConfigureHandler.PROXY_CAPABILITIES,
                Clock.systemUTC());
        var configured = OptionsMapper.effectiveConfiguration(
                OptionsMapper.toOptionsBuilder(configure).build(), OptionsMapper.subscriptionOf(configure),
                ConfigureHandler.PROXY_CAPABILITIES, liveness);

        assertThat(liveness.leasesEnabled()).isTrue();
        assertThat(javaDuration(configured.getLeaseDuration()))
                .isEqualTo(LivenessSettings.DEFAULT_LEASE_DURATION);
        assertThat(javaDuration(configured.getHeartbeatInterval()))
                .isEqualTo(LivenessSettings.DEFAULT_HEARTBEAT_INTERVAL);
        assertThat(javaDuration(configured.getReconnectWindow()))
                .isEqualTo(LivenessSettings.DEFAULT_RECONNECT_WINDOW);
    }

    /**
     * The specification's carve-out: a capability-gated number is ABSENT, not defaulted, when its capability
     * was not negotiated - a client must never be handed an interval for a lease that does not exist.
     */
    @Test
    void theLivenessNumbersAreAbsentWhenTheirCapabilityWasNotNegotiated() {
        var configure = Configure.newBuilder().addTopics("in").build();
        var negotiated = List.of(ConfigureHandler.CAPABILITY_DISPATCH);

        var liveness = OptionsMapper.livenessSettingsOf(configure, negotiated, Clock.systemUTC());
        var configured = OptionsMapper.effectiveConfiguration(
                OptionsMapper.toOptionsBuilder(configure).build(), OptionsMapper.subscriptionOf(configure),
                negotiated, liveness);

        assertWithMessage("no heartbeat capability means no lease machinery at all")
                .that(liveness.leasesEnabled()).isFalse();
        assertThat(configured.hasLeaseDuration()).isFalse();
        assertThat(configured.hasHeartbeatInterval()).isFalse();
        assertThat(configured.hasReconnectWindow()).isFalse();
    }

    /** A client's own numbers travel, and the echo reports what it actually got. */
    @Test
    void aClientsOwnLivenessNumbersAreHonoured() {
        var configure = Configure.newBuilder()
                .addTopics("in")
                .setLeaseDuration(wireDuration(Duration.ofSeconds(90)))
                .setHeartbeatInterval(wireDuration(Duration.ofSeconds(15)))
                .setReconnectWindow(wireDuration(Duration.ofSeconds(45)))
                .build();

        var liveness = OptionsMapper.livenessSettingsOf(configure, ConfigureHandler.PROXY_CAPABILITIES,
                Clock.systemUTC());

        assertThat(liveness.leaseDuration()).isEqualTo(Duration.ofSeconds(90));
        assertThat(liveness.heartbeatInterval()).isEqualTo(Duration.ofSeconds(15));
        assertThat(liveness.reconnectWindow()).isEqualTo(Duration.ofSeconds(45));
    }

    /**
     * A heartbeat interval at or above the lease reclaims records from a client heartbeating exactly as
     * instructed - refused here, before any Kafka client is constructed.
     */
    @Test
    void aHeartbeatIntervalTheLeaseCannotSurviveIsRefused() {
        var configure = Configure.newBuilder()
                .addTopics("in")
                .setLeaseDuration(wireDuration(Duration.ofSeconds(10)))
                .setHeartbeatInterval(wireDuration(Duration.ofSeconds(10)))
                .build();

        var rejected = assertThrows(OptionsMapper.ConfigureRejectedException.class, () ->
                OptionsMapper.livenessSettingsOf(configure, ConfigureHandler.PROXY_CAPABILITIES,
                        Clock.systemUTC()));

        assertThat(rejected).hasMessageThat().contains("heartbeat_interval");
    }

    @Test
    void aNonPositiveLivenessDurationIsRefusedByName() {
        var configure = Configure.newBuilder()
                .addTopics("in")
                .setReconnectWindow(wireDuration(Duration.ZERO))
                .build();

        var rejected = assertThrows(OptionsMapper.ConfigureRejectedException.class, () ->
                OptionsMapper.livenessSettingsOf(configure, ConfigureHandler.PROXY_CAPABILITIES,
                        Clock.systemUTC()));

        assertThat(rejected).hasMessageThat().contains("reconnect_window");
    }

    /**
     * The effective echo excludes the credential-bearing property map STRUCTURALLY: the wire {@code Configured}
     * has no field that could carry it, so no code path can leak what cannot be expressed.
     */
    @Test
    void theEffectiveEchoHasNoFieldForTheCredentialMap() {
        assertWithMessage("Configure carries the credential map")
                .that(Configure.getDescriptor().findFieldByName("kafka_properties")).isNotNull();
        assertWithMessage("Configured must have no counterpart field for it")
                .that(Configured.getDescriptor().findFieldByName("kafka_properties")).isNull();
    }

    /** A pattern subscription echoes as the pattern, not as an empty topic list a client could misread. */
    @Test
    void aPatternSubscriptionEchoesAsThePattern() {
        var configure = Configure.newBuilder().setTopicPattern("in-.*").build();

        var configured = OptionsMapper.effectiveConfiguration(
                OptionsMapper.toOptionsBuilder(configure).build(),
                OptionsMapper.subscriptionOf(configure), List.of(),
                OptionsMapper.livenessSettingsOf(configure, List.of(), Clock.systemUTC()));

        assertThat(configured.getTopicPattern()).isEqualTo("in-.*");
        assertThat(configured.getTopicsList()).isEmpty();
    }

    private static com.google.protobuf.Duration wireDuration(Duration duration) {
        return com.google.protobuf.Duration.newBuilder()
                .setSeconds(duration.getSeconds())
                .setNanos(duration.getNano())
                .build();
    }

    private static Duration javaDuration(com.google.protobuf.Duration duration) {
        return Duration.ofSeconds(duration.getSeconds(), duration.getNanos());
    }
}
