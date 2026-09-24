package bz.stub.parallelconsumer.internal;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ExceptionInUserFunctionException;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TransactionalIdAuthorizationException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

/**
 * The lock-free half of producer recovery, as pure functions: what a replacement build failure means, how the
 * attempts are paced, and what may be logged of a failure (astubbs#225, R7).
 */
class ProducerRecoveryPolicyTest {

    @Test
    void backoffDoublesPerAttemptFromTheInitialDelayAndStopsAtTheCap() {
        Duration initial = Duration.ofSeconds(1);
        Duration max = Duration.ofSeconds(30);

        assertThat(ProducerRecoveryPolicy.backoffFor(1, initial, max)).isEqualTo(Duration.ofSeconds(1));
        assertThat(ProducerRecoveryPolicy.backoffFor(2, initial, max)).isEqualTo(Duration.ofSeconds(2));
        assertThat(ProducerRecoveryPolicy.backoffFor(3, initial, max)).isEqualTo(Duration.ofSeconds(4));
        assertThat(ProducerRecoveryPolicy.backoffFor(6, initial, max)).isEqualTo(Duration.ofSeconds(30));
        assertWithMessage("never above the cap, however many attempts").that(ProducerRecoveryPolicy.backoffFor(40, initial, max)).isEqualTo(max);
    }

    @Test
    void anAuthorizationOrVersionRefusalIsTerminalAndSoIsAnErrorHoweverItArrives() {
        assertThat(ProducerRecoveryPolicy.isTerminalBuildFailure(new TransactionalIdAuthorizationException("no ACL"))).isTrue();
        assertThat(ProducerRecoveryPolicy.isTerminalBuildFailure(new UnsupportedVersionException("old broker"))).isTrue();
        assertWithMessage("an Error from the build arrives wrapped as a user-function failure, and is terminal through the wrapper")
                .that(ProducerRecoveryPolicy.isTerminalBuildFailure(new ExceptionInUserFunctionException("build", new NoClassDefFoundError("com/example/Serializer")))).isTrue();
    }

    @Test
    void aTimeoutOrAnyOtherKafkaExceptionIsRetried() {
        assertThat(ProducerRecoveryPolicy.isTerminalBuildFailure(new TimeoutException("coordinator not available"))).isFalse();
        assertThat(ProducerRecoveryPolicy.isTerminalBuildFailure(new KafkaException("something transient"))).isFalse();
    }

    @Test
    void theSanitisedFailureKeepsTheTypeAndTheStackButNotTheMessage() {
        var raw = new KafkaException("password=hunter2 in the offending value");

        Throwable sanitised = ProducerRecoveryPolicy.sanitised(raw);

        assertThat(sanitised.getMessage()).contains(KafkaException.class.getName());
        assertWithMessage("a ConfigException carries the offending configuration value in its message, so no message survives")
                .that(sanitised.getMessage()).doesNotContain("hunter2");
        assertThat(sanitised.getStackTrace()).isEqualTo(raw.getStackTrace());
    }
}
