package bz.stub.parallelconsumer.state;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.FakeRuntimeException;
import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.RecordContext;
import bz.stub.parallelconsumer.internal.PCModule;
import bz.stub.parallelconsumer.internal.PCModuleTestEnv;
import com.google.common.truth.Truth8;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.function.Function;

import static bz.stub.parallelconsumer.ManagedTruth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static org.mockito.Mockito.mock;

class WorkContainerTest {

    @Test
    void basics() {
        var workContainer = new ModelUtils(new PCModuleTestEnv()).createWorkFor(0);
        assertThat(workContainer).getDelayUntilRetryDue().isNegative();
    }

    /**
     * astubbs#533: before the first failure the reason is an empty Optional, never null - through the public
     * {@link RecordContext} accessor a user function calls on first delivery, and the container it reads from. Then,
     * after a failure, it holds that failure.
     */
    @Test
    void lastFailureReasonIsEmptyNotNullBeforeTheFirstFailure() {
        WorkContainer<String, String> wc = new WorkContainer<>(0, mock(ConsumerRecord.class), new PCModuleTestEnv());

        var firstDelivery = new RecordContext<>(wc);
        assertWithMessage("RecordContext promises an Optional, so it must never be null")
                .that(firstDelivery.getLastFailureReason()).isNotNull();
        Truth8.assertThat(firstDelivery.getLastFailureReason()).isEmpty();
        Truth8.assertThat(wc.getLastFailureReason()).isEmpty();

        var failure = new FakeRuntimeException("first attempt failed");
        wc.onUserFunctionFailure(failure);

        Truth8.assertThat(new RecordContext<>(wc).getLastFailureReason()).hasValue(failure);
    }

    @Test
    void retryDelayProvider() {
        int uniqueMultiplier = 7;

        Function<RecordContext<String, String>, Duration> retryDelayProvider = context -> {
            final int numberOfFailedAttempts = context.getNumberOfFailedAttempts();
            return Duration.ofSeconds(numberOfFailedAttempts * uniqueMultiplier);
        };

        //
        var opts = ParallelConsumerOptions.<String, String>builder()
                .retryDelayProvider(retryDelayProvider)
                .build();
        PCModule module = new PCModuleTestEnv(opts);

        WorkContainer<String, String> wc = new WorkContainer<String, String>(0,
                mock(ConsumerRecord.class),
                module);

        //
        int numberOfFailures = 3;
        wc.onUserFunctionFailure(new FakeRuntimeException(""));
        wc.onUserFunctionFailure(new FakeRuntimeException(""));
        wc.onUserFunctionFailure(new FakeRuntimeException(""));

        //
        Duration retryDelayConfig = wc.getRetryDelayConfig();

        //
        assertThat(retryDelayConfig).getSeconds().isEqualTo(numberOfFailures * uniqueMultiplier);
    }
}
