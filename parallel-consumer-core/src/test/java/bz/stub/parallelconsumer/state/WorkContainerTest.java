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
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.function.Function;

import static bz.stub.parallelconsumer.ManagedTruth.assertThat;
import static org.mockito.Mockito.mock;

class WorkContainerTest {

    @Test
    void basics() {
        var workContainer = new ModelUtils(new PCModuleTestEnv()).createWorkFor(0);
        assertThat(workContainer).getDelayUntilRetryDue().isNegative();
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
