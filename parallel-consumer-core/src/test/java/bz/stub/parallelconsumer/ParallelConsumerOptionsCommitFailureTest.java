package bz.stub.parallelconsumer;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions.CommitFailureContinueMode;
import bz.stub.parallelconsumer.ParallelConsumerOptions.CommitMode;
import bz.stub.parallelconsumer.internal.utils.LongPollingMockConsumer;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.jupiter.api.Test;

import static com.google.common.truth.Truth.assertThat;
import static org.apache.kafka.clients.consumer.OffsetResetStrategy.EARLIEST;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * The commit-failure options: defaults, the EOS coercion of {@link CommitFailureContinueMode#KEEP_PROCESSING} to
 * {@link CommitFailureContinueMode#PAUSE_INTAKE}, and the rejection of non-default commit-failure configuration under
 * the unsupported async commit mode.
 *
 * @author Antony Stubbs
 * @see ParallelConsumerOptions#validate()
 * @see CommitFailurePolicies
 */
class ParallelConsumerOptionsCommitFailureTest {

    private ParallelConsumerOptions.ParallelConsumerOptionsBuilder<String, String> base() {
        return ParallelConsumerOptions.<String, String>builder()
                .consumer(new LongPollingMockConsumer<>(EARLIEST));
    }

    private MockProducer<String, String> mockProducer() {
        return new MockProducer<>(true, Serdes.String().serializer(), Serdes.String().serializer());
    }

    @Test
    void defaultsAreShutDownHandlerAndKeepProcessing() {
        var options = base().build();

        assertThat(options.getCommitFailureHandler()).isSameInstanceAs(CommitFailurePolicies.shutDown());
        assertThat(options.getCommitFailureContinueMode()).isEqualTo(CommitFailureContinueMode.KEEP_PROCESSING);
    }

    /**
     * The default commit mode IS the async one - an all-defaults configuration must keep building and validating.
     */
    @Test
    void allDefaultsAsyncConfigurationStillValidates() {
        var options = base().build();

        assertThat(options.getCommitMode()).isEqualTo(CommitMode.PERIODIC_CONSUMER_ASYNCHRONOUS);
        options.validate(); // must not throw
        assertThat(options.getCommitFailureContinueMode()).isEqualTo(CommitFailureContinueMode.KEEP_PROCESSING);
    }

    @Test
    void transactionalCommitModeCoercesKeepProcessingToPauseIntake() {
        var options = base()
                .producer(mockProducer())
                .commitMode(CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER)
                .build();

        options.validate();

        assertThat(options.getCommitFailureContinueMode()).isEqualTo(CommitFailureContinueMode.PAUSE_INTAKE);
    }

    /**
     * The coercion is about what EOS can tolerate, not about defaults - an explicit KEEP_PROCESSING is coerced too.
     */
    @Test
    void transactionalCommitModeCoercesExplicitKeepProcessing() {
        var options = base()
                .producer(mockProducer())
                .commitMode(CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER)
                .commitFailureContinueMode(CommitFailureContinueMode.KEEP_PROCESSING)
                .build();

        options.validate();

        assertThat(options.getCommitFailureContinueMode()).isEqualTo(CommitFailureContinueMode.PAUSE_INTAKE);
    }

    @Test
    void asyncCommitModeRejectsNonDefaultHandler() {
        var options = base()
                .commitMode(CommitMode.PERIODIC_CONSUMER_ASYNCHRONOUS)
                .commitFailureHandler(CommitFailurePolicies.continueBounded())
                .build();

        var failure = assertThrows(IllegalArgumentException.class, options::validate);

        assertThat(failure).hasMessageThat().contains(CommitMode.PERIODIC_CONSUMER_SYNC.toString());
        assertThat(failure).hasMessageThat().contains(CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER.toString());
        assertThat(failure).hasMessageThat().contains("astubbs/parallel-consumer#317");
    }

    @Test
    void asyncCommitModeRejectsNonDefaultContinueMode() {
        var options = base()
                .commitMode(CommitMode.PERIODIC_CONSUMER_ASYNCHRONOUS)
                .commitFailureContinueMode(CommitFailureContinueMode.PAUSE_INTAKE)
                .build();

        var failure = assertThrows(IllegalArgumentException.class, options::validate);

        assertThat(failure).hasMessageThat().contains(CommitMode.PERIODIC_CONSUMER_SYNC.toString());
        assertThat(failure).hasMessageThat().contains(CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER.toString());
        assertThat(failure).hasMessageThat().contains("astubbs/parallel-consumer#317");
    }

    /**
     * Explicitly configuring the default handler under async is semantically the default configuration - it builds.
     * {@link CommitFailurePolicies#shutDown()} being a shared instance is what makes this detectable.
     */
    @Test
    void asyncCommitModeAcceptsAnExplicitDefaultHandler() {
        var options = base()
                .commitMode(CommitMode.PERIODIC_CONSUMER_ASYNCHRONOUS)
                .commitFailureHandler(CommitFailurePolicies.shutDown())
                .build();

        options.validate(); // must not throw
    }

    @Test
    void syncCommitModeAcceptsHandlerAndPauseIntake() {
        var options = base()
                .commitMode(CommitMode.PERIODIC_CONSUMER_SYNC)
                .commitFailureHandler(CommitFailurePolicies.continueUnbounded())
                .commitFailureContinueMode(CommitFailureContinueMode.PAUSE_INTAKE)
                .build();

        options.validate();

        assertThat(options.getCommitFailureHandler()).isSameInstanceAs(CommitFailurePolicies.continueUnbounded());
        assertThat(options.getCommitFailureContinueMode()).isEqualTo(CommitFailureContinueMode.PAUSE_INTAKE);
    }

    /**
     * The coercion is EOS-only - sync commit mode leaves KEEP_PROCESSING alone.
     */
    @Test
    void syncCommitModeDoesNotCoerceKeepProcessing() {
        var options = base()
                .commitMode(CommitMode.PERIODIC_CONSUMER_SYNC)
                .commitFailureHandler(CommitFailurePolicies.continueBounded())
                .build();

        options.validate();

        assertThat(options.getCommitFailureContinueMode()).isEqualTo(CommitFailureContinueMode.KEEP_PROCESSING);
    }
}
