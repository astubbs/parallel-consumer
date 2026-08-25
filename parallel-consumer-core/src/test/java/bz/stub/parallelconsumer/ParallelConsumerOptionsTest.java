package bz.stub.parallelconsumer;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.internal.utils.LongPollingMockConsumer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static com.google.common.truth.Truth.assertThat;
import static org.apache.kafka.clients.consumer.OffsetResetStrategy.EARLIEST;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Check that various validation and combinations of {@link ParallelConsumerOptions} works.
 *
 * @author Antony Stubbs
 * @see ParallelConsumerOptions
 */
@Tag("transactions")
@Tag("confluentinc#355")
class ParallelConsumerOptionsTest {

    /**
     * Test the deprecation phase of commit frequency
     */
    @Test
    void setTimeBetweenCommits() {
        var newFreq = Duration.ofMillis(100);
        var options = ParallelConsumerOptions.<String, String>builder()
                .commitInterval(newFreq)
                .consumer(new LongPollingMockConsumer<>(EARLIEST))
                .build();

        //
        assertThat(options.getCommitInterval()).isEqualTo(newFreq);

        //
        var pc = new ParallelEoSStreamProcessor<>(options);

        //
        assertThat(pc.getTimeBetweenCommits()).isEqualTo(newFreq);

        //
        var testFreq = Duration.ofMillis(9);
        pc.setTimeBetweenCommits(testFreq);

        //
        assertThat(pc.getTimeBetweenCommits()).isEqualTo(testFreq);
        assertThat(options.getCommitInterval()).isEqualTo(testFreq);
    }

    /**
     * {@code batchSize} must be bounded at {@link ParallelConsumerOptions#validate()} (astubbs#311). Unvalidated, the
     * same misconfiguration failed three different ways depending on unrelated settings: zero silently requested no
     * work forever, zero plus a {@code messageBufferSize} threw {@code ArithmeticException} at construction, and
     * {@code null} NPEd unboxing in {@code isUsingBatching()}.
     */
    @Test
    void aBatchSizeOfZeroIsRejectedAtValidationNamingTheField() {
        assertThatThrownBy(() -> optionsWithBatchSize(0).validate())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("batchSize")
                .hasMessageContaining("0");
    }

    @Test
    void aNegativeBatchSizeIsRejectedAtValidationNamingTheField() {
        assertThatThrownBy(() -> optionsWithBatchSize(-5).validate())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("batchSize")
                .hasMessageContaining("-5");
    }

    /**
     * The field is a boxed {@link Integer}, so {@code null} is representable through the builder - it must be the
     * same clear error, not an NPE at the first {@code isUsingBatching()} call.
     */
    @Test
    void aNullBatchSizeIsRejectedAtValidationNamingTheField() {
        assertThatThrownBy(() -> optionsWithBatchSize(null).validate())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("batchSize")
                .hasMessageContaining("null");
    }

    /**
     * Guards against the bound over-reaching: the default of 1 (no batching) and any positive size stay valid.
     */
    @Test
    void batchSizesOfOneAndAboveAreAccepted() {
        assertThatCode(() -> optionsWithBatchSize(1).validate()).doesNotThrowAnyException();
        assertThatCode(() -> optionsWithBatchSize(5).validate()).doesNotThrowAnyException();
    }

    private ParallelConsumerOptions<String, String> optionsWithBatchSize(Integer batchSize) {
        return ParallelConsumerOptions.<String, String>builder()
                .consumer(new LongPollingMockConsumer<>(EARLIEST))
                .batchSize(batchSize)
                .build();
    }
}