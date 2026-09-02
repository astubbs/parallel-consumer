package bz.stub.parallelconsumer;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.internal.DynamicLoadFactor;
import bz.stub.parallelconsumer.internal.utils.LongPollingMockConsumer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static com.google.common.truth.Truth.assertThat;
import static org.apache.kafka.clients.consumer.OffsetResetStrategy.EARLIEST;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Check that various validation and combinations of {@link ParallelConsumerOptions} works.
 *
 * @author Antony Stubbs
 * @see ParallelConsumerOptions
 */
class ParallelConsumerOptionsTest {

    private ParallelConsumerOptions.ParallelConsumerOptionsBuilder<String, String> optionsBuilder() {
        return ParallelConsumerOptions.<String, String>builder()
                .consumer(new LongPollingMockConsumer<>(EARLIEST));
    }

    /**
     * Test the deprecation phase of commit frequency
     */
    // these tags classified the whole class when it held only this test - they describe this test, not the file
    @Tag("transactions")
    @Tag("confluentinc#355")
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
     * An initial load factor above the maximum can never step up, so it is a typo rather than a configuration - and
     * before this check it was accepted and pinned at the initial value, visible only as an inverted pair inside a
     * saturation warning that fires under load (astubbs#155). The message has to name both options and both values,
     * or finding the transposition means reading the library's source.
     */
    @Test
    void invertedLoadFactorPairIsRejected() {
        var options = optionsBuilder()
                .initialLoadFactor(100)
                .maximumLoadFactor(9)
                .build();

        var thrown = assertThrows(IllegalArgumentException.class, options::validate);

        // each option paired with the value it was actually given - a name or a number alone does not locate the typo
        assertThat(thrown).hasMessageThat().contains(ParallelConsumerOptions.Fields.initialLoadFactor + " (100)");
        assertThat(thrown).hasMessageThat().contains(ParallelConsumerOptions.Fields.maximumLoadFactor + " (9)");
    }

    /**
     * The equal pair is the legitimate fixed-factor configuration - what
     * {@link ParallelConsumerOptions#messageBufferSize} produces internally, and what a user asks for directly when
     * they want a buffer that does not grow. Rejecting it would break the very case
     * {@link DynamicLoadFactor#isStaticFactor()} exists to serve.
     */
    @Test
    void equalLoadFactorPairIsAccepted() {
        var options = optionsBuilder()
                .initialLoadFactor(7)
                .maximumLoadFactor(7)
                .build();

        options.validate();
    }

    /**
     * The ordinary dynamic configuration: headroom to step up from the initial factor towards the ceiling.
     */
    @Test
    void ascendingLoadFactorPairIsAccepted() {
        var options = optionsBuilder()
                .initialLoadFactor(DynamicLoadFactor.DEFAULT_INITIAL_LOADING_FACTOR)
                .maximumLoadFactor(DynamicLoadFactor.DEFAULT_MAX_LOADING_FACTOR)
                .build();

        options.validate();
    }
}