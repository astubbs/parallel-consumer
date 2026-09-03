package bz.stub.parallelconsumer;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.internal.DynamicLoadFactor;
import bz.stub.parallelconsumer.internal.utils.LongPollingMockConsumer;
import org.apache.kafka.clients.producer.Producer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.time.Duration;

import static bz.stub.parallelconsumer.ParallelConsumerOptions.CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER;
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
     * {@code transactionsValidation} decides "did the user set a commit interval?" by reference identity
     * ({@code getCommitInterval() == DEFAULT_COMMIT_INTERVAL}), not {@code equals}. {@link Duration} never interns -
     * confirmed directly (JDK 17): {@code Duration.ofSeconds(5) == Duration.ofMillis(5000)} is {@code false} even
     * though they are {@code equals}. So any explicitly-constructed value, including one numerically equal to the
     * default, is a different object from the constant and is correctly kept. This guards the identity check against
     * a well-intentioned but wrong fix to {@code equals()}: that would make this exact case - a user who explicitly
     * asks for 5 seconds under transactions - get silently reduced to 100ms, which contradicts
     * {@code docs/features/commit-interval.yaml}'s documented boundary that an explicitly set value is kept.
     */
    @Test
    void explicitCommitIntervalEqualToDefaultIsKeptUnderTransactions() {
        var explicit = Duration.ofSeconds(5);
        var options = optionsBuilder()
                .commitMode(PERIODIC_TRANSACTIONAL_PRODUCER)
                .producer(Mockito.mock(Producer.class))
                .commitInterval(explicit)
                .build();

        options.validate();

        assertThat(options.getCommitInterval()).isEqualTo(explicit);
    }

    /**
     * The reference-identity check's one genuine failure mode: a user who explicitly hands back the public
     * {@code DEFAULT_COMMIT_INTERVAL} constant itself - the same object, not merely an equal value - is
     * indistinguishable from never having called {@code commitInterval(...)} at all, and gets silently reduced to
     * the 100ms transactional default despite the explicit call.
     */
    @Test
    void explicitCommitIntervalReusingTheDefaultConstantIsKeptUnderTransactions() {
        var options = optionsBuilder()
                .commitMode(PERIODIC_TRANSACTIONAL_PRODUCER)
                .producer(Mockito.mock(Producer.class))
                .commitInterval(ParallelConsumerOptions.DEFAULT_COMMIT_INTERVAL)
                .build();

        options.validate();

        assertThat(options.getCommitInterval()).isEqualTo(ParallelConsumerOptions.DEFAULT_COMMIT_INTERVAL);
    }

    /**
     * Sibling case: an options instance that never sets a commit interval at all still gets the shorter
     * transactional default substituted in - so the fix above cannot simply disable the auto-reduction.
     */
    @Test
    void unsetCommitIntervalIsAutoReducedUnderTransactions() {
        var options = optionsBuilder()
                .commitMode(PERIODIC_TRANSACTIONAL_PRODUCER)
                .producer(Mockito.mock(Producer.class))
                .build();

        options.validate();

        assertThat(options.getCommitInterval()).isEqualTo(ParallelConsumerOptions.DEFAULT_COMMIT_INTERVAL_FOR_TRANSACTIONS);
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