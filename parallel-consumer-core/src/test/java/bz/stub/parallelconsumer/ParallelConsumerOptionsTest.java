package bz.stub.parallelconsumer;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.internal.AbstractParallelEoSStreamProcessor;
import bz.stub.parallelconsumer.internal.DynamicLoadFactor;
import bz.stub.parallelconsumer.internal.utils.LongPollingMockConsumer;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.apache.kafka.clients.producer.Producer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;

import static bz.stub.parallelconsumer.ParallelConsumerOptions.CommitMode.PERIODIC_CONSUMER_ASYNCHRONOUS;
import static bz.stub.parallelconsumer.ParallelConsumerOptions.CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER;
import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
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

    /*
     * The cases below are the explicit half of the truth table for "unset is the absence of a value, never inferred
     * from the value itself" (astubbs#422), plus the one way a caller can return an options object to unset after it
     * has been built. The unset-from-the-builder half is already proved, in the same module and the same suite, by
     * TransactionalBulkCommitTest#transactionalModeWithNoExplicitCommitIntervalResolvesTo100ms and
     * #nonTransactionalModeKeepsTheFiveSecondDefault - the claim C5 arms - so it is not restated here.
     *
     * Each asserts the literal duration rather than the DEFAULT_* constant, as those arms do: comparing the resolved
     * value to the constant would still pass if the constant moved and the javadoc were left behind. Each calls
     * validate() first, so they also show that validation does not disturb the resolution.
     */

    /**
     * Transactional mode needs a producer to get past validation, so every case below would otherwise repeat the same
     * builder chain and differ only in the interval.
     *
     * @param commitInterval the interval to configure, or null for the unset case
     */
    private ParallelConsumerOptions<String, String> validatedTransactionalOptions(Duration commitInterval) {
        var options = optionsBuilder()
                .producer(Mockito.mock(Producer.class))
                .commitMode(PERIODIC_TRANSACTIONAL_PRODUCER)
                .commitInterval(commitInterval)
                .build();

        options.validate();

        return options;
    }

    /**
     * The guard against the rejected {@code equals} fix. A fresh five-second {@link Duration} is {@code equals} to
     * {@link ParallelConsumerOptions#DEFAULT_COMMIT_INTERVAL} but is a value the user chose, so it must be kept.
     */
    @Tag("transactions")
    @Test
    @ProvesClaim(TransactionalClaim.COMMIT_INTERVAL_AUTO_REDUCED)
    void explicitFiveSecondsInTransactionalModeIsKept() {
        var options = validatedTransactionalOptions(Duration.ofSeconds(5));

        assertWithMessage("a fresh Duration equal to the default is explicit and must be kept - this is the case an "
                        + "equals-based check would have broken")
                .that(options.getCommitInterval())
                .isEqualTo(Duration.ofSeconds(5));
    }

    /**
     * The astubbs#422 reproduction. Handing the builder the very constant the default is built from used to be
     * indistinguishable from handing it nothing, so the value was silently replaced with 100ms - fifty times the
     * broker load configured, with no log line.
     */
    @Tag("transactions")
    @Test
    @ProvesClaim(TransactionalClaim.COMMIT_INTERVAL_AUTO_REDUCED)
    void explicitDefaultConstantInTransactionalModeIsKept() {
        var options = validatedTransactionalOptions(ParallelConsumerOptions.DEFAULT_COMMIT_INTERVAL);

        assertWithMessage("passing the default constant explicitly is still a choice the user made, so it must "
                        + "survive transactional mode's auto-reduction (astubbs#422)")
                .that(options.getCommitInterval())
                .isEqualTo(Duration.ofSeconds(5));
    }

    /**
     * Null is the sentinel for "unset", so the deprecated setter can put an already-built options object back into
     * that state - and the getter has to resolve it there too. Worth its own case because it is the only path that
     * reaches the null branch after construction, and because the resolved value is dereferenced unguarded in the
     * control loop, so a null escaping here would surface as an NPE in commit scheduling rather than as a wrong
     * interval.
     */
    @Tag("transactions")
    @Test
    void clearingTheIntervalThroughTheDeprecatedSetterReturnsItToUnset() {
        var options = validatedTransactionalOptions(Duration.ofSeconds(5));

        options.setCommitInterval(null);

        assertWithMessage("clearing the interval means unset, so it resolves from the commit mode again rather than "
                        + "returning null or keeping the value that was just cleared")
                .that(options.getCommitInterval())
                .isEqualTo(Duration.ofMillis(100));
    }

    /**
     * The second defect astubbs#422 carried, and the one the fix closes structurally rather than by a named change:
     * {@code validate()} used to write the resolved interval back into the field, so {@link
     * ParallelConsumerOptions#toBuilder()} copied it forward as if the caller had chosen it. Re-building a validated
     * transactional options object with a non-transactional commit mode then kept 100ms - a value that was never
     * anybody's configuration and that no reader could tell from an explicit one. Unset now round-trips as unset, so
     * the copy re-resolves against its own commit mode.
     */
    @Tag("transactions")
    @Test
    void anUnsetIntervalRoundTripsThroughToBuilderInsteadOfBeingBakedInByValidation() {
        var transactional = validatedTransactionalOptions(null);

        assertWithMessage("precondition - the transactional original does resolve an unset interval to 100ms, so the "
                        + "value below could only have come from it")
                .that(transactional.getCommitInterval())
                .isEqualTo(Duration.ofMillis(100));

        var rebuiltAsNonTransactional = transactional.toBuilder()
                .commitMode(PERIODIC_CONSUMER_ASYNCHRONOUS)
                .build();
        rebuiltAsNonTransactional.validate();

        assertWithMessage("the copy inherits an unset interval, not the 100ms its transactional parent resolved to, "
                        + "so it resolves against its own commit mode (astubbs#422)")
                .that(rebuiltAsNonTransactional.getCommitInterval())
                .isEqualTo(Duration.ofSeconds(5));

        assertWithMessage("and validating the copy still does not disturb the original")
                .that(transactional.getCommitInterval())
                .isEqualTo(Duration.ofMillis(100));
    }

    /**
     * Reading options has never been able to fail, and moving the unset resolution into
     * {@link ParallelConsumerOptions#getCommitInterval()} must not change that. The builder accepts an explicit null
     * commit mode - it is a misconfiguration, and {@link ParallelConsumerOptions#validate()} is what rejects it - but
     * until then the getter has to answer rather than throw, exactly as the generated accessor it replaced did.
     */
    @Test
    void aNullCommitModeIsRejectedByValidationRatherThanByTheGetter() {
        var options = optionsBuilder()
                .commitMode(null)
                .build();

        assertWithMessage("a null commit mode is not transactional as far as an unset interval is concerned, so the "
                        + "getter answers with Kafka's default instead of throwing")
                .that(options.getCommitInterval())
                .isEqualTo(Duration.ofSeconds(5));

        assertThrows(NullPointerException.class, options::validate,
                "and validation is still where a null commit mode stops the run");
    }

    /*
     * The rider supplier hook (R13, R17). Its own guard, its call site and the thread it runs on are
     * RiderSupplierGuardTest's; what belongs here is the option itself - that leaving it unset changes nothing, and
     * that setting it announces the compatibility hazard exactly once.
     */

    /**
     * R13: supplying no rider is the default, and the default has to be silent. The INFO line below is a warning
     * about a durable, group-wide hazard; logged at anyone who has not configured a rider it would be noise that
     * teaches readers to skip it.
     */
    @Test
    void optionsWithNoRiderSupplierValidateAndSayNothingAboutRiders() {
        var options = optionsBuilder().build();

        options.validate();

        assertWithMessage("unset means unset - no builder default, so the encoder path can test for null")
                .that(options.getRiderSupplier())
                .isNull();

        var lines = riderLinesLoggedWhileConstructing(options);

        assertWithMessage("a build with no rider configured must say nothing about riders. Saw: %s", lines)
                .that(lines)
                .isEmpty();
    }

    /**
     * R17/KTD11: the hazard a rider creates is not this instance's - it is every OTHER member of the group, and
     * any rollback, because the payload is durable and every released Parallel Consumer throws on an unknown magic
     * byte from inside the rebalance callback. A javadoc nobody re-reads at deploy time is not enough, so the
     * requirement and its recovery procedure are logged when the option is configured.
     * <p>
     * Once, and at startup: this is configuration, not an event.
     */
    @Test
    void configuringARiderSupplierAnnouncesTheReaderRequirementAndTheRecoveryProcedureOnce() {
        var options = optionsBuilder()
                .riderSupplier(context -> new byte[]{1})
                .build();

        options.validate();

        var lines = riderLinesLoggedWhileConstructing(options);

        assertWithMessage("exactly one line, at startup - repeating it would be an event, and this is a "
                        + "configuration fact. Saw: %s", lines)
                .that(lines)
                .hasSize(1);
        assertWithMessage("the line has to name the recovery procedure, because by the time anyone needs it the "
                        + "group is already crash-looping and this log line is what they will search for")
                .that(lines.get(0))
                .contains("kafka-consumer-groups --reset-offsets");
    }

    /**
     * Builds a processor from the given options and returns the {@code INFO} lines it logged about the rider
     * option while doing so.
     * <p>
     * Scoped to the calling thread: surefire runs this module's test methods in parallel, the appender attaches to
     * a class logger every one of them shares, and other suites construct processors with rider suppliers of their
     * own. A log event carries the thread that emitted it, and construction happens on the thread that asked for
     * it, so the thread name is the cheapest correct filter.
     * <p>
     * The processor is deliberately not closed, as {@link #setTimeBetweenCommits()} above also does not: nothing
     * here starts a control thread, and closing a processor that never polled is a longer story than this
     * assertion is worth.
     */
    private List<String> riderLinesLoggedWhileConstructing(ParallelConsumerOptions<String, String> options) {
        var thisThread = Thread.currentThread().getName();
        var logger = (Logger) LoggerFactory.getLogger(AbstractParallelEoSStreamProcessor.class);
        var appender = new ListAppender<ILoggingEvent>();
        appender.start();
        // The test profile runs above INFO, and logback drops a call below the effective level before any appender
        // sees it - so without this the capture is empty whether the line is logged or not, and both cases below
        // would pass for the wrong reason. Restored in the finally, because the level is on a shared logger.
        var levelBefore = logger.getLevel();
        logger.setLevel(Level.INFO);
        logger.addAppender(appender);
        try {
            var ignoredProcessor = new ParallelEoSStreamProcessor<>(options); // constructed for its logging only
            assertThat(ignoredProcessor).isNotNull();
        } finally {
            logger.detachAppender(appender);
            logger.setLevel(levelBefore);
            appender.stop();
        }
        return appender.list.stream()
                .filter(event -> event.getLevel() == Level.INFO)
                .filter(event -> thisThread.equals(event.getThreadName()))
                .map(ILoggingEvent::getFormattedMessage)
                // startsWith, not contains: the options object renders every field name including this one, so the
                // ordinary "initialise..." line mentions riderSupplier whether one is configured or not, and a
                // contains filter would match it and make the negative case pass for the wrong reason
                .filter(message -> message.startsWith("A " + ParallelConsumerOptions.Fields.riderSupplier
                        + " is configured"))
                .collect(Collectors.toList());
    }

}
