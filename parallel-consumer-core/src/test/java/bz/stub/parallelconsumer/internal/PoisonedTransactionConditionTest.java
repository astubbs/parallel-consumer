package bz.stub.parallelconsumer.internal;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.errors.InvalidProducerEpochException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TransactionalIdAuthorizationException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

/**
 * The send-side condition: which callback failures leave the transaction abortable rather than the producer dead.
 * <p>
 * The two cases below are the two arms of {@code TransactionManager#maybeTransitionToErrorState}, so this is really a
 * characterisation of kafka-clients. Read from the 3.9.2 bytecode - there is no sources jar in this repo's dependency
 * set - and if a Kafka upgrade moves a type between the arms, this is the test that says so.
 */
class PoisonedTransactionConditionTest {

    /**
     * The four types {@code maybeTransitionToErrorState} routes to {@code transitionToFatalError}. A new producer is
     * the only repair for these, so they are not this class's business.
     */
    static Stream<RuntimeException> theFatalSet() {
        return Stream.of(
                new ClusterAuthorizationException("no cluster auth"),
                new TransactionalIdAuthorizationException("no txn id auth"),
                new ProducerFencedException("fenced"),
                new UnsupportedVersionException("broker too old for transactions"));
    }

    /**
     * Failures that reach a producer callback and, in transactional mode, land in
     * {@code transitionToAbortableError}. {@link RecordTooLargeException} is the motivating case: the client rejects
     * the record outright, and nothing at all is wrong with the producer.
     */
    static Stream<RuntimeException> thePoisoningSet() {
        return Stream.of(
                new RecordTooLargeException("the result record exceeds max.request.size"),
                new TimeoutException("expired in the accumulator before it could be sent"),
                new InvalidProducerEpochException("stale epoch from the partition leader"),
                new RuntimeException("an exception type Kafka does not name, which still poisons"));
    }

    @ParameterizedTest
    @MethodSource("theFatalSet")
    void aFailureThatKillsTheProducerIsNotPoison(RuntimeException fatal) {
        assertWithMessage("%s kills the producer, so a replacement is the repair, not an abort",
                fatal.getClass().getSimpleName())
                .that(PoisonedTransactionCondition.poisonsTheTransaction(fatal))
                .isFalse();
    }

    @ParameterizedTest
    @MethodSource("thePoisoningSet")
    void aFailureThatOnlyKillsTheTransactionIsPoison(RuntimeException poison) {
        assertWithMessage("%s leaves the producer usable and the transaction abortable",
                poison.getClass().getSimpleName())
                .that(PoisonedTransactionCondition.poisonsTheTransaction(poison))
                .isTrue();
    }

    /**
     * The two sets must not overlap, or one event records two conditions. {@link ProducerFencedException} is the only
     * type in both classes' vocabulary, and it belongs to {@link RecoverableProducerCondition}.
     */
    @Test
    void fencingIsRoutedToTheRecoverableSetAndNotToThisOne() {
        var fenced = new ProducerFencedException("fenced");

        assertThat(PoisonedTransactionCondition.poisonsTheTransaction(fenced)).isFalse();
        assertThat(RecoverableProducerCondition.find(fenced)).hasValue(fenced);
    }

    /**
     * Matched on the type handed to the callback, not on a cause chain - unlike {@link RecoverableProducerCondition},
     * which has to walk one. A callback is handed the failure bare, and the fatal set is what kafka-clients tests with
     * a bare {@code instanceof} too, so a fatal type buried under a wrapper is not fatal to that switch either.
     */
    @Test
    void theOutermostTypeDecidesBecauseThatIsWhatKafkaClientsTests() {
        var wrappedFatal = new RuntimeException("wrapper", new ClusterAuthorizationException("no cluster auth"));

        assertThat(PoisonedTransactionCondition.poisonsTheTransaction(wrappedFatal)).isTrue();
    }
}
