package bz.stub.parallelconsumer.internal;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.InvalidPidMappingException;
import org.apache.kafka.common.errors.InvalidProducerEpochException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.UnknownProducerIdException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

/**
 * R8 and R9: the one recoverable condition, found through every wrapper kafka-clients puts around it.
 */
class RecoverableProducerConditionTest {

    static Stream<RuntimeException> theRecoverableSet() {
        return Stream.of(
                new ProducerFencedException("fenced"),
                new InvalidProducerEpochException("stale epoch"),
                new InvalidPidMappingException("pid expired"),
                new OutOfOrderSequenceException("out of order"),
                new UnknownProducerIdException("unknown pid - a subtype of OutOfOrderSequenceException"),
                new CommitFailedException("Transaction offset Commit failed due to consumer group metadata mismatch"));
    }

    @ParameterizedTest
    @MethodSource("theRecoverableSet")
    void eachConditionIsFoundBareAndUnderEveryWrapperKafkaClientsUses(RuntimeException condition) {
        assertThat(RecoverableProducerCondition.find(condition)).hasValue(condition);

        var fromSendFuture = new ExecutionException(condition);
        assertWithMessage("FutureRecordMetadata.valueOrError wraps in ExecutionException")
                .that(RecoverableProducerCondition.find(fromSendFuture)).hasValue(condition);

        var fromErrorState = new KafkaException("Cannot execute transactional method because we are in an error state", condition);
        assertWithMessage("TransactionManager.maybeFailWithError wraps a stored abortable error in KafkaException")
                .that(RecoverableProducerCondition.find(fromErrorState)).hasValue(condition);

        var doubleWrapped = new ExecutionException(new KafkaException("error state", condition));
        assertThat(RecoverableProducerCondition.find(doubleWrapped)).hasValue(condition);

        var underPcsOwnWrapper = new PCInternalRuntimeException("Error while waiting for produce results", fromSendFuture);
        assertThat(RecoverableProducerCondition.find(underPcsOwnWrapper)).hasValue(condition);
    }

    @Test
    void conditionsOutsideTheSetAreNotFound() {
        assertThat(RecoverableProducerCondition.find(new TimeoutException("commit took too long"))).isEmpty();
        assertThat(RecoverableProducerCondition.find(new IllegalStateException("no transaction started"))).isEmpty();
        assertThat(RecoverableProducerCondition.find(new RuntimeException("something else"))).isEmpty();
        assertThat(RecoverableProducerCondition.find(new KafkaException("bare, no cause"))).isEmpty();
        assertThat(RecoverableProducerCondition.find(new ExecutionException(new TimeoutException("send timed out")))).isEmpty();
        assertThat(RecoverableProducerCondition.find(null)).isEmpty();
    }

    @Test
    void aCauseCycleTerminates() {
        var a = new RuntimeException("a");
        var b = new RuntimeException("b", a);
        a.initCause(b);

        assertThat(RecoverableProducerCondition.find(a)).isEmpty();
    }

    @Test
    void theInnermostMatchIsReturnedNotTheWrapper() {
        var fenced = new ProducerFencedException("fenced");
        var wrapped = new KafkaException("outer", new KafkaException("inner", fenced));

        assertThat(RecoverableProducerCondition.find(wrapped)).hasValue(fenced);
    }
}
