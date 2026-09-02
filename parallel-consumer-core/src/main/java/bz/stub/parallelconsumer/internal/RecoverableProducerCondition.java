package bz.stub.parallelconsumer.internal;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.internal.utils.ThrowableUtils;
import lombok.experimental.UtilityClass;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.common.errors.InvalidPidMappingException;
import org.apache.kafka.common.errors.InvalidProducerEpochException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;

import java.util.Optional;

/**
 * The one recoverable condition: the broker has told PC its transactional producer is no longer usable, and the
 * answer is the same whichever way it said so - abort what can be aborted, discard the producer, build another under
 * the same {@code transactional.id}, and run the discarded work again.
 * <p>
 * The set is Kafka Streams' {@code TaskMigratedException} set plus the two that arrive from a partition leader when
 * producer state has expired: {@link ProducerFencedException}, {@link InvalidProducerEpochException},
 * {@link InvalidPidMappingException}, {@link OutOfOrderSequenceException} (and so its subtype
 * {@code UnknownProducerIdException}), and {@link CommitFailedException}, which is how a lost group generation
 * surfaces from {@code sendOffsetsToTransaction}. {@code FencedInstanceIdException} is deliberately outside it: it
 * means another live member holds this {@code group.instance.id}, which a new producer cannot repair.
 * <p>
 * <b>Why a cause walk.</b> A condition rarely arrives bare. A send future raises it inside
 * {@code ExecutionException} ({@code FutureRecordMetadata.valueOrError}); once kafka-clients has stored it as the
 * transaction's error, every later transactional call rethrows it inside
 * {@code KafkaException("Cannot execute transactional method because we are in an error state")}; and PC's own
 * produce path wraps whatever it caught in {@link PCInternalRuntimeException}. Matching the outermost type is how
 * confluentinc#839's catch came to fire only on the synchronous shape and never on the one the field report showed.
 */
@UtilityClass
public class RecoverableProducerCondition {

    /**
     * @return the innermost throwable in {@code failure}'s cause chain that is one of the recoverable conditions,
     *         or empty when none is - including for a null failure
     */
    public static Optional<Throwable> find(Throwable failure) {
        return ThrowableUtils.innermostInCauseChain(failure, RecoverableProducerCondition::isCondition);
    }

    private static boolean isCondition(Throwable throwable) {
        return throwable instanceof ProducerFencedException
                || throwable instanceof InvalidProducerEpochException
                || throwable instanceof InvalidPidMappingException
                || throwable instanceof OutOfOrderSequenceException
                || throwable instanceof CommitFailedException;
    }
}
