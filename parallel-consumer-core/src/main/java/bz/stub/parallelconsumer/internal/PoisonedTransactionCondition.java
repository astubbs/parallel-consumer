package bz.stub.parallelconsumer.internal;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.experimental.UtilityClass;
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.TransactionalIdAuthorizationException;
import org.apache.kafka.common.errors.UnsupportedVersionException;

/**
 * Whether a terminally failed send has left the open transaction poisoned - in kafka-clients' abortable error state -
 * as opposed to killing the producer outright.
 * <p>
 * This is the send-side sibling of {@link RecoverableProducerCondition}, and the two are deliberately disjoint. That
 * one answers "the broker says this producer is finished"; this one answers "the producer is fine, the transaction it
 * holds is not". A result record the client rejects outright - an oversized one raising
 * {@code RecordTooLargeException} - is the case that motivated it: nothing is wrong with the producer, but every
 * subsequent transactional call rethrows
 * {@code KafkaException("Cannot execute transactional method because we are in an error state")} until something
 * aborts.
 * <p>
 * <b>The set is defined by exclusion, and that is deliberate.</b>
 * {@code TransactionManager#maybeTransitionToErrorState} - read from the kafka-clients 3.9.2 bytecode, which has no
 * sources jar in this repo's dependency set - routes exactly four exception types to
 * {@code transitionToFatalError} and, when transactional, <em>everything else</em> to
 * {@code transitionToAbortableError}. Enumerating the abortable side would mean tracking every exception a send can
 * fail with; enumerating the fatal side tracks a list Kafka states in one place. The cost of the choice is that a new
 * fatal type added upstream would be misread as poison here - and the failure is soft, because recovery's own
 * replacement then fails and its terminal path closes the instance.
 * <p>
 * {@link ProducerFencedException} sits on the fatal side of that switch and so is excluded here, but it is not
 * excluded from recovery: it is in {@link RecoverableProducerCondition}'s set, and reaches recovery from the produce
 * future and the commit path instead. Routing it from both would record the same condition twice for one event.
 */
@UtilityClass
public class PoisonedTransactionCondition {

    /**
     * @param sendFailure the exception a producer {@code Callback} was handed; never null at the one call site
     * @return true when this failure poisons the open transaction rather than killing the producer, so the answer is
     *         to abort rather than to close
     */
    public static boolean poisonsTheTransaction(Throwable sendFailure) {
        return !isFatalToTheProducer(sendFailure);
    }

    /**
     * The four types {@code maybeTransitionToErrorState} sends to {@code transitionToFatalError}, in its order.
     */
    private static boolean isFatalToTheProducer(Throwable sendFailure) {
        return sendFailure instanceof ClusterAuthorizationException
                || sendFailure instanceof TransactionalIdAuthorizationException
                || sendFailure instanceof ProducerFencedException
                || sendFailure instanceof UnsupportedVersionException;
    }
}
