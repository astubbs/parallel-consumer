package bz.stub.parallelconsumer.fluent;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.internals.TransactionManager;

import java.lang.reflect.Field;
import java.util.Optional;

/**
 * What can be established about a producer the caller built, at the moment they hand it over.
 *
 * <h2>Why it is read here and not later</h2>
 * The facade never holds a pre-built client in a field of its own - a supplied producer goes straight into the
 * options builder, which is what keeps core's raw-client architecture rule intact (KTD3). So a definition-time rule
 * about that producer cannot go and look at it: whatever is worth knowing has to be read off the client while it is
 * in hand and kept as a fact rather than as a reference. That is exactly how
 * {@link RawBytesConsumerFaultException#describe(org.apache.kafka.clients.consumer.Consumer)} treats a supplied
 * consumer's deserialisers, and this is the same pattern for the one question the commit mode turns on.
 *
 * <h2>Why the answer has three values</h2>
 * Whether a producer is transactional is decided by whether it was built with a {@code transactional.id}, and the
 * client does not expose that - so it is read reflectively, as the engine's own {@code ProducerWrapper} does for the
 * same question at start. Reflection over client internals can always decline: the class may not be a
 * {@link KafkaProducer} at all, the field may not be there in this client version, or the module may not open. Those
 * are not "not transactional", and treating them as such would refuse a definition that is perfectly well formed -
 * so they are an <b>empty</b> answer and the definition-time rule stands aside, leaving the engine's start-time
 * check as the backstop it already was. The three states are an {@code Optional<Boolean>} rather than a three-valued
 * enum because the Truth assertion generator generates a subject for every enum in this package and references it by
 * name from generated code, which does not compile for an enum nested in a package-private class - and making an
 * internal helper public to satisfy a test generator would be the wrong way round.
 * <p>
 * This is deliberately not shared with {@code ProducerWrapper}, which asks a different question: it wants one
 * boolean and falls back to what the options declared, and it caches the reflective members it will use again later
 * for the transaction itself. What is here answers only whether the definition may be refused, and never guesses.
 */
@Slf4j
final class SuppliedProducer {

    /**
     * The field a {@link KafkaProducer} keeps its transaction manager in - present and non-null only when the
     * producer was built with a transactional id. Named once so the two places this class mentions it cannot drift.
     */
    private static final String TRANSACTION_MANAGER_FIELD = "transactionManager";

    private SuppliedProducer() {
        // Static: it holds nothing, and every answer is about the argument.
    }

    /**
     * Reads whether this producer was built for transactions.
     *
     * @param producer the client the caller supplied, read and not retained
     * @return true when it can open a transaction, false when it was built without a transactional id, and
     * <b>empty when the probe could not tell</b> - which is not a synonym for either
     */
    static Optional<Boolean> isTransactional(Producer<byte[], byte[]> producer) {
        if (!(producer instanceof KafkaProducer)) {
            // A MockProducer can act as either and says nothing about which was intended; anything else is not a
            // client whose internals this knows.
            log.debug("The supplied producer is a {}, whose transactional configuration cannot be read, so the "
                    + "commit-mode rule stands aside", producer.getClass().getName());
            return Optional.empty();
        }
        try {
            Field transactionManagerField = producer.getClass().getDeclaredField(TRANSACTION_MANAGER_FIELD);
            transactionManagerField.setAccessible(true);
            TransactionManager transactionManager = (TransactionManager) transactionManagerField.get(producer);
            return Optional.of(transactionManager != null && transactionManager.isTransactional());
        } catch (RuntimeException | ReflectiveOperationException | LinkageError cannotSee) {
            // Every way of failing here is the same answer: this probe cannot tell, so nothing may be refused on it.
            log.debug("Could not read whether the supplied producer is transactional, so the commit-mode rule "
                    + "stands aside", cannotSee);
            return Optional.empty();
        }
    }
}
