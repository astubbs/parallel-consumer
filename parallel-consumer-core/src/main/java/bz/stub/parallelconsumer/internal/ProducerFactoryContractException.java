package bz.stub.parallelconsumer.internal;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

/**
 * The {@link bz.stub.parallelconsumer.ProducerFactory} broke the contract its javadoc states: it returned null,
 * returned the producer it had already returned, or built a producer that does not carry the {@code transactional.id}
 * PC handed it.
 * <p>
 * Its own type, rather than a bare {@link IllegalStateException}, because the recovery path has to tell it apart from
 * a transient build failure: every one of these violations is deterministic - a caching factory caches on every
 * rebuild, not just the first - so retrying a replacement build against it can only loop for the life of the
 * instance, each attempt logged as "deferred". {@link ProducerManager} treats it as terminal, and the instance closes
 * naming the cause.
 */
public class ProducerFactoryContractException extends IllegalStateException {

    public ProducerFactoryContractException(String message) {
        super(message);
    }
}
