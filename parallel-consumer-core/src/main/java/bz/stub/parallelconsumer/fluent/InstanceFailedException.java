package bz.stub.parallelconsumer.fluent;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerException;
import org.apache.kafka.common.annotation.InterfaceStability;

/**
 * The instance's control thread died, and this is what {@link ConsumerHandle#awaitShutdown()} throws to say so
 * (R17).
 *
 * <h2>What an instance is</h2>
 * An <em>instance</em> is one running Parallel Consumer, started from one definition: a single Kafka consumer that
 * is one member of a consumer group, with its own control thread, its own worker pool, and whatever share of the
 * group's partitions the group gives it. Several instances started from the same definition may run side by side,
 * in one process or across machines, and the group divides the partitions between them. An instance is what starts,
 * fails and closes as a whole - so this exception reports the end of one instance, not of a record and not of the
 * group - and it carries the failure that ended it.
 * <p>
 * <b>It wraps rather than rethrows.</b> The cause came off the engine's own failure record, which is a checked
 * {@link Exception} the awaiting caller never declared - and wrapping is also what distinguishes it from a
 * definition fault, which is the facade's own and is rethrown as it was thrown (KTD3).
 */
@InterfaceStability.Unstable
public class InstanceFailedException extends ParallelConsumerException {

    /**
     * Fixed rather than computed: this exception crosses a thread boundary and a {@link Throwable} is serialisable
     * whether or not that was wanted, so the identity is pinned here.
     */
    private static final long serialVersionUID = 1L;

    /**
     * Package-private: only {@link ConsumerHandle#awaitShutdown()} builds one, wrapping the checked cause it read off
     * the engine's failure record. A cause is always supplied - an instance failure with nothing under it would say
     * nothing that the return of the await did not already say.
     */
    InstanceFailedException(String message, Throwable cause) {
        super(message, cause);
    }
}
