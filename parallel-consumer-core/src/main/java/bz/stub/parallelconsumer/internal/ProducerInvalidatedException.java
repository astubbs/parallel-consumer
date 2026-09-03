package bz.stub.parallelconsumer.internal;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

/**
 * The producer has been reported invalid by the broker (a {@link RecoverableProducerCondition}), the condition has
 * been recorded on the {@link ProducerManager}, and the operation that observed it is unwinding so that the control
 * thread can recover on its next pass.
 * <p>
 * Thrown by every detection site - the worker's produce-and-ack block, the control thread's commit, the poll thread's
 * revoke-path commit. A worker's record fails and returns through the mailbox; a commit unwinds through
 * {@code AbstractOffsetCommitter.retrieveOffsetsAndCommit}'s lock release. It is a {@link PCInternalRuntimeException}
 * so the existing finally-blocks treat it as any other commit failure, and the control loop catches it by type before
 * the arm that would treat an internal error as fatal.
 * <p>
 * Also raised to a worker parked on the produce lock when the outage it was waiting out ends in a terminal state or
 * a shutdown - there is no producer the record could ever be sent to, and the record must fail so the shutdown does
 * not wait on it.
 */
public class ProducerInvalidatedException extends PCInternalRuntimeException {

    public ProducerInvalidatedException(String message, Throwable condition) {
        super(message, condition);
    }

    public ProducerInvalidatedException(Throwable condition) {
        super("The transactional producer has been reported invalid by the broker and will be replaced: " + condition, condition);
    }
}
