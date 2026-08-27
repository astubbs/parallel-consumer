package bz.stub.parallelconsumer.internal;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

/**
 * A record could not be returned to the mailbox, so PC terminated instead of continuing.
 * <p>
 * <b>A distinct type rather than a message, because the thing that must recognise this is a test harness.</b> The
 * operator's requirement is that if this ever fires it fires in our own test infrastructure and is impossible to miss;
 * a harness that matched on log text would be one reword away from silently stopping. The type survives rewording,
 * survives translation into a suppressed cause, and is what
 * {@code AbstractParallelEoSStreamProcessorTestBase} asserts against at teardown.
 * <p>
 * It can only be raised by a bug in PC's own bookkeeping - {@code addToMailbox} is a queue add and a hook, not user
 * code - and its consequence is a record that is neither in flight nor completed. Nothing retries it and nothing
 * reports it, so continuing risks committing past work that was never done.
 *
 * @see AbstractParallelEoSStreamProcessor#failFatallyOnUnmailboxableRecord
 */
public class UnmailboxableRecordException extends PCInternalRuntimeException {

    public UnmailboxableRecordException(String message, Throwable cause) {
        super(message, cause);
    }
}
