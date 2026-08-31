package bz.stub.parallelconsumer.internal;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

/**
 * The produce lock was expected to be held and was not.
 * <p>
 * <b>A named type because the generic one hid this route.</b> The mailbox guards on astubbs#267 were written as
 * {@code catch (Throwable)} against {@code addToMailbox}, and nobody could say from the code what could actually throw
 * there - the answer was three levels down, in {@link ProducerManager#finishProducing}'s
 * {@code ensureProduceStarted} check, raised as a bare {@link PCInternalRuntimeException} carrying a sentence. A catch
 * naming this type says what it is guarding against; a catch naming {@code Throwable} says only that the author was
 * being careful.
 * <p>
 * Reachable in transactional commit mode when a produce lock is released without being held - the double-release
 * question {@code docs/inflight/bug-producing-lock-double-release.md} records, on a path that has already produced two
 * flakes. It is always a PC bug rather than an operating condition, which is why the mailbox path treats it as
 * terminal: see {@link AbstractParallelEoSStreamProcessor#failFatallyOnUnmailboxableRecord}.
 * <p>
 * {@code docs/inflight/core-exception-hierarchy-cleanup.md} owns the wider cleanup this is one instance of.
 */
public class ProduceLockNotHeldException extends PCInternalRuntimeException {

    public ProduceLockNotHeldException(String message) {
        super(message);
    }
}
