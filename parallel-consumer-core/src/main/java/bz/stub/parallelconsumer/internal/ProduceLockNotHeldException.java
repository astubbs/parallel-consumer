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
 * Raised in transactional commit mode when a produce lock is released without being held. <b>The double release that
 * named this type has since been fixed</b> - astubbs#257 made {@code AbstractParallelEoSStreamProcessor#cleanUpContext}
 * the single release point and took the lock out of the context as it releases it, so the second release is a no-op
 * rather than a throw, and the mailbox path no longer releases the lock at all. The type stays because
 * {@link ProducerManager#finishProducing}'s {@code ensureProduceStarted} check stays: it is the assertion that the
 * invariant still holds, and it needs a name to be caught by. It is always a PC bug rather than an operating
 * condition, which is why the mailbox path treats it as terminal: see
 * {@link AbstractParallelEoSStreamProcessor#failFatallyOnUnmailboxableRecord}.
 * <p>
 * {@code docs/inflight/core-exception-hierarchy-cleanup.md} owns the wider cleanup this is one instance of.
 */
public class ProduceLockNotHeldException extends PCInternalRuntimeException {

    public ProduceLockNotHeldException(String message) {
        super(message);
    }
}
