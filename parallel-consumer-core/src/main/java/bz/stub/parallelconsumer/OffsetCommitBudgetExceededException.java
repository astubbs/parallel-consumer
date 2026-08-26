package bz.stub.parallelconsumer;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

/**
 * A commit gave up because its configured budget was spent, rather than because the commit itself was
 * rejected. The broker's own exception is always the {@link #getCause() cause}.
 * <p>
 * This exists to be <b>actionable</b>. The underlying Kafka exception says a commit timed out; it
 * cannot say which of PC's options bounded it, what the relationship between that option and the
 * consumer's own timeouts is, or what the alternatives are - and users who met the bare
 * {@code TimeoutException} spent a long time looking in the wrong place for it (astubbs#177,
 * confluentinc#833). The message this carries names the knob that ran out, so the reader can act
 * without reading PC's source.
 * <p>
 * <b>Why this terminates PC.</b> A commit that cannot complete is not something PC can currently hand
 * back to the application: there is no seam for "the commit failed, you decide". Kafka's own client
 * throws a retriable exception and lets the caller choose; PC closes. That gap is tracked as a
 * feature request in astubbs/parallel-consumer#317, and the message points there so a user meeting
 * this knows the behaviour is a known limit rather than an accident.
 *
 * @author Antony Stubbs
 */
// Hand-written ctors (not Lombok @StandardException) - see InternalRuntimeException for why.
public class OffsetCommitBudgetExceededException extends ParallelConsumerException {

    public OffsetCommitBudgetExceededException(String message, Throwable cause) {
        super(message, cause);
    }

}
