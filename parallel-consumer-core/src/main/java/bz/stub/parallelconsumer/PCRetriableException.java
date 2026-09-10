package bz.stub.parallelconsumer;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.internal.utils.ThrowableUtils;

import java.time.Duration;

/**
 * A user's processing function can throw this exception, which signals to PC that processing of the message has failed,
 * and that it should be retired at a later time.
 * <p>
 * The advantage of throwing this exception explicitly, is that PC will not log an ERROR. If any other type of exception
 * is thrown by the user's function, that will be logged as an error (but will still be retried later).
 * <p>
 * So in short, if this exception is thrown, nothing will be logged (except at DEBUG level), any other exception will be
 * logged as an error.
 *
 * <h2>Saying more than "try again"</h2>
 * A throw is the only way a processing function can hand a record back, so this exception is also where it says what
 * it meant by it. Three optional facts ride on the instance, and each is read by
 * {@code WorkContainer.updateFailureHistory} on the failure path, <b>before</b> the configured
 * {@code retryDelayProvider} is consulted - so a throw that says nothing behaves exactly as it always has:
 * <ul>
 *     <li>{@link #retryAfter(Duration)} - retry this record after that delay, instead of the delay the
 *     {@code retryDelayProvider} or {@code defaultMessageRetryDelay} would give it. Per throw, so a function can back
 *     a record off further each time without a provider.</li>
 *     <li>{@link #notAnAttempt()} - do not count this as a failed attempt. For a hand-back that is not a failure of
 *     the user's work: the record was never run, or was withheld.</li>
 *     <li>{@link #park(String)} - the record is <em>parked</em>: it stays incomplete in the offset map, holds no
 *     worker, and is never due again until something acts on it. It implies never-due, and it is not a very long
 *     retry delay: {@code WorkContainer.isParked()} is its own state, so no arithmetic can turn it back into a
 *     hot retry.</li>
 * </ul>
 * They combine: a payload that can never be decoded is {@code park(reason).notAnAttempt()}.
 *
 * @author Antony Stubbs
 */
// Hand-written ctors (not Lombok @StandardException) - see PCInternalRuntimeException for why.
public class PCRetriableException extends RuntimeException {

    /**
     * The delay this throw asks for, or null to leave the choice to the configured provider. Written once at the
     * throw site, on the thread that throws, and read on that same thread inside the failure path.
     */
    private Duration retryAfter;

    private boolean countsAsAttempt = true;

    private String parkReason;

    public PCRetriableException() {
        super();
    }

    public PCRetriableException(String message) {
        super(message);
    }

    public PCRetriableException(String message, Throwable cause) {
        super(message, cause);
    }

    public PCRetriableException(Throwable cause) {
        super(cause);
    }

    /**
     * Retry this record after {@code delay} rather than after the configured retry delay.
     *
     * @throws IllegalArgumentException for a null or negative delay - a coding error, refused where it is written
     *                                  rather than degraded silently at the point it would be applied
     */
    public PCRetriableException retryAfter(Duration delay) {
        if (delay == null || delay.isNegative()) {
            throw new IllegalArgumentException("retryAfter needs a non-negative delay, but was given " + delay);
        }
        this.retryAfter = delay;
        return this;
    }

    /**
     * This hand-back is not an attempt at the user's work, so it must not advance the record's failure count.
     */
    public PCRetriableException notAnAttempt() {
        this.countsAsAttempt = false;
        return this;
    }

    /**
     * Park this record: never due again until something acts on it, with {@code reason} recorded on the record for
     * whoever lists the parked set.
     *
     * @param reason why it parked, in one phrase; not null - a record an operator cannot be told anything about is
     *               not one they can be asked to act on
     */
    public PCRetriableException park(String reason) {
        if (reason == null) {
            throw new IllegalArgumentException("A park needs a reason: it is what an operator reads when they "
                    + "find the record, and a park with nothing to say is one nobody can act on");
        }
        this.parkReason = reason;
        return this;
    }

    /**
     * @return the delay this throw asked for, or null when it asked for none and the configured provider decides
     */
    public Duration getRetryAfter() {
        return retryAfter;
    }

    public boolean countsAsAttempt() {
        return countsAsAttempt;
    }

    /**
     * @return whether this record must never become due again on its own, which is what a park is
     */
    public boolean isParked() {
        return parkReason != null;
    }

    /**
     * @return why the record parked, or null when this throw is not a park
     */
    public String getParkReason() {
        return parkReason;
    }

    /**
     * The instance carrying the facts above, found the same way {@link #isPresentIn(Throwable)} classifies a failure -
     * PC's own pass-through wrappers are peeled and the failure underneath is tested, so a genuinely different
     * exception that merely has one further down its chain carries nothing.
     *
     * @return the carrying exception, or null when this failure says nothing beyond "it failed"
     */
    public static PCRetriableException handbackIn(Throwable t) {
        Throwable unwrapped = ThrowableUtils.unwrapTransparentWrappers(t);
        return unwrapped instanceof PCRetriableException ? (PCRetriableException) unwrapped : null;
    }

    /**
     * Whether this failure is one the user marked as expected - the question every engine asks before deciding
     * whether to log at debug or at error.
     * <p>
     * Here rather than at each engine because it is a policy, not a mechanism, and re-deriving it per engine is how
     * the engines came to disagree. Three tested only the outermost throwable, so an instance that arrived wrapped -
     * routine, since the reactive engines repackage what they propagate - was logged as an error; a fourth never
     * asked at all.
     * <p>
     * <b>Expected means this failure IS retriable, not that a retriable is somewhere beneath it.</b> PC's own
     * pass-through wrappers are peeled first, then the failure underneath is tested. A genuinely different exception
     * that merely happens to carry a {@code PCRetriableException} further down its chain is NOT expected, and stays
     * at error - the alternative silences a real fault because of something buried under it.
     * <p>
     * A framework that repackages exceptions on the way out is the caller's to unwrap first, with that framework's
     * own helper, since core cannot name those types. {@code ReactorProcessor} does this.
     *
     * @param t the failure to classify; null is not expected
     */
    public static boolean isPresentIn(Throwable t) {
        return ThrowableUtils.unwrapTransparentWrappers(t) instanceof PCRetriableException;
    }

}
