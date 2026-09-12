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
 * They combine: a payload that can never be decoded is {@code park(reason).notAnAttempt()}. One combination has a
 * precedence rather than an effect each, and it is on {@link #retryAfter(Duration)}.
 *
 * <h2>Throw a FRESH instance every time</h2>
 * <b>An instance carries mutable state, so it belongs to exactly one throw.</b> A cached or {@code static} exception
 * is an ordinary thing to do with an exception type that carries nothing, and it is wrong here: the three fields are
 * plain and unsynchronised, so a shared instance read by whichever record fails next - on whichever thread the engine
 * happens to fail it from - has no defined value. PC cannot enforce this; a constructor call per throw is the whole
 * requirement.
 * <p>
 * <b>What makes a per-throw instance safe is the publication edge, not a thread.</b> The fields are written at the
 * throw site, on the thread running the user's function, and read by {@code WorkContainer.updateFailureHistory} on
 * the thread that handles the failure. Those are the same thread in the classic engine, which catches and calls
 * {@code onUserFunctionFailure} synchronously - but <b>not</b> in the reactive engines, where
 * {@code ExternalEngine.recordFailureAndReturnBatchToMailbox} is reached from an async completion callback whose
 * thread the framework chooses. The edge that holds in both cases is the completion boundary the object crosses:
 * whatever hands the failure from the user's work to PC's failure path establishes happens-before, because it has to
 * publish the throwable itself for PC to have anything to read. An earlier version of this javadoc claimed
 * same-thread confinement instead, which is true of one engine out of four - corrected by the review of
 * astubbs/parallel-consumer#506, which is also where the single-use requirement was found missing.
 *
 * @author Antony Stubbs
 */
// Hand-written ctors (not Lombok @StandardException) - see PCInternalRuntimeException for why.
public class PCRetriableException extends RuntimeException {

    /**
     * The delay this throw asks for, or null to leave the choice to the configured provider. Written once at the
     * throw site and read once inside the failure path; what publishes it across the two is the completion boundary
     * the throwable crosses, not a shared thread - see the class javadoc, and throw a fresh instance per failure.
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
     * The largest delay PC can do arithmetic with: {@link Duration#toMillis()} is what the control loop's block
     * calculation reaches for, and it overflows above this - around 292 million years, which is well inside
     * {@link java.time.Instant}'s range, so nothing else on the way refuses it.
     */
    private static final Duration LONGEST_REPRESENTABLE_DELAY = Duration.ofMillis(Long.MAX_VALUE);

    /**
     * Retry this record after {@code delay} rather than after the configured retry delay.
     * <p>
     * <b>When this throw also {@link #park(String) parks}, the park wins and the delay is not applied.</b> A park is
     * "never due again", which no delay can express, so there is nothing for a deadline to mean; the engine logs the
     * discarded delay at DEBUG rather than failing the throw, because the combination reads as a user who changed
     * their mind mid-chain rather than as a coding error. Ask for one or the other.
     *
     * @throws IllegalArgumentException for a null or negative delay, or one too large for PC's own arithmetic - each
     *                                 a coding error, refused where it is written rather than degraded silently at
     *                                 the point it would be applied
     */
    public PCRetriableException retryAfter(Duration delay) {
        if (delay == null || delay.isNegative()) {
            throw new IllegalArgumentException("retryAfter needs a non-negative delay, but was given " + delay);
        }
        if (delay.compareTo(LONGEST_REPRESENTABLE_DELAY) > 0) {
            // Refused rather than clamped. The band between what Instant.plus accepts and what Duration.toMillis can
            // represent is reachable - computeRetryDueAt's own fallback only fires once the deadline leaves Instant's
            // range, so a delay in between produces a valid retryDueAt and then overflows getTimeToBlockFor's
            // multiplyExact on the control thread. A caller asking for a geological delay means "never", and park is
            // how "never" is spelled here: it is a state, so no arithmetic can lose it.
            throw new IllegalArgumentException("retryAfter was given " + delay + ", which is longer than PC can "
                    + "schedule (" + LONGEST_REPRESENTABLE_DELAY + "). A delay this large means the record should "
                    + "never come back on its own - park(reason) is how to say that, and unlike a very long delay "
                    + "it cannot be turned back into a hot retry by arithmetic");
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
     * <p>
     * <b>Parking wins over {@link #retryAfter(Duration)}</b>, in either chaining order - a park is not a very long
     * delay, so there is no deadline the two could be reconciled into. A hand-back carrying both logs the discarded
     * delay at DEBUG. Combining with {@link #notAnAttempt()} is different and composes as advertised: it suppresses
     * the attempt count, which a park has no opinion about.
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
     * The instance carrying the facts above: PC's own pass-through wrappers are peeled and the failure underneath is
     * tested, so a genuinely different exception that merely has one further down its chain carries nothing.
     * <p>
     * <b>The one place that peel-and-test lives.</b> {@link #isPresentIn(Throwable)} is the same question asked as a
     * boolean and defers to this, so the two can never come to disagree about what counts - which is the failure
     * that put the policy on this class in the first place, one engine at a time.
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
        // Asked of handbackIn rather than re-derived: the peel-and-test is one behaviour, and the whole reason this
        // policy sits on this class is that re-deriving it per site is how the sites came to disagree.
        return handbackIn(t) != null;
    }

}
