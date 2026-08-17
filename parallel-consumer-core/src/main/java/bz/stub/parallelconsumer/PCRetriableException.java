package bz.stub.parallelconsumer;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.internal.utils.ThrowableUtils;

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
 * @author Antony Stubbs
 */
// Hand-written ctors (not Lombok @StandardException) - see InternalRuntimeException for why.
public class PCRetriableException extends RuntimeException {

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
