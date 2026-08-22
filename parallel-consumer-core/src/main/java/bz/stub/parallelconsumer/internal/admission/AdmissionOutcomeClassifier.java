package bz.stub.parallelconsumer.internal.admission;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.internal.admission.AdmissionController.Outcome;

/**
 * Classifies a completed user-function invocation's failure into an admission {@link Outcome} - the one place the
 * "is this failure the DOWNSTREAM saying stop?" question will ever be answered.
 * <p>
 * <b>v1 classifies every failure as {@link Outcome#IGNORE}</b>: a plain exception from the user's function is a
 * business-logic failure, on the first attempt and on every retry alike, and business failures must never cut the
 * admission limit - a poison-pill record would otherwise throttle the whole instance (the control law's gradient
 * reads {@code IGNORE} as neutral). This deliberately errs toward under-reporting overload: a mis-labelled
 * {@code OVERLOAD_DROP} shrinks capacity on healthy traffic, while a mis-labelled {@code IGNORE} merely leaves the
 * latency signal to catch real overload, which it does.
 * <p>
 * <b>The overload socket.</b> {@link Outcome#OVERLOAD_DROP} is reserved for failure shapes that STRUCTURALLY name
 * downstream overload, planned as:
 * <ul>
 * <li>a dedicated rate-limit exception type (the plan's structured {@code PCOverloadException} / HTTP 429 shape -
 * a user function that KNOWS the downstream refused for capacity, not correctness), and</li>
 * <li>timeout shapes (e.g. {@code java.util.concurrent.TimeoutException} as the root cause), once the retry story
 * distinguishes "slow because overloaded" from "unreachable".</li>
 * </ul>
 * Until those exist, no cause matches: adding a match here is the ONLY change needed to light up the
 * overload-drop signal - callers already route every failure through this classifier.
 * <p>
 * Package-private by design: {@link AdmissionController#recordCompletion(boolean, Throwable)} is the public entry,
 * so the engine never names outcomes itself.
 */
final class AdmissionOutcomeClassifier {

    private AdmissionOutcomeClassifier() {
    }

    /**
     * Classifies one FAILED invocation by its cause.
     *
     * @param failureCause the exception the user function failed with; may be {@code null} when no cause was
     *                     recorded
     * @return {@link Outcome#IGNORE} for every cause in v1 - see the class javadoc for the overload socket
     */
    static Outcome classifyFailure(@SuppressWarnings("unused") Throwable failureCause) {
        return Outcome.IGNORE;
    }
}
