package bz.stub.parallelconsumer.internal.admission;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.internal.admission.AdmissionController.Outcome;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeoutException;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

/**
 * Pins the classifier's v1 default: EVERY failure is {@link Outcome#IGNORE} - business failures must never cut the
 * admission limit. The {@link Outcome#OVERLOAD_DROP} socket (the structured rate-limit exception and timeout
 * shapes) is documented on {@link AdmissionOutcomeClassifier} and deliberately matches nothing yet, which the
 * timeout case here pins: when the socket lands, THAT test moves, consciously.
 */
class AdmissionOutcomeClassifierTest {

    @Test
    void aPlainRuntimeExceptionClassifiesAsIgnore() {
        assertThat(AdmissionOutcomeClassifier.classifyFailure(new RuntimeException("business failure")))
                .isEqualTo(Outcome.IGNORE);
    }

    @Test
    void aMissingCauseClassifiesAsIgnore() {
        assertThat(AdmissionOutcomeClassifier.classifyFailure(null)).isEqualTo(Outcome.IGNORE);
    }

    /**
     * The socket's v1 emptiness, pinned on its most tempting future member: even a timeout is IGNORE until the
     * retry story can tell "slow because overloaded" from "unreachable".
     */
    @Test
    void aTimeoutStillClassifiesAsIgnoreUntilTheOverloadSocketLands() {
        assertWithMessage("v1 reserves OVERLOAD_DROP - nothing may classify into it yet")
                .that(AdmissionOutcomeClassifier.classifyFailure(new RuntimeException(new TimeoutException())))
                .isEqualTo(Outcome.IGNORE);
    }
}
