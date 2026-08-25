package bz.stub.parallelconsumer.internal.admission;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.Value;

/**
 * The outcome of one {@link AdmissionControlLaw#onWindowClosed} evaluation: the new target concurrency and the
 * reason the law chose it.
 */
@Value
public class AdmissionDecision {

    /**
     * The new admission limit (target concurrency) in slots, already clamped to [floor, ceiling].
     */
    int targetConcurrency;

    /**
     * Which arm of the control law produced this decision.
     */
    AdmissionDecisionReason reason;
}
