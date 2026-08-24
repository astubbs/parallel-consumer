package bz.stub.parallelconsumer.internal.admission;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

/**
 * Adapter putting the CURRENT {@link AdmissionControlLaw} behind the {@link AdmissionPolicy} seam, so the
 * falsifier scenarios can drive the committed law as the ablation control before the U5 rewrite replaces it
 * (the plan's U8 execution note).
 * <p>
 * The law is stateful and holds its own limit; the adapter therefore ignores {@code previousTarget} and returns
 * whatever {@link AdmissionControlLaw#onWindowClosed} decides.
 */
final class OldLawAdmissionPolicy implements AdmissionPolicy {

    private final AdmissionControlLaw law;

    OldLawAdmissionPolicy(int initialTarget, int ceiling) {
        this.law = AdmissionControlLaw.newBuilder()
                .initialLimit(initialTarget)
                .ceiling(ceiling)
                .build();
    }

    @Override
    public int nextTarget(int previousTarget, ClosedAdmissionWindow window) {
        return law.onWindowClosed(window).getTargetConcurrency();
    }
}
