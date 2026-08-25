package bz.stub.parallelconsumer.internal.admission;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

/**
 * Adapter putting the {@link AdmissionControlLaw band-machine law} behind the {@link AdmissionPolicy} seam, so
 * the falsifier scenarios drive the REAL law at its production calibration - the U5 flip of the harness's
 * control run (the deleted {@code OldLawAdmissionPolicy} drove the Gradient2 port through the same seam and
 * FAILED the plateau; this adapter is asserted to pass it, in {@link AdmissionLawFalsifierTest}).
 * <p>
 * The law is stateful and holds its own limit; the adapter therefore ignores {@code previousTarget} and returns
 * whatever {@link AdmissionControlLaw#onWindowClosed} decides.
 */
final class LawAdmissionPolicy implements AdmissionPolicy {

    private final AdmissionControlLaw law;

    LawAdmissionPolicy(int initialTarget, int ceiling) {
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
