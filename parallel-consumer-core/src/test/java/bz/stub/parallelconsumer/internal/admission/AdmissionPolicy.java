package bz.stub.parallelconsumer.internal.admission;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

/**
 * The controller-under-test seam of the falsifier harness (the design's R14): given the target the controller
 * last commanded and the {@link ClosedAdmissionWindow} the plant produced under it, return the next target in
 * slots.
 * <p>
 * Implementations are the real law behind an adapter ({@link OldLawAdmissionPolicy} today, the U5 rewrite
 * tomorrow) and the negative-control mutants ({@link MutantPolicies}) that every scenario must be able to fail.
 * The {@link ScenarioRunner} owns the loop; a policy owns only the decision.
 */
interface AdmissionPolicy {

    /**
     * One control decision: the target (slots) to command for the NEXT window.
     *
     * @param previousTarget the target that produced {@code window}; stateful policies may ignore it
     * @param window         the closed window the plant produced under {@code previousTarget}
     */
    int nextTarget(int previousTarget, ClosedAdmissionWindow window);
}
