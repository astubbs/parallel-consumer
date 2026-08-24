package bz.stub.parallelconsumer.internal.admission;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

/**
 * The negative-control mutants of the falsifier suite (the design's "How each decision can be proven wrong",
 * fix 3): deliberately broken controllers that every applicable scenario must be asserted to FAIL. A scenario
 * a mutant passes is a safety-only scenario satisfied by inaction - the exact defect the suite exists to
 * exclude.
 * <ul>
 * <li>{@link FrozenLimit} never moves - it fails any scenario that demands convergence from a wrong start
 * (liveness), and it passes any scenario started on the oracle, which is why the sweep parameterises over
 * initial conditions.</li>
 * <li>{@link AlwaysMaxLimit} jumps straight to the ceiling - it fails the band and latency brackets (a
 * too-high limit queues).</li>
 * <li>{@link AlwaysMinLimit} pins the floor - it fails the throughput bracket (a too-low limit starves).</li>
 * </ul>
 */
final class MutantPolicies {

    private MutantPolicies() {
    }

    /** Never moves: {@code nextTarget == previousTarget} forever. */
    static final class FrozenLimit implements AdmissionPolicy {
        @Override
        public int nextTarget(int previousTarget, ClosedAdmissionWindow window) {
            return previousTarget;
        }
    }

    /** Jumps to the ceiling on the first window and stays there. */
    static final class AlwaysMaxLimit implements AdmissionPolicy {
        private final int ceiling;

        AlwaysMaxLimit(int ceiling) {
            this.ceiling = ceiling;
        }

        @Override
        public int nextTarget(int previousTarget, ClosedAdmissionWindow window) {
            return ceiling;
        }
    }

    /** Pins the floor (one slot) whatever the evidence. */
    static final class AlwaysMinLimit implements AdmissionPolicy {
        @Override
        public int nextTarget(int previousTarget, ClosedAdmissionWindow window) {
            return 1;
        }
    }
}
