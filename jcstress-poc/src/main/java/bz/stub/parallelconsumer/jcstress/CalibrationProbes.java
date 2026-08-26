package bz.stub.parallelconsumer.jcstress;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.Description;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.State;
import org.openjdk.jcstress.infra.results.JJ_Result;
import org.openjdk.jcstress.infra.results.J_Result;

import static org.openjdk.jcstress.annotations.Expect.ACCEPTABLE;
import static org.openjdk.jcstress.annotations.Expect.ACCEPTABLE_INTERESTING;
import static org.openjdk.jcstress.annotations.Expect.FORBIDDEN;

/**
 * Calibration for the harness itself. Neither probe says anything about Parallel Consumer; together
 * they say whether a clean result from the probes that DO is worth believing.
 * <p>
 * <b>A clean run of the negative control alone proves nothing</b> - a harness that never observes
 * anything also comes back clean. Discrimination needs both arms: one shape that must stay clean and
 * one that must fire on this machine. Read the two results together or not at all.
 */
public class CalibrationProbes {

    /**
     * NEGATIVE CONTROL - expected clean.
     * <p>
     * Word tearing of a plain {@code long}. The JMM permits a non-volatile 64-bit write to be split
     * into two 32-bit halves (JLS 17.7), which is why the plain {@code long} fields in
     * {@code PartitionState} are a live question at the language level at all. On 64-bit HotSpot the
     * store is a single instruction, so this must come back with only the two whole values.
     * <p>
     * A FORBIDDEN hit here would mean a half-written offset is reachable, which is a different and
     * much worse bug than staleness.
     */
    @JCStressTest
    @Description("Plain long word tearing - JMM-permitted, expected absent on 64-bit HotSpot")
    @Outcome(id = "0", expect = ACCEPTABLE, desc = "Reader ran before the write - default value")
    @Outcome(id = "-1", expect = ACCEPTABLE, desc = "Reader saw the whole write")
    @Outcome(expect = FORBIDDEN, desc = "Word tearing - half of the 64-bit write is visible")
    @State
    public static class PlainLongWordTearing {

        long offsetHighestSucceeded;

        @Actor
        public void controlThread() {
            // all bits set, so either 32-bit half is instantly recognisable on its own
            offsetHighestSucceeded = -1L;
        }

        @Actor
        public void brokerPollThread(J_Result r) {
            r.r1 = offsetHighestSucceeded;
        }
    }

    /**
     * POSITIVE CONTROL - expected to FIRE.
     * <p>
     * The textbook store-load (Dekker) shape. Both actors write their own plain field then read the
     * other's; sequential consistency forbids both reads returning 0, and every mainstream CPU
     * (including this arm64 one) produces it anyway via store buffering.
     * <p>
     * If this does not fire, the harness is not exercising real concurrency - too few iterations, the
     * actors not actually overlapping, or the JIT having folded the test away - and every "zero
     * anomalies observed" result in this module is uninterpretable.
     */
    @JCStressTest
    @Description("Store-load reordering on plain fields - must fire, proving the harness discriminates")
    @Outcome(id = "0, 0", expect = ACCEPTABLE_INTERESTING, desc = "Store-load reordering observed - harness is live")
    @Outcome(id = {"0, 1", "1, 0", "1, 1"}, expect = ACCEPTABLE, desc = "Sequentially consistent orderings")
    @State
    public static class PlainFieldStoreLoadReordering {

        long x;
        long y;

        @Actor
        public void actor1(JJ_Result r) {
            x = 1;
            r.r1 = y;
        }

        @Actor
        public void actor2(JJ_Result r) {
            y = 1;
            r.r2 = x;
        }
    }
}
