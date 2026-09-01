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

import static org.openjdk.jcstress.annotations.Expect.ACCEPTABLE;
import static org.openjdk.jcstress.annotations.Expect.ACCEPTABLE_INTERESTING;
import static org.openjdk.jcstress.annotations.Expect.FORBIDDEN;

/**
 * Question 1 of the standing residual: can a reader observe {@code offsetHighestSucceeded} advanced
 * while {@code offsetHighestSeen} is still stale - an ordering the code's own documentation treats as
 * impossible?
 *
 * <h2>Correspondence to production code (check this if the source drifts)</h2>
 *
 * In {@code parallel-consumer-core/.../state/PartitionState.java}:
 * <ul>
 *   <li>{@code private long offsetHighestSeen} and {@code private long offsetHighestSucceeded} - both
 *       plain, neither volatile, neither guarded by any lock.</li>
 *   <li><b>Write order, control thread, per offset N.</b> {@code addNewIncompleteRecord} calls
 *       {@code maybeRaiseHighestSeenOffset}, which writes {@code offsetHighestSeen = offset}. Later,
 *       when the user function returns, {@code onSuccess} calls
 *       {@code updateHighestSucceededOffsetSoFar}, which writes {@code this.offsetHighestSucceeded = thisOffset}.
 *       So for any single offset, <i>seen is raised strictly before succeeded</i>, and the javadoc on
 *       {@code offsetHighestSucceeded} states the two are equal at bootstrap - i.e. the code assumes
 *       {@code offsetHighestSucceeded <= offsetHighestSeen} always holds.</li>
 *   <li><b>Read order, other threads.</b> Both are exposed by Lombok {@code @Getter(PUBLIC)}. The
 *       commit path reads {@code offsetHighestSucceeded} in {@code getOffsetHighestSequentialSucceeded}
 *       ("use offsetHighestSucceeded instead of offsetHighestSeen to fix confluentinc issue #826") and
 *       again in {@code tryToEncodeOffsets}; the Micrometer gauges registered in {@code initMetrics}
 *       read both, from whichever thread scrapes.</li>
 *   <li>The class comment already points at confluentinc#200 as the structural fix
 *       ("Consider a shared nothing architecture"); nothing in the interim added a happens-before edge.</li>
 * </ul>
 *
 * The probes below reduce that to the message-passing (MP) shape: two plain writes in a fixed order,
 * read back in the opposite order. That is the minimal pattern; it deliberately drops the record
 * payload, the map and the epoch, none of which participate in the ordering question.
 */
public class SeenSucceededOrderingProbes {

    /**
     * As shipped: both fields plain.
     * <p>
     * The reader takes {@code offsetHighestSucceeded} first (as the commit path does) and
     * {@code offsetHighestSeen} second. Seeing {@code succeeded == 1, seen == 0} means the later write
     * became visible before the earlier one - the invariant
     * {@code offsetHighestSucceeded <= offsetHighestSeen} broken by the memory model alone.
     */
    @JCStressTest
    @Description("PartitionState as shipped: plain offsetHighestSeen written before plain offsetHighestSucceeded")
    @Outcome(id = "1, 0", expect = ACCEPTABLE_INTERESTING,
            desc = "ANOMALY: succeeded advanced while seen is stale - violates offsetHighestSucceeded <= offsetHighestSeen")
    @Outcome(id = "0, 0", expect = ACCEPTABLE, desc = "Reader saw neither write")
    @Outcome(id = "0, 1", expect = ACCEPTABLE, desc = "Reader saw only the seen write - the intended intermediate")
    @Outcome(id = "1, 1", expect = ACCEPTABLE, desc = "Reader saw both writes")
    @State
    public static class PlainFields {

        long offsetHighestSeen;
        long offsetHighestSucceeded;

        /**
         * Control thread: registers offset 1 as work (raising seen), then completes it (raising succeeded).
         */
        @Actor
        public void controlThread() {
            offsetHighestSeen = 1;       // maybeRaiseHighestSeenOffset
            offsetHighestSucceeded = 1;  // updateHighestSucceededOffsetSoFar
        }

        /**
         * Broker-poll thread: the commit path reads succeeded; the metrics gauge also reads seen.
         */
        @Actor
        public void brokerPollThread(JJ_Result r) {
            r.r1 = offsetHighestSucceeded;
            r.r2 = offsetHighestSeen;
        }
    }

    /**
     * The candidate fix, as a control arm: identical shape with both fields {@code volatile}.
     * <p>
     * The anomaly outcome is FORBIDDEN here. If {@link PlainFields} fires and this one does not, the
     * volatile qualifier is demonstrated to close that specific hole on this hardware, rather than
     * merely argued to.
     */
    @JCStressTest
    @Description("Control arm: the same shape with both fields volatile - the anomaly must vanish")
    @Outcome(id = "1, 0", expect = FORBIDDEN,
            desc = "volatile did not establish the ordering - would invalidate the proposed fix")
    @Outcome(id = {"0, 0", "0, 1", "1, 1"}, expect = ACCEPTABLE, desc = "Orderings volatile permits")
    @State
    public static class VolatileFields {

        volatile long offsetHighestSeen;
        volatile long offsetHighestSucceeded;

        @Actor
        public void controlThread() {
            offsetHighestSeen = 1;
            offsetHighestSucceeded = 1;
        }

        @Actor
        public void brokerPollThread(JJ_Result r) {
            r.r1 = offsetHighestSucceeded;
            r.r2 = offsetHighestSeen;
        }
    }
}
