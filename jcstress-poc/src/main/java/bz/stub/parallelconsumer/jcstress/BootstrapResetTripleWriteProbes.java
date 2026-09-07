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

import java.util.Optional;
import java.util.concurrent.ConcurrentSkipListMap;

import static org.openjdk.jcstress.annotations.Expect.ACCEPTABLE;
import static org.openjdk.jcstress.annotations.Expect.ACCEPTABLE_INTERESTING;

/**
 * Question 3: the {@code initStateFromOffsetData} triple write - dossier candidate 1, the
 * bootstrap-reset tear.
 *
 * <h2>Correspondence to production code (check this if the source drifts)</h2>
 *
 * {@code PartitionState.maybeTruncateBelowOrAbove}, reset-below arm ("Bootstrap polled offset has been
 * reset to an earlier offset"), calls {@code initStateFromOffsetData}, which performs three writes in
 * this order, none of them volatile:
 * <ol>
 *   <li>{@code this.offsetHighestSeen = offsetData.getHighestSeenOffset().orElse(KAFKA_OFFSET_ABSENCE)} - here, -1;</li>
 *   <li>{@code this.incompleteOffsets = new ConcurrentSkipListMap<>()} - a plain <b>reference</b> write,
 *       swapping in a fresh, empty map (the reset passes
 *       {@code OffsetMapCodecManager.HighestOffsetAndIncompletes.of()}, so nothing is put into it);</li>
 *   <li>{@code this.offsetHighestSucceeded = this.offsetHighestSeen} - here, -1.</li>
 * </ol>
 *
 * The reader is the commit path on the broker-poll thread,
 * {@code PartitionState.getOffsetHighestSequentialSucceeded}, which reads in this order:
 * <ol>
 *   <li>{@code long currentOffsetHighestSeen = offsetHighestSucceeded;}</li>
 *   <li>{@code Long firstIncompleteOffset = incompleteOffsets.keySet().ceiling(KAFKA_OFFSET_ABSENCE);}</li>
 *   <li>returns {@code currentOffsetHighestSeen} when the map is empty, else {@code firstIncompleteOffset - 1}.</li>
 * </ol>
 * {@code getOffsetToCommit()} adds one, and that is what is committed to the broker.
 *
 * <h2>Pre-reset state modelled here</h2>
 *
 * A partition whose highest succeeded offset was 100 with offset 51 still incomplete, which the broker has just
 * reset below - so the correct post-reset commit is 0 and every record from 0 must be replayed.
 * {@code r1} is the observed {@code offsetHighestSucceeded}; {@code r2} is the offset the commit path
 * would send.
 *
 * <h2>What this probe can and cannot settle</h2>
 *
 * The harmful combination needs no memory-model exotica at all - it is ordinary program-order
 * staleness, the reader landing between writes 2 and 3 - so it should be common, and a high count is
 * <b>not</b> evidence of a JMM anomaly. The genuinely JMM-only outcome is the mirror image,
 * {@code succeeded == -1} with the <b>old</b> map still visible: seeing the third write without the
 * second. Both are broken out below so the two causes are never read as one number.
 * <p>
 * Note also that candidate 1 is documented as <b>fenced in production</b> by the dirty check - the
 * commit path collects only dirty states and a bootstrap-phase state cannot have been dirtied except
 * through candidate 3. This probe measures the window, not a live production defect.
 */
public class BootstrapResetTripleWriteProbes {

    private static final long KAFKA_OFFSET_ABSENCE = -1L;

    @JCStressTest
    @Description("initStateFromOffsetData: three plain writes observed by the commit path's two reads")
    @Outcome(id = "100, 51", expect = ACCEPTABLE,
            desc = "Wholly pre-reset view: old succeeded, old map - commits 51, correct for the pre-reset state")
    @Outcome(id = "-1, 0", expect = ACCEPTABLE,
            desc = "Wholly post-reset view: commits 0, the replay the broker asked for")
    @Outcome(id = "100, 101", expect = ACCEPTABLE_INTERESTING,
            desc = "TEAR (program-order staleness): new empty map combined with the old succeeded - commits 101, "
                    + "re-asserting a pre-reset offset and cancelling the mandated replay")
    @Outcome(id = "-1, 51", expect = ACCEPTABLE_INTERESTING,
            desc = "TEAR (reordering only): the third write visible without the second - commits 51, safe direction")
    @State
    public static class PlainTripleWrite {

        /**
         * Plain reference field in production too - {@code @NonNull @Setter(PACKAGE)}, not final, not volatile.
         */
        ConcurrentSkipListMap<Long, Optional<Object>> incompleteOffsets = new ConcurrentSkipListMap<>();

        long offsetHighestSeen = 100;
        long offsetHighestSucceeded = 100;

        public PlainTripleWrite() {
            // one record was still incomplete before the reset
            incompleteOffsets.put(51L, Optional.empty());
        }

        /**
         * Control thread: {@code maybeTruncateBelowOrAbove} → {@code initStateFromOffsetData}.
         */
        @Actor
        public void controlThread() {
            this.offsetHighestSeen = KAFKA_OFFSET_ABSENCE;
            this.incompleteOffsets = new ConcurrentSkipListMap<>();
            this.offsetHighestSucceeded = this.offsetHighestSeen;
        }

        /**
         * Broker-poll thread: {@code getOffsetHighestSequentialSucceeded()} then {@code getOffsetToCommit()}.
         */
        @Actor
        public void brokerPollThread(JJ_Result r) {
            long currentOffsetHighestSeen = offsetHighestSucceeded;
            Long firstIncompleteOffset = incompleteOffsets.keySet().ceiling(KAFKA_OFFSET_ABSENCE);

            long sequentialSucceeded = (firstIncompleteOffset == null)
                    ? currentOffsetHighestSeen
                    : firstIncompleteOffset - 1;

            r.r1 = currentOffsetHighestSeen;
            r.r2 = sequentialSucceeded + 1; // getOffsetToCommit()
        }
    }
}
