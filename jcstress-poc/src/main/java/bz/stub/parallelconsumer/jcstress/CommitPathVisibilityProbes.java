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
import static org.openjdk.jcstress.annotations.Expect.FORBIDDEN;

/**
 * Question 1, second read pair: the one the <b>commit path actually performs</b>.
 *
 * <h2>Correspondence to production code (check this if the source drifts)</h2>
 *
 * Writer, control thread - {@code PartitionState.onSuccess}, in this exact order:
 * <ol>
 *   <li>{@code this.incompleteOffsets.remove(offset)} - a {@link ConcurrentSkipListMap}, so this one
 *       access has its own ordering guarantees;</li>
 *   <li>{@code updateHighestSucceededOffsetSoFar(offset)} - plain write to {@code offsetHighestSucceeded};</li>
 *   <li>{@code setDirty()} - plain write {@code stateChangedSinceCommitStart = true}, then plain write
 *       {@code dirty = true} through the Lombok {@code @Setter(PRIVATE)}.</li>
 * </ol>
 *
 * Reader, broker-poll thread - {@code PartitionStateManager.collectDirtyCommitData} calls
 * {@code PartitionState.getCommitDataIfDirty}, in this exact order:
 * <ol>
 *   <li>{@code isDirty()} - plain read;</li>
 *   <li>{@code stateChangedSinceCommitStart = false} - plain write, from the reading thread;</li>
 *   <li>{@code createOffsetAndMetadata()} → {@code tryToEncodeOffsets()} → {@code incompleteOffsets.isEmpty()};</li>
 *   <li>{@code getOffsetToCommit()} → {@code getOffsetHighestSequentialSucceeded()} → plain read of
 *       {@code offsetHighestSucceeded}.</li>
 * </ol>
 *
 * The chain reaches the broker poll thread via {@code AbstractOffsetCommitter.retrieveOffsetsAndCommit},
 * which is where {@code ConsumerOffsetCommitter} runs in the default
 * {@code PERIODIC_CONSUMER_ASYNCHRONOUS} mode.
 *
 * <h2>What these probes do NOT model, and what it bounds</h2>
 *
 * The poll thread only enters that chain after {@code ConsumerOffsetCommitter.maybeDoCommit} takes a
 * {@code CommitRequest} off {@code commitRequestQueue}, which the control thread put there with
 * {@code commitRequestQueue.add(request)} in {@code requestCommitInternal}. That queue is a
 * {@link java.util.concurrent.ConcurrentLinkedQueue}, so the add/poll pair <b>is itself a
 * happens-before edge</b>, and everything the control thread completed <i>before</i> requesting the
 * commit - {@code offsetHighestSucceeded} and {@code dirty} included - is already published to the
 * collector.
 * <p>
 * So the pair modelled below is the residue, not the whole exposure: an {@code onSuccess} that
 * interleaves a collection already in progress. The window is <b>one commit cycle wide, not
 * unbounded</b>, and the rates these arms report are per raced pair - they are not a production
 * incidence rate, and must not be quoted as one.
 *
 * <h2>The anomaly, and what it would cost</h2>
 *
 * {@code dirty} is written <b>last</b> and read <b>first</b>: the classic message-passing shape, with
 * {@code dirty} as the flag and {@code offsetHighestSucceeded} as the payload. Observing
 * {@code dirty == true} with a stale {@code offsetHighestSucceeded} means the commit cycle fires on
 * the strength of offset N having succeeded, commits an offset that does not include N, and then
 * {@code onOffsetCommitSuccess} → {@code setClean()} marks the state clean - so N is not committed until
 * some later success re-dirties the partition. The direction is safe (replay, never skip), but the
 * commit cycle is lost.
 */
public class CommitPathVisibilityProbes {

    /**
     * As shipped, reduced to the two plain fields: {@code offsetHighestSucceeded} then {@code dirty}.
     */
    @JCStressTest
    @Description("Commit path as shipped: plain offsetHighestSucceeded published by a plain dirty flag")
    @Outcome(id = "1, 0", expect = ACCEPTABLE_INTERESTING,
            desc = "ANOMALY: dirty seen set, offsetHighestSucceeded still stale - commit cycle burnt on stale state")
    @Outcome(id = "0, 0", expect = ACCEPTABLE, desc = "Reader saw neither write - no commit this cycle")
    @Outcome(id = "0, 1", expect = ACCEPTABLE, desc = "Reader saw succeeded but not dirty - commit deferred, benign")
    @Outcome(id = "1, 1", expect = ACCEPTABLE, desc = "Reader saw both - the intended case")
    @State
    public static class PlainDirtyPublishesSucceeded {

        long offsetHighestSucceeded;
        boolean dirty;

        @Actor
        public void controlThread() {
            offsetHighestSucceeded = 1; // updateHighestSucceededOffsetSoFar
            dirty = true;               // setDirty
        }

        @Actor
        public void brokerPollThread(JJ_Result r) {
            r.r1 = dirty ? 1 : 0;               // isDirty()
            r.r2 = offsetHighestSucceeded;      // getOffsetHighestSequentialSucceeded()
        }
    }

    /**
     * Control arm for the candidate fix: {@code dirty} volatile is enough on its own, because a
     * volatile write releases everything before it and a volatile read acquires everything after it -
     * so {@code offsetHighestSucceeded} can stay plain and still be published safely.
     * <p>
     * Included because it is the cheaper fix of the two, and if it holds the recommendation does not
     * need to make the hot {@code long} volatile at all.
     */
    @JCStressTest
    @Description("Control arm: only the dirty flag volatile - the payload should ride the release/acquire edge")
    @Outcome(id = "1, 0", expect = FORBIDDEN,
            desc = "Publication through the volatile flag failed - would invalidate the cheap fix")
    @Outcome(id = {"0, 0", "0, 1", "1, 1"}, expect = ACCEPTABLE, desc = "Orderings the release/acquire edge permits")
    @State
    public static class VolatileDirtyPublishesPlainSucceeded {

        long offsetHighestSucceeded;
        volatile boolean dirty;

        @Actor
        public void controlThread() {
            offsetHighestSucceeded = 1;
            dirty = true;
        }

        @Actor
        public void brokerPollThread(JJ_Result r) {
            r.r1 = dirty ? 1 : 0;
            r.r2 = offsetHighestSucceeded;
        }
    }

    /**
     * The faithful arm: every real access in the two sequences it models, including the
     * {@link ConcurrentSkipListMap} touches and the reader's own plain write to
     * {@code stateChangedSinceCommitStart}.
     * <p>
     * <b>Faithful to those two sequences, not to the whole path.</b> It still omits the
     * commit-request handoff described above, and the non-empty branch of
     * {@code tryToEncodeOffsets}; its map is {@code final} where production's
     * {@code incompleteOffsets} is {@code @NonNull @Setter(PACKAGE)} and reassignable. None of the
     * three can add ordering the reduced arm lacks, so they do not explain the gap between the two
     * arms - but the arm bounds the window rather than reproducing the path end to end.
     * <p>
     * <b>What it is for.</b> The reduced probe drops accesses that the reduction argues cannot matter.
     * That argument can be wrong in either direction: a concurrent-map CAS is a full fence on most
     * hardware, so the surrounding code might already be closing the hole by accident - in which case
     * a synchronisation fix buys nothing real. Running both arms measures that instead of assuming it.
     * A materially lower anomaly rate here than in {@link PlainDirtyPublishesSucceeded} is the signal;
     * <b>zero here with a firing reduced arm would mean the shipped code is incidentally fenced</b>,
     * which is worth knowing and is not something the source can be read for.
     */
    @JCStressTest
    @Description("Faithful arm: onSuccess and getCommitDataIfDirty with their real map accesses in place")
    @Outcome(id = "1, 0", expect = ACCEPTABLE_INTERESTING,
            desc = "ANOMALY: reproduced with the real surrounding accesses present")
    @Outcome(id = {"0, 0", "0, 1", "1, 1"}, expect = ACCEPTABLE, desc = "Non-anomalous orderings")
    @State
    public static class FaithfulOnSuccessVersusCommitCollection {

        final ConcurrentSkipListMap<Long, Optional<Object>> incompleteOffsets = new ConcurrentSkipListMap<>();

        long offsetHighestSucceeded;
        boolean dirty;
        boolean stateChangedSinceCommitStart;

        /**
         * Instrumentation only - nothing in {@code PartitionState} corresponds to it. Its job is to be
         * a store the JIT may not remove, so the map access ahead of it cannot be optimised away and
         * stop fencing. Reporting the map observation in the result instead would conflate two
         * distinct causes of a burnt commit cycle; this arm is measuring the plain-field one.
         */
        boolean lastObservedIncompletesEmpty;

        public FaithfulOnSuccessVersusCommitCollection() {
            incompleteOffsets.put(1L, Optional.empty());
        }

        /**
         * {@code PartitionState.onSuccess(1)}.
         */
        @Actor
        public void controlThread() {
            incompleteOffsets.remove(1L);
            offsetHighestSucceeded = 1;
            stateChangedSinceCommitStart = true;
            dirty = true;
        }

        /**
         * {@code PartitionState.getCommitDataIfDirty()} down to the read of {@code offsetHighestSucceeded}.
         */
        @Actor
        public void brokerPollThread(JJ_Result r) {
            boolean isDirty = dirty;
            if (isDirty) {
                stateChangedSinceCommitStart = false;
            }
            // tryToEncodeOffsets() reaches the map before the offset is read
            lastObservedIncompletesEmpty = incompleteOffsets.isEmpty();
            long succeeded = offsetHighestSucceeded;

            r.r1 = isDirty ? 1 : 0;
            r.r2 = succeeded;
        }
    }
}
