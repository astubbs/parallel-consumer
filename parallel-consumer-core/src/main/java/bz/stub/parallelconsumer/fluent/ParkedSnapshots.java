package bz.stub.parallelconsumer.fluent;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The parked view's control-thread snapshot: the facade's parked set, reconciled against what the engine still
 * holds outstanding, taken at the end of a control loop (R28, KTD4).
 *
 * <h2>Why it is a snapshot rather than a live read</h2>
 * The facade's parked map and the engine's offset map are written by different threads at different moments, and a
 * caller reading both would see them mid-step. Worse, a worker that finishes <em>after</em> its partition was
 * revoked can leave a <b>phantom</b> entry: a record this instance no longer owns, or one that has since completed,
 * still listed as parked. Reconciling on the control thread - where the engine's state is quiescent between loop
 * passes - is what removes those, and it is why the reconciliation is not simply done in the accessor.
 * <p>
 * A phantom is dropped from the facade's own map, not merely filtered out of the answer: it will never come back,
 * and leaving it there would grow the map by one entry per revocation race for the life of the instance.
 *
 * <h2>Why it must never throw</h2>
 * It runs through {@code addLoopEndCallBack}, and <b>the control loop treats a throwing hook as fatal</b> - it is
 * run as user code and the failure takes the instance down. So the whole pass is contained here, and a fault is
 * logged once rather than every loop: reporting must not be able to stop consuming. {@link #refresh()} is the only
 * entry point, and it is the one that catches.
 *
 * <h2>Why it is throttled</h2>
 * The control loop runs far more often than an operator reads a parked list, and reconciliation is linear in the
 * parked set - thousands of entries on a partition would be re-walked every pass for nobody. The interval is the
 * freshness the view promises, and nothing here needs to be more current than that: the two commands that would
 * need it, resume and dlq, are not supported in this version.
 */
@Slf4j
class ParkedSnapshots {

    /**
     * How stale the parked view may be. Chosen to be far shorter than a person reading a list and far longer than a
     * control loop pass.
     */
    static final Duration INTERVAL = Duration.ofMillis(200);

    /**
     * The engine's half of the parked set: whether the engine has stopped holding this record outstanding.
     * <p>
     * <b>The substitution seam for the test that proves a throwing snapshot cannot stop the instance.</b> Replacing
     * it is the only way to make a real running instance's reconciliation fail, since the engine's own accessor
     * does not throw - and a containment claim nothing exercises is a comment.
     */
    interface EngineOffsets {

        /**
         * <b>It must answer false whenever it cannot tell</b>, because a true condemns the entry. A partition the
         * engine has no state for, or one whose state has not yet seen this offset, is "cannot tell": the parked
         * entry is written by a worker thread and the engine's own bookkeeping is not a snapshot, so a read that
         * finds nothing is far more often early than it is authoritative. Dropping a live parked record - a record
         * an operator was told they could still act on - is the expensive mistake here; carrying a phantom for one
         * more pass is the cheap one.
         */
        boolean isNoLongerOutstanding(ParkedRecord parked);
    }

    private final ParkedRecords parkedRecords;

    private volatile EngineOffsets engineOffsets;

    /**
     * The last reconciled list. Immutable and replaced wholesale, so a reader never sees a half-built one.
     */
    private volatile List<ParkedRecord> snapshot = Collections.emptyList();

    private volatile Instant takenAt = Instant.EPOCH;

    /**
     * Said once. A reconciliation that fails once usually fails every pass, and a warning per control loop would
     * bury the log it is trying to be visible in.
     */
    private final AtomicBoolean faultLogged = new AtomicBoolean();

    /**
     * Starts with no engine half at all - every parked entry is taken at face value - because this is built while
     * the definition is still assembling the instance, before there is a processor to read state from. The handle
     * supplies the real one the moment it registers the loop-end hook.
     */
    ParkedSnapshots(ParkedRecords parkedRecords) {
        this.parkedRecords = parkedRecords;
        this.engineOffsets = parked -> false;
    }

    /**
     * @see EngineOffsets
     */
    void engineOffsets(EngineOffsets source) {
        this.engineOffsets = source;
    }

    /**
     * Take a snapshot if one is due. <b>Never throws</b>, whatever the engine, the clock or the map does.
     */
    void refresh() {
        try {
            Instant now = Instant.now();
            if (Duration.between(takenAt, now).compareTo(INTERVAL) < 0) {
                return;
            }
            snapshot = reconcile();
            takenAt = now;
        } catch (Throwable snapshotFailed) { //NOSONAR - the control loop treats a throwing hook as fatal
            logOnce(snapshotFailed);
        }
    }

    /**
     * The last snapshot: every parked record this instance still holds, whichever route it belongs to. Empty until
     * the first control loop has ended.
     */
    List<ParkedRecord> current() {
        return snapshot;
    }

    /**
     * When {@link #current()} was taken, so a view can say how old its answer is rather than implying it is live.
     * {@link Instant#EPOCH} until the first one.
     */
    Instant takenAt() {
        return takenAt;
    }

    private List<ParkedRecord> reconcile() {
        List<ParkedRecord> live = new ArrayList<>();
        List<ParkedRecord> phantoms = new ArrayList<>();
        for (ParkedRecord parked : parkedRecords.all()) {
            if (engineOffsets.isNoLongerOutstanding(parked)) {
                phantoms.add(parked);
            } else {
                live.add(parked);
            }
        }
        for (ParkedRecord phantom : phantoms) {
            // Dropped from the map itself, not merely filtered out of the answer: the engine has moved past this
            // record and never comes back to it, so the entry would otherwise sit there for the life of the
            // instance, one per revocation race.
            log.debug("Dropping a parked entry the engine no longer holds outstanding: {}", phantom);
            parkedRecords.forget(phantom.raw());
        }
        return Collections.unmodifiableList(live);
    }

    private void logOnce(Throwable snapshotFailed) {
        if (faultLogged.compareAndSet(false, true)) {
            log.warn("The fluent API's parked-view snapshot failed and is contained - the parked view may be stale "
                    + "or empty until it succeeds again. This cannot stop the instance: reporting must not be able "
                    + "to break consuming. Said once.", snapshotFailed);
        } else {
            log.debug("The parked-view snapshot failed again", snapshotFailed);
        }
    }

    /**
     * Whether a snapshot has failed at any point - what a test asserts on instead of scraping the log, and what
     * keeps the containment claim honest rather than merely quiet.
     */
    boolean hasFailed() {
        return faultLogged.get();
    }
}
