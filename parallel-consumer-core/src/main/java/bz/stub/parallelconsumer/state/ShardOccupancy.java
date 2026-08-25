package bz.stub.parallelconsumer.state;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import java.util.Iterator;
import java.util.NavigableSet;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.LongAdder;

/**
 * Which of a shard's records are out at a worker, and which are not - the two answers the dispatch scan needs
 * before it looks at a record, kept beside each other because they are the two sides of one transition.
 *
 * <h2>Why the "not in flight" half exists</h2>
 *
 * Under {@code UNORDERED} a shard holds every record of its topic-partition, in-flight ones included: a record
 * leaves {@link ProcessingShard} when it <em>succeeds</em>, not when it is taken. The scan therefore used to walk
 * the whole in-flight prefix on every pass to reach the first record it could claim, at a cost of roughly
 * {@code in-flight / shards} examinations per record dispatched. That is invisible in the engine PC ships, which
 * pays it once per batch on one thread - and catastrophic in the direct-pull engine, which pays it once per record
 * on every worker. Measured with {@link DispatchScanMeter} and a single scanner, so no claim contention could be
 * confused for it: 1.00 examinations per record at ten in flight, 440.13 at five thousand. See
 * {@code docs/inflight/perf-direct-pull-collapse-is-the-scan.md}.
 * <p>
 * Iterating {@link #selectableOffsets()} instead of the shard's entries removes that prefix: the offsets in it are
 * exactly the ones no worker is holding, so the first one is nearly always claimable.
 *
 * <h2>Why this cannot strand a record, which is the failure mode that matters</h2>
 *
 * An index that is missing a selectable record would stall it forever, which is the shape of the
 * confluentinc#857 family. Two properties stop it:
 * <ol>
 *     <li><b>Every departure from the set is paired with an arrival</b>, by the same two halves of one state
 *     transition on the record itself - claimed ({@link #onFlightBegan}) and landed ({@link #onFlightEnded}) -
 *     rather than by the paths that add and remove shard entries. This is the arrangement that already keeps
 *     {@link #inFlightCount()} from drifting, and it is reused rather than reinvented: there is no removal site
 *     whose condition can be got wrong.</li>
 *     <li><b>Admission and retirement are the only other writers</b>, and both funnel through a single place in
 *     {@link ProcessingShard} each - the entry map's only insertion, and the two retirement helpers every removal
 *     already calls.</li>
 * </ol>
 * The remaining inaccuracy is one-directional and self-healing: an offset can outlive its entry (a record revoked
 * while in flight retires first and lands afterwards), which costs the scan one examination and is then dropped by
 * {@link #forget} on the spot. An offset can also be present while its record is not yet claimable - one waiting
 * out a retry delay, or one that has just succeeded and not yet been retired. <b>The set means "no worker is
 * holding it", not "it can be taken right now"</b>, and the authority on the latter is, as before, the claim in
 * {@link WorkContainer#onQueueingForExecution()}.
 *
 * <h2>Why the ordered modes do not read it</h2>
 *
 * They have no prefix to skip. {@code KEY} and {@code PARTITION} may have at most one record of a shard out at a
 * time, so a shard with anything in flight is closed outright ({@link ProcessingShard#isBlockedByWorkInFlight()})
 * and a shard with nothing in flight has its next record at the head of its entries already. The set is still
 * maintained for them - the maintenance is what makes it trustworthy, and it is two set operations per delivery -
 * but the ordered scan continues to walk the entry map exactly as it did, which is deliberate: in-flight records
 * remaining visible to that walk is part of how ordering is enforced, and removing them from its view has already
 * broken ten tests once ({@code docs/inflight/parked-resume-shard-dispatch-scan.md}).
 *
 * @author Antony Stubbs
 * @see ProcessingShard#getWorkIfAvailable
 * @see DispatchScanMeter
 */
class ShardOccupancy {

    /**
     * How many of the shard's records are out at a worker.
     * <p>
     * {@link LongAdder} because it is written twice per delivery from whichever worker took the record, and read at
     * most once per shard per dispatch pass.
     */
    private final LongAdder inFlight = new LongAdder();

    /**
     * Offsets of the shard's records that no worker is holding, in offset order.
     * <p>
     * A set of offsets rather than of containers, so the entry map stays the one place a record lives and this
     * cannot disagree with it about <em>which</em> container occupies an offset - only about whether one is there
     * at all, which {@link #forget} repairs on sight.
     */
    private final NavigableSet<Long> notInFlight = new ConcurrentSkipListSet<>();

    /** A record has entered the shard, available to be claimed. */
    void onAdmitted(long offset) {
        notInFlight.add(offset);
    }

    /** A record has left the shard for good, whether it succeeded, was revoked, or was swept as stale. */
    void onRetired(long offset) {
        notInFlight.remove(offset);
    }

    /** A worker won the claim: the record is out and must not be offered again until it lands. */
    void onFlightBegan(long offset) {
        inFlight.increment();
        notInFlight.remove(offset);
    }

    /** The delivery landed - succeeded, failed or came back with no verdict - so the record is offerable again. */
    void onFlightEnded(long offset) {
        inFlight.decrement();
        notInFlight.add(offset);
    }

    /**
     * Drops an offset the scan found had no entry behind it. Distinct from {@link #onRetired} only in intent: this
     * is the self-healing path for an index entry that outlived its record, and naming it separately is what stops
     * that being read as a second retirement site.
     */
    void forget(long offset) {
        notInFlight.remove(offset);
    }

    long inFlightCount() {
        return inFlight.sum();
    }

    /**
     * The offsets to try, lowest first.
     * <p>
     * Weakly consistent, like every view of a {@link ConcurrentSkipListSet}: a scan in progress may or may not see
     * a concurrent addition. That is the same guarantee the entry-map walk it replaces had, and it is sufficient -
     * a missed addition is picked up by the next pass, and the claim decides in either case.
     */
    Iterator<Long> selectableOffsets() {
        return notInFlight.iterator();
    }

    /**
     * Ground truth's counterpart for tests: how many offsets the index currently believes are unheld. O(n), and
     * never read in production.
     */
    // visible for testing
    int countSelectable() {
        return notInFlight.size();
    }
}
