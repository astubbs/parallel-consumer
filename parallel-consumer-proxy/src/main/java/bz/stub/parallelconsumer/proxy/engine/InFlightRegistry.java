package bz.stub.parallelconsumer.proxy.engine;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.PollContextInternal;
import bz.stub.parallelconsumer.state.WorkContainer;
import com.github.bsideup.jabel.Desugar;

import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static bz.stub.parallelconsumer.internal.utils.StringUtils.msg;

/**
 * The engine's map of records currently out at the client: token {@code record_id} to
 * {@code (WorkContainer, capturedEpoch, leaseDeadline)}.
 * <p>
 * <b>It is a map, not a counter.</b> No quantity is ever derived from its size that {@code WorkManager} already
 * tracks - backpressure is the engine's in-flight target and nothing else (KTD6), and
 * {@code WorkManager#numberRecordsOutForProcessing} is the accumulator whose drift stalls the consumer silently,
 * so this class adds no second one.
 * <p>
 * <b>Leak discipline (the plan's U6 execution note):</b> every path that removes an entry must end in a mailbox
 * add, or {@code numberRecordsOutForProcessing} drifts. Removal is therefore only possible through
 * {@link #claim(String, long)}, whose single caller ({@link ProxyProcessor#report}) mailbox-adds
 * unconditionally after claiming, and {@link #unregister(String)}, whose single caller is the dispatch path's
 * exception handler - where core's own {@code runUserFunction} catch block performs the mailbox add.
 * <p>
 * Keyed by {@code record_id} alone, not the full {@code (record_id, epoch)} token: core guarantees at most one
 * delivery of a record is in flight at a time, so the epoch is not needed for uniqueness - it is the fencing
 * comparison ({@link InFlight#capturedEpoch()} versus the epoch a report echoes), per KTD8.
 *
 * @author Antony Stubbs
 */
class InFlightRegistry {

    /**
     * One record out at the client.
     *
     * @param wc            the container whose completion hooks the report resolution will call
     * @param context       the dispatch-time context, held because the mailbox add needs it -
     *                      {@code addToMailbox(context, wc)}, the vert.x hook pattern applied per record
     * @param capturedEpoch {@link WorkContainer#getDeliveryCount()} <b>captured at dispatch</b>, never re-read
     *                      (KTD8) - the value a live report must echo
     * @param leaseDeadline when this delivery's liveness lease expires. {@link Instant#MAX} in this unit: no
     *                      lease is negotiated yet, and expiry, heartbeats and reclamation are the liveness
     *                      unit's (U8's) - which is also the unit that reconciles an entry stranded by a
     *                      rebalance, the one case where a container can move on while its entry is still here
     */
    @Desugar
    record InFlight(WorkContainer<byte[], byte[]> wc,
                    PollContextInternal<byte[], byte[]> context,
                    long capturedEpoch,
                    Instant leaseDeadline) {
    }

    private final ConcurrentMap<String, InFlight> byRecordId = new ConcurrentHashMap<>();

    /**
     * Registers a delivery at dispatch. A collision means two deliveries of one record are in flight at once,
     * which core's scheduling makes impossible - so it is an invariant violation to fail loudly on, never to
     * paper over by replacement (replacing would orphan the first entry's mailbox add).
     */
    void register(String recordId, InFlight entry) {
        var previous = byRecordId.putIfAbsent(recordId, entry);
        if (previous != null) {
            throw new IllegalStateException(msg(
                    "Two deliveries of record {} in flight at once: registered epoch {}, arriving epoch {} - "
                            + "core guarantees one delivery per record, so this is an engine bookkeeping bug",
                    recordId, previous.capturedEpoch(), entry.capturedEpoch()));
        }
    }

    /** The live entry for a record, if one is out - a read, disturbing nothing. */
    Optional<InFlight> peek(String recordId) {
        return Optional.ofNullable(byRecordId.get(recordId));
    }

    /**
     * Atomically removes and returns the entry, but only when its captured epoch matches the one the report
     * echoed - a stale report must not claim (and thereby end) a live delivery it does not name. Empty when
     * there is no entry or the epoch does not match; the caller distinguishes the two via {@link #peek}.
     */
    Optional<InFlight> claim(String recordId, long reportedEpoch) {
        var entry = byRecordId.get(recordId);
        if (entry == null || entry.capturedEpoch() != reportedEpoch) {
            return Optional.empty(); // no entry, or a stale report - either way the live delivery is untouched
        }
        // conditional on the exact entry, so two report threads racing on one token resolve to one winner
        boolean won = byRecordId.remove(recordId, entry);
        return won ? Optional.of(entry) : Optional.empty();
    }

    /**
     * Backs out a registration whose dispatch failed before the record ever left the engine. Only legal from
     * the dispatch path's own exception handler, where core's user-function catch block owns the mailbox add -
     * see the class javadoc's leak discipline.
     */
    void unregister(String recordId) {
        byRecordId.remove(recordId);
    }
}
