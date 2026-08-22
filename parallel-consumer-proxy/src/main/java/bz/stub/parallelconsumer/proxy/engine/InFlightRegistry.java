package bz.stub.parallelconsumer.proxy.engine;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.PollContextInternal;
import bz.stub.parallelconsumer.state.WorkContainer;
import com.github.bsideup.jabel.Desugar;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Predicate;

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
 * {@link #claim(String, InFlight)}, whose callers ({@link ProxyProcessor#report} and every liveness return
 * path) mailbox-add unconditionally after claiming, and {@link #unregister(String)}, whose single caller is the
 * dispatch path's exception handler - where core's own {@code runUserFunction} catch block performs the mailbox
 * add. {@link #register} has the same duty for what it displaces, which is why it hands the displaced entry
 * back rather than dropping it.
 * <p>
 * Keyed by {@code record_id} alone, not the full {@code (record_id, epoch)} token: core guarantees at most one
 * delivery of a record is in flight at a time, so the epoch is not needed for uniqueness - it is the fencing
 * comparison ({@link InFlight#capturedEpoch()} versus the epoch a report echoes), per KTD8.
 *
 * @author Antony Stubbs
 */
@Slf4j
class InFlightRegistry {

    /**
     * One record out at the client.
     *
     * @param wc            the container whose completion hooks the report resolution will call
     * @param context       the dispatch-time context, held because the mailbox add needs it -
     *                      {@code addToMailbox(context, wc)}, the vert.x hook pattern applied per record
     * @param capturedEpoch {@link WorkContainer#getDeliveryCount()} <b>captured at dispatch</b>, never re-read
     *                      (KTD8) - the value a live report must echo, and the value every return path passes
     *                      to {@code markAbandoned}
     * @param leaseDeadline the liveness lease this delivery was dispatched with ({@link Instant#MAX} on a
     *                      session that did not negotiate {@code heartbeat}). Every heartbeat extends the
     *                      session's own deadline rather than this one, so no entry is ever rewritten -
     *                      {@link LivenessLease#hasExpired} compares against the later of the two
     */
    @Desugar
    record InFlight(WorkContainer<byte[], byte[]> wc,
                    PollContextInternal<byte[], byte[]> context,
                    long capturedEpoch,
                    Instant leaseDeadline) {
    }

    /**
     * The seam that lets a test stop one thread inside the registry and run another past it - the plan's
     * "force the overlap with a latch, do not approximate it with sleeps", which needs a hook in production
     * code because the interleavings it proves are between the dispatcher thread and a transport report
     * thread. The natural spot is here rather than {@code PCModule}: {@code ExternalEngine} has no
     * module-taking constructor, and both halves of the race this unit must prove - a claim losing to a
     * redelivery, and a registration meeting an entry a rebalance stranded - happen inside these two methods.
     * Production wiring never sets one, so the default is unobservable.
     */
    interface Hook {
        Hook NO_OP = new Hook() {
        };

        default void beforeRegister(String recordId) {
        }

        default void beforeClaim(String recordId) {
        }
    }

    private final ConcurrentMap<String, InFlight> byRecordId = new ConcurrentHashMap<>();

    /**
     * Whether a container's partition generation has moved on - {@code WorkManager#checkIfWorkIsStale}, passed
     * in rather than reached for, so this class stays a map and the engine keeps its one route to core.
     */
    private final Predicate<WorkContainer<byte[], byte[]>> stale;

    private final Hook hook;

    InFlightRegistry(Predicate<WorkContainer<byte[], byte[]>> stale) {
        this(stale, Hook.NO_OP);
    }

    InFlightRegistry(Predicate<WorkContainer<byte[], byte[]>> stale, Hook hook) {
        this.stale = stale;
        this.hook = hook;
    }

    /**
     * Registers a delivery at dispatch.
     * <p>
     * <b>A collision is not always the bug it looks like.</b> Core guarantees one delivery of a record at a
     * time, so two <em>live</em> registrations of one record id would be an engine bookkeeping bug - and that
     * case still throws, loudly. But a rebalance strands entries: a dispatched-unreported record whose
     * partition is revoked and reassigned is re-polled into a <b>fresh</b> {@code WorkContainer}, and its
     * dispatch collides with the entry the old generation left behind. Throwing there escapes into core's
     * user-function catch block, which error-retries the record forever - a blocked shard under KEY or
     * PARTITION ordering, from a record nothing is wrong with. So a collision whose registered entry
     * <em>cannot</em> be live is replaced and warned about, and the displaced entry is handed back for the
     * caller to return to scheduling; the leak discipline is unchanged, only its owner moves.
     * <p>
     * "Cannot be live" is two tests: a different container instance for this record id (identity, deliberately
     * not {@code equals} - {@code WorkContainer} equality is topic/partition/offset, so the redelivery of a
     * stranded record is <em>equal to</em> the entry it collides with and would hide exactly this case), or a
     * container whose partition generation has moved on.
     *
     * @return the displaced entry, which the caller must return to scheduling; empty on an ordinary
     *         registration
     */
    Optional<InFlight> register(String recordId, InFlight entry) {
        hook.beforeRegister(recordId);
        // DO NOT replace this loop with ConcurrentHashMap.compute. Proposed as exactly equivalent during
        // the 2026-08-17 simplification pass, and it is not: compute would run the staleness predicate -
        // a call into core's WorkManager - and log, both under the map's bin lock, where this loop is
        // lock-free and structurally cannot deadlock against core's own locks. No test in the suite
        // fails on the difference. See parallel-consumer-proxy/docs/simplifications-declined.md.
        while (true) {
            var previous = byRecordId.putIfAbsent(recordId, entry);
            if (previous == null) {
                return Optional.empty();
            }
            if (previous.wc() == entry.wc() && !stale.test(previous.wc())) {
                throw new IllegalStateException(msg(
                        "Two deliveries of record {} in flight at once: registered epoch {}, arriving epoch {} - "
                                + "core guarantees one delivery per record, so this is an engine bookkeeping bug",
                        recordId, previous.capturedEpoch(), entry.capturedEpoch()));
            }
            if (byRecordId.replace(recordId, previous, entry)) {
                log.warn("Replacing a stranded registration for {}: its delivery (epoch {}) can no longer be "
                                + "live, and this dispatch (epoch {}) is the record's redelivery. The stranded "
                                + "delivery is being returned to scheduling",
                        recordId, previous.capturedEpoch(), entry.capturedEpoch());
                return Optional.of(previous);
            }
            // another thread changed the entry between the two operations; re-read and decide again
        }
    }

    /** The live entry for a record, if one is out - a read, disturbing nothing. */
    Optional<InFlight> peek(String recordId) {
        return Optional.ofNullable(byRecordId.get(recordId));
    }

    /** Every entry currently out, as one consistent picture for a sweep or a manifest reconciliation. */
    Map<String, InFlight> snapshot() {
        return Map.copyOf(byRecordId);
    }

    /**
     * Atomically removes and returns the entry the caller {@link #peek}ed - conditional on that exact entry
     * still being the registered one, so two report threads racing on one token resolve to one winner, and a
     * delivery superseded between peek and claim is left untouched (its entry no longer matches). Empty when
     * this call lost that race; the caller has already fenced the epoch against the peeked entry (KTD8).
     */
    Optional<InFlight> claim(String recordId, InFlight peeked) {
        hook.beforeClaim(recordId);
        boolean won = byRecordId.remove(recordId, peeked);
        return won ? Optional.of(peeked) : Optional.empty();
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
