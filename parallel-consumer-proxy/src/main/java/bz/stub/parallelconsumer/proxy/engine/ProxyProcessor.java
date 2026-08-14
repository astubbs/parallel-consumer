package bz.stub.parallelconsumer.proxy.engine;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.ParallelConsumerOptions.ProcessingOrder;
import bz.stub.parallelconsumer.PollContextInternal;
import bz.stub.parallelconsumer.internal.ExternalEngine;
import bz.stub.parallelconsumer.proxy.protocol.v1.ProduceRecord;
import bz.stub.parallelconsumer.proxy.protocol.v1.Report;
import bz.stub.parallelconsumer.proxy.protocol.v1.Token;
import bz.stub.parallelconsumer.state.ShardKey;
import com.github.bsideup.jabel.Desugar;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static bz.stub.parallelconsumer.internal.utils.StringUtils.msg;

/**
 * The proxy's engine: an {@link ExternalEngine} whose "user function" hands records to a connected client over
 * the {@link DispatchSink} and completes them later, per record, when the client's {@link Report}s arrive.
 * <p>
 * <b>Backpressure is the engine's in-flight target and nothing else (KTD6):</b> the control loop recomputes
 * {@code target - numberRecordsOutForProcessing} every pass, where the target is
 * {@code maxConcurrency * batchSize} - and {@code batchSize} is pinned to 1 here (KTD10), so the ceiling is
 * exactly max concurrency (R40, R49). This class adds no counter of its own anywhere; every quantity is derived
 * from state core already tracks.
 * <p>
 * <b>Completion is the vert.x hook pattern, applied per record:</b> the wrapped user function returns the
 * in-flight registration as a sentinel, which {@link #isAsyncFutureWork} recognises so {@link ExternalEngine}
 * withholds the mailbox entry; {@link #report} later calls the container's success/failure hook and then
 * {@code addToMailbox} - the one and only completion path, so ordering and retry are core's, untouched (R2, R4).
 * <p>
 * <b>Fencing is Kafka's own exactly-once model, borrowed (KTD8):</b> each dispatch carries the delivery count
 * <em>captured at dispatch</em> as its epoch; a report is applied only when its echoed epoch names the delivery
 * that is actually outstanding, and a stale echo is discarded without touching the live delivery.
 * <p>
 * The record types are fixed at {@code byte[]}: the proxy never deserializes - a record's key and value cross
 * the wire as the bytes Kafka held, and deserialization happens in the worker's own language. A generic engine
 * here would need per-type serializers for state that is only ever bytes on both sides.
 * <p>
 * A success report's produce payload is produced here with the engine's own producer (R6, landed by the
 * spike U29): the worker's only sanctioned Kafka output (KTD7), sent and acked before the input record's
 * offset may become eligible to commit.
 * <p>
 * <b>Every way a record can come back without a verdict funnels through one method</b>
 * ({@code returnToScheduling}): lease expiry, reconnect-window expiry, a manifest that does not name it, a
 * reported worker death, and a partition revocation. All of them claim the entry and then call
 * {@code markAbandoned} with the epoch <em>captured at dispatch</em> (KTD8) followed by a mailbox add, so the
 * record returns with its attempt count unchanged and the in-flight accounting nets out exactly. This engine
 * is core's first production caller of that superseded-delivery machinery.
 * <p>
 * <b>While no client is connected the engine keeps selecting work, and that is bounded rather than stopped:</b>
 * records dispatched into a stream that is not there stay registered, so the in-flight ceiling fills and core's
 * own backpressure halts selection. They are returned as unmanifested on the next reconnect, or by the
 * reconnect window's expiry - never resolved by an invented verdict.
 * <p>
 * Not this unit's scope, deliberately: terminal failure (U9), drain and the {@code Released} outcome (U10 per
 * KTD17).
 *
 * @author Antony Stubbs
 * @see DispatchSink
 */
@Slf4j
public class ProxyProcessor extends ExternalEngine<byte[], byte[]> {

    /**
     * @see bz.stub.parallelconsumer.state.WorkContainer#getWorkType()
     */
    private static final String PROXY_TYPE = "proxy.x-type";

    /**
     * Default coalescing window: the backstop before which an under-cap wave must emit. Generous on purpose -
     * the control-loop-end flush is the primary emission path and runs every few milliseconds, so this only
     * governs emission when the control loop has stalled.
     */
    public static final Duration DEFAULT_COALESCING_WINDOW = Duration.ofMillis(100);

    /** How {@link #report(Report)} disposed of a report - the transport's material for a protocol error reply. */
    public enum ReportResult {
        /** The outcome was applied: the record completed and returns to core for commit accounting. */
        APPLIED_SUCCESS,
        /** The outcome was applied: the failure entered core's retry scheduling, attempt count incremented. */
        APPLIED_FAILURE,
        /**
         * The token's epoch names a superseded delivery of a record that is live at a newer one - the report is
         * discarded and the live delivery continues unaffected (KTD8).
         */
        SUPERSEDED_EPOCH,
        /** The token names no record currently in flight - late duplicate or fabrication; nothing is disturbed. */
        UNKNOWN_TOKEN,
        /** The report is structurally unusable: no token, an empty record id, or no outcome. */
        MALFORMED,
        /**
         * The outcome is in the frozen schema but this engine does not answer it yet ({@code Terminal} until
         * U9, {@code Released} until the drain unit U10) - the record is left in flight untouched, so the
         * liveness machinery reclaims it rather than a guessed verdict resolving it.
         */
        UNSUPPORTED_OUTCOME
    }

    /**
     * What a reconnect manifest came to: the {@code Drop} orders the transport must send, and the counts the
     * session log reports. The engine has already applied the returns by the time this is handed back.
     *
     * @param drops        tokens the client must drop - superseded deliveries its workers still hold (R43)
     * @param kept         how many held records the manifest named at their current delivery
     * @param returned     how many held records the manifest did not name, and were returned to scheduling
     * @param unissued     tokens naming a record the proxy holds nothing for - rejected, nothing disturbed
     */
    @Desugar
    public record ManifestOutcome(List<Token> drops, int kept, int returned, List<Token> unissued) {
    }

    private final InFlightRegistry inFlight;
    private final DispatchWaveAssembler waveAssembler;
    private final LivenessLease lease;
    private final ReconnectWindow reconnectWindow;

    public ProxyProcessor(ParallelConsumerOptions<byte[], byte[]> options, DispatchSink sink) {
        this(options, sink, DEFAULT_COALESCING_WINDOW);
    }

    public ProxyProcessor(ParallelConsumerOptions<byte[], byte[]> options, DispatchSink sink,
                          Duration coalescingWindow) {
        this(options, sink, coalescingWindow, LivenessSettings.defaults());
    }

    public ProxyProcessor(ParallelConsumerOptions<byte[], byte[]> options, DispatchSink sink,
                          Duration coalescingWindow, LivenessSettings livenessSettings) {
        this(options, sink, coalescingWindow, livenessSettings, InFlightRegistry.Hook.NO_OP);
    }

    /**
     * @param hook the registry's test seam - {@link InFlightRegistry.Hook#NO_OP} in production, a latch in the
     *             tests that must force an interleaving rather than approximate it with sleeps
     */
    ProxyProcessor(ParallelConsumerOptions<byte[], byte[]> options, DispatchSink sink,
                   Duration coalescingWindow, LivenessSettings livenessSettings, InFlightRegistry.Hook hook) {
        super(options);
        if (options.getBatchSize() > 1) {
            // KTD10: wire efficiency comes from coalescing waves, not from core's batching - a larger batch
            // size would multiply the in-flight ceiling (maxConcurrency * batchSize) rather than fill waves
            throw new IllegalArgumentException(msg(
                    "The proxy requires batchSize 1 (got {}): records are coalesced into dispatch waves for wire "
                            + "efficiency instead, and a larger batch size would multiply the in-flight ceiling",
                    options.getBatchSize()));
        }
        boolean orderingRestricted = options.getOrdering() != ProcessingOrder.UNORDERED;
        // the wave can never hold more than the engine allows out for processing, so the in-flight target IS
        // the size cap - derived from options, not accumulated (KTD6)
        int sizeCap = options.getTargetAmountOfRecordsInFlight();
        this.waveAssembler = new DispatchWaveAssembler(orderingRestricted, sizeCap, coalescingWindow,
                sink::dispatch);
        // read through the field on every call rather than binding a method reference to today's instance
        this.inFlight = new InFlightRegistry(wc -> wm.checkIfWorkIsStale(wc), hook);
        this.lease = new LivenessLease(livenessSettings);
        this.reconnectWindow = new ReconnectWindow(livenessSettings);
    }

    /**
     * Starts the engine: records selected by the control loop flow out through the {@link DispatchSink} from
     * here on. The control-loop-end hook is what keeps a lone record from waiting out the coalescing window -
     * everything offered during a loop pass is flushed by that pass's end.
     * <p>
     * The liveness sweep rides the same hook, deliberately: it needs no thread and no timer of its own, it
     * runs on the control thread that owns the mailbox, and it is paced by the loop rather than by a clock
     * this class would then have to stop on close. A stalled control loop delays reclamation - which is
     * sound, because a stalled control loop is not scheduling the reclaimed records either.
     */
    public void start() {
        addLoopEndCallBack(waveAssembler::flush);
        addLoopEndCallBack(this::reclaimExpired);
        supervisorLoop(this::dispatchRecords, ignore -> log.trace("Void callback applied."));
    }

    /**
     * The wrapped user function: runs on the engine's single dispatcher thread once per selected record
     * (batchSize is pinned to 1). Registers the delivery, hands it to the wave assembler, and returns the
     * registration as the async-work sentinel so core withholds the completion until {@link #report} supplies it.
     */
    private List<Object> dispatchRecords(PollContextInternal<byte[], byte[]> context) {
        var sentinels = new ArrayList<>(1);
        context.streamWorkContainers().forEach(wc -> {
            wc.setWorkType(PROXY_TYPE);

            // KTD8's structural discipline: the epoch is the delivery count captured AT DISPATCH, never
            // re-read at return time - reading it late would relabel a stale return as live
            long capturedEpoch = wc.getDeliveryCount();

            var dispatch = RecordCodec.toDispatchRecord(wc, capturedEpoch);
            var recordId = dispatch.getToken().getRecordId();

            // register BEFORE offering: once a wave is emitted, a report can race back on the transport
            // thread, and it must find the registration
            var entry = new InFlightRegistry.InFlight(wc, context, capturedEpoch, lease.deadlineAtDispatch());
            inFlight.register(recordId, entry).ifPresent(displaced ->
                    // a rebalance stranded the previous registration; the replacement is this dispatch, and
                    // the leak discipline says whoever removes an entry hands it back to core
                    returnDisplaced(recordId, displaced));
            try {
                waveAssembler.offer(ShardKey.of(wc, options.getOrdering()), dispatch);
            } catch (RuntimeException e) {
                // the record never left the engine: back the registration out and rethrow into core's
                // user-function catch block, which owns the failure hook and the mailbox add for this path
                inFlight.unregister(recordId);
                throw e;
            }
            sentinels.add(new AsyncDispatchSentinel(recordId));
        });
        return sentinels;
    }

    /**
     * The return half of the engine&harr;transport boundary: the transport calls this once per {@link Report}
     * it receives, from its own thread. Applies the worker's outcome to the delivery the token names, or
     * discards the report - and says which, so the transport can answer the client truthfully.
     */
    public ReportResult report(Report report) {
        if (!report.hasToken() || report.getToken().getRecordId().isEmpty()
                || report.getOutcomeCase() == Report.OutcomeCase.OUTCOME_NOT_SET) {
            log.warn("Discarding malformed report (token present: {}, outcome case: {})",
                    report.hasToken(), report.getOutcomeCase());
            return ReportResult.MALFORMED;
        }
        var token = report.getToken();

        var live = inFlight.peek(token.getRecordId());
        if (live.isEmpty()) {
            log.debug("Discarding report for unknown token {} - nothing in flight for it", token.getRecordId());
            return ReportResult.UNKNOWN_TOKEN;
        }
        if (live.get().capturedEpoch() != token.getEpoch()) {
            log.debug("Discarding superseded report for {}: names epoch {}, live delivery is epoch {}",
                    token.getRecordId(), token.getEpoch(), live.get().capturedEpoch());
            return ReportResult.SUPERSEDED_EPOCH;
        }
        if (report.getOutcomeCase() == Report.OutcomeCase.TERMINAL
                || report.getOutcomeCase() == Report.OutcomeCase.RELEASED) {
            // frozen-schema outcomes this engine does not answer yet: Terminal is U9's, Released is U8's
            // shutdown path. Peek-only, deliberately - the record stays in flight for the liveness machinery
            // rather than being resolved by a verdict the engine would have to invent
            log.warn("Discarding {} report for {}: this engine does not implement the outcome yet",
                    report.getOutcomeCase(), token.getRecordId());
            return ReportResult.UNSUPPORTED_OUTCOME;
        }

        var claimed = inFlight.claim(token.getRecordId(), live.get());
        if (claimed.isEmpty()) {
            // another report thread won the race between peek and claim; for this one the token is now dead
            return ReportResult.UNKNOWN_TOKEN;
        }
        var entry = claimed.get();
        var wc = entry.wc();

        // the per-record vert.x hook pattern: verdict hook, then the mailbox add - unconditionally, because a
        // claimed entry that never reaches the mailbox is exactly the counter drift the leak check exists for
        ReportResult result;
        if (report.getOutcomeCase() == Report.OutcomeCase.SUCCESS) {
            try {
                // R6: the produce payload is the worker's only sanctioned Kafka output (KTD7), and it is
                // produced BEFORE the success hook, so the input offset cannot become eligible to commit
                // ahead of the output existing - the at-least-once ordering
                producePayload(report.getSuccess());
                wc.onUserFunctionSuccess();
                result = ReportResult.APPLIED_SUCCESS;
            } catch (RuntimeException produceFailure) {
                // the worker succeeded but its output did not: applied as a failure so the record returns to
                // retry scheduling. The redelivered worker may produce duplicates - the at-least-once
                // contract R6 states, not a defect
                log.warn("Produce payload of a success report failed; applying the report as a failure so the "
                        + "record is redelivered", produceFailure);
                wc.onUserFunctionFailure(produceFailure);
                result = ReportResult.APPLIED_FAILURE;
            }
        } else {
            wc.onUserFunctionFailure(RecordCodec.toFailureCause(report.getFailure()));
            result = ReportResult.APPLIED_FAILURE;
        }
        addToMailbox(entry.context(), wc);
        return result;
    }

    /**
     * A connection-level {@code Heartbeat}: extends the lease of every record this session has out, at once
     * (R46). It says the client is alive and nothing else - no record is named, and none needs to be.
     */
    public void heartbeat() {
        lease.heartbeat();
    }

    /**
     * The connection dropped. The records stay in flight and the reconnect window starts governing them
     * (R42); leases suspend, because no heartbeat can arrive while there is no connection, and two clocks
     * over one record is precisely what R46 forbids.
     */
    public void onConnectionLost() {
        lease.suspend();
        reconnectWindow.open();
    }

    /**
     * A client reconnected within the window and opened with the tokens its live workers still hold (R43).
     * Reconciles three ways, applies the returns, and hands back the {@code Drop} orders for the transport to
     * send - the engine sends nothing itself.
     */
    public ManifestOutcome reconcileManifest(List<Token> manifestTokens) {
        var held = inFlight.snapshot();
        var heldEpochs = new HashMap<String, Long>();
        held.forEach((recordId, entry) -> heldEpochs.put(recordId, entry.capturedEpoch()));

        var reconciliation = ManifestReconciler.reconcile(heldEpochs, manifestTokens);

        int returned = 0;
        for (String recordId : reconciliation.unmanifested()) {
            if (returnToScheduling(recordId, held.get(recordId), "no live worker holds it after the reconnect")) {
                returned++;
            }
        }
        // the window stops governing and the lease takes over again, for exactly the records the manifest
        // kept - the reconnect handshake is itself the first heartbeat of the resumed session (R46)
        reconnectWindow.close();
        lease.resume();

        if (!reconciliation.unissued().isEmpty()) {
            log.warn("Rejecting {} manifest token(s) naming records this proxy holds nothing for; nothing held "
                    + "was disturbed", reconciliation.unissued().size());
        }
        log.info("Reconnect manifest reconciled: {} kept in flight, {} ordered dropped, {} returned to "
                        + "scheduling, {} rejected", reconciliation.kept().size(), reconciliation.drops().size(),
                returned, reconciliation.unissued().size());
        return new ManifestOutcome(reconciliation.drops(), reconciliation.kept().size(), returned,
                reconciliation.unissued());
    }

    /**
     * The client reports one of its workers died, naming the tokens it held (R45). Those records return to
     * scheduling immediately, without waiting for the window or the lease - this is the primary reclaim path,
     * because the client library can observe its own worker exiting, and the two clocks are backstops for the
     * cases where it cannot.
     * <p>
     * A token naming a delivery that has already ended is ignored rather than acted on: the record it names
     * may already be out at a live worker, and the same fencing that discards a stale report discards a stale
     * death notice.
     *
     * @return how many records this returned to scheduling
     */
    public int onWorkerDied(List<Token> tokens) {
        int returned = 0;
        for (Token token : tokens) {
            var entry = inFlight.peek(token.getRecordId());
            if (entry.isEmpty()) {
                log.debug("Worker-death token {} names no record in flight; ignoring", token.getRecordId());
                continue;
            }
            if (entry.get().capturedEpoch() != token.getEpoch()) {
                log.debug("Worker-death token {} names epoch {}, live delivery is epoch {}; ignoring the stale "
                        + "notice", token.getRecordId(), token.getEpoch(), entry.get().capturedEpoch());
                continue;
            }
            if (returnToScheduling(token.getRecordId(), entry.get(), "the worker holding it died")) {
                returned++;
            }
        }
        log.info("Worker death reported over {} token(s): {} record(s) returned to scheduling with their "
                + "attempt counts unchanged", tokens.size(), returned);
        return returned;
    }

    /**
     * The liveness sweep, run at the end of every control loop pass. Two clocks, never both over one record:
     * while the reconnect window is holding, it alone governs; otherwise the lease does.
     */
    private void reclaimExpired() {
        if (reconnectWindow.expireIfDue()) {
            var held = inFlight.snapshot();
            int returned = 0;
            for (var entry : held.entrySet()) {
                if (returnToScheduling(entry.getKey(), entry.getValue(),
                        "the reconnect window expired with no reconnect")) {
                    returned++;
                }
            }
            log.warn("Reconnect window expired with no reconnect: returned {} record(s) to scheduling with "
                    + "their attempt counts unchanged (R44)", returned);
            return;
        }
        if (reconnectWindow.isHolding() || !lease.enabled()) {
            return;
        }
        for (var entry : inFlight.snapshot().entrySet()) {
            if (lease.hasExpired(entry.getValue().leaseDeadline())) {
                log.warn("Lease expired for {}: the client stopped heartbeating, so the record returns to "
                        + "scheduling with its attempt count unchanged (R46)", entry.getKey());
                returnToScheduling(entry.getKey(), entry.getValue(), "its liveness lease expired");
            }
        }
    }

    /**
     * Partitions revoked: nothing this engine holds for them can ever be committed by this consumer again, so
     * their registrations are returned now rather than left for a redelivery that may never come. Without
     * this, a revoked-and-never-reassigned record's entry outlives every reclaim path - its lease is alive
     * while the client heartbeats, and no redelivery arrives to displace it - and
     * {@code numberRecordsOutForProcessing} keeps its increment forever, which is the silent-stall signature.
     * <p>
     * The sweep cannot be the whole answer, which is why {@link InFlightRegistry#register} also replaces a
     * stranded entry: a dispatch already in progress on the dispatcher thread registers <em>after</em> this
     * sweep has run, and lands an entry the sweep has no chance to see.
     */
    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        super.onPartitionsRevoked(partitions);
        returnRegistrationsFor(partitions, "its partition was revoked");
    }

    @Override
    public void onPartitionsLost(Collection<TopicPartition> partitions) {
        super.onPartitionsLost(partitions);
        returnRegistrationsFor(partitions, "its partition was lost");
    }

    private void returnRegistrationsFor(Collection<TopicPartition> partitions, String reason) {
        for (var entry : inFlight.snapshot().entrySet()) {
            if (partitions.contains(entry.getValue().wc().getTopicPartition())) {
                returnToScheduling(entry.getKey(), entry.getValue(), reason);
            }
        }
    }

    /**
     * The one way a record comes back without a verdict: claim the entry, mark the delivery abandoned at the
     * epoch <b>captured at dispatch</b> (KTD8 - reading the delivery count here would relabel a stale return
     * as live), then hand the container to the mailbox, which is core's only sanctioned route back onto the
     * control thread. Core does the rest: an abandonment on a live delivery returns the record with its
     * attempt count untouched, one on a delivery that has already ended is discarded, and one on a revoked
     * partition nets the in-flight accounting out without rescheduling.
     *
     * @return false when another thread claimed the entry first - the return has already happened, or the
     *         delivery it named has ended
     */
    private boolean returnToScheduling(String recordId, InFlightRegistry.InFlight entry, String reason) {
        if (entry == null) {
            return false;
        }
        var claimed = inFlight.claim(recordId, entry);
        if (claimed.isEmpty()) {
            log.debug("Not returning {}: its entry was claimed by another path first", recordId);
            return false;
        }
        log.debug("Returning {} (epoch {}) to scheduling: {}", recordId, entry.capturedEpoch(), reason);
        returnDisplaced(recordId, claimed.get());
        return true;
    }

    /**
     * The mailbox half of a return, for an entry that is already out of the registry - either just claimed,
     * or displaced by {@link InFlightRegistry#register}, which removes it as part of the same atomic
     * replacement. Every removal ends here, or {@code numberRecordsOutForProcessing} drifts.
     */
    private void returnDisplaced(String recordId, InFlightRegistry.InFlight entry) {
        entry.wc().markAbandoned(entry.capturedEpoch());
        addToMailbox(entry.context(), entry.wc());
        log.trace("Record {} handed back to the control thread at epoch {}", recordId, entry.capturedEpoch());
    }

    /**
     * Produces a success report's payload with the engine's own producer, blocking on the acks within the
     * configured send timeout - mirroring core's own poll-and-produce flow, which waits for acks before the
     * record may complete. Runs on the transport's report thread; the send itself is thread-safe, and no
     * produce lock is needed outside the transactional commit mode, which the proxy refuses (KTD7).
     */
    private void producePayload(Report.Success success) {
        if (success.getProduceCount() == 0) {
            return;
        }
        var producerManager = getProducerManager().orElseThrow(() -> new IllegalStateException(
                "a success report carries a produce payload but the engine has no producer"));
        var outbound = new ArrayList<ProducerRecord<byte[], byte[]>>(success.getProduceCount());
        for (ProduceRecord produceRecord : success.getProduceList()) {
            outbound.add(RecordCodec.toProducerRecord(produceRecord));
        }
        var futures = producerManager.produceMessages(outbound);
        for (var future : futures) {
            try {
                future.getRight().get(options.getSendTimeout().toMillis(), TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("interrupted awaiting a produce payload's ack", e);
            } catch (ExecutionException | TimeoutException e) {
                throw new IllegalStateException("a produce payload record was not acknowledged", e);
            }
        }
    }

    /**
     * How many records core currently counts as out for processing - exposed for the standing leak check: after
     * any mix of successes, failures, superseded and rejected reports, this returns to zero. Delegates to
     * {@code WorkManager}'s existing accumulator; the engine keeps no counter of its own (KTD6).
     */
    public int getNumberRecordsOutForProcessing() {
        return wm.getNumberRecordsOutForProcessing();
    }

    @Override
    protected boolean isAsyncFutureWork(List<?> resultsFromUserFunction) {
        // the smallest complete precedent is ReactorProcessor: recognise this engine's own sentinel type
        for (Object result : resultsFromUserFunction) {
            return result instanceof AsyncDispatchSentinel;
        }
        return false;
    }

    /**
     * Overridden at {@code close(DrainingMode)} - the funnel EVERY close route dispatches through: the no-arg
     * {@code close()} and {@code closeDontDrainFirst()} defaults call it directly, and core's
     * {@code close(Duration, DrainingMode)} sets the timeout then virtually calls it. Overriding only the
     * {@code (Duration, DrainingMode)} overload missed the no-arg route entirely. The teardown sits in a
     * {@code finally} because {@code super.close} sneaky-throws {@code TimeoutException} - and a close that
     * times out must still stop the wave-window timer thread, or it leaks.
     */
    @Override
    public void close(DrainingMode drainMode) {
        try {
            super.close(drainMode);
        } finally {
            waveAssembler.close();
        }
    }

    /**
     * What the wrapped user function returns per record: the marker {@link #isAsyncFutureWork} recognises, so
     * {@link ExternalEngine} withholds the mailbox entry until the client's report supplies the verdict.
     * Deliberately not the registry entry itself - core holds these in the work container's future, and a
     * handle that leaked there must not keep the context alive or offer a second path to the container.
     */
    private static final class AsyncDispatchSentinel {
        private final String recordId;

        private AsyncDispatchSentinel(String recordId) {
            this.recordId = recordId;
        }

        @Override
        public String toString() {
            return "AsyncDispatchSentinel(" + recordId + ")";
        }
    }
}
