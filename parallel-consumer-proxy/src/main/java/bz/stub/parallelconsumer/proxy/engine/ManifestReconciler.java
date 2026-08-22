package bz.stub.parallelconsumer.proxy.engine;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.proxy.protocol.v1.Token;
import com.github.bsideup.jabel.Desugar;
import lombok.experimental.UtilityClass;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

/**
 * The three-way reconnect reconciliation of R43, as a pure function: what the proxy still holds, versus the
 * delivery tokens a reconnecting client says its live workers still hold.
 * <p>
 * It decides and returns; it moves nothing. {@link ProxyProcessor} owns every effect - the returns, the
 * {@code Drop} orders it hands the transport - so the decision can be tested exhaustively without an engine,
 * and so no reconciliation rule is expressible in two places.
 *
 * @author Antony Stubbs
 * @see ProxyProcessor#reconcileManifest
 */
@UtilityClass
class ManifestReconciler {

    /**
     * What a manifest means for the held set.
     *
     * @param kept         record ids the manifest names at exactly the delivery that is out - untouched, and
     *                     their leases resume
     * @param drops        tokens naming a delivery that has been superseded: the client is ordered to drop
     *                     them, and any report they produce anyway is discarded by the epoch fencing. The
     *                     record's <em>current</em> delivery stays in flight - the manifest accounted for that
     *                     record, and if in fact no worker holds it the lease is the backstop that returns it
     * @param unmanifested record ids the manifest names not at all: no live worker holds them, so they return
     *                     to scheduling with their attempt counts unchanged
     * @param unissued     tokens naming a record the proxy is not holding at all - rejected, disturbing
     *                     nothing held. A client bug or a fabrication; never a reason to touch the held set
     */
    @Desugar
    record Reconciliation(List<String> kept,
                          List<Token> drops,
                          List<String> unmanifested,
                          List<Token> unissued) {
    }

    /**
     * @param heldEpochs the epoch currently out for each held record id - the registry's own view, snapshotted
     *                   by the caller so one consistent picture answers every token
     * @param tokens     the reconnecting client's manifest, verbatim
     */
    static Reconciliation reconcile(Map<String, Long> heldEpochs, List<Token> tokens) {
        var kept = new ArrayList<String>();
        var drops = new ArrayList<Token>();
        var unissued = new ArrayList<Token>();
        var accountedFor = new HashSet<String>();

        for (Token token : tokens) {
            Long heldEpoch = heldEpochs.get(token.getRecordId());
            if (heldEpoch == null) {
                unissued.add(token);
                continue;
            }
            accountedFor.add(token.getRecordId());
            if (heldEpoch == token.getEpoch()) {
                kept.add(token.getRecordId());
            } else {
                drops.add(token);
            }
        }

        var unmanifested = new ArrayList<String>();
        for (String recordId : heldEpochs.keySet()) {
            if (!accountedFor.contains(recordId)) {
                unmanifested.add(recordId);
            }
        }
        return new Reconciliation(kept, drops, unmanifested, unissued);
    }
}
