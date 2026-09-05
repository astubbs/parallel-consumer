package bz.stub.parallelconsumer.integrationTests.utils;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.navigator.ConservationLedger;
import lombok.Builder;
import lombok.Value;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * One child's end-of-run conservation record for one resource, as emitted to the ledger topic by
 * {@link ChildPcMain} and aggregated by {@link FiringLedger#fleetLedger()}. The fleet-conservation decision:
 * fleet identity is aggregated from the BROKER, never from stdout, so a child's counters reach the parent by
 * the same path its firings do.
 * <p>
 * The five monotonic counters are the child's own {@link ConservationLedger} snapshot at close. {@code sharesSummed}
 * is the harness's observation of the share the child was ENTITLED to: the allocator's EXACT per-index
 * entitlement ({@code PartitionShareResourceAllocator#entitledCredits} - what the index's read mints, not the
 * rotation-averaged gauge the view reports), sampled several times per quantum and summed over every quantum
 * index observed - a sum of what the assignment granted, against which {@code minted} (what was actually
 * materialised) is checked; the emission samples once more after the processor closes, so the last minted
 * index is always in the sum. An untagged child emits one record with {@link #UNTAGGED_RESOURCE} and zero
 * counters, so the parent still learns it ended cleanly.
 * <p>
 * Wire form: space-separated {@code key=value} pairs, one record per (instance, resource); the record key is the
 * instance id.
 *
 * @author Antony Stubbs
 */
@Value
@Builder
public class ChildLedgerRecord {

    /** The resource name carried by an untagged child's record. */
    public static final String UNTAGGED_RESOURCE = "-";

    String instanceId;
    String resource;
    long minted;
    long spent;
    long expired;
    long overdraft;
    long overdraftBeyondBurst;
    long outstanding;
    /** Sum over observed quantum indexes of the child's exact per-index entitlement (see class javadoc). */
    double sharesSummed;
    /** How many distinct quantum indexes the share sampler observed. */
    long quantaObserved;
    /** Records the child dispatched over its whole life. */
    long fired;

    /** Whether the child's own identity closes: {@code minted + overdraft == spent + expired + outstanding}. */
    public boolean identityBalances() {
        return minted + overdraft == spent + expired + outstanding;
    }

    public static ChildLedgerRecord of(String instanceId, ConservationLedger ledger, double sharesSummed,
                                       long quantaObserved, long fired) {
        return ChildLedgerRecord.builder()
                .instanceId(instanceId)
                .resource(ledger.getResourceName())
                .minted(ledger.getMinted())
                .spent(ledger.getSpent())
                .expired(ledger.getExpired())
                .overdraft(ledger.getOverdraft())
                .overdraftBeyondBurst(ledger.getOverdraftBeyondBurst())
                .outstanding(ledger.getOutstanding())
                .sharesSummed(sharesSummed)
                .quantaObserved(quantaObserved)
                .fired(fired)
                .build();
    }

    public static ChildLedgerRecord untagged(String instanceId, long fired) {
        return ChildLedgerRecord.builder().instanceId(instanceId).resource(UNTAGGED_RESOURCE).fired(fired).build();
    }

    public String format() {
        return "instance=" + instanceId
                + " resource=" + resource
                + " minted=" + minted
                + " spent=" + spent
                + " expired=" + expired
                + " overdraft=" + overdraft
                + " beyondBurst=" + overdraftBeyondBurst
                + " outstanding=" + outstanding
                + " sharesSummed=" + String.format(Locale.ROOT, "%.6f", sharesSummed)
                + " quantaObserved=" + quantaObserved
                + " fired=" + fired;
    }

    public static ChildLedgerRecord parse(String line) {
        Map<String, String> fields = new HashMap<>();
        for (String pair : line.trim().split(" ", -1)) {
            int eq = pair.indexOf('=');
            if (eq <= 0) {
                throw new IllegalArgumentException("ChildLedgerRecord: malformed field '" + pair + "' in '" + line + "'");
            }
            fields.put(pair.substring(0, eq), pair.substring(eq + 1));
        }
        return ChildLedgerRecord.builder()
                .instanceId(field(fields, "instance", line))
                .resource(field(fields, "resource", line))
                .minted(Long.parseLong(field(fields, "minted", line)))
                .spent(Long.parseLong(field(fields, "spent", line)))
                .expired(Long.parseLong(field(fields, "expired", line)))
                .overdraft(Long.parseLong(field(fields, "overdraft", line)))
                .overdraftBeyondBurst(Long.parseLong(field(fields, "beyondBurst", line)))
                .outstanding(Long.parseLong(field(fields, "outstanding", line)))
                .sharesSummed(Double.parseDouble(field(fields, "sharesSummed", line)))
                .quantaObserved(Long.parseLong(field(fields, "quantaObserved", line)))
                .fired(Long.parseLong(field(fields, "fired", line)))
                .build();
    }

    private static String field(Map<String, String> fields, String key, String line) {
        String value = fields.get(key);
        if (value == null) {
            throw new IllegalArgumentException("ChildLedgerRecord: missing '" + key + "' in '" + line + "'");
        }
        return value;
    }
}
