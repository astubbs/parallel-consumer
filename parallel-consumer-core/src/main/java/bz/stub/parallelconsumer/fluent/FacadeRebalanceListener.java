package bz.stub.parallelconsumer.fluent;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;

/**
 * The facade's own rebalance listener, with the user's chained after it (KTD2).
 *
 * <h2>Three properties, and why each is load-bearing</h2>
 * <b>It runs first.</b> The attempt ledger and the parked set are per assignment (R10, R27), so a partition that is
 * no longer ours must stop counting and stop being listed before anything else looks at either. Chaining the user's
 * listener after this one is what lets a user listener see a state that is already consistent - and a user listener
 * that throws still cannot prevent the clear, because it has already happened.
 * <p>
 * <b>It never blocks.</b> This runs on the poll thread inside Kafka's rebalance callback, where every millisecond
 * delays the whole consumer group's rebalance. It takes no lock of its own: the clear is one removal per revoked
 * partition from a concurrent map, never a scan of the records inside it.
 * <p>
 * <b>It never throws.</b> A throw here would surface as a failed rebalance and take the instance with it, for
 * bookkeeping that is only an optimisation - a stale ledger entry costs an attempt count, not correctness. What is
 * <em>not</em> swallowed is the user's own listener: its exception propagates exactly as it does on the classic API,
 * where the engine wraps it and rethrows.
 */
@Slf4j
class FacadeRebalanceListener implements ConsumerRebalanceListener {

    private final AttemptLedger ledger;

    /**
     * The parked set, which learns the current assignment here and nowhere else: it is what keeps a worker
     * finishing after a revoke from writing a phantom entry for a partition somebody else now owns.
     */
    private final ParkedRecords parkedRecords;

    /**
     * The user's own listener, or null when the definition declared none.
     */
    private final ConsumerRebalanceListener usersListener;

    FacadeRebalanceListener(AttemptLedger ledger, ParkedRecords parkedRecords,
                            ConsumerRebalanceListener usersListener) {
        this.ledger = ledger;
        this.parkedRecords = parkedRecords;
        this.usersListener = usersListener;
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        forget(partitions, "revoked");
        if (usersListener != null) {
            usersListener.onPartitionsRevoked(partitions);
        }
    }

    /**
     * A lost partition was taken away without a revoke, so the same clear applies - and it is the case that would
     * otherwise leave a ledger entry behind forever.
     */
    @Override
    public void onPartitionsLost(Collection<TopicPartition> partitions) {
        forget(partitions, "lost");
        if (usersListener != null) {
            usersListener.onPartitionsLost(partitions);
        }
    }

    /**
     * Also clears, which is belt and braces rather than duplication: a partition arriving here must start counting
     * from zero (R10), and this is the one place that holds whether the revoke that should have preceded it
     * happened at all.
     */
    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        forget(partitions, "assigned");
        try {
            // After the clear, never before: a partition arriving must start with no parked entries and then be
            // marked ours, and doing it the other way round would leave a window where a stale entry reads as live.
            parkedRecords.onAssigned(partitions);
        } catch (RuntimeException trackingFailed) {
            log.warn("Could not record the assignment of partitions {} in the fluent API's parked set", partitions,
                    trackingFailed);
        }
        if (usersListener != null) {
            usersListener.onPartitionsAssigned(partitions);
        }
    }

    private void forget(Collection<TopicPartition> partitions, String why) {
        try {
            ledger.forget(partitions);
        } catch (RuntimeException clearFailed) {
            // A stale attempt count is worth a log line, never a failed rebalance.
            log.warn("Could not clear the fluent API's attempt ledger for {} partitions {} - attempt counts for "
                    + "those records may be stale until they complete", why, partitions, clearFailed);
        }
        try {
            parkedRecords.onNoLongerOurs(partitions);
        } catch (RuntimeException clearFailed) {
            // Same reasoning as the ledger: a stale parked entry is a wrong list, never a failed rebalance.
            log.warn("Could not clear the fluent API's parked set for {} partitions {} - they may still be listed "
                    + "as parked on this instance", why, partitions, clearFailed);
        }
    }
}
