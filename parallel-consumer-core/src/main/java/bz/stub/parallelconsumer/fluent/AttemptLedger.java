package bz.stub.parallelconsumer.fluent;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * How many times the facade has actually run a route's function for a record - the count the retry limit is measured
 * against (KTD4, R10).
 *
 * <h2>Why the facade keeps its own count</h2>
 * The engine keeps a failure count per record too, and it is <em>not</em> the same number. Every hand-back the facade
 * makes advances the engine's counter as if the function had failed, because failing a record is the only hand-back
 * path the engine has today: a park, a permanent decode failure, and in later units a breaker withhold, a route at
 * its admission limit and an export re-dispatch. None of those is an attempt at the user's work. So the limit is
 * measured against this ledger, which advances only when the function ran, or when a decode failure was classified
 * transient (R12). In this milestone every hand-back the wrapper makes is a real failure, so the two counts agree -
 * which is why the engine's count is read as a cross-check in the tests.
 *
 * <h2>Why it is keyed and cleared the way it is</h2>
 * Keyed by topic-partition first and offset second, so revoking a partition is one map removal rather than a scan:
 * the clear runs on the poll thread inside the rebalance callback, where anything slow delays the whole group's
 * rebalance. The count is per assignment (R10): a rebalance, restart or crash resets it.
 *
 * @see FacadeRebalanceListener
 */
class AttemptLedger {

    private final ConcurrentMap<TopicPartition, ConcurrentMap<Long, Integer>> counts = new ConcurrentHashMap<>();

    /**
     * Count one run of the route's function for this record.
     *
     * @return the number of runs so far, including this one - so the first run answers one
     */
    int advance(ConsumerRecord<?, ?> record) {
        return counts.computeIfAbsent(partitionOf(record), tp -> new ConcurrentHashMap<>())
                .merge(record.offset(), 1, Integer::sum);
    }

    /**
     * @return how many times the function has run for this record, zero when it never has
     */
    int attempts(String topic, int partition, long offset) {
        ConcurrentMap<Long, Integer> forPartition = counts.get(new TopicPartition(topic, partition));
        if (forPartition == null) {
            return 0;
        }
        Integer attempts = forPartition.get(offset);
        return attempts == null ? 0 : attempts;
    }

    int attempts(ConsumerRecord<?, ?> record) {
        return attempts(record.topic(), record.partition(), record.offset());
    }

    /**
     * Forget this record: it reached a terminal outcome that completes it, so nothing will ask again.
     * <p>
     * A <em>parked</em> record deliberately keeps its entry - the parked view reports its attempt count (R28).
     */
    void forget(ConsumerRecord<?, ?> record) {
        ConcurrentMap<Long, Integer> forPartition = counts.get(partitionOf(record));
        if (forPartition != null) {
            forPartition.remove(record.offset());
        }
    }

    /**
     * Forget every record of these partitions, because they are no longer ours (R10's per-assignment rule). One
     * removal per partition, no scan: this runs on the poll thread inside a rebalance callback.
     */
    void forget(Collection<TopicPartition> partitions) {
        for (TopicPartition partition : partitions) {
            counts.remove(partition);
        }
    }

    /**
     * Visible for tests: how many records this ledger is counting, across every partition.
     */
    int size() {
        int total = 0;
        for (Map<Long, Integer> forPartition : counts.values()) {
            total += forPartition.size();
        }
        return total;
    }

    /**
     * Visible for tests: whether this partition is tracked at all, which is what a revoke removes.
     */
    boolean tracks(TopicPartition partition) {
        return counts.containsKey(partition);
    }

    private static TopicPartition partitionOf(ConsumerRecord<?, ?> record) {
        return new TopicPartition(record.topic(), record.partition());
    }

    @Override
    public String toString() {
        return "AttemptLedger(partitions=" + counts.size() + ", records=" + size() + ")";
    }
}
