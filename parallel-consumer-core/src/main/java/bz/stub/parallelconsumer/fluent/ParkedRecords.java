package bz.stub.parallelconsumer.fluent;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Which records are parked, how many park cycles each has used, and which partitions are still ours (R27, R28).
 * <p>
 * It is the facade's half of the parked view. The other half is the engine's incomplete offsets, and the two are
 * reconciled in the control-thread snapshot the handle takes (KTD4) - what is here is the per-record detail the
 * engine does not hold: the key as the route read it, the attempt count the ledger measured the limit against, the
 * last failure, why it parked, and since when.
 *
 * <h2>Why it tracks the assignment</h2>
 * A worker can finish after its partition was revoked. Its throw still runs the whole park path, and an entry
 * written then is a <b>phantom</b>: a record this instance no longer owns, listed as parked, counted, and reported
 * to an observer that should never have heard of it. So a park for a partition that is not ours is dropped here,
 * which is the one place every park goes through.
 * <p>
 * <b>Until the first assignment callback arrives, everything is ours.</b> A definition driven directly by a test -
 * or an instance whose first records arrive before its listener has been called - has no assignment view at all,
 * and suppressing every park there would be a silent loss far worse than a phantom. Failing open is the safe
 * direction, and the first {@code onPartitionsAssigned} closes the window for good.
 *
 * @see FacadeRebalanceListener
 */
@Slf4j
class ParkedRecords {

    private final ConcurrentMap<TopicPartition, ConcurrentMap<Long, ParkedRecord>> parked = new ConcurrentHashMap<>();

    /**
     * How many park cycles each record has used - a record being re-attempted after a park delay is <em>not</em>
     * parked, so this deliberately does not live on {@link ParkedRecord}. It is copied onto the entry when the
     * record finally parks (R27).
     */
    private final ConcurrentMap<TopicPartition, ConcurrentMap<Long, Integer>> cycles = new ConcurrentHashMap<>();

    private final Set<TopicPartition> assigned = ConcurrentHashMap.newKeySet();

    private volatile boolean anAssignmentHasBeenSeen;

    // ---------------------------------------------------------------- the assignment

    void onAssigned(Collection<TopicPartition> partitions) {
        assigned.addAll(partitions);
        anAssignmentHasBeenSeen = true;
    }

    /**
     * These partitions are no longer ours, so their parked entries and cycle counts go with them - a parked record
     * belongs to whoever owns its partition now, and the count is per assignment (R10).
     */
    void onNoLongerOurs(Collection<TopicPartition> partitions) {
        assigned.removeAll(partitions);
        for (TopicPartition partition : partitions) {
            parked.remove(partition);
            cycles.remove(partition);
        }
    }

    /**
     * @return whether this partition is one this instance currently owns - true for everything until the first
     * assignment callback has been seen
     */
    boolean isOurs(TopicPartition partition) {
        return !anAssignmentHasBeenSeen || assigned.contains(partition);
    }

    // ---------------------------------------------------------------- parking

    /**
     * Record that this record has parked.
     *
     * @return true when this call is what parked it - false when its partition is not ours, or when it was already
     * parked in this assignment, which is what makes the observer and the counter fire exactly once (R16)
     */
    boolean park(ParkedRecord record) {
        TopicPartition partition = record.topicPartition();
        if (!isOurs(partition)) {
            log.debug("Not recording a park for {} - the partition is no longer assigned to this instance, so the "
                    + "record belongs to its next owner", record);
            return false;
        }
        ParkedRecord previous = parked.computeIfAbsent(partition, tp -> new ConcurrentHashMap<>())
                .putIfAbsent(record.offset(), record);
        return previous == null;
    }

    /**
     * This record reached an outcome that completes it - it succeeded after a resume, or it was exported - so it is
     * neither parked nor cycling any more.
     */
    void forget(ConsumerRecord<?, ?> record) {
        TopicPartition partition = new TopicPartition(record.topic(), record.partition());
        ConcurrentMap<Long, ParkedRecord> parkedHere = parked.get(partition);
        if (parkedHere != null) {
            parkedHere.remove(record.offset());
        }
        ConcurrentMap<Long, Integer> cyclesHere = cycles.get(partition);
        if (cyclesHere != null) {
            cyclesHere.remove(record.offset());
        }
    }

    // ---------------------------------------------------------------- park cycles

    /**
     * @return how many park cycles this record has already used (R27)
     */
    int cyclesUsed(ConsumerRecord<?, ?> record) {
        ConcurrentMap<Long, Integer> forPartition = cycles.get(new TopicPartition(record.topic(),
                record.partition()));
        if (forPartition == null) {
            return 0;
        }
        Integer used = forPartition.get(record.offset());
        return used == null ? 0 : used;
    }

    /**
     * Spend one park cycle on this record: it waits the park delay and is then attempted once more.
     *
     * @return the number of cycles used so far, including this one
     */
    int spendCycle(ConsumerRecord<?, ?> record) {
        return cycles.computeIfAbsent(new TopicPartition(record.topic(), record.partition()),
                        tp -> new ConcurrentHashMap<>())
                .merge(record.offset(), 1, Integer::sum);
    }

    // ---------------------------------------------------------------- the views

    /**
     * Every parked record on these topics - one route's view, which spans every partition (R28).
     */
    List<ParkedRecord> forTopics(Set<String> topics) {
        List<ParkedRecord> found = new ArrayList<>();
        for (Map.Entry<TopicPartition, ConcurrentMap<Long, ParkedRecord>> entry : parked.entrySet()) {
            if (topics.contains(entry.getKey().topic())) {
                found.addAll(entry.getValue().values());
            }
        }
        return Collections.unmodifiableList(found);
    }

    /**
     * Every parked record on this instance, whichever route it belongs to - the roll-up (R28).
     */
    List<ParkedRecord> all() {
        List<ParkedRecord> found = new ArrayList<>();
        for (ConcurrentMap<Long, ParkedRecord> forPartition : parked.values()) {
            found.addAll(forPartition.values());
        }
        return Collections.unmodifiableList(found);
    }

    /**
     * How many records are parked right now - which is not the outcome counter: that one counts park <em>events</em>
     * and never goes down, this one is the size of the set an operator can act on.
     */
    int count() {
        int total = 0;
        for (ConcurrentMap<Long, ParkedRecord> forPartition : parked.values()) {
            total += forPartition.size();
        }
        return total;
    }

    @Override
    public String toString() {
        return "ParkedRecords(partitions=" + parked.size() + ", records=" + count() + ")";
    }
}
