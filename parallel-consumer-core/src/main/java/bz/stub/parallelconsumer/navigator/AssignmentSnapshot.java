package bz.stub.parallelconsumer.navigator;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import static bz.stub.parallelconsumer.internal.utils.StringUtils.msg;

/**
 * One instance's view of the consumer group's assignment, as {@link PartitionShareResourceAllocator} mints
 * from it (the partition-share plan's KTD2): the partitions this instance holds, and how many partitions each
 * subscribed topic has - the numerator and the denominator of the instance's share, captured TOGETHER so a
 * quantum is never minted from a torn numerator over a stale denominator. Immutable, so it can cross from the
 * rebalance callbacks to the control loop through an atomic reference with nothing to guard.
 * <p>
 * <b>The fleet-stable ordinal (KTD1).</b> Every instance must derive the same slot for the same partition
 * without talking to each other, so the remainder rotation in {@link QuantumArithmetic#shareFor} sums to
 * exactly the grant across the fleet (R3). A partition's ordinal is the cumulative partition count of the
 * subscribed topics sorted by name BEFORE its topic, plus its own index - never the bare partition index,
 * which collides across topics (partition 0 of two topics would share slot 0, and the high slots would never
 * be held). Two instances subscribed to the same topic set with the same totals derive identical ordinals;
 * R2 assumes that identical topic set and says a mismatch is undetected on this rung.
 * <p>
 * <b>Resolved or not.</b> The total is known only after a metadata read (KTD3). Until it resolves, or when
 * a read declines, the snapshot is {@link #isResolved() unresolved} and R5 applies: no share, nothing minted.
 */
@EqualsAndHashCode
@ToString
public final class AssignmentSnapshot {

    private static final AssignmentSnapshot NONE = new AssignmentSnapshot(Collections.emptySet(),
            Collections.emptySortedMap(), false);

    private final Set<TopicPartition> heldPartitions;

    /** Each subscribed topic's partition total, in the sorted-name order the ordinals are defined over. */
    private final SortedMap<String, Integer> partitionsPerTopic;

    private final boolean resolved;

    /** The held partitions' ordinals, ascending - the slots this instance mints for. */
    @EqualsAndHashCode.Exclude
    @ToString.Exclude
    private final List<Integer> heldOrdinals;

    @EqualsAndHashCode.Exclude
    @ToString.Exclude
    private final int totalPartitions;

    private AssignmentSnapshot(Set<TopicPartition> heldPartitions, SortedMap<String, Integer> partitionsPerTopic,
                               boolean resolved) {
        this.heldPartitions = Collections.unmodifiableSet(new LinkedHashSet<>(heldPartitions));
        this.partitionsPerTopic = Collections.unmodifiableSortedMap(new TreeMap<>(partitionsPerTopic));
        this.resolved = resolved;
        Map<String, Integer> ordinalBase = new HashMap<>();
        int cumulative = 0;
        for (Map.Entry<String, Integer> topic : this.partitionsPerTopic.entrySet()) {
            ordinalBase.put(topic.getKey(), cumulative);
            cumulative += topic.getValue();
        }
        this.totalPartitions = cumulative;
        List<Integer> ordinals = new ArrayList<>(this.heldPartitions.size());
        if (resolved) {
            for (TopicPartition held : this.heldPartitions) {
                Integer base = ordinalBase.get(held.topic());
                if (base == null) {
                    throw new IllegalArgumentException(msg(
                            "Held partition {} belongs to a topic the resolved totals {} do not name - the "
                                    + "numerator and denominator must describe the same subscription; publish "
                                    + "an unresolved snapshot instead when the total is not known (R5).",
                            held, this.partitionsPerTopic));
                }
                ordinals.add(base + held.partition());
            }
            Collections.sort(ordinals);
        }
        this.heldOrdinals = Collections.unmodifiableList(ordinals);
    }

    /** The state before any assignment was published: nothing held, total unknown - no share (R5). */
    public static AssignmentSnapshot none() {
        return NONE;
    }

    /**
     * An assignment whose partition total is NOT known - the first metadata read has not happened, or it
     * declined (KTD3). R5 treats it as no share: nothing mints until a resolved snapshot is published.
     */
    public static AssignmentSnapshot unresolved(Set<TopicPartition> heldPartitions) {
        return new AssignmentSnapshot(heldPartitions, Collections.emptySortedMap(), false);
    }

    /**
     * An assignment with its denominator: {@code partitionsPerTopic} names every subscribed topic's partition
     * total (each positive), and every held partition must fall inside its topic's total.
     *
     * @throws IllegalArgumentException a held partition's topic is not in the totals, its index is outside
     *                                  its topic's total, or a total is not positive - a torn or stale read
     *                                  must be published as {@link #unresolved}, never minted from (R5)
     */
    public static AssignmentSnapshot resolved(Set<TopicPartition> heldPartitions,
                                              Map<String, Integer> partitionsPerTopic) {
        SortedMap<String, Integer> totals = new TreeMap<>(partitionsPerTopic);
        for (Map.Entry<String, Integer> topic : totals.entrySet()) {
            if (topic.getValue() == null || topic.getValue() <= 0) {
                throw new IllegalArgumentException(msg(
                        "Topic '{}' resolves to {} partitions - a subscribed topic's total must be positive; "
                                + "publish an unresolved snapshot when the metadata read declined (R5).",
                        topic.getKey(), topic.getValue()));
            }
        }
        for (TopicPartition held : heldPartitions) {
            Integer total = totals.get(held.topic());
            if (total != null && (held.partition() < 0 || held.partition() >= total)) {
                throw new IllegalArgumentException(msg(
                        "Held partition {} lies outside its topic's resolved total of {} - the metadata the "
                                + "total was read from predates this assignment; publish an unresolved snapshot "
                                + "until the total is re-read (R5).",
                        held, total));
            }
        }
        return new AssignmentSnapshot(heldPartitions, totals, true);
    }

    /** Whether the subscription's partition total is known - false means R5's no-share state. */
    public boolean isResolved() {
        return resolved;
    }

    public Set<TopicPartition> getHeldPartitions() {
        return heldPartitions;
    }

    /** Every subscribed topic's partition total, sorted by topic name; empty while unresolved. */
    public SortedMap<String, Integer> getPartitionsPerTopic() {
        return partitionsPerTopic;
    }

    /** The subscription's partition total - the slot count the grant divides over; {@code 0} while unresolved. */
    public int getTotalPartitions() {
        return totalPartitions;
    }

    /** The held partitions' fleet-stable ordinals (KTD1), ascending; empty while unresolved. */
    public List<Integer> getHeldOrdinals() {
        return heldOrdinals;
    }

    /**
     * The instance's share of the subscription: held over total, summed across every subscribed topic (R2).
     * {@code 0.0} while unresolved or holding nothing - the rotation-averaged fraction of the rate this
     * instance mints, which is what {@link ResourceAllocator#localRatePerSecond} reports.
     */
    public double fraction() {
        return totalPartitions == 0 ? 0.0 : (double) heldOrdinals.size() / totalPartitions;
    }

    /**
     * This instance's burst budget under {@code contract} (R2): the contract's burst scaled by the fraction,
     * rounded UP - so a holder of at least one partition keeps a budget of at least one credit whenever the
     * contract declares any burst at all, and a contract declaring burst zero stays at zero here exactly as
     * it does under the in-process allocator. {@code 0} while holding nothing or unresolved. Burst is a
     * fleet-wide budget divided by share, never a per-process one - and the round-up means the fleet's summed
     * budgets can exceed the contract's burst by up to one credit per partition-holding instance, the slack
     * R8's bound carries.
     */
    public long burstBudgetFor(ResourceContract contract) {
        return (long) Math.ceil(contract.getBurst() * fraction());
    }
}
