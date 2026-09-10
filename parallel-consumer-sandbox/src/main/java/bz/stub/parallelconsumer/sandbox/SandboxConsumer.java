package bz.stub.parallelconsumer.sandbox;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.internal.utils.LongPollingMockConsumer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.annotation.InterfaceStability;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The broker: a mock consumer with beginning offsets already recorded, partitions it can hand out, and a
 * per-partition offset counter so that generated records arrive at ascending offsets the way real ones do.
 *
 * <h2>Seed the offsets, THEN assign - in that order, and it is not a style preference</h2>
 * {@code MockConsumer#rebalance} both assigns the partitions and, since kafka-clients 3.7, fires the registered
 * rebalance listener from inside the call - so Parallel Consumer is polling before the method returns. If the
 * beginning offsets are recorded after that, a poll landing in the window throws
 * {@code IllegalStateException: MockConsumer didn't have beginning offset specified, but tried to seek to
 * beginning}, which kills the broker-poll thread; the run then waits out its whole budget for a commit that
 * became impossible in its first second. This class closes the window by construction: the constructor records
 * every partition's beginning offset, and {@link #assignAfterSeeding()} is the only way to assign, so the wrong
 * order is not reachable from here.
 * <p>
 * Recorded in {@code docs/solutions/test-flakiness/assign-the-mock-consumer-after-seeding-its-offsets-2026-08-15.md},
 * which is carried by branches that have not merged - print it with
 * {@code node bin/inflight.mjs docs show <path>}. Note that
 * {@link LongPollingMockConsumer#subscribeWithRebalanceAndAssignment} still has the two calls the other way round,
 * which is why this does not reuse it.
 * <!-- file-refs: N/A - that solution write-up is branch-only and has not merged to master -->
 *
 * @param <K> the key type the engine above sees - raw bytes under the fluent API, the user's own type under the
 *            classic one
 * @param <V> the value type, likewise
 */
@Slf4j
@InterfaceStability.Unstable
public class SandboxConsumer<K, V> extends LongPollingMockConsumer<K, V> {

    /**
     * How long {@link #awaitAllPublishedRecordsPolled} waits before giving up and saying so.
     */
    private static final Duration POLLED_OUT_BUDGET = Duration.ofSeconds(30);

    private final List<TopicPartition> partitions;

    /**
     * The next offset to publish at, per partition. Guarded by this consumer's own monitor, which
     * {@code MockConsumer} already uses for {@code addRecord}, {@code poll} and {@code close}.
     */
    private final Map<TopicPartition, Long> nextOffsets = new LinkedHashMap<>();

    /**
     * Published and handed out, counted so that a bounded run can wait for the second to catch up with the first
     * before it closes. <b>A drain is not a fetch</b>: closing drain-first finishes the work the engine already
     * holds and does not go back for records still sitting here, so a bound that closed the instant its last
     * record was published would leave that record generated, never delivered and never committed - which read
     * as an off-by-one in a commit assertion and would have been a missing parked record in a test of the parked
     * view.
     */
    private final AtomicLong publishedRecords = new AtomicLong();

    private final AtomicLong polledOutRecords = new AtomicLong();

    /**
     * Polls that have RETURNED. Counted because "every record has been handed out" is not the same as "the engine
     * has registered every record": the poll thread registers a batch after {@code poll} returns and before it
     * polls again, so a batch handed out microseconds before the close can still be dropped. One poll that
     * started after the last record was handed out, and finished, proves the batch before it was registered - and
     * two completions are what it takes to be sure of that, since one may have been in flight already.
     */
    private final AtomicLong pollsFinished = new AtomicLong();

    /**
     * Records every partition's beginning offset before anything can be assigned. Nothing is assigned yet - call
     * {@link #assignAfterSeeding()} once the engine above has subscribed and its rebalance listener exists.
     */
    public SandboxConsumer(Collection<String> topics, int partitionsPerTopic) {
        super(OffsetResetStrategy.EARLIEST);
        if (partitionsPerTopic < 1) {
            throw new IllegalArgumentException("A sandbox topic needs at least one partition, not "
                    + partitionsPerTopic);
        }
        List<TopicPartition> assignable = new ArrayList<>();
        for (String topic : topics) {
            for (int partition = 0; partition < partitionsPerTopic; partition++) {
                assignable.add(new TopicPartition(topic, partition));
            }
        }
        this.partitions = Collections.unmodifiableList(assignable);

        Map<TopicPartition, Long> zero = new HashMap<>();
        for (TopicPartition partition : assignable) {
            zero.put(partition, 0L);
            nextOffsets.put(partition, 0L);
        }
        updateBeginningOffsets(zero);
        updateEndOffsets(zero);
    }

    public List<TopicPartition> partitions() {
        return partitions;
    }

    /**
     * Assigns every partition, firing the rebalance listener the engine registered when it subscribed. Safe only
     * because the constructor already recorded the beginning offsets - see this class's own documentation.
     */
    public void assignAfterSeeding() {
        if (subscription().isEmpty()) {
            throw new IllegalStateException("Nothing has subscribed to this sandbox consumer yet, so there is no "
                    + "rebalance listener to assign to and MockConsumer#rebalance refuses a dynamic assignment. "
                    + "Assign only after the instance above has subscribed: the fluent API does it in "
                    + "ClientRuntime#started, and the classic API in ClassicSandbox#startGenerating.");
        }
        // rebalance, not assign, and exactly once. LongPollingMockConsumer overrides assign() to fire the
        // rebalance listener WITHOUT calling super, so a partition assigned that way is never actually held by
        // the consumer and nothing is polled; rebalance() does both halves, and since kafka-clients 3.7 it fires
        // the listener itself. Calling both - belt and braces - notifies the engine twice, and PartitionStateManager
        // answers a second assignment of a live partition with "Could be a state bug ... Please file a GH issue",
        // which is a warning nobody should be reading in a sandbox run.
        rebalanceWithoutAssignment(partitions);
    }

    /**
     * Publishes one record at the next offset for its partition, and moves the partition's end offset up to
     * match - so a consumer-group lag reading in the sandbox is the same shape as one against a broker.
     *
     * @return the offset it was published at, or -1 when this consumer has already been closed - which is how a
     * generator learns that its run is over rather than by an exception it would have to classify
     */
    public synchronized long publish(String topic, int partition, K key, V value) {
        if (closed()) {
            return -1;
        }
        TopicPartition target = new TopicPartition(topic, partition);
        Long next = nextOffsets.get(target);
        if (next == null) {
            throw new IllegalArgumentException("Nothing in this sandbox holds " + target + " - it holds "
                    + partitions + ". A record for a topic no route claims would never be delivered.");
        }
        addRecord(new ConsumerRecord<>(topic, partition, next, key, value));
        publishedRecords.incrementAndGet();
        nextOffsets.put(target, next + 1);
        updateEndOffsets(Collections.singletonMap(target, next + 1));
        return next;
    }

    /**
     * How many records this sandbox has published to each partition. Read by tests that need to know what was
     * offered before asserting on what was consumed.
     */
    public synchronized Map<TopicPartition, Long> publishedCounts() {
        return new LinkedHashMap<>(nextOffsets);
    }

    public long publishedRecords() {
        return publishedRecords.get();
    }

    /**
     * How many records this consumer has handed out of {@code poll}. Not the same as how many have been
     * processed - the engine may still be working through them - but it is the line a drain can reach back over.
     */
    public long polledOutRecords() {
        return polledOutRecords.get();
    }

    // Synchronized to match the method it overrides: MockConsumer guards addRecord, poll, commitSync and close
    // with one monitor, and an unsynchronized override would quietly widen that contract. The wait inside the
    // simulated long poll releases the monitor, so this does not block a generator publishing into it.
    @Override
    public synchronized ConsumerRecords<K, V> poll(Duration timeout) {
        ConsumerRecords<K, V> records = super.poll(timeout);
        polledOutRecords.addAndGet(records.count());
        pollsFinished.incrementAndGet();
        return records;
    }

    /**
     * Waits until everything published has been handed out of {@code poll}, so that a close which drains reaches
     * all of it. Deliberately not synchronized: it sleeps, and {@code addRecord}, {@code poll} and {@code close}
     * all share this object's monitor, so holding it here would wedge the very poll it is waiting for.
     *
     * @return false when the budget ran out with records still unpolled
     */
    public boolean awaitAllPublishedRecordsPolled() {
        long deadline = System.nanoTime() + POLLED_OUT_BUDGET.toNanos();
        if (!awaitUntil(deadline, () -> polledOutRecords.get() >= publishedRecords.get())) {
            log.warn("Gave up after {} waiting for the instance to poll the last {} generated records; the close "
                            + "that follows drains what it holds, so those records will not be processed",
                    POLLED_OUT_BUDGET, publishedRecords.get() - polledOutRecords.get());
            return false;
        }
        long mark = pollsFinished.get();
        if (!awaitUntil(deadline, () -> pollsFinished.get() >= mark + 2)) {
            log.warn("Gave up after {} waiting for the poll loop to come round again; the last records handed out "
                    + "may not have been registered before the close", POLLED_OUT_BUDGET);
            return false;
        }
        return true;
    }

    /**
     * Deliberately not synchronized, and neither is its caller: this sleeps, and {@code addRecord}, {@code poll}
     * and {@code close} all share this object's monitor, so holding it here would wedge the very poll it waits for.
     */
    private boolean awaitUntil(long deadlineNanos, java.util.function.BooleanSupplier condition) {
        while (!condition.getAsBoolean()) {
            if (closed() || System.nanoTime() > deadlineNanos) {
                return closed();
            }
            // Cut the simulated long poll short so the loop comes round now rather than at the end of its
            // timeout. LongPollingMockConsumer overrides wakeup() to end its own sleep WITHOUT calling
            // MockConsumer's, so this does not arm a WakeupException - and without it a bounded run pays two
            // full poll timeouts on its way out, which measured at four seconds a test.
            wakeup();
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return true;
    }
}
