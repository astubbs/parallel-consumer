package bz.stub.parallelconsumer.sandbox;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.internal.utils.LongPollingMockConsumer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.MockProducer;
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
     * How long {@link #awaitEveryPublishedRecordCommitted()} waits for the engine to account for what was
     * published. Generous on purpose, and not a guess at how long the work takes: the engine commits on a
     * <em>cadence</em> rather than on demand ({@code ParallelConsumerOptions.DEFAULT_COMMIT_INTERVAL}, five
     * seconds), so a run whose work finished in a millisecond still waits out most of one interval before its
     * offsets appear. Four of those intervals is room for a loaded machine to miss a few without the budget
     * becoming the thing the test measures.
     */
    private static final Duration COMMITTED_BUDGET = Duration.ofSeconds(20);

    /**
     * How often the wait re-reads the commit ledger. Short enough that a bounded run does not sit on a whole
     * poll timeout after its last commit, long enough that twenty seconds of waiting is a few thousand cheap
     * reads rather than a spin.
     */
    private static final long WAIT_INTERVAL_MS = 5;

    private final List<TopicPartition> partitions;

    /**
     * The next offset to publish at, per partition. Guarded by this consumer's own monitor, which
     * {@code MockConsumer} already uses for {@code addRecord}, {@code poll} and {@code close}.
     */
    private final Map<TopicPartition, Long> nextOffsets = new LinkedHashMap<>();

    /**
     * Published and handed out, counted so that a bounded run can wait for the engine to account for all of it
     * before it closes - see {@link #awaitEveryPublishedRecordCommitted()}.
     */
    private final AtomicLong publishedRecords = new AtomicLong();

    private final AtomicLong polledOutRecords = new AtomicLong();

    /**
     * Offsets committed inside a producer transaction never reach this consumer at all - {@code ProducerManager}
     * sends them with {@code sendOffsetsToTransaction} - so under
     * {@code ParallelConsumerOptions.CommitMode#PERIODIC_TRANSACTIONAL_PRODUCER} this consumer's own commit
     * history stays empty however much was committed, and a wait that read only that would time out on a run
     * that had committed everything. Told to us by whoever built the producer.
     */
    private volatile MockProducer<?, ?> transactionalCommitter;

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
     * How many records this consumer has handed out of {@code poll}. <b>Not</b> evidence that any of them were
     * processed, and deliberately not what a bounded run waits for - see
     * {@link #awaitEveryPublishedRecordCommitted()}. It is kept because it separates "the engine never fetched
     * it" from "the engine fetched it and dropped it" when a wait does fail.
     */
    public long polledOutRecords() {
        return polledOutRecords.get();
    }

    /**
     * Also count offsets committed inside this producer's transactions as commits of this consumer's partitions.
     * Call it whenever a producer is handed to the instance: under the transactional commit mode the offsets go
     * that way instead of through the consumer, and nothing else would see them.
     */
    public void alsoCountingCommitsThrough(MockProducer<?, ?> producer) {
        this.transactionalCommitter = producer;
    }

    /**
     * The highest offset committed for each partition, whichever way it was committed - through this consumer, or
     * through a transactional producer named with {@link #alsoCountingCommitsThrough(MockProducer)}.
     * <p>
     * A committed offset is the <em>next</em> offset to be consumed, so a partition whose committed offset equals
     * the number of records published to it has had every one of them completed and committed.
     */
    public Map<TopicPartition, Long> highestCommittedOffsets() {
        Map<TopicPartition, Long> highest = new LinkedHashMap<>();
        for (Map<TopicPartition, OffsetAndMetadata> commit : getCommitHistoryInt()) {
            keepHighest(highest, commit);
        }
        MockProducer<?, ?> transactional = transactionalCommitter;
        if (transactional != null) {
            for (Map<String, Map<TopicPartition, OffsetAndMetadata>> byGroup
                    : transactional.consumerGroupOffsetsHistory()) {
                for (Map<TopicPartition, OffsetAndMetadata> commit : byGroup.values()) {
                    keepHighest(highest, commit);
                }
            }
        }
        return highest;
    }

    /**
     * Waits until the engine has accounted for every record this sandbox published: each partition's committed
     * offset has reached the number of records published to it.
     *
     * <h2>Why the committed offset, and not "everything has been polled"</h2>
     * This used to wait for every published record to have been handed out of {@code poll} plus two further poll
     * cycles, and then close draining first. That was a timing model of the engine rather than a fact about it - a
     * poll hands a batch to the work manager, and dispatch and completion happen afterwards on other threads - and
     * the drain does not close the gap either: {@code AbstractParallelEoSStreamProcessor} transitions to closing
     * once the work manager has nothing <em>awaiting selection</em>, while the worker pool may still hold queued
     * tasks, and the close then clears that queue outright. Records that were polled, dispatched and queued are
     * therefore dropped - never completed, never committed. On a loaded runner that surfaced as one record missing
     * from a collected set, and as a highest committed offset of 40 where 50 was expected (astubbs#504).
     * <p>
     * A committed offset is the one thing the engine publishes that means the work behind it is finished, so it is
     * what this waits for. The wait replaces the timing model; it is not a longer version of it.
     *
     * <h2>A parked record pins its partition here</h2>
     * The committed offset is the highest <em>sequentially succeeded</em> offset plus one
     * ({@code PartitionState#getOffsetHighestSequentialSucceeded}), and a parked record stays incomplete in the
     * offset map for as long as it is parked - it holds no worker and is never retried, but it is not complete
     * either. So it holds its partition's committed offset at its own offset, and offsets above it are carried in
     * the commit's metadata rather than in the number this reads. A <b>bounded</b> run whose records park cannot
     * satisfy this wait and will fail it, naming the partition and the shortfall, rather than hanging. Drive a run
     * that parks with {@link Bound#none()} and close the handle yourself.
     *
     * @throws IllegalStateException when the budget ran out, naming every partition still short and by how much
     */
    public void awaitEveryPublishedRecordCommitted() {
        awaitEveryPublishedRecordCommitted(COMMITTED_BUDGET);
    }

    /**
     * @param budget how long to wait before failing - {@link #COMMITTED_BUDGET} unless a caller has a reason, and
     *               the one caller that does is the test of the refusal itself
     * @see #awaitEveryPublishedRecordCommitted()
     */
    public void awaitEveryPublishedRecordCommitted(Duration budget) {
        long deadline = System.nanoTime() + budget.toNanos();
        Map<TopicPartition, Long> outstanding = uncommittedByPartition();
        while (!outstanding.isEmpty()) {
            if (closed()) {
                // Closed under us: the run is over and nothing else will ever be committed. That is the ordinary
                // end of an unbounded run rather than a fault, so it is not one here either.
                log.debug("The sandbox consumer closed while waiting for commits, still outstanding: {}", outstanding);
                return;
            }
            if (System.nanoTime() > deadline) {
                throw new IllegalStateException(shortfallMessage(outstanding, budget));
            }
            if (polledOutRecords.get() < publishedRecords.get()) {
                // Something published is still sitting here, so cut the simulated long poll short and let the
                // poll loop come round now rather than at the end of its timeout. LongPollingMockConsumer
                // overrides wakeup() to end its own sleep WITHOUT calling MockConsumer's, so this does not arm a
                // WakeupException. Gated, because once everything has been handed out there is nothing left for
                // a poll to fetch and waking it every few milliseconds would only spin the poll thread against
                // this consumer's monitor for as long as the commit cadence takes.
                wakeup();
            }
            try {
                Thread.sleep(WAIT_INTERVAL_MS);
            } catch (InterruptedException e) {
                // The only interrupt that reaches here is RecordGenerator#close asking this generator thread to
                // stop, and that caller closes the instance itself - so returning is right, and the flag is put
                // back for whatever runs next on this thread.
                Thread.currentThread().interrupt();
                log.debug("Interrupted while waiting for commits, still outstanding: {}", outstanding);
                return;
            }
            outstanding = uncommittedByPartition();
        }
        log.debug("Every one of the {} published record(s) has been committed", publishedRecords.get());
    }

    // Synchronized to match the method it overrides: MockConsumer guards addRecord, poll, commitSync and close
    // with one monitor, and an unsynchronized override would quietly widen that contract. The wait inside the
    // simulated long poll releases the monitor, so this does not block a generator publishing into it.
    @Override
    public synchronized ConsumerRecords<K, V> poll(Duration timeout) {
        ConsumerRecords<K, V> records = super.poll(timeout);
        polledOutRecords.addAndGet(records.count());
        return records;
    }

    /**
     * How many records each partition has published but not yet had committed - empty when the engine has
     * accounted for everything this sandbox offered it.
     */
    private Map<TopicPartition, Long> uncommittedByPartition() {
        Map<TopicPartition, Long> committed = highestCommittedOffsets();
        Map<TopicPartition, Long> outstanding = new LinkedHashMap<>();
        for (Map.Entry<TopicPartition, Long> published : publishedCounts().entrySet()) {
            Long done = committed.get(published.getKey());
            long shortfall = published.getValue() - (done == null ? 0L : done);
            if (shortfall > 0) {
                outstanding.put(published.getKey(), shortfall);
            }
        }
        return outstanding;
    }

    private String shortfallMessage(Map<TopicPartition, Long> outstanding, Duration budget) {
        return "The sandbox published " + publishedRecords.get() + " record(s) and waited " + budget
                + " for the instance to commit them, and these never arrived (partition: how many of its records "
                + "are still uncommitted): " + outstanding + ". Published per partition: " + publishedCounts()
                + ". Committed so far: " + highestCommittedOffsets() + ". Handed out of poll: "
                + polledOutRecords.get() + " - a count well short of what was published means the engine never "
                + "fetched them, and one that matches means it fetched them and did not finish them. Note that a "
                + "record which PARKS stays incomplete in the offset map and pins its partition's committed "
                + "offset here for good; see SandboxConsumer#awaitEveryPublishedRecordCommitted.";
    }

    private static void keepHighest(Map<TopicPartition, Long> into, Map<TopicPartition, OffsetAndMetadata> commit) {
        for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : commit.entrySet()) {
            Long previous = into.get(entry.getKey());
            long offset = entry.getValue().offset();
            if (previous == null || offset > previous) {
                into.put(entry.getKey(), offset);
            }
        }
    }
}
