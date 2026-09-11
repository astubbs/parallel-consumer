package bz.stub.parallelconsumer.sandbox;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.internal.utils.LongPollingMockConsumer;
import bz.stub.parallelconsumer.offsets.OffsetDecodingError;
import bz.stub.parallelconsumer.offsets.OffsetMapCodecManager;
import bz.stub.parallelconsumer.offsets.OffsetMapCodecManager.HighestOffsetAndIncompletes;
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
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

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
     * How many records are parked on each partition right now, or an empty map when nothing on this path can park.
     * <p>
     * A <em>supplier of counts</em> rather than the fluent API's handle, deliberately: this class is the broker
     * under both APIs and knows nothing about either, and the classic API has no park at all. The fluent path
     * supplies the handle's parked view; {@link ClassicSandbox} supplies nothing and gets the empty answer.
     */
    private volatile Supplier<Map<TopicPartition, Long>> parkedCounts = Collections::emptyMap;

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
     * Tell the wait how to find out what is parked, so that a parked record counts as accounted for rather than as
     * a record the instance still owes - see {@link #awaitEveryPublishedRecordCommitted()}.
     *
     * @param parkedCountsByPartition how many records are parked on each partition at the moment it is asked;
     *                                partitions with nothing parked may be absent
     */
    public void countingParkedRecordsWith(Supplier<Map<TopicPartition, Long>> parkedCountsByPartition) {
        this.parkedCounts = Objects.requireNonNull(parkedCountsByPartition,
                "A parked-count supplier must be supplied - the default already answers zero for every partition");
    }

    /**
     * The highest offset committed for each partition, whichever way it was committed - through this consumer, or
     * through a transactional producer named with {@link #alsoCountingCommitsThrough(MockProducer)}.
     * <p>
     * A committed offset is the <em>next</em> offset to be consumed, so a partition whose committed offset equals
     * the number of records published to it has had every one of them completed and committed.
     */
    public Map<TopicPartition, Long> highestCommittedOffsets() {
        Map<TopicPartition, Long> offsets = new LinkedHashMap<>();
        for (Map.Entry<TopicPartition, OffsetAndMetadata> commit : highestCommits().entrySet()) {
            offsets.put(commit.getKey(), commit.getValue().offset());
        }
        return offsets;
    }

    /**
     * The highest commit made for each partition, <b>whole</b>: the offset and the offset map committed beside it.
     * The metadata is half the answer to how much of a partition is done - everything below the committed offset,
     * plus everything inside the encoded range that the map does not list as incomplete - so the wait reads the
     * commit rather than only its offset. See {@link #completedOn}.
     */
    private Map<TopicPartition, OffsetAndMetadata> highestCommits() {
        Map<TopicPartition, OffsetAndMetadata> highest = new LinkedHashMap<>();
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
     * Waits until the engine has accounted for every record this sandbox published: on every partition, what the
     * commit says is complete plus what is parked there right now equals what was published to it.
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
     * what this waits for - together with the parked set, below, which is the other way a record reaches an end.
     * The wait replaces the timing model; it is not a longer version of it.
     *
     * <h2>A parked record is accounted for, and this reverses the decision that said otherwise</h2>
     * The committed offset is the highest <em>sequentially succeeded</em> offset plus one
     * ({@code PartitionState#getOffsetHighestSequentialSucceeded}), and a parked record stays incomplete in the
     * offset map for as long as it is parked - it holds no worker and is never retried, but it is not complete
     * either. So it holds its partition's committed offset at its own offset for good.
     * <p>
     * The first version of this wait read that as a record the instance still owed, and so <b>refused a bounded run
     * whose records park</b> - telling the caller to use {@link Bound#none()} and close the handle itself. That is
     * the decision being overridden here, and it was wrong for the product rather than merely inconvenient: the
     * README's own quickstart parks by design and is bounded, so every build spent the whole twenty-second budget
     * waiting for a commit that could never come, logged the refusal at error, and then closed anyway - about
     * thirty-six seconds for a ten-second run (astubbs#504).
     * <p>
     * <b>Parking is a terminal outcome for the run</b>, so this counts it as accounted for. Per partition:
     * <ul>
     *   <li><b>completed</b> is what the commit says is done - every offset below the committed one, plus every
     *       offset inside the commit metadata's encoded range that the map does not list as incomplete
     *       ({@link #completedOn});</li>
     *   <li><b>parked</b> is how many records are parked on that partition right now, from the supplier
     *       {@link #countingParkedRecordsWith(Supplier)} was given - the fluent API's parked view, or zero on the
     *       classic path, which has no park;</li>
     *   <li>the wait ends when {@code published == completed + parked} on every partition.</li>
     * </ul>
     * A parked record below the highest succeeded offset is one of the incompletes (so it is <em>not</em> in
     * completed) and one of parked, so it is counted once; one above that range is outside the encoding and is
     * counted only through parked. A record still queued or in flight is in neither, so the wait continues - which
     * is the whole point of waiting rather than closing.
     * <p>
     * The parked view is a control-loop snapshot, so a record that has just parked may take one loop to appear.
     * This polls, so that resolves itself.
     *
     * @throws IllegalStateException when the budget ran out, naming every partition still short and what it
     *                               published, completed and parked
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
        Map<TopicPartition, PartitionAccount> outstanding = unaccountedByPartition();
        while (!outstanding.isEmpty()) {
            if (closed()) {
                // Closed under us: the run is over and nothing else will ever be committed. That is the ordinary
                // end of an unbounded run rather than a fault, so it is not one here either.
                log.debug("The sandbox consumer closed while waiting for the instance to account for what was "
                        + "published, still outstanding: {}", outstanding);
                return;
            }
            // Subtract, never compare directly: System.nanoTime() is explicitly allowed to be negative and to
            // wrap, so `nanoTime() + toNanos()` can overflow and a bare `nanoTime() > deadline` is then true on
            // the very first pass - the wait refuses instantly with a shortfall message about a run that had no
            // chance to start. The difference is correct across the wrap for any two readings less than about
            // 292 years apart, which is the standard form and what Object#wait-style deadline loops use.
            if (System.nanoTime() - deadline > 0) {
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
                log.debug("Interrupted while waiting for the instance to account for what was published, still "
                        + "outstanding: {}", outstanding);
                return;
            }
            outstanding = unaccountedByPartition();
        }
        log.debug("Every one of the {} published record(s) is accounted for - completed, or parked",
                publishedRecords.get());
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
     * Every partition with records the instance has neither completed nor parked - empty when it has accounted for
     * everything this sandbox offered it.
     */
    private Map<TopicPartition, PartitionAccount> unaccountedByPartition() {
        Map<TopicPartition, OffsetAndMetadata> commits = highestCommits();
        Map<TopicPartition, Long> parked = parkedCounts.get();
        Map<TopicPartition, PartitionAccount> outstanding = new LinkedHashMap<>();
        for (Map.Entry<TopicPartition, Long> published : publishedCounts().entrySet()) {
            TopicPartition partition = published.getKey();
            Long parkedHere = parked.get(partition);
            PartitionAccount account = new PartitionAccount(published.getValue(),
                    completedOn(partition, commits.get(partition)),
                    parkedHere == null ? 0L : parkedHere);
            if (account.unaccounted() > 0) {
                outstanding.put(partition, account);
            }
        }
        return outstanding;
    }

    /**
     * How many of a partition's records the instance has finished, read off its highest commit: everything below
     * the committed offset, plus everything inside the encoded range that the offset map does not list as
     * incomplete.
     * <p>
     * The range the engine encodes runs from the offset it is committing to the highest offset it has succeeded
     * ({@code OffsetMapCodecManager#encodeOffsetsCompressed}), so the count is
     * {@code highestSucceeded + 1 - incompletes}. Absent or empty metadata is not an error and not a special case
     * of the decoder either - it is a partition with nothing above its committed offset, and the decode answers a
     * highest-seen of one below the committed offset, which is the same arithmetic.
     *
     * @param partition the partition being counted, named in the refusal when its offset map cannot be read
     * @param commit the partition's highest commit, or null when it has never committed - which is what a
     *               partition whose every record parked looks like: not one of them completed, so it was never
     *               dirty, so it never committed at all
     */
    private static long completedOn(TopicPartition partition, OffsetAndMetadata commit) {
        if (commit == null) {
            return 0;
        }
        long committedOffset = commit.offset();
        String offsetMap = commit.metadata();
        if (offsetMap == null || offsetMap.isEmpty()) {
            return committedOffset;
        }
        try {
            HighestOffsetAndIncompletes encoded =
                    OffsetMapCodecManager.deserialiseIncompleteOffsetMapFromBase64(committedOffset, offsetMap);
            long highestSucceeded = encoded.getHighestSeenOffset().orElse(committedOffset - 1);
            // Never below the committed offset: a decode that claimed less than the commit itself would make the
            // wait ask for records the instance has already reported as done.
            return Math.max(committedOffset, highestSucceeded + 1 - encoded.getIncompleteOffsets().size());
        } catch (OffsetDecodingError e) {
            // Loud rather than quietly pessimistic. Metadata this build cannot read means the wait is counting the
            // wrong thing, and a run that then spent its whole budget would read as the engine's fault.
            throw new IllegalStateException("The sandbox cannot read the offset map the instance committed against "
                    + partition + " at offset " + committedOffset + " (" + offsetMap + "), so it cannot tell how "
                    + "much of that partition is finished", e);
        }
    }

    private String shortfallMessage(Map<TopicPartition, PartitionAccount> outstanding, Duration budget) {
        return "The sandbox published " + publishedRecords.get() + " record(s) and waited " + budget
                + " for the instance to account for them, and these partitions never got there: " + outstanding
                + ". Committed so far: " + highestCommittedOffsets() + ". Handed out of poll: "
                + polledOutRecords.get() + " - a count well short of what was published means the engine never "
                + "fetched them, and one that matches means it fetched them and did not finish them. A record that "
                + "is neither complete nor parked is one the instance is still holding: queued, in flight, or "
                + "waiting on a retry delay. See SandboxConsumer#awaitEveryPublishedRecordCommitted.";
    }

    /**
     * Keeps, per partition, the commit that says the most - which is <b>not</b> the same as the one with the
     * highest offset.
     * <p>
     * Parallel Consumer's committed offset is {@code highestSequentialSucceeded + 1}, so a record that stalls low
     * in a partition - a parked one above all - pins that offset while everything above it keeps completing. What
     * changes between successive commits is then only the <em>metadata</em>: the incompletes shrink and the
     * highest-seen grows at one unmoving offset. Picking with a strict {@code >} on the offset keeps the FIRST
     * commit seen at that offset and discards every later, more complete one, so {@link #completedOn} reads the
     * oldest map for the rest of the run and the wait burns its whole budget - then blames the engine, in a
     * message about what the instance is still holding, for a defect in this reader.
     * <p>
     * So a tie on the offset is broken by what the two commits account for, rather than by which was seen first.
     * The offset still decides where the offsets differ, which is every case the old comparison got right.
     */
    private static void keepHighest(Map<TopicPartition, OffsetAndMetadata> into,
                                    Map<TopicPartition, OffsetAndMetadata> commit) {
        for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : commit.entrySet()) {
            OffsetAndMetadata previous = into.get(entry.getKey());
            if (previous == null || accountsForMore(entry.getKey(), entry.getValue(), previous)) {
                into.put(entry.getKey(), entry.getValue());
            }
        }
    }

    /**
     * @return whether {@code candidate} is the commit to keep over {@code previous} for {@code partition}
     */
    private static boolean accountsForMore(TopicPartition partition,
                                           OffsetAndMetadata candidate,
                                           OffsetAndMetadata previous) {
        if (candidate.offset() != previous.offset()) {
            return candidate.offset() > previous.offset();
        }
        return completedOn(partition, candidate) >= completedOn(partition, previous);
    }

    /**
     * One partition's accounting at the moment it was read, and how the refusal renders it: what was published,
     * what the commit says is complete, what is parked, and the difference - which is what the instance is still
     * holding.
     */
    private static final class PartitionAccount {

        private final long published;

        private final long completed;

        private final long parked;

        private PartitionAccount(long published, long completed, long parked) {
            this.published = published;
            this.completed = completed;
            this.parked = parked;
        }

        private long unaccounted() {
            return published - completed - parked;
        }

        @Override
        public String toString() {
            return "published " + published + ", completed " + completed + ", parked " + parked + ", so "
                    + unaccounted() + " unaccounted for";
        }
    }
}
