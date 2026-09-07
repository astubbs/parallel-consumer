package bz.stub.parallelconsumer;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.internal.PCInternalRuntimeException;
import bz.stub.parallelconsumer.internal.ProducerManager;
import bz.stub.parallelconsumer.internal.utils.RecordBatchSummary;
import bz.stub.parallelconsumer.state.WorkContainer;
import lombok.Getter;
import lombok.ToString;
import lombok.experimental.Delegate;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static bz.stub.parallelconsumer.internal.utils.StringUtils.msg;

/**
 * Internal only view on the {@link PollContext}.
 */
@ToString
public class PollContextInternal<K, V> {

    @Delegate
    @Getter
    private final PollContext<K, V> pollContext;

    /**
     * Used when running in {@link ParallelConsumerOptions.CommitMode#isUsingTransactionCommitMode()} then the produce
     * lock will be passed around here. It needs to be unlocked when work has been put back in the inbox.
     * <p>
     * Exactly one produce lock is acquired per context, so exactly one release is owed. Ownership is therefore handed
     * over by {@link #takeProducingLock()} rather than merely read: a released lock leaves the context empty, so a
     * second release attempt is a no-op instead of an {@link IllegalMonitorStateException} on a read lock this thread
     * no longer holds.
     */
    private Optional<ProducerManager<K, V>.ProducingLock> producingLock = Optional.empty();

    public synchronized Optional<ProducerManager<K, V>.ProducingLock> getProducingLock() {
        return producingLock;
    }

    /**
     * Sets the produce lock this context owns, refusing to overwrite one it is already holding.
     * <p>
     * The one-lock-one-release invariant above is otherwise enforced only by caller convention:
     * {@code ParallelEoSStreamProcessor#processAndProduceResults} acquires from one of two
     * branches that are mutually exclusive on
     * {@link ParallelConsumerOptions#isAllowEagerProcessingDuringTransactionCommit()}, so today a second set cannot
     * happen. A future call site that broke that exclusivity would silently drop the first lock - never released,
     * and the next commit's write-lock acquisition would then block forever on a read hold nobody can free. Failing
     * loudly here turns that into an immediate, attributable error instead of a hang.
     * <p>
     * <b>Rejecting is not enough on its own - the refused lock has to be released.</b> Acquisition happens before
     * the hand-over: each call site in {@code ParallelEoSStreamProcessor#processAndProduceResults} calls
     * {@code pm.beginProducing(context)} and passes the result straight here, so the second read hold is already
     * taken by the time this method can refuse it. Neither site releases that hold on the throw path, and
     * {@link bz.stub.parallelconsumer.internal.AbstractParallelEoSStreamProcessor#cleanUpContext} releases only the
     * lock the CONTEXT kept - so without the release below the second hold is orphaned, and the next commit's
     * write-lock acquisition blocks forever. The guard would report the misuse and still cause the hang it
     * advertises preventing. Codex review on astubbs#262 caught that; {@code PollContextInternalTest} holds it,
     * with a control arm that removes only the release.
     */
    public synchronized void setProducingLock(Optional<ProducerManager<K, V>.ProducingLock> producingLock) {
        if (this.producingLock.isPresent()) {
            // An EMPTY argument is refused too, not just a second lock. Testing the incoming Optional as well
            // would let `setProducingLock(empty())` fall through and overwrite a held lock with nothing - dropping
            // the only reference to a live read hold, silently, which is the precise failure this guard's javadoc
            // promises to turn into an attributable error. takeProducingLock() is the sanctioned way to clear.
            if (producingLock.isPresent() && producingLock.get() != this.producingLock.get()) {
                // Release the hold we are refusing, before throwing - see the javadoc above.
                //
                // ONLY when it is a genuinely distinct acquisition. ProducingLock is not a token: every instance
                // wraps the one ReadLock of producerTransactionLock, so unlock() drops one of THIS THREAD's holds
                // whichever instance it is called on. Handed the same instance back - a retry path, a defensive
                // re-assignment, a merge that duplicates the hand-over line - there is only one acquisition, and
                // an unconditional release would take the count to zero while the worker is still inside its
                // produce section. The commit thread could then take the write lock, gather offsets, and let
                // records land outside the transaction that accounts for them: the exact race the produce lock
                // exists to close, failing silently. Codex review on astubbs#262 found the orphan; the follow-up
                // review found this, the hazard the first fix introduced.
                producingLock.get().unlock();
            }
            // Offsets, not the whole context: this is an exception message, and PollContextInternal's toString
            // carries every record's key and value. ProducerManager's produce-lock logging identifies a context the
            // same way for the same reason.
            throw new PCInternalRuntimeException(msg("Produce lock already held for context: {} - a distinct "
                    + "second acquisition has been released rather than orphaned, but the call site is still "
                    + "wrong: exactly one produce lock is owed per context, and only takeProducingLock() clears "
                    + "it", getOffsets()));
        }
        this.producingLock = producingLock;
    }

    /**
     * Claims the produce lock for release, clearing it from this context so it can only ever be released once.
     *
     * @return the lock, if this context still owns one
     */
    public synchronized Optional<ProducerManager<K, V>.ProducingLock> takeProducingLock() {
        var claimed = producingLock;
        producingLock = Optional.empty();
        return claimed;
    }

    public PollContextInternal(List<WorkContainer<K, V>> workContainers) {
        this.pollContext = new PollContext<>(workContainers);
    }

    /**
     * @return a stream of {@link WorkContainer}s
     */
    public Stream<WorkContainer<K, V>> streamWorkContainers() {
        return pollContext.streamInternal().map(RecordContextInternal::getWorkContainer);
    }

    /**
     * @return a flat {@link List} of {@link WorkContainer}s, which wrap the {@link ConsumerRecord}s in this result set
     */
    public List<WorkContainer<K, V>> getWorkContainers() {
        return streamWorkContainers().collect(Collectors.toList());
    }

    /**
     * @return the producer replay generation the control thread stamped on this batch at dispatch, for the produce
     *         lock to compare against; empty when no container carries one - a batch that did not come through the
     *         control loop's dispatch, which opts out of the check
     * @see WorkContainer#getDispatchedAtReplayGeneration()
     */
    public OptionalLong replayGenerationAtDispatch() {
        return streamWorkContainers()
                .mapToLong(WorkContainer::getDispatchedAtReplayGeneration)
                .filter(generation -> generation != WorkContainer.NEVER_DISPATCHED)
                .min();
    }

    /**
     * A short, <b>bounded</b> description of the records in this context - topic-partitions, record counts and offset
     * ranges - for log lines that must not grow with the batch size.
     * <p>
     * {@link #toString()} renders every record (keys and values included), which made the user-function failure log
     * long enough for log tooling to truncate it (astubbs#170 / confluentinc#640). Use this in the message, and leave
     * the full object for {@code DEBUG}.
     *
     * @return e.g. {@code 3 records across 2 partitions: my-topic-0: 2 records, offsets 5-6; my-topic-1: 1 record,
     * offset 9}
     */
    public String summariseForLog() {
        return RecordBatchSummary.summariseOffsets(pollContext.getOffsets());
    }

}
