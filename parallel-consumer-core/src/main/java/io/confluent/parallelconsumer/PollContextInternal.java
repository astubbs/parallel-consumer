package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import io.confluent.parallelconsumer.internal.ProducerManager;
import io.confluent.parallelconsumer.state.WorkContainer;
import lombok.Getter;
import lombok.ToString;
import lombok.experimental.Delegate;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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

    public synchronized void setProducingLock(Optional<ProducerManager<K, V>.ProducingLock> producingLock) {
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

}
