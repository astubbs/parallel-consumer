package bz.stub.parallelconsumer;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.internal.ProducerManager;
import bz.stub.parallelconsumer.navigator.NavigatorView;
import bz.stub.parallelconsumer.state.WorkContainer;
import lombok.Getter;
import lombok.Setter;
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
     */
    @Getter
    @Setter
    protected Optional<ProducerManager<K, V>.ProducingLock> producingLock = Optional.empty();

    public PollContextInternal(List<WorkContainer<K, V>> workContainers) {
        this(workContainers, NavigatorView.inert());
    }

    /**
     * The engine's construction shape (U5): the two sites in {@code AbstractParallelEoSStreamProcessor} pass
     * the module's {@link NavigatorView} here, so the user function's {@link PollContext#getNavigatorView()}
     * answers with THIS instance's observed state. The view-less constructor above survives for callers with no
     * navigator to speak of (tests, tooling) and yields the inert view - the same answers an untagged instance
     * gives (AE6), never null.
     */
    public PollContextInternal(List<WorkContainer<K, V>> workContainers, NavigatorView navigatorView) {
        this.pollContext = new PollContext<>(workContainers, navigatorView);
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
