package bz.stub.parallelconsumer.model;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.internal.utils.CollectionUtils;
import bz.stub.parallelconsumer.offsets.OffsetMapCodecManager;
import bz.stub.parallelconsumer.offsets.OffsetMapCodecManager.HighestOffsetAndIncompletes;
import lombok.NonNull;
import lombok.SneakyThrows;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

public class CommitHistory {

    private final List<OffsetAndMetadata> history;

    public CommitHistory(final List<OffsetAndMetadata> collect) {
        super();
        this.history = collect;
    }

    /**
     * Reads one partition's commits out of a mock consumer's raw commit history - the shape
     * {@code LongPollingMockConsumer#getCommitHistoryInt()} returns, a list of commit instants each mapping
     * partition to offset.
     * <p>
     * This is the step that made {@link #highestCommit()} unreachable for anything but an assertion: the
     * constructor wants a flat per-partition list, and until this existed the only code that produced one was
     * inside a Truth {@code Subject}. What a fixture usually needs instead is a <em>value</em> to poll on with
     * Awaitility, and an assertion cannot be polled for a value - so each one hand-wrote the same reverse scan.
     * <p>
     * The raw list is copied before reading: it is a {@code CopyOnWriteArrayList} being appended to by the
     * commit thread while a test reads it.
     * <p>
     * <b>The parameter is a {@code List} and must stay one, though a {@code Collection} would compile.</b>
     * Everything this class answers is positional - {@link #highestCommit()} and {@link #getEncoding()} read
     * the LAST element, which is the most recent commit - so encounter order is the whole meaning of the input.
     * A {@code Collection} parameter would accept a set and return an arbitrary commit while looking correct,
     * which is worse than not compiling. SpotBugs suggests the widening; it is declined for that reason.
     */
    public static CommitHistory forPartition(final List<Map<TopicPartition, OffsetAndMetadata>> rawHistory,
                                             final TopicPartition partition) {
        List<OffsetAndMetadata> forPartition = new ArrayList<>(rawHistory).stream()
                .map(commitInstant -> commitInstant.get(partition))
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
        return new CommitHistory(forPartition);
    }

    public boolean contains(final int offset) {
        return history.stream().anyMatch(x -> x.offset() == offset);
    }

    public Optional<Long> highestCommit() {
        Optional<OffsetAndMetadata> last = CollectionUtils.getLast(history);
        return last.map(OffsetAndMetadata::offset);
    }

    public List<Long> getOffsetHistory() {
        return history.stream().map(OffsetAndMetadata::offset).collect(Collectors.toList());
    }

    @SneakyThrows
    public HighestOffsetAndIncompletes getEncodedSucceeded() {
        Optional<OffsetAndMetadata> first = getHead();
        OffsetAndMetadata offsetAndMetadata = first.get();
        HighestOffsetAndIncompletes highestOffsetAndIncompletes =
                OffsetMapCodecManager.deserialiseIncompleteOffsetMapFromBase64(offsetAndMetadata.offset(), offsetAndMetadata.metadata());
        return highestOffsetAndIncompletes;
    }

    @NonNull
    private Optional<OffsetAndMetadata> getHead() {
        Optional<OffsetAndMetadata> first = history.isEmpty()
                ? Optional.empty()
                : Optional.of(history.get(history.size() - 1));
        return first;
    }

    public String getEncoding() {
        return getHead().get().metadata();
    }
}
