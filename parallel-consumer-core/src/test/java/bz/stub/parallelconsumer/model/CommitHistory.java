package bz.stub.parallelconsumer.model;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.internal.utils.CollectionUtils;
import bz.stub.parallelconsumer.offsets.OffsetMapCodecManager;
import bz.stub.parallelconsumer.offsets.OffsetMapCodecManager.HighestOffsetAndIncompletes;
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
     * inside a Truth {@code Subject}. Three fixtures across the proxy family and the Java direct client had
     * each hand-written the same reverse scan instead, because what they need is a <em>value</em> to poll on
     * with Awaitility, and an assertion cannot be polled for a value.
     * <p>
     * The raw list is copied before reading: it is a {@code CopyOnWriteArrayList} being appended to by the
     * commit thread while a test reads it.
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

    /**
     * The most recent commit, with its metadata - a value, so it can be polled on rather than asserted about.
     * <p>
     * "Most recent" and not "highest": this reads the last commit recorded, which is what
     * {@link #highestCommit()} has always meant and what the fixtures that used to scan by hand relied on. A
     * commit frontier only moves forward, so on a healthy run the two coincide; where they do not, the last
     * commit is the state of the world and the maximum is a claim about history.
     */
    public Optional<OffsetAndMetadata> lastCommit() {
        return CollectionUtils.getLast(history);
    }

    public Optional<Long> highestCommit() {
        return lastCommit().map(OffsetAndMetadata::offset);
    }

    public List<Long> getOffsetHistory() {
        return history.stream().map(OffsetAndMetadata::offset).collect(Collectors.toList());
    }

    @SneakyThrows
    public HighestOffsetAndIncompletes getEncodedSucceeded() {
        OffsetAndMetadata offsetAndMetadata = lastCommit().get();
        HighestOffsetAndIncompletes highestOffsetAndIncompletes =
                OffsetMapCodecManager.deserialiseIncompleteOffsetMapFromBase64(offsetAndMetadata.offset(), offsetAndMetadata.metadata());
        return highestOffsetAndIncompletes;
    }

    public String getEncoding() {
        return lastCommit().get().metadata();
    }
}
