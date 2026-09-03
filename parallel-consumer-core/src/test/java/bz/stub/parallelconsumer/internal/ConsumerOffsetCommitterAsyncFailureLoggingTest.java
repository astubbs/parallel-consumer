package bz.stub.parallelconsumer.internal;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.internal.utils.LogCapture;
import bz.stub.parallelconsumer.offsets.OffsetMapCodecManager;
import bz.stub.parallelconsumer.state.WorkManager;
import ch.qos.logback.classic.Level;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RebalanceInProgressException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.api.parallel.ResourceAccessMode;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.mockito.ArgumentCaptor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static bz.stub.parallelconsumer.ParallelConsumerOptions.CommitMode.PERIODIC_CONSUMER_ASYNCHRONOUS;
import static com.google.common.truth.Truth.assertThat;
import static java.util.Collections.nCopies;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * The ERROR line for a failed asynchronous commit is a contract with the operator, so these tests read the line the
 * committer actually <em>emits</em> - not the format string, and not the summariser in isolation.
 * <p>
 * What it must carry is every topic, partition and offset the commit attempted (astubbs#168 / confluentinc#629 asked
 * for exactly those). What it must not carry is each entry's {@code metadata}: PC's base64-encoded offset map, up to
 * {@link OffsetMapCodecManager#DefaultMaxMetadataSize} characters <em>per partition</em>, which grew the line to
 * partitions x 4KB on the one occasion it most needs to survive log truncation. Interpolating the map again would
 * restore both properties' opposite and no ordinary assertion would notice - which is what
 * {@link LogCapture} is for.
 *
 * @author Antony Stubbs
 */
// SAME_THREAD, not @Isolated: ConsumerOffsetCommitter's logger is quiet and not on a timing-sensitive path, so
// raising it does not perturb other classes, and every read here is already scoped to this class's own topic name -
// LogCapture's second obligation. What scoping cannot fix is two captures of the SAME logger overlapping: the level
// is shared state, so whichever closes first restores it to INFO and silently swallows the other's DEBUG line. That
// is these two methods, and only SAME_THREAD separates them (observed - the DEBUG assertion read an empty list).
// Same reasoning as LoadFactorCeilingReportingTest, which needs @Isolated on top because its logger is a busy one.
@Execution(ExecutionMode.SAME_THREAD)
// DefaultMaxMetadataSize is a MUTABLE static that OffsetEncodingBackPressureTest and its unit sibling write; this
// module runs tests concurrently, so reading it is only stable under the lock those two already take for writing
@ResourceLock(value = OffsetMapCodecManager.METADATA_DATA_SIZE_RESOURCE_LOCK, mode = ResourceAccessMode.READ)
class ConsumerOffsetCommitterAsyncFailureLoggingTest {

    /**
     * The logger is shared JVM-wide and this module runs tests concurrently, so every read is filtered on a string
     * unique to this test - {@link LogCapture}'s second obligation.
     */
    private static final String TOPIC = ConsumerOffsetCommitterAsyncFailureLoggingTest.class.getSimpleName();

    @Test
    void asyncCommitFailureLineNamesEveryPartitionAndOffsetButNotTheMetadata() {
        var consumerMgr = consumerManagerMock();
        var committer = committerFor(consumerMgr);
        String metadata = largestOffsetMapPcWillWrite();
        Map<TopicPartition, OffsetAndMetadata> offsets = twoPartitionCommit(metadata);

        try (var logs = LogCapture.of(ConsumerOffsetCommitter.class, Level.DEBUG)) {
            committer.commitOffsets(offsets, new ConsumerGroupMetadata("a-group"));

            completeCallbackWith(consumerMgr, offsets, new RebalanceInProgressException(
                    "Offset commit cannot be completed since the consumer is undergoing a rebalance (mocked)"));

            String errorLine = only(logs.messagesAt(Level.ERROR), TOPIC);
            assertThat(errorLine).contains(TOPIC + "-0: offset 1000, " + metadata.length() + " chars of metadata");
            assertThat(errorLine).contains(TOPIC + "-1: offset 5, no metadata");
            assertThat(errorLine).doesNotContain(metadata);
            assertThat(errorLine).doesNotContain("OffsetAndMetadata{");
            assertThat(errorLine.length()).isLessThan(300);

            // the unabridged map is still available, one level down, where it has to be asked for
            assertThat(only(logs.messagesAt(Level.DEBUG), "Failed commit in full")).contains(metadata);
        }
    }

    /**
     * Pairs with the test above so its {@code doesNotContain} assertions are not vacuous: the capture does see this
     * committer's ERROR lines when there are any, so seeing none here is the {@code exception != null} guard working
     * rather than a capture that reads nothing.
     */
    @Test
    void asyncCommitSuccessLogsNothingAtError() {
        var consumerMgr = consumerManagerMock();
        var committer = committerFor(consumerMgr);
        Map<TopicPartition, OffsetAndMetadata> offsets = twoPartitionCommit(largestOffsetMapPcWillWrite());

        try (var logs = LogCapture.of(ConsumerOffsetCommitter.class, Level.DEBUG)) {
            committer.commitOffsets(offsets, new ConsumerGroupMetadata("a-group"));

            completeCallbackWith(consumerMgr, offsets, null);

            assertThat(linesMentioning(logs.messagesAt(Level.ERROR), TOPIC)).isEmpty();
        }
    }

    @SuppressWarnings("unchecked")
    private static ConsumerManager<String, String> consumerManagerMock() {
        return mock(ConsumerManager.class);
    }

    /**
     * {@code build()} does not validate - {@code validate()} is what needs a real consumer, and nothing here calls it.
     */
    private static ConsumerOffsetCommitter<String, String> committerFor(ConsumerManager<String, String> consumerMgr) {
        @SuppressWarnings("unchecked")
        WorkManager<String, String> workManager = mock(WorkManager.class);
        var options = ParallelConsumerOptions.<String, String>builder()
                .commitMode(PERIODIC_CONSUMER_ASYNCHRONOUS)
                .build();
        return new ConsumerOffsetCommitter<>(consumerMgr, workManager, options);
    }

    /**
     * Read inside a test rather than into a constant, so it is read under the class's resource lock - a field
     * initialiser runs whenever the class first loads, which no lock covers.
     */
    private static String largestOffsetMapPcWillWrite() {
        return String.join("", nCopies(OffsetMapCodecManager.DefaultMaxMetadataSize, "x"));
    }

    /**
     * One partition carrying the largest offset map PC will ever write, one carrying none - the two shapes the line
     * has to render differently.
     */
    private static Map<TopicPartition, OffsetAndMetadata> twoPartitionCommit(String metadata) {
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(new TopicPartition(TOPIC, 0), new OffsetAndMetadata(1000, metadata));
        offsets.put(new TopicPartition(TOPIC, 1), new OffsetAndMetadata(5));
        return offsets;
    }

    /**
     * Captures the callback the committer handed to {@code commitAsync} and completes it as the broker's response
     * thread would - with {@code null} for a success.
     */
    private static void completeCallbackWith(ConsumerManager<String, String> consumerMgr,
                                             Map<TopicPartition, OffsetAndMetadata> offsets,
                                             Exception exception) {
        var callback = ArgumentCaptor.forClass(OffsetCommitCallback.class);
        verify(consumerMgr).commitAsync(eq(offsets), callback.capture());
        callback.getValue().onComplete(offsets, exception);
    }

    /**
     * @return the one captured line mentioning {@code unique} - asserting there is exactly one, so a second matching
     * line is a failure rather than something silently discarded by a {@code findFirst()}
     */
    private static String only(List<String> messages, String unique) {
        List<String> matches = linesMentioning(messages, unique);
        assertThat(matches).hasSize(1);
        return matches.get(0);
    }

    private static List<String> linesMentioning(List<String> messages, String unique) {
        return messages.stream().filter(message -> message.contains(unique)).collect(Collectors.toList());
    }

}
