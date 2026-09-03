package bz.stub.parallelconsumer.internal;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.internal.utils.LogCapture;
import bz.stub.parallelconsumer.offsets.OffsetMapCodecManager;
import bz.stub.parallelconsumer.state.WorkManager;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.IThrowableProxy;
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
import static com.google.common.truth.Truth.assertWithMessage;
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
// SAME_THREAD is the load-bearing one, and it fixes an OBSERVED failure, not a hypothetical: this module's pom sets
// junit.jupiter.execution.parallel.mode.default=concurrent, so the METHODS of one class run in parallel with each
// other. Both methods below capture the SAME logger, and LogCapture's level is shared state - whichever closes first
// restores the level to INFO and silently swallows the other's DEBUG line, which is exactly how the DEBUG assertion
// came to read an empty list. Scoping reads by topic name cannot fix that; only serialising the methods can.
// Not @Isolated: that separates CLASSES, and the pom leaves mode.classes.default at JUnit's same_thread, so classes
// do not overlap today anyway. LoadFactorCeilingReportingTest carries both because its logger is a busy one whose
// raised level perturbs the timing-sensitive close/shutdown tests; ConsumerOffsetCommitter's is quiet.
@Execution(ExecutionMode.SAME_THREAD)
// DefaultMaxMetadataSize is a MUTABLE static that OffsetEncodingBackPressureTest and its unit sibling write. Taking
// the READ side of the lock those two already hold for writing is what WorkManagerOffsetMapCodecManagerTest and
// OffsetEncodingTests do. Belt-and-braces rather than load-bearing while classes run sequentially - it is what keeps
// this test correct if mode.classes.default is ever turned on, and it costs nothing.
@ResourceLock(value = OffsetMapCodecManager.METADATA_DATA_SIZE_RESOURCE_LOCK, mode = ResourceAccessMode.READ)
class ConsumerOffsetCommitterAsyncFailureLoggingTest {

    /**
     * The logger is shared JVM-wide, so every read is filtered on a string unique to this test - {@link LogCapture}'s
     * second obligation.
     */
    private static final String TOPIC = ConsumerOffsetCommitterAsyncFailureLoggingTest.class.getSimpleName();

    /**
     * The DEBUG detail line's own prefix. Read together with {@link #TOPIC} rather than on its own - it is shared
     * text, so it identifies the statement while the topic identifies this test.
     */
    private static final String FULL_MAP_LINE = "Failed commit in full";

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

            // the exception is the other half of the diagnostic, and messagesAt() projects it away - so dropping the
            // trailing argument would leave every assertion above still passing
            assertThat(throwableOfOnlyEventAt(logs, Level.ERROR))
                    .isEqualTo(RebalanceInProgressException.class.getName());

            // the unabridged map is still available, one level down, where it has to be asked for
            assertThat(only(linesMentioning(logs.messagesAt(Level.DEBUG), TOPIC), FULL_MAP_LINE)).contains(metadata);
        }
    }

    /**
     * Pairs with the test above so its {@code doesNotContain} assertions are not vacuous: the capture does see this
     * committer's lines when there are any, so seeing none here is the {@code exception != null} guard working rather
     * than a capture that reads nothing.
     * <p>
     * The DEBUG half is the one that matters. Both log statements sit behind that one guard, so moving or duplicating
     * the unabridged dump outside it would put the whole map - every partition's metadata, the thing astubbs#168 is
     * about - on the hot path of every SUCCESSFUL commit, while the test above still passed.
     */
    @Test
    void asyncCommitSuccessLogsNothingAtErrorAndDumpsNoOffsetMap() {
        var consumerMgr = consumerManagerMock();
        var committer = committerFor(consumerMgr);
        Map<TopicPartition, OffsetAndMetadata> offsets = twoPartitionCommit(largestOffsetMapPcWillWrite());

        try (var logs = LogCapture.of(ConsumerOffsetCommitter.class, Level.DEBUG)) {
            committer.commitOffsets(offsets, new ConsumerGroupMetadata("a-group"));

            completeCallbackWith(consumerMgr, offsets, null);

            assertThat(linesMentioning(logs.messagesAt(Level.ERROR), TOPIC)).isEmpty();
            assertThat(linesMentioning(logs.messagesAt(Level.DEBUG), FULL_MAP_LINE)).isEmpty();
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
     * @return the class name of the throwable attached to the one captured event at this level - what
     * {@link LogCapture#messagesAt} cannot show, since it formats the message and drops the throwable
     */
    private static String throwableOfOnlyEventAt(LogCapture logs, Level level) {
        List<ILoggingEvent> events = logs.events().stream()
                .filter(event -> event.getLevel() == level)
                .filter(event -> event.getFormattedMessage().contains(TOPIC))
                .collect(Collectors.toList());
        assertThat(events).hasSize(1);
        IThrowableProxy thrown = events.get(0).getThrowableProxy();
        // asserted rather than dereferenced, so dropping the trailing argument reports "no throwable attached" instead
        // of an NPE inside the helper
        assertWithMessage("no throwable attached to the %s event - was the exception argument dropped?", level)
                .that(thrown).isNotNull();
        return thrown.getClassName();
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
