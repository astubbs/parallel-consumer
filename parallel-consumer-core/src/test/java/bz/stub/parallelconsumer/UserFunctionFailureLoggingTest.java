package bz.stub.parallelconsumer;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import ch.qos.logback.classic.Level;
import bz.stub.parallelconsumer.internal.utils.LogCapture;
import bz.stub.parallelconsumer.internal.AbstractParallelEoSStreamProcessor;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.parallel.Isolated;

import java.util.Collection;
import java.util.Optional;
import java.util.stream.Stream;

import static com.google.common.truth.Truth.assertThat;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.awaitility.Awaitility.await;

/**
 * Asserts the SHAPE of the user-function failure log line, end to end through the processor.
 * <p>
 * The line used to interpolate the whole {@code PollContextInternal}, which renders every record in the batch - so it
 * grew with {@code batchSize} / {@code max.poll.records} and log tooling truncated away the exception that the line
 * existed to report (astubbs#170 / confluentinc#640). A test on the message string alone would not catch a regression
 * here; capturing what is actually emitted does.
 * <p>
 * {@link Isolated} because the capture raises the level of a logger shared by every {@link
 * AbstractParallelEoSStreamProcessor} in the JVM: run concurrently, this test would both read other tests' lines and
 * flood them with DEBUG logging (which perturbs the timing-sensitive close/shutdown tests). Lines are additionally
 * matched on this test's unique topic name, so a future change to the parallel config cannot silently make it read
 * someone else's log.
 * <p>
 * That annotation is measured, not precautionary, and the observation is recorded here because it is the only place a
 * future reader meets it: without it, {@code ParallelEoSStreamProcessorTest}'s
 * {@code closeAfterSingleMessageShouldBeEventBasedFast} and
 * {@code queuedMessagesNotProcessedOrCommittedIfSubmittedDuringShutdown} failed. If either of those two goes
 * intermittent again, an unisolated log capture somewhere is the first thing to suspect.
 *
 * @author Antony Stubbs
 */
@Isolated
@Timeout(value = 2, unit = MINUTES)
class UserFunctionFailureLoggingTest extends ParallelEoSStreamProcessorTestBase {

    @Test
    void errorLineSummarisesTheBatchAndLeavesTheDetailToDebug() {
        primeFirstRecord();

        try (var logs = LogCapture.of(AbstractParallelEoSStreamProcessor.class, Level.DEBUG)) {
            parallelConsumer.poll(context -> {
                throw new RuntimeException("Fake user function failure, to assert on how it is logged");
            });

            await().atMost(defaultTimeout).untilAsserted(() ->
                    assertThat(failureLine(logs.messagesAt(Level.ERROR)).isPresent()).isTrue());

            String errorLine = failureLine(logs.messagesAt(Level.ERROR)).get();

            // identifies the work, in a line whose length does not depend on the batch
            assertThat(errorLine).contains(topicPartition + ": 1 record, offset 0");
            assertThat(errorLine).doesNotContain(firstRecord.value());
            assertThat(errorLine.length()).isLessThan(300);

            // The whole context is still logged - one level down, where it can be turned on deliberately.
            // This await is load-bearing: the DEBUG line is emitted AFTER the ERROR one, so the await above is a
            // signal that fires strictly before this line exists. Reusing it here would be awaiting a proxy that
            // leads the value under assertion - the race fixed in MultiInstanceMetricsTest (see docs/solutions/
            // test-flakiness/vacuous-await-condition-brokerpoller-backpressure-2026-07-31.md, second instance).
            // Await the thing you are about to assert, not something that precedes it.
            await().atMost(defaultTimeout).untilAsserted(() ->
                    assertThat(fullContextLine(logs.messagesAt(Level.DEBUG)).isPresent()).isTrue());
            assertThat(fullContextLine(logs.messagesAt(Level.DEBUG)).get()).contains(firstRecord.value());
        }
    }

    private Optional<String> failureLine(Collection<String> messages) {
        return mine(messages).filter(message -> message.contains("Exception caught in user function running stage"))
                .findFirst();
    }

    private Optional<String> fullContextLine(Collection<String> messages) {
        return mine(messages)
                .filter(message -> message.startsWith("Full context of the batch that failed in the user function:"))
                .findFirst();
    }

    /**
     * @return only the lines about this test's own consumer - the logger is shared JVM-wide
     */
    private Stream<String> mine(Collection<String> messages) {
        return messages.stream().filter(message -> message.contains(INPUT_TOPIC));
    }

}
