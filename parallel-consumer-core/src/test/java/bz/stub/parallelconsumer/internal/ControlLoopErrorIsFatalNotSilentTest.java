package bz.stub.parallelconsumer.internal;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.ParallelEoSStreamProcessor;
import bz.stub.parallelconsumer.PollContextInternal;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static org.awaitility.Awaitility.await;

/**
 * An {@link Error} thrown on the control thread fails the processor the same way an {@link Exception} does: the cause
 * is recorded, and the processor shuts down.
 * <p>
 * astubbs#539: on Kafka 4.4.0 the control thread hit a {@link NoClassDefFoundError} on its first pass. The handler
 * caught only {@code Exception}, so the Error escaped it - nothing logged, no {@code failureReason}, no close - and
 * the processor stopped consuming while still reporting RUNNING. The cause appeared only when {@code close()}
 * rethrew it. The Error is injected by overriding {@code controlLoop}, the one call inside that handler, because
 * reproducing the real one needs a Kafka jar the build does not have.
 */
class ControlLoopErrorIsFatalNotSilentTest {

    @Test
    void anErrorOnTheControlThreadIsRecordedAndShutsTheProcessorDown() {
        var thrown = new NoClassDefFoundError("org/apache/kafka/common/utils/CloseableIterator");
        var pc = new ParallelEoSStreamProcessor<String, String>(ParallelConsumerOptions.<String, String>builder()
                .consumer(new MockConsumer<>(OffsetResetStrategy.EARLIEST))
                .build()) {
            @Override
            protected <R> void controlLoop(Function<PollContextInternal<String, String>, List<R>> userFunction,
                                           Consumer<R> callback) {
                throw thrown;
            }
        };

        try {
            pc.poll(context -> {
            });

            await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                    assertWithMessage("the processor must not keep reporting RUNNING after its control thread died")
                            .that(pc.getState()).isEqualTo(State.CLOSED));

            assertWithMessage("the Error is the recorded failure cause, so getFailureCause() explains the stop")
                    .that(pc.getFailureCause()).isNotNull();
            assertThat(pc.getFailureCause().getCause()).isSameInstanceAs(thrown);
            assertThat(pc.isClosedOrFailed()).isTrue();
        } finally {
            // already closed by the control thread when the fix is in place; without it, this is what reaps the
            // instance (and rethrows the Error, which is the late, only sighting the issue describes)
            try {
                pc.close();
            } catch (Exception | Error ignoredCloseRethrowsTheFailure) {
                // the failure is asserted above; close re-raising it (sneakily, as a checked ExecutionException) is
                // expected and not under test here
            }
        }
    }
}
