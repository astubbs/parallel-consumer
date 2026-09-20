package bz.stub.parallelconsumer;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.turbo.TurboFilter;
import ch.qos.logback.core.spi.FilterReply;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.parallel.Isolated;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * The control task's completion must be armed on <b>every</b> way out of the task body, including the clean one -
 * which means the arming has to sit inside the guard, not after it.
 * <p>
 * <b>Why this is a test and not a comment.</b> The task body carries a comment reading "The WHOLE body is inside this
 * try, not just the loop", and it was not: the clean-exit log line and the {@code complete(null)} beside it sat
 * <em>after</em> the catch. The review of astubbs/parallel-consumer#506 found the code and its own comment
 * disagreeing, and the code was the wrong half - a throw from that log line left the completion never completed while
 * the submitted {@code Future} still failed, so a caller waiting on the completion waited for an event that had
 * already happened and would never be announced. That is the one case
 * {@code AbstractParallelEoSStreamProcessor#controlThreadCompletion()} promises cannot happen, and nothing was red.
 * <p>
 * <b>The hazard is real rather than theoretical, which is why the log line is what this injects into.</b> Rendering
 * that line runs the embedding application's logging binding, and the same reasoning is already written down one
 * frame above, on the try itself: the MDC adoption at the top of the body runs the caller's binding too. A binding
 * that throws is the hazard the try exists for, so the test supplies one.
 * <p>
 * {@link Isolated} because a {@link TurboFilter} is installed on the JVM-wide {@link LoggerContext}: while it is
 * there, every log call in the process passes through it, and any other instance ending its control loop concurrently
 * would be given the same failure.
 *
 * @see ControlThreadCompletionTest for the three ordinary cases
 */
@Isolated
@Slf4j
class ControlThreadCompletionOnLoggingFailureTest extends ParallelEoSStreamProcessorTestBase {

    /** The message rendered on the clean exit path, matched exactly so nothing else in the JVM is affected. */
    private static final String CLEAN_EXIT_MESSAGE = "Control loop ending clean (state:{})...";

    private static final long WAIT_SECONDS = 30;

    @BeforeEach
    void primeARecordToProcess() {
        super.primeFirstRecord();
    }

    @Test
    @Timeout(60)
    void aThrowFromTheCleanExitLogStillCompletesTheFuture() {
        parallelConsumer.poll(recordContexts -> log.debug("Processed {}", recordContexts));
        CompletableFuture<Void> completion = parallelConsumer.controlThreadCompletion();

        try (var ignoredInstalled = new FailTheLoggingBindingFor(CLEAN_EXIT_MESSAGE)) {
            try {
                parallelConsumer.closeDrainFirst();
            } catch (Exception surfacedByTheDyingControlThread) {
                // Expected, and not what this test is about: close() waits on the task's own Future, which now
                // fails. The point is what the SEPARATE completion did while that happened.
                log.debug("close reported the control thread's failure, as it should",
                        surfacedByTheDyingControlThread);
            }

            assertWithMessage("the control task ended, so its completion must have been armed - a waiter told "
                    + "nothing about it waits for an event that has already happened")
                    .that(completion.isDone())
                    .isTrue();
            ExecutionException thrown = assertThrows(ExecutionException.class,
                    () -> completion.get(WAIT_SECONDS, TimeUnit.SECONDS));
            assertWithMessage("and it must carry the throwable that ended the task, so the completion and the "
                    + "submitted Future do not disagree about why it ended")
                    .that(thrown)
                    .hasCauseThat()
                    .isInstanceOf(FakeRuntimeException.class);
        }
    }

    /**
     * Makes the logging binding throw for one exact message, for as long as this is open. A {@link TurboFilter}
     * rather than an appender: logback swallows an exception thrown from an appender and reports it as a status, so
     * an appender cannot stand in for a binding that throws, whereas the turbo-filter chain is consulted inline by
     * {@link Logger} and anything it throws propagates to the caller.
     */
    private static final class FailTheLoggingBindingFor extends TurboFilter implements AutoCloseable {

        private final String messageToFail;

        private FailTheLoggingBindingFor(String messageToFail) {
            this.messageToFail = messageToFail;
            ((LoggerContext) LoggerFactory.getILoggerFactory()).addTurboFilter(this);
            start();
        }

        @Override
        public FilterReply decide(Marker marker, Logger logger, Level level, String format, Object[] params,
                                  Throwable t) {
            if (messageToFail.equals(format)) {
                throw new FakeRuntimeException("deliberate - the logging binding throws on the clean exit line");
            }
            return FilterReply.NEUTRAL;
        }

        @Override
        public void close() {
            ((LoggerContext) LoggerFactory.getILoggerFactory()).getTurboFilterList().remove(this);
            stop();
        }
    }
}
