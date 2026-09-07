package bz.stub.parallelconsumer.internal.utils;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * Captures the log events emitted by one class, so a test can assert on the <em>shape</em> of a log line rather than
 * only on the code that produces it.
 * <p>
 * Some log lines are a contract with the operator: a line that must stay bounded (see
 * {@link RecordBatchSummary}) is only bounded until someone interpolates a collection into it again, and no
 * ordinary assertion notices. Capturing the line is what makes that regression fail a test.
 * <p>
 * Raises the logger to the requested level for the duration of the capture (test config runs at {@code info}, so
 * {@code DEBUG} detail lines would otherwise never reach an appender at all) and restores it on {@link #close()}.
 * Events accumulate in a {@link ConcurrentLinkedQueue} because the code under test typically logs from the
 * parallel-consumer's own threads while the test reads.
 * <p>
 * <b>This mutates JVM-global state and nothing enforces the discipline that makes it safe</b> - a logger is shared by
 * every instance of its class, and this module runs JUnit thread-parallel outside {@code -Pci}
 * ({@code junit.jupiter.execution.parallel.mode.default=concurrent} in {@code parallel-consumer-core/pom.xml}). Two
 * obligations follow, and a capture that skips either reads someone else's lines or floods them with detail:
 * <ol>
 *     <li><b>Always in a try-with-resources.</b> {@link #close()} is the only thing that detaches the appender and
 *     puts the level back; an un-closed capture leaks both into every test that follows, silently.</li>
 *     <li><b>Either isolate the test, or scope what you read.</b> Raising a busy logger (an
 *     {@code AbstractParallelEoSStreamProcessor}) to {@code DEBUG} perturbs the timing-sensitive close and shutdown
 *     tests, so those callers carry {@link org.junit.jupiter.api.parallel.Isolated}. A quiet logger does not need
 *     that, but its reader still must filter on something unique to itself - a per-test topic name - or a concurrent
 *     test's line lands in the capture and an exact count becomes a race.</li>
 * </ol>
 *
 * @author Antony Stubbs
 */
public class LogCapture implements AutoCloseable {

    private final Logger logger;

    private final Level originalLevel;

    private final Queue<ILoggingEvent> captured = new ConcurrentLinkedQueue<>();

    private final AppenderBase<ILoggingEvent> appender = new AppenderBase<ILoggingEvent>() {
        @Override
        protected void append(ILoggingEvent event) {
            captured.add(event);
        }
    };

    /**
     * Capture everything the class logs, down to {@link Level#TRACE}.
     */
    public static LogCapture of(Class<?> loggingClass) {
        return of(loggingClass, Level.TRACE);
    }

    public static LogCapture of(Class<?> loggingClass, Level captureFrom) {
        return new LogCapture(loggingClass, captureFrom);
    }

    private LogCapture(Class<?> loggingClass, Level captureFrom) {
        this.logger = (Logger) getLogger(loggingClass);
        this.originalLevel = logger.getLevel();
        // level first, appender last: if anything here threw, the constructor's caller never gets an instance to
        // close, and a leaked raised level is only noisy while a leaked appender corrupts every later test's reads
        logger.setLevel(captureFrom);

        appender.setName("log-capture-" + loggingClass.getSimpleName());
        appender.setContext(logger.getLoggerContext());
        appender.start();
        logger.addAppender(appender);
    }

    /**
     * @return a snapshot, so the caller can iterate it while the code under test keeps logging
     */
    public List<ILoggingEvent> events() {
        return new ArrayList<>(captured);
    }

    /**
     * @return the fully formatted messages logged at exactly this level - i.e. what an operator would read
     */
    public List<String> messagesAt(Level level) {
        return events().stream()
                .filter(event -> event.getLevel() == level)
                .map(ILoggingEvent::getFormattedMessage)
                .collect(Collectors.toList());
    }

    @Override
    public void close() {
        logger.detachAppender(appender);
        appender.stop();
        logger.setLevel(originalLevel);
    }

}
