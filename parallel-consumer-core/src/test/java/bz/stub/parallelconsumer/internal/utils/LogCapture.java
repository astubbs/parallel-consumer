package bz.stub.parallelconsumer.internal.utils;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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
 * Events are held in a synchronized list because the code under test typically logs from the parallel-consumer's own
 * threads while the test reads.
 *
 * @author Antony Stubbs
 */
public class LogCapture implements AutoCloseable {

    private final Logger logger;

    private final Level originalLevel;

    private final List<ILoggingEvent> captured = Collections.synchronizedList(new ArrayList<>());

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
        logger.setLevel(captureFrom);

        appender.setName("log-capture-" + loggingClass.getSimpleName());
        appender.setContext(logger.getLoggerContext());
        appender.start();
        logger.addAppender(appender);
    }

    public List<ILoggingEvent> events() {
        synchronized (captured) {
            return new ArrayList<>(captured);
        }
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
