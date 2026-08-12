package io.confluent.parallelconsumer.integrationTests.utils;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.IThrowableProxy;
import ch.qos.logback.core.AppenderBase;
import io.confluent.parallelconsumer.internal.AbstractParallelEoSStreamProcessor;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Makes exceptions thrown on the WORKER path observable to a test.
 * <p>
 * Nothing in main reads {@code WorkContainer#future}, so when
 * {@code AbstractParallelEoSStreamProcessor#runUserFunction}'s catch-all fails a batch it logs the exception and
 * rethrows it into a future nobody looks at. From the outside the only symptom is a redelivery - which is
 * indistinguishable from a legitimate retry, and completely invisible if the record is never redelivered because
 * the instance is shutting down. Attaching to the logger the exception is reported on is the only way to name it.
 * <p>
 * This covers the produce path too, and that is why a transactional test needs it: {@code pollAndProduceMany}
 * sends the user function's result records and blocks on their acks <em>after</em> the user function has
 * returned, inside {@code ParallelEoSStreamProcessor#processAndProduceResults}. A send that fails terminally
 * therefore surfaces nowhere the user function could catch it - it arrives here, or nowhere.
 * <p>
 * Filtered by thread name: PC names its worker threads {@code pc-pool-N-thread-M-<myId>}, so an instance built
 * with a per-test nonce in its {@code myId} cannot have its failures confused with those of a sibling test
 * sharing the JVM - which matters, because the integration lane runs methods in parallel.
 *
 * @author Antony Stubbs
 */
public class WorkerFunctionFailureCapture extends AppenderBase<ILoggingEvent> implements AutoCloseable {

    /**
     * The message {@code runUserFunction}'s catch-all logs. Matched rather than the exception type, because the
     * exception is whatever the user function or the produce path threw.
     */
    private static final String USER_FUNCTION_STAGE = "Exception caught in user function running stage";

    private final String threadNameMarker;

    private final List<String> failures = new CopyOnWriteArrayList<>();

    private final Logger target = (Logger) LoggerFactory.getLogger(AbstractParallelEoSStreamProcessor.class);

    /**
     * @param threadNameMarker a string carried by the {@code myId} of every instance whose failures this capture
     *                         is about - normally a per-test nonce
     */
    public WorkerFunctionFailureCapture(String threadNameMarker) {
        this.threadNameMarker = threadNameMarker;
        setName("worker-function-failure-capture-" + threadNameMarker);
        setContext(target.getLoggerContext());
        start();
        target.addAppender(this);
    }

    @Override
    protected void append(ILoggingEvent event) {
        if (!event.getThreadName().contains(threadNameMarker)) {
            return;
        }
        if (!event.getFormattedMessage().contains(USER_FUNCTION_STAGE)) {
            return;
        }
        failures.add(event.getThreadName() + ": " + describe(event.getThrowableProxy()));
    }

    private static String describe(IThrowableProxy proxy) {
        StringBuilder chain = new StringBuilder();
        for (IThrowableProxy p = proxy; p != null; p = p.getCause()) {
            if (chain.length() > 0) {
                chain.append(" <- ");
            }
            chain.append(p.getClassName()).append('(').append(p.getMessage()).append(')');
        }
        return chain.toString();
    }

    /**
     * @return one line per captured worker-path failure, each holding the failing thread and the whole cause
     *         chain - so an assertion that fails on this reports what actually went wrong rather than only that
     *         something did
     */
    public List<String> describe() {
        return new ArrayList<>(failures);
    }

    /**
     * @return the captured failures whose cause chain mentions {@code exceptionType} anywhere - by simple or
     *         fully qualified name, since the chain is rendered as text
     */
    public List<String> mentioning(Class<? extends Throwable> exceptionType) {
        return failures.stream()
                .filter(failure -> failure.contains(exceptionType.getName())
                        || failure.contains(exceptionType.getSimpleName()))
                .collect(java.util.stream.Collectors.toList());
    }

    @Override
    public void close() {
        target.detachAppender(this);
        stop();
    }
}
