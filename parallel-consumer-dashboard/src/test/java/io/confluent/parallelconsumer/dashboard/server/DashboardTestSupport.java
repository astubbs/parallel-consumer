package io.confluent.parallelconsumer.dashboard.server;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.confluent.parallelconsumer.dashboard.DashboardOptions;
import io.confluent.parallelconsumer.dashboard.snapshot.PcMeterFixture;
import io.confluent.parallelconsumer.dashboard.snapshot.SnapshotPublisher;
import io.confluent.parallelconsumer.dashboard.snapshot.StateSampler;
import io.confluent.parallelconsumer.internal.AbstractParallelEoSStreamProcessor;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ServerSocket;

/**
 * Shared fixtures for the server tests: publishers seeded without a broker, free ports, and log capture.
 * <p>
 * None of these tests need a running Parallel Consumer or a Kafka broker. The whole point of the snapshot design is
 * that the HTTP layer reads an immutable value, so it can be handed one built from a
 * {@link PcMeterFixture}-shaped registry and be exercised exactly as it would be in production.
 */
final class DashboardTestSupport {

    private DashboardTestSupport() {
    }

    /**
     * A publisher over a registry shaped like a fully-populated Parallel Consumer, with no samples taken yet.
     */
    static SnapshotPublisher populatedPublisher() {
        return new SnapshotPublisher(
                new StateSampler((AbstractParallelEoSStreamProcessor<?, ?>) null, PcMeterFixture.fullyPopulated().getRegistry()));
    }

    /**
     * A publisher over an empty registry - what a dashboard wired to the wrong registry sees.
     */
    static SnapshotPublisher emptyPublisher() {
        return new SnapshotPublisher(
                new StateSampler((AbstractParallelEoSStreamProcessor<?, ?>) null, new PcMeterFixture().getRegistry()));
    }

    /**
     * Options starting from a port that was free a moment ago, with room to walk if something grabbed it in between.
     * Every server test uses this rather than 8080, so a developer with something on 8080 does not see phantom
     * failures - and so the tests do not fight each other when surefire forks.
     */
    static DashboardOptions.DashboardOptionsBuilder testOptions() {
        return DashboardOptions.builder().port(freePort()).maxPortAttempts(50);
    }

    /**
     * A port that was unbound at the moment this returned. Deliberately not a promise - the port walk is what makes
     * the residual race a non-event, and using this <em>with</em> the walk is closer to how the server really starts.
     */
    static int freePort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            socket.setReuseAddress(true);
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new UncheckedIOException("Could not find a free port for a dashboard test", e);
        }
    }

    /**
     * Attaches a list appender to a class's logger at INFO, so a test can assert on what was - and crucially what was
     * not - logged. The level is forced rather than inherited: the assertions about the startup line are assertions
     * about INFO events, and a repository-wide logback configuration raising the threshold would turn them into
     * silent passes.
     */
    static LogCapture captureLogs(Class<?> type) {
        ch.qos.logback.classic.Logger logger = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(type);
        return new LogCapture(logger);
    }

    static final class LogCapture implements AutoCloseable {

        private final ch.qos.logback.classic.Logger logger;

        private final ListAppender<ILoggingEvent> appender = new ListAppender<>();

        private final Level originalLevel;

        private LogCapture(ch.qos.logback.classic.Logger logger) {
            this.logger = logger;
            this.originalLevel = logger.getLevel();
            logger.setLevel(Level.INFO);
            appender.start();
            logger.addAppender(appender);
        }

        java.util.List<ILoggingEvent> events() {
            return new java.util.ArrayList<>(appender.list);
        }

        java.util.List<String> formattedMessages() {
            java.util.List<String> messages = new java.util.ArrayList<>();
            for (ILoggingEvent event : appender.list) {
                messages.add(event.getFormattedMessage());
            }
            return messages;
        }

        @Override
        public void close() {
            logger.detachAppender(appender);
            appender.stop();
            logger.setLevel(originalLevel);
        }
    }
}
