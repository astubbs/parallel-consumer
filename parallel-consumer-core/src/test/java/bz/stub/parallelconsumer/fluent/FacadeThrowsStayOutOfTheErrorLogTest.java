package bz.stub.parallelconsumer.fluent;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.PCRetriableException;
import bz.stub.parallelconsumer.ParallelConsumer;
import bz.stub.parallelconsumer.internal.AbstractParallelEoSStreamProcessor;
import bz.stub.parallelconsumer.internal.utils.LogCapture;
import ch.qos.logback.classic.Level;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.parallel.Isolated;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.truth.Truth.assertThat;

/**
 * A park is an outcome the definition asked for, not a fault, so it must not read as one in the log (KTD4).
 *
 * <h2>Why this needs a test rather than a comment</h2>
 * The facade hands a record back by <b>failing</b> it, because that is the only hand-back the engine has. The engine
 * logs a failed user function at error level unless the exception is its own retriable one - so an ordinary
 * exception used as a park marker would print a stack trace per parked record, and an operator would read a working
 * definition as a broken one. What keeps it quiet is that every facade-originated throw extends
 * {@code PCRetriableException}, which is one word in a class declaration and nothing else.
 * <p>
 * The second test is the control arm: it proves the assertion can fail, by throwing something the facade did
 * <em>not</em> originate and finding the error line the first test asserts is absent.
 * <p>
 * {@link Isolated} because {@link LogCapture} raises a JVM-global logger level, and this module runs its tests
 * concurrently - the class it captures is the busiest logger in the engine.
 */
@Isolated
@Timeout(120)
class FacadeThrowsStayOutOfTheErrorLogTest extends AbstractFluentEngineTest {


    /**
     * The engine's one error line for a failed user function. Asserting on its text rather than on "no errors at
     * all" keeps the test about this claim: an unrelated engine error should fail its own test, not this one.
     */
    private static final String USER_FUNCTION_FAILURE_LINE = "Exception caught in user function running stage";



    @Test
    void parkingARecordPrintsNoErrorAtAll() {
        var pc = ParallelConsumer.connect(props());
        pc.string(TOPIC)
                .retryLimit(0)
                .retryDelay(Duration.ofMillis(10))
                .process(context -> Outcome.park("this record is hopeless and the function knows it"));

        try (LogCapture logs = LogCapture.of(AbstractParallelEoSStreamProcessor.class, Level.DEBUG)) {
            runUntilOneRecordParks(pc);

            assertThat(logs.messagesAt(Level.ERROR, USER_FUNCTION_FAILURE_LINE)).isEmpty();
            // ...and it is not merely unlogged: the engine saw the failure and classified it as retriable.
            assertThat(logs.messagesAt(Level.DEBUG, "PCRetriableException", USER_FUNCTION_FAILURE_LINE))
                    .isNotEmpty();
        }
    }

    /**
     * A record that runs out of attempts takes the same quiet path - and this is the case an operator would meet in
     * volume, since every record that exhausts its retries parks.
     */
    @Test
    void aRecordThatRunsOutOfAttemptsPrintsNoErrorForTheParkItself() {
        var pc = ParallelConsumer.connect(props());
        pc.string(TOPIC)
                .retryLimit(1)
                .retryDelay(Duration.ofMillis(10))
                .process(context -> {
                    // A retriable throw, so the retries themselves are quiet too and what remains in the capture
                    // could only have come from the park.
                    throw new PCRetriableException("the downstream is down");
                });

        try (LogCapture logs = LogCapture.of(AbstractParallelEoSStreamProcessor.class, Level.DEBUG)) {
            runUntilOneRecordParks(pc);

            assertThat(logs.messagesAt(Level.ERROR, USER_FUNCTION_FAILURE_LINE)).isEmpty();
        }
    }

    /**
     * The control arm. A throw the facade did not originate - the user's own, and not a retriable one - is logged
     * at error exactly as it is on the classic API, which is R9's whole content: the distinction is logging.
     * <p>
     * Without this the first two tests would pass against a capture that was simply never wired up.
     */
    @Test
    void aPlainUserExceptionIsStillLoggedAtErrorAsItAlwaysHasBeen() {
        var attempts = new AtomicInteger();
        var pc = ParallelConsumer.connect(props());
        pc.string(TOPIC)
                .retryLimit(1)
                .retryDelay(Duration.ofMillis(10))
                .process(context -> {
                    attempts.incrementAndGet();
                    // NOT FakeRuntimeException: that extends the engine's retriable exception, so it would be
                    // logged at debug and this arm would prove nothing. Finding that out is what the arm is for.
                    throw new IllegalStateException("the user's own failure");
                });

        try (LogCapture logs = LogCapture.of(AbstractParallelEoSStreamProcessor.class, Level.DEBUG)) {
            runUntilOneRecordParks(pc);

            // Two runs: the first attempt and its one retry.
            assertThat(attempts.get()).isEqualTo(2);
            // ...and ONE error line, not two. The first failure is logged at error exactly as the classic API
            // logs it, which is what makes this arm discriminating. The second is the attempt that exhausted the
            // limit, so its exception left the wrapper as the cause of the park's retriable throw rather than on
            // its own - which is why the wrapper logs the park at WARN with that failure attached: it is then
            // the only place an operator sees why the record gave up.
            assertThat(logs.messagesAt(Level.ERROR, USER_FUNCTION_FAILURE_LINE)).hasSize(1);
        }
    }

    /**
     * Quiet is not the same as silent. A record that gives up is reported once, at warning level, with the failure
     * that ended it - because the engine will not report that last failure: it left the wrapper as the cause of a
     * retriable throw, which is logged at debug.
     */
    @Test
    void theParkItselfIsReportedOnceWithTheFailureThatEndedIt() {
        var pc = ParallelConsumer.connect(props());
        pc.string(TOPIC)
                .retryLimit(1)
                .retryDelay(Duration.ofMillis(10))
                .process(context -> {
                    throw new IllegalStateException("the user's own failure");
                });

        try (LogCapture parkLines = LogCapture.of(RouteDispatcher.class, Level.WARN)) {
            runUntilOneRecordParks(pc);

            // Once per record, not once per attempt.
            String line = parkLines.onlyMessageAt(Level.WARN, "Parked", TOPIC + "-0@0");
            assertThat(line).contains("after 2 attempt(s)");
            assertThat(line).contains("ran out of attempts");
        }
    }

    /**
     * Start the definition, publish the one record every scenario here uses, and wait for it to park. Every test in
     * this class is the same three steps around a different definition and a different log assertion, so the steps
     * are here and only the difference is in each test.
     */
    private void runUntilOneRecordParks(ParallelConsumerDefinition pc) {
        handle = runtime.startAndAssign(pc, 1);
        runtime.publish(TOPIC, 0, 0, "key-0", "an order");
        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(pc.dispatcher().parkedCount()).isEqualTo(1));
    }
}
