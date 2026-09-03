package bz.stub.parallelconsumer.internal;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ForeignThread;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.util.concurrent.ExecutionException;

/**
 * The two-threads-one-consumer fixture every ownership test needs: a {@link MockConsumer} behind a
 * {@link ThreadConfinedConsumer}, and one named foreign thread to play the other party.
 * <p>
 * Shared rather than repeated because the guard's whole subject is <em>which thread</em> called, so
 * every test of it needs a second thread it can name - and two independent copies of that fixture had
 * already drifted: only one of them unwrapped {@link ExecutionException}, so an assertion failing on
 * the foreign thread surfaced in the other as a wrapper naming neither the assertion nor the thread.
 * <p>
 * The thread half is now {@link ForeignThread}, because a confinement test in another package needed the
 * same fixture and would otherwise have been the third copy. What stays here is the consumer half.
 *
 * @see <a href="https://github.com/confluentinc/parallel-consumer/issues/857">#857</a>
 */
abstract class ThreadConfinedConsumerTestBase {

    /**
     * The foreign thread's name. A constant because the guard's rejection message must carry the
     * offending thread's name, and a test asserting that should not restate the literal.
     */
    static final String FOREIGN_THREAD_NAME = ForeignThread.DEFAULT_NAME;

    MockConsumer<String, String> delegate;

    ThreadConfinedConsumer<String, String> confined;

    private ForeignThread foreignThread;

    @BeforeEach
    void setUpConfinedConsumerFixture() {
        delegate = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        confined = new ThreadConfinedConsumer<>(delegate);
        // Stop whatever is already here before the field stops pointing at it. JUnit runs the
        // teardown below between tests so this cannot fire today, and the threads are daemons so a
        // leaked pool could not hold a JVM open either - but overwriting a live ExecutorService
        // field is a leak shape regardless of who currently guarantees it cannot happen, and
        // fb-contrib reports it here (HES_EXECUTOR_OVERWRITTEN_WITHOUT_SHUTDOWN). Not having the
        // shape costs three lines; the only sanctioned alternative is switching the rule off for the
        // whole repository, which docs/inflight/static-spotbugs-rule-registry.md prices far higher.
        if (foreignThread != null) {
            foreignThread.close();
        }
        foreignThread = new ForeignThread();
    }

    @AfterEach
    void tearDownConfinedConsumerFixture() {
        foreignThread.close();
    }

    /**
     * Runs {@code action} on the foreign thread and waits for it, rethrowing whatever it threw rather
     * than the {@link ExecutionException} wrapper - an assertion that fails over there has to fail the
     * test over here, naming its own cause.
     */
    void onOtherThread(Runnable action) throws Exception {
        foreignThread.run(action);
    }
}
