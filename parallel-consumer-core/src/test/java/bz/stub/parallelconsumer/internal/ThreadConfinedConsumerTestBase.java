package bz.stub.parallelconsumer.internal;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * The two-threads-one-consumer fixture every ownership test needs: a {@link MockConsumer} behind a
 * {@link ThreadConfinedConsumer}, and one named foreign thread to play the other party.
 * <p>
 * Shared rather than repeated because the guard's whole subject is <em>which thread</em> called, so
 * every test of it needs a second thread it can name - and two independent copies of that fixture had
 * already drifted: only one of them unwrapped {@link ExecutionException}, so an assertion failing on
 * the foreign thread surfaced in the other as a wrapper naming neither the assertion nor the thread.
 *
 * @see <a href="https://github.com/confluentinc/parallel-consumer/issues/857">#857</a>
 */
abstract class ThreadConfinedConsumerTestBase {

    /**
     * The foreign thread's name. A constant because the guard's rejection message must carry the
     * offending thread's name, and a test asserting that should not restate the literal.
     */
    static final String FOREIGN_THREAD_NAME = "test-foreign-thread";

    MockConsumer<String, String> delegate;

    ThreadConfinedConsumer<String, String> confined;

    private ExecutorService foreignThread;

    @BeforeEach
    void setUpConfinedConsumerFixture() {
        delegate = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        confined = new ThreadConfinedConsumer<>(delegate);
        foreignThread = Executors.newSingleThreadExecutor(runnable -> {
            Thread thread = new Thread(runnable, FOREIGN_THREAD_NAME);
            thread.setDaemon(true);
            return thread;
        });
    }

    @AfterEach
    void tearDownConfinedConsumerFixture() throws InterruptedException {
        foreignThread.shutdownNow();
        foreignThread.awaitTermination(5, TimeUnit.SECONDS);
    }

    /**
     * Runs {@code action} on the foreign thread and waits for it, rethrowing whatever it threw rather
     * than the {@link ExecutionException} wrapper - an assertion that fails over there has to fail the
     * test over here, naming its own cause.
     */
    void onOtherThread(Runnable action) throws Exception {
        try {
            foreignThread.submit(action).get(10, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof Exception cause) throw cause;
            throw e;
        }
    }
}
