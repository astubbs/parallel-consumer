package bz.stub.parallelconsumer.internal;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.truth.Truth.assertThat;

/**
 * Proves the virtual-thread path <b>ran</b>, rather than proving it exists.
 *
 * <h2>Why an assumption would have been the wrong tool</h2>
 * <p>
 * The natural way to write these is {@code Assumptions.assumeTrue(virtualThreadsAvailable())}, and it is the
 * failure mode this repository has already shipped once: a test that skips on the JDK CI runs and <em>passes</em>
 * reports green having verified nothing
 * ({@code docs/solutions/workflow-issues/a-check-that-reports-success-without-having-run.md}).
 * <p>
 * So the assumption is inverted and keyed on <b>intent</b> rather than on capability. When
 * {@code -Dpc.virtualThreads=true} selects the mode - which is how CI's execution-mode matrix runs the suite - a
 * runtime that cannot provide virtual threads is a <b>failure</b>, because the lane was asked for a thing it did
 * not deliver. An assumption remains only for the developer running the default suite on a JDK 17 with no mode
 * selected, where nothing was asked for and nothing is owed.
 *
 * <p>
 * <b>Every test in this class requires the mode.</b> That is a contract, not a coincidence:
 * {@code bin/check-execution-mode.sh} counts how many tests in this class actually executed and calls a lane broken
 * when the answer is zero, so a test that can pass without virtual threads would inflate that count and let a JDK 17
 * run claim it had exercised the mode. The option-level tests that run on any JDK live in
 * {@link VirtualThreadOptionTest}.
 *
 * @see ParallelConsumerOptions#isUseVirtualThreads()
 */
class VirtualThreadExecutionModeTest {

    /**
     * The system property {@link ParallelConsumerOptions} defaults {@code useVirtualThreads} from, and the one
     * CI's execution-mode axis sets. Its presence is what turns a skip into a failure below.
     */
    private static final String MODE_PROPERTY = "pc.virtualThreads";

    /**
     * Mirrors {@code ParallelConsumerOptions#virtualThreadsValidation()} exactly - both reflective targets, not
     * just {@code Thread.ofVirtual}. Probing a subset here would let the test and production disagree about what
     * "supported" means on some future JVM that has one and not the other.
     */
    private static boolean virtualThreadsAvailable() {
        try {
            Class.forName("java.lang.Thread").getMethod("ofVirtual");
            java.util.concurrent.Executors.class
                    .getMethod("newThreadPerTaskExecutor", java.util.concurrent.ThreadFactory.class);
            return true;
        } catch (ReflectiveOperationException e) {
            return false;
        }
    }

    private static boolean isVirtual(Thread thread) {
        try {
            Method isVirtual = Thread.class.getMethod("isVirtual");
            return (boolean) isVirtual.invoke(thread);
        } catch (ReflectiveOperationException e) {
            return false;
        }
    }

    /**
     * Fails - never skips - when the mode was selected but the runtime cannot serve it.
     */
    private static void requireVirtualThreadsWhenModeSelected() {
        boolean modeSelected = Boolean.getBoolean(MODE_PROPERTY);
        if (modeSelected && !virtualThreadsAvailable()) {
            throw new AssertionError(String.format(
                    "-D%s=true selected the virtual-thread execution mode, but this JVM (%s %s) cannot provide "
                            + "virtual threads. This run has verified nothing about that mode. Run it on a JDK 21+ "
                            + "runtime - the build JDK does not have to move, only the test JVM (-Djvm.location).",
                    MODE_PROPERTY, System.getProperty("java.vm.name"), System.getProperty("java.version")));
        }
        Assumptions.assumeTrue(virtualThreadsAvailable(),
                "No execution mode was selected and this JVM has no virtual threads - nothing was asked for. "
                        + "Set -D" + MODE_PROPERTY + "=true to make this a failure instead.");
    }

    private static ParallelConsumerOptions.ParallelConsumerOptionsBuilder<String, String> options() {
        return ParallelConsumerOptions.<String, String>builder()
                .consumer(new MockConsumer<>(OffsetResetStrategy.EARLIEST));
    }

    @Test
    void theUserFunctionRunsOnANamedVirtualThread() throws Exception {
        requireVirtualThreadsWhenModeSelected();

        try (var pc = new TestParallelEoSStreamProcessor<>(options().useVirtualThreads(true).build())) {
            ExecutorService pool = pc.getWorkerThreadPool().get();
            var seen = new AtomicReference<Thread>();
            var ran = new CountDownLatch(1);

            pool.execute(() -> {
                seen.set(Thread.currentThread());
                ran.countDown();
            });
            assertThat(ran.await(30, TimeUnit.SECONDS)).isTrue();

            assertThat(isVirtual(seen.get())).isTrue();
            // Named, because a thread dump at the concurrency where virtual threads earn their place holds
            // thousands of them and anonymous ones make it unreadable.
            assertThat(seen.get().getName()).startsWith("pc-vt-");
        }
    }

    @Test
    void thePoolIsNotAThreadPoolExecutorInThisMode() {
        requireVirtualThreadsWhenModeSelected();

        try (var pc = new TestParallelEoSStreamProcessor<>(options().useVirtualThreads(true).build())) {
            // The premise of the whole change: the executor the pressure system used to introspect is gone.
            assertThat(pc.getWorkerThreadPool().get()).isNotInstanceOf(ThreadPoolExecutor.class);
        }
    }

    @Test
    void theInFlightTargetIsNotMultipliedByTheLoadFactorInThisMode() {
        requireVirtualThreadsWhenModeSelected();

        var withVirtualThreads = options().useVirtualThreads(true).maxConcurrency(7).batchSize(3).build();
        var withPlatformThreads = options().useVirtualThreads(false).maxConcurrency(7).batchSize(3).build();

        try (var vt = new TestParallelEoSStreamProcessor<>(withVirtualThreads);
             var platform = new TestParallelEoSStreamProcessor<>(withPlatformThreads)) {

            int target = withVirtualThreads.getTargetAmountOfRecordsInFlight();

            // There is no executor queue for the surplus to wait in, so multiplying would put N x maxConcurrency
            // records RUNNING rather than N x maxConcurrency records buffered.
            assertThat(vt.getTargetLoad()).isEqualTo(target);

            // ...and the default engine is untouched: still target x the factor.
            assertThat(platform.getTargetLoad()).isGreaterThan(target);
        }
    }

    @Test
    void aPoolWithNoQueueReportsAnEmptyQueueRatherThanAnUnknownOne() {
        requireVirtualThreadsWhenModeSelected();

        try (var pc = new TestParallelEoSStreamProcessor<>(options().useVirtualThreads(true).build())) {
            var accounting = pc.userFunctionTaskAccounting();

            // Not a hardcoded zero standing in for "cannot tell": nothing has been submitted, so the derived
            // figure is genuinely zero and would move if something were.
            assertThat(accounting.getQueued()).isEqualTo(0);
            assertThat(accounting.getActive()).isEqualTo(0);

            accounting.onSubmitting();
            assertThat(accounting.getQueued()).isEqualTo(1);
        }
    }

    @Test
    void aVirtualThreadWorkerIsInterruptedByShutdown() {
        requireVirtualThreadsWhenModeSelected();

        var pc = new TestParallelEoSStreamProcessor<>(options().useVirtualThreads(true).build());
        pc.getWorkerThreadPool().get().execute(() -> {
            try {
                Thread.sleep(TimeUnit.MINUTES.toMillis(5));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        long start = System.nanoTime();
        pc.close();
        // An unbounded pool still has to come down promptly; shutdownNow() must reach a parked virtual thread.
        assertThat(TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - start)).isLessThan(60L);
    }

}
