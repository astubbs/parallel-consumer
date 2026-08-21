package bz.stub.parallelconsumer.internal;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.PollContextInternal;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.Consumer;
import java.util.function.Function;

import static com.google.common.truth.Truth.assertWithMessage;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Closing must release the worker pool even when an earlier step of the close sequence throws.
 * <p>
 * {@code innerDoClose} used to run {@code brokerPollSubsystem.drain()} and then shut the pool down as bare
 * sequential statements, so a drain that threw returned from {@code close()} without ever calling
 * {@code shutdown()}. The pool's threads come from {@code Executors.defaultThreadFactory()} and are therefore
 * <b>not daemon threads</b>, and the pool is built eagerly in the constructor - so every thread the instance
 * started stays alive for the rest of the JVM's life. Nothing fails at the time: the drain exception propagates and
 * looks like the whole story, so the leak is invisible where it happens and surfaces later as CPU contention with
 * no obvious owner. Inside a surefire fork, which reuses one JVM across many classes, one badly closed instance
 * quietly slows down or times out unrelated tests that run afterwards.
 * <p>
 * This is the un-hardened half of an existing fix. Upstream's confluentinc#809 fix ({@code 5ce0b2f69}) created
 * {@code innerDoClose} and wrapped the <em>tail</em> of the sequence - the commit, the poller close, the consumer
 * close - each in its own try/catch, and left the <em>head</em> (drain, then the pool shutdown) unguarded.
 *
 * <h2>Why the injected failure is the poller's {@code drain()}, and why it throws every time</h2>
 * The obvious seam looks like the consumer: {@link BrokerPollSystem#drain()}'s only real action is
 * {@code consumerManager.wakeup()}, so a {@link MockConsumer} whose {@code wakeup()} throws would make the drain
 * throw. It does not work, for two independent reasons, and both are the reason this test injects one level higher.
 * <ol>
 *     <li><b>{@code ConsumerManager#wakeup()} is guarded by {@code if (pollingBroker.get())}</b> - it forwards to
 *     {@code consumer.wakeup()} only while the poll thread is actually inside {@code consumer.poll()}. So a
 *     throwing {@code wakeup()} fires or does not fire depending on where the poll thread happens to be, which is a
 *     race, not a test.</li>
 *     <li><b>A consumer-level failure is single-shot, and the retry hides it.</b> {@code BrokerPollSystem#drain()}
 *     sets {@code runState = DRAINING} <em>before</em> calling {@code wakeup()}, and is guarded by
 *     {@code if (runState != State.DRAINING)}. Meanwhile {@code supervisorLoop}'s control task catches the escaping
 *     exception and calls {@code doClose} a <em>second</em> time. On that second pass the guard short-circuits, the
 *     drain no longer throws, and the pool is shut down after all - so a wakeup-based test would pass against the
 *     unfixed code and prove nothing. Only a drain that fails <em>persistently</em> exposes the leak, and that is
 *     also the case the {@code finally} genuinely has to cover.</li>
 * </ol>
 * The close mode is {@code DONT_DRAIN}, and that is load-bearing too. On the {@code DRAIN} path the control loop
 * has already run its own {@code drain()} - putting the poll system into {@code DRAINING} - before
 * {@code innerDoClose} is reached, so {@code innerDoClose}'s drain hits the same idempotence guard and does
 * nothing at all. {@code DONT_DRAIN} goes {@code RUNNING -> CLOSING} directly, never visiting the control loop's
 * {@code DRAINING} case, so {@code innerDoClose}'s drain is the first one and is the call that really acts.
 * <p>
 * Asserted on {@code isShutdown()} rather than a JVM-wide thread count deliberately: a thread count is
 * contaminated by everything else in the fork - including, ironically, the leak this test is about.
 */
@Slf4j
class CloseShutsDownWorkerPoolTest {

    /**
     * Distinctive enough to identify unambiguously as this test's own injected failure when it surfaces at the far
     * end of the close, wrapped in an {@code ExecutionException}.
     */
    static final String DRAIN_FAILURE = "injected drain failure - the poller refuses to drain";

    final Function<PollContextInternal<String, String>, List<String>> userFunction = context -> new ArrayList<>();
    final Consumer<String> callback = result -> {
    };

    TestParallelEoSStreamProcessor<String, String> pc;
    ThreadPoolExecutor pool;

    /**
     * Kept so teardown can stop the poll thread by hand. The normal route - {@code innerDoClose} reaching
     * {@code brokerPollSubsystem.closeAndWait()} - is exactly what the injected failure prevents, so without this
     * the test would leak a poll thread while asserting about a leaked worker pool.
     */
    FailingDrainPollSystem<String, String> poller;

    @BeforeEach
    void setup() {
        var options = ParallelConsumerOptions.<String, String>builder()
                // a real MockConsumer, not a Mockito mock: a mocked Consumer makes the instance self-close almost
                // immediately, so the pool reads as already shut down before the close under test even begins.
                // No producer, and the default PERIODIC_CONSUMER_ASYNCHRONOUS commit mode - a transactional
                // producer would need a real KafkaProducer for the producer wrapper's reflection.
                .consumer(new MockConsumer<String, String>(OffsetResetStrategy.LATEST))
                // short, so a close that fails to terminate fails fast rather than sitting on the 30s default
                .shutdownTimeout(Duration.ofSeconds(5))
                .drainTimeout(Duration.ofSeconds(5))
                .commitInterval(Duration.ofMillis(50))
                .build();

        var module = new PCModule<String, String>(options) {
            @Override
            protected BrokerPollSystem<String, String> brokerPoller(AbstractParallelEoSStreamProcessor<String, String> pc) {
                if (poller == null) {
                    poller = new FailingDrainPollSystem<>(consumerManager(), workManager(), pc, options());
                }
                return poller;
            }
        };

        pc = new TestParallelEoSStreamProcessor<>(options, module);
        // the pool is a lazy memoized supplier, already resolved by the constructor - pin the instance so both the
        // before and after assertions act on the same object
        pool = pc.workerThreadPool.get();
    }

    @AfterEach
    void tearDown() {
        if (poller != null) {
            try {
                poller.closeAndWait();
            } catch (Exception e) {
                log.debug("Poller did not close cleanly in teardown", e);
            }
        }
        if (pool != null) {
            pool.shutdownNow();
        }
    }

    /**
     * The defect, and both halves of the contract: guarding the shutdown must not swallow the original failure, and
     * the pool must be shut down anyway.
     */
    @Test
    void aDrainThatThrowsStillShutsTheWorkerPoolDown() {
        // innerDoClose runs on the control thread and the failure surfaces through close()'s wait on that thread's
        // future, so the instance genuinely has to be started. Built-but-never-started fails somewhere else
        // entirely, in waitForClose, on an empty controlThreadFuture.
        pc.supervisorLoop(userFunction, callback);
        poller.setFailOnDrain(true);

        assertWithMessage("fixture: the pool must be live before the close, or a passing test proves nothing - "
                + "an already-shut-down pool satisfies the assertion below for free")
                .that(pool.isShutdown()).isFalse();

        assertThatThrownBy(pc::close)
                .as("guarding the shutdown must not swallow the drain failure - the caller still has to be told "
                        + "the close went wrong")
                .hasRootCauseMessage(DRAIN_FAILURE);

        assertWithMessage("a drain that threw must still leave the worker pool shut down - its threads are not "
                + "daemon threads, so anything left running outlives the instance and the whole test fork")
                .that(pool.isShutdown()).isTrue();
    }

    /**
     * Guards against the fix over-reaching: an ordinary close, with nothing throwing, must still shut the pool down
     * and must still not throw. A {@code finally} that changed the happy path would be caught here.
     */
    @Test
    void anOrdinaryCloseStillShutsTheWorkerPoolDown() {
        pc.supervisorLoop(userFunction, callback);

        assertWithMessage("fixture: the pool must be live before the close")
                .that(pool.isShutdown()).isFalse();

        assertThatCode(pc::close)
                .as("a close with nothing failing must complete normally")
                .doesNotThrowAnyException();

        assertWithMessage("normal operation must be unaffected - the pool is still shut down")
                .that(pool.isShutdown()).isTrue();
    }

    /**
     * A poll system whose {@code drain()} fails persistently, armed only once the instance is running.
     * <p>
     * Arming matters: {@code drain()} is not called during startup, but a subsystem rigged to fail from
     * construction is a subsystem that can fail during wiring, and then the test measures the wiring rather than
     * the close. Persistence matters for the reason set out on the enclosing class - a single-shot failure is
     * absorbed by {@code supervisorLoop}'s second {@code doClose} attempt.
     */
    static class FailingDrainPollSystem<K, V> extends BrokerPollSystem<K, V> {

        private volatile boolean failOnDrain = false;

        FailingDrainPollSystem(ConsumerManager<K, V> consumerMgr,
                               bz.stub.parallelconsumer.state.WorkManager<K, V> wm,
                               AbstractParallelEoSStreamProcessor<K, V> pc,
                               ParallelConsumerOptions<K, V> options) {
            super(consumerMgr, wm, pc, options);
        }

        void setFailOnDrain(boolean failOnDrain) {
            this.failOnDrain = failOnDrain;
        }

        @Override
        public void drain() {
            if (failOnDrain) {
                // a fresh instance each call, so the exception that arrives at the assertion cannot be a stale one
                // held from an earlier attempt
                throw new IllegalStateException(DRAIN_FAILURE);
            }
            super.drain();
        }
    }
}
