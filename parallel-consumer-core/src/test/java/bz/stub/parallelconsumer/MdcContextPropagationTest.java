package bz.stub.parallelconsumer;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.internal.AbstractParallelEoSStreamProcessor;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

import static com.google.common.truth.Truth.assertThat;
import static bz.stub.parallelconsumer.MdcBoundaryProbe.CALLER_KEY;
import static bz.stub.parallelconsumer.MdcBoundaryProbe.CALLER_VALUE;
import static bz.stub.parallelconsumer.ParallelConsumerOptions.CommitMode.PERIODIC_CONSUMER_SYNC;
import static bz.stub.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.UNORDERED;
import static org.awaitility.Awaitility.await;

/**
 * End to end tests for propagating the caller's SLF4J {@link MDC} into the threads that run the user function.
 * <p>
 * Reported against astubbs#197, against the confluentinc#907 thread: a caller who has established diagnostic context - a
 * {@code trace_id}, a {@code request_id}, a tenant - used to lose all of it the moment work crossed into the worker
 * pool, so the logs their function wrote could not be correlated back to the originating request.
 * <p>
 * <b>These tests are known to detect the defect</b>, not merely to pass: flipping
 * {@link #setupWithSingleWorkerThread(boolean)} to {@code false} in
 * {@link #contextDoesNotLeakToTheNextTaskOnTheSamePooledThread()} reproduces the pre-fix behaviour and the leak
 * assertion fails with four poisoned keys carried across five records on one pooled thread. Re-run that flip if you
 * change the isolation logic.
 *
 * @author Antony Stubbs
 * @see bz.stub.parallelconsumer.internal.MdcPropagation
 */
@Slf4j
class MdcContextPropagationTest extends ParallelEoSStreamProcessorTestBase {

    /**
     * What a user function saw when it started, plus which thread it ran on - the pair is what makes the leak assertion
     * meaningful, since a leak can only happen on a REUSED thread.
     */
    @Value
    private static class Observation {
        long offset;
        String threadName;
        Map<String, String> contextOnEntry;
    }

    private final List<Observation> observations = new CopyOnWriteArrayList<>();

    /**
     * Before AND after, not only after - the shape its sibling {@code MdcPropagationTest} already had. The runner
     * thread is shared with every other class in the fork, and two of them - {@code ProducerManagerTest} and
     * {@code TransactionalBulkCommitTest} - drive {@code AbstractParallelEoSStreamProcessor#processWorkCompleteMailBox}
     * on it, whose {@code MDC.put(...); MDC.remove(...)} pair leaves logback holding an EMPTY map rather than none: on
     * logback 1.6.1, {@code remove} of the last key gives {@code {}} where {@code clear()} gives {@code null}, measured
     * against the build's own jars. A test may only assert a precondition it has established itself, and which class
     * ran before this one is decided by fork placement, not by this class - so
     * {@link #anEmptyCallerContextIsHandledAndNothingLeaks()} gets its clear BEFORE it runs. Without it that
     * precondition was a per-run coin: it failed 2 of 2 full runs on one tree and 0 of 3 on another, same code.
     */
    @BeforeEach
    @AfterEach
    void clearCallersContext() {
        // JUnit reuses its runner thread, so context set by one test must not be inherited by (or from) the next
        MDC.clear();
    }

    /**
     * Single worker thread, so that consecutive records are guaranteed to land on the SAME pooled thread - which is
     * what makes the leak test test anything.
     */
    private void setupWithSingleWorkerThread(boolean propagateMdc) {
        setupParallelConsumerInstance(ParallelConsumerOptions.<String, String>builder()
                .commitMode(PERIODIC_CONSUMER_SYNC)
                .ordering(UNORDERED)
                .maxConcurrency(1)
                .propagateMdc(propagateMdc)
                .build());
    }

    /**
     * Records what the MDC looked like on entry to the user function.
     */
    private void observe(PollContext<String, String> context) {
        Map<String, String> onEntry = MDC.getCopyOfContextMap();
        observations.add(new Observation(context.offset(),
                Thread.currentThread().getName(),
                onEntry == null ? Collections.emptyMap() : onEntry));
    }

    private void awaitObservations(int howMany) {
        await().timeout(defaultTimeout)
                .untilAsserted(() -> assertThat(observations).hasSize(howMany));
    }

    @Test
    void callersContextIsVisibleInsideTheUserFunction() {
        setupWithSingleWorkerThread(true);
        ktu.sendRecords(3);

        MDC.put(CALLER_KEY, CALLER_VALUE);
        parallelConsumer.poll(this::observe);

        awaitObservations(3);

        for (Observation observation : observations) {
            assertThat(observation.getContextOnEntry()).containsEntry(CALLER_KEY, CALLER_VALUE);
        }
    }

    /**
     * PC's own {@code pcId} must survive the caller's context being installed, and must WIN if the caller happens to
     * use the same key - PC's own log lines are read by it.
     */
    @Test
    void pcsOwnKeysSurviveAndWinACollision() {
        setupWithSingleWorkerThread(true);
        ktu.sendRecords(2);

        String pcInstanceId = "pc-instance-under-test";
        parallelConsumer.setMyId(Optional.of(pcInstanceId));

        MDC.put(CALLER_KEY, CALLER_VALUE);
        MDC.put(AbstractParallelEoSStreamProcessor.MDC_INSTANCE_ID, "callers-clashing-value");

        parallelConsumer.poll(this::observe);

        awaitObservations(2);

        for (Observation observation : observations) {
            assertThat(observation.getContextOnEntry())
                    .containsEntry(AbstractParallelEoSStreamProcessor.MDC_INSTANCE_ID, pcInstanceId);
            assertThat(observation.getContextOnEntry()).containsEntry(CALLER_KEY, CALLER_VALUE);
        }
    }

    /**
     * The case that matters most: a pooled thread that keeps a previous task's context produces logs that are actively
     * misleading - worse than no context at all.
     * <p>
     * Each invocation poisons the MDC the way a user function realistically would ({@code MDC.put("order_id", ...)}),
     * and the next invocation on the same thread must not see it. The same-thread assertion is not decoration: without
     * it, this test would pass trivially if every record happened to get a fresh thread.
     */
    @Test
    void contextDoesNotLeakToTheNextTaskOnTheSamePooledThread() {
        setupWithSingleWorkerThread(true);
        int recordCount = 5;
        ktu.sendRecords(recordCount);

        MDC.put(CALLER_KEY, CALLER_VALUE);
        parallelConsumer.poll(context -> {
            observe(context);
            // what a user function does all the time - and what used to be left behind on the pooled thread
            MDC.put(poisonKeyFor(context.offset()), "left-behind-by-offset-" + context.offset());
        });

        awaitObservations(recordCount);

        // the leak assertion is only meaningful if the thread really was reused
        Set<String> threadsUsed = observations.stream().map(Observation::getThreadName).collect(Collectors.toSet());
        assertThat(threadsUsed).hasSize(1);

        // asserted across all observations at once, and BEFORE the caller-context check, so that this is the assertion
        // that fires if the isolation regresses (verified by temporarily disabling propagation - see the class docs)
        Set<String> leakedKeys = observations.stream()
                .flatMap(observation -> observation.getContextOnEntry().keySet().stream())
                .filter(key -> key.startsWith(POISON_KEY_PREFIX))
                .collect(Collectors.toSet());
        assertThat(leakedKeys).isEmpty();

        // and the context that SHOULD be there still is
        for (Observation observation : observations) {
            assertThat(observation.getContextOnEntry()).containsEntry(CALLER_KEY, CALLER_VALUE);
        }
    }

    private static final String POISON_KEY_PREFIX = "poison_from_offset_";

    private static String poisonKeyFor(long offset) {
        return POISON_KEY_PREFIX + offset;
    }

    /**
     * {@link MDC#getCopyOfContextMap()} returns null for an empty context - the normal case for a caller who never
     * touches the MDC. Nothing may NPE, and processing must be unaffected.
     */
    @Test
    void anEmptyCallerContextIsHandledAndNothingLeaks() {
        setupWithSingleWorkerThread(true);
        int recordCount = 3;
        ktu.sendRecords(recordCount);

        // deliberately NO MDC.put here - the caller has no diagnostic context at all. clearCallersContext() has just
        // run, so on logback that reads null; a binding that keeps an emptied map reads {}. PC treats both as "no
        // context", so both are legal here - asserting null alone was asserting the logging binding, not PC, and it
        // went red whenever an earlier class in the same fork had put and removed a key on this thread (the javadoc
        // on clearCallersContext() names them). Belt and braces on purpose: the @BeforeEach establishes the state,
        // this accepts either spelling of it. Reproduced by class order alone; docs/solutions/test-flakiness/ has the
        // experiment.
        Map<String, String> callerContextOnEntry = MDC.getCopyOfContextMap();
        if (callerContextOnEntry != null) {
            assertThat(callerContextOnEntry).isEmpty();
        }

        parallelConsumer.poll(context -> {
            observe(context);
            MDC.put(poisonKeyFor(context.offset()), "left-behind");
        });

        awaitObservations(recordCount);

        assertThat(parallelConsumer.getFailureCause()).isNull();

        Set<String> threadsUsed = observations.stream().map(Observation::getThreadName).collect(Collectors.toSet());
        assertThat(threadsUsed).hasSize(1);

        for (Observation observation : observations) {
            // no caller context to inherit, and no poison from the previous record either
            assertThat(observation.getContextOnEntry()).isEmpty();
        }
    }

    /**
     * The escape hatch. Propagation is ON by default; setting {@code propagateMdc=false} restores the old behaviour
     * exactly - including the pre-existing leak of the user function's own {@code MDC.put} calls onto the pooled
     * thread. That is deliberate, so the flag is a true kill switch rather than half a revert.
     */
    @Test
    void propagationCanBeTurnedOff() {
        setupWithSingleWorkerThread(false);
        ktu.sendRecords(2);

        MDC.put(CALLER_KEY, CALLER_VALUE);
        parallelConsumer.poll(this::observe);

        awaitObservations(2);

        for (Observation observation : observations) {
            assertThat(observation.getContextOnEntry()).doesNotContainKey(CALLER_KEY);
        }
    }

}
