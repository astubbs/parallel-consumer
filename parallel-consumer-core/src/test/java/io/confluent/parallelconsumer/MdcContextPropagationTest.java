package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import io.confluent.parallelconsumer.internal.AbstractParallelEoSStreamProcessor;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
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
import static io.confluent.parallelconsumer.ParallelConsumerOptions.CommitMode.PERIODIC_CONSUMER_SYNC;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.UNORDERED;
import static org.awaitility.Awaitility.await;

/**
 * End to end tests for propagating the caller's SLF4J {@link MDC} into the threads that run the user function.
 * <p>
 * Reported against {@code #197} (mirror of {@code upstream #907}): a caller who has established diagnostic context - a
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
 * @see io.confluent.parallelconsumer.internal.MdcPropagation
 */
@Slf4j
class MdcContextPropagationTest extends ParallelEoSStreamProcessorTestBase {

    private static final String CALLER_KEY = "trace_id";
    private static final String CALLER_VALUE = "caller-trace-abc";

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

    @AfterEach
    void clearCallersContext() {
        // JUnit reuses its runner thread, so context set by one test must not be inherited by the next
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

        // deliberately NO MDC.put here - the caller has no diagnostic context at all
        assertThat(MDC.getCopyOfContextMap()).isNull();

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
     * The escape hatch restores the old behaviour exactly - including the leak, which is why it is off by default.
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
