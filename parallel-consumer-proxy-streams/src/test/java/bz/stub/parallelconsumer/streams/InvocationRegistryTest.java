package bz.stub.parallelconsumer.streams;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * The registry is the engine's only shared mutable state under concurrency, so these tests exist to catch the
 * failure that a single-threaded run cannot: a result delivered to the wrong waiting thread.
 */
class InvocationRegistryTest {

    private static final Duration GENEROUS = Duration.ofSeconds(5);

    private final InvocationRegistry registry = new InvocationRegistry();

    @Test
    void aResultCompletesTheInvocationThatMintedItsCorrelation() throws Exception {
        AtomicReference<Long> correlation = new AtomicReference<>();
        InvocationSink sink = (c, token, call) -> correlation.set(c);

        ExecutorService caller = Executors.newSingleThreadExecutor();
        try {
            var result = caller.submit(() ->
                    registry.awaitResult(42, ForeignCall.map(bytes("k"), bytes("v")), sink, GENEROUS));

            awaitOutstanding(1);
            registry.complete(correlation.get(), bytes("mapped"));

            assertThat(new String(result.get(5, TimeUnit.SECONDS), StandardCharsets.UTF_8)).isEqualTo("mapped");
        } finally {
            caller.shutdownNow();
        }
    }

    @Test
    void concurrentInvocationsEachReceiveTheirOwnResult() throws Exception {
        int callers = 8;
        Map<Long, String> correlationToPayload = new ConcurrentHashMap<>();
        InvocationSink sink = (c, token, call) ->
                correlationToPayload.put(c, new String(call.value(), StandardCharsets.UTF_8));

        ExecutorService pool = Executors.newFixedThreadPool(callers);
        List<String> received = new CopyOnWriteArrayList<>();
        CountDownLatch done = new CountDownLatch(callers);
        try {
            for (int i = 0; i < callers; i++) {
                String payload = "payload-" + i;
                pool.submit(() -> {
                    try {
                        byte[] answer = registry.awaitResult(1, ForeignCall.map(bytes("k"), bytes(payload)), sink, GENEROUS);
                        received.add(new String(answer, StandardCharsets.UTF_8));
                    } finally {
                        done.countDown();
                    }
                });
            }

            awaitOutstanding(callers);
            // Answer every invocation with a value derived from the payload IT sent. If the registry crosses two
            // waiters, a caller gets a payload it never sent - which a count-based assertion would not notice.
            correlationToPayload.forEach((c, payload) -> registry.complete(c, bytes("echo:" + payload)));

            assertThat(done.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(received).hasSize(callers);
            for (int i = 0; i < callers; i++) {
                assertThat(received).contains("echo:payload-" + i);
            }
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void aResultForAnUnknownCorrelationIsDiscardedRatherThanAppliedElsewhere() throws Exception {
        AtomicReference<Long> correlation = new AtomicReference<>();
        InvocationSink sink = (c, token, call) -> correlation.set(c);

        ExecutorService caller = Executors.newSingleThreadExecutor();
        try {
            var result = caller.submit(() ->
                    registry.awaitResult(1, ForeignCall.map(bytes("k"), bytes("v")), sink, GENEROUS));
            awaitOutstanding(1);

            registry.complete(correlation.get() + 9999, bytes("stray"));

            assertThat(registry.outstanding()).isEqualTo(1);
            registry.complete(correlation.get(), bytes("correct"));
            assertThat(new String(result.get(5, TimeUnit.SECONDS), StandardCharsets.UTF_8)).isEqualTo("correct");
        } finally {
            caller.shutdownNow();
        }
    }

    @Test
    void anInvocationThatIsNeverAnsweredFailsWithTheTimeoutNamed() {
        InvocationSink silent = (c, token, call) -> { };

        InvocationFailedException thrown = assertThrows(InvocationFailedException.class, () ->
                registry.awaitResult(1, ForeignCall.map(bytes("k"), bytes("v")), silent, Duration.ofMillis(50)));

        assertThat(thrown).hasMessageThat().ignoringCase().contains("timed out");
        assertThat(registry.outstanding()).isEqualTo(0);
    }

    @Test
    void anErrorFromTheHostFailsTheRecordRatherThanSubstitutingAValue() throws Exception {
        AtomicReference<Long> correlation = new AtomicReference<>();
        InvocationSink sink = (c, token, call) -> correlation.set(c);

        ExecutorService caller = Executors.newSingleThreadExecutor();
        try {
            var result = caller.submit(() ->
                    registry.awaitResult(1, ForeignCall.map(bytes("k"), bytes("v")), sink, GENEROUS));
            awaitOutstanding(1);

            registry.fail(correlation.get(), "the host's function raised");

            var thrown = assertThrows(java.util.concurrent.ExecutionException.class,
                    () -> result.get(5, TimeUnit.SECONDS));
            assertThat(thrown).hasCauseThat().isInstanceOf(InvocationFailedException.class);
            assertThat(thrown).hasCauseThat().hasMessageThat().contains("the host's function raised");
        } finally {
            caller.shutdownNow();
        }
    }

    private void awaitOutstanding(int expected) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (registry.outstanding() != expected && System.nanoTime() < deadline) {
            Thread.sleep(1);
        }
        assertThat(registry.outstanding()).isEqualTo(expected);
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }
}
