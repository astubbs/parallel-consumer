package bz.stub.parallelconsumer.streams;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Matches results returned by the host to the stream threads waiting for them.
 *
 * <p>This is the one place in the engine where getting concurrency wrong is invisible to a single-threaded run.
 * Several stream threads are in flight at once, and a result delivered to the wrong caller produces a plausible
 * wrong answer rather than an error - so the tests answer each invocation with a value derived from what that
 * invocation sent, which a count-based assertion would not catch.
 */
public class InvocationRegistry {

    private final AtomicLong nextCorrelation = new AtomicLong(1);
    private final Map<Long, CompletableFuture<byte[]>> pending = new ConcurrentHashMap<>();

    /**
     * Registers an invocation, hands it to the host, and blocks the calling stream thread until its result arrives
     * or the timeout elapses.
     */
    public byte[] awaitResult(long functionToken, byte[] key, byte[] value, byte[] aggregate,
                              InvocationSink sink, Duration timeout) {
        long correlation = nextCorrelation.getAndIncrement();
        CompletableFuture<byte[]> answer = new CompletableFuture<>();

        // Registered BEFORE the invocation leaves, and the order is load-bearing: a host fast enough to answer
        // during emit would otherwise find no waiter and have its result discarded as unknown.
        pending.put(correlation, answer);
        try {
            sink.emit(correlation, functionToken, key, value, aggregate);
            return answer.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (TimeoutException notAnswered) {
            throw new InvocationFailedException(
                    "invocation " + correlation + " timed out after " + timeout + " waiting for the host");
        } catch (ExecutionException failed) {
            if (failed.getCause() instanceof InvocationFailedException reported) {
                throw reported;
            }
            throw new InvocationFailedException("invocation " + correlation + " failed: " + failed.getCause());
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new InvocationFailedException("invocation " + correlation + " was interrupted");
        } finally {
            // Always, including the timeout path: a registry that leaks entries on timeout grows without bound
            // under exactly the conditions that produced the timeout.
            pending.remove(correlation);
        }
    }

    /**
     * Completes the invocation that minted this correlation.
     *
     * <p>An unknown correlation is discarded rather than applied to anything else. It is reachable without a bug -
     * a result that arrives after its invocation timed out has nobody left to answer.
     */
    public void complete(long correlation, byte[] value) {
        CompletableFuture<byte[]> waiting = pending.get(correlation);
        if (waiting != null) {
            waiting.complete(value);
        }
    }

    /** Fails the invocation that minted this correlation. Unknown correlations are discarded, as above. */
    public void fail(long correlation, String error) {
        CompletableFuture<byte[]> waiting = pending.get(correlation);
        if (waiting != null) {
            waiting.completeExceptionally(new InvocationFailedException(error));
        }
    }

    /** How many invocations are waiting. A test seam, and the thing that would grow if the finally above were lost. */
    public int outstanding() {
        return pending.size();
    }
}
