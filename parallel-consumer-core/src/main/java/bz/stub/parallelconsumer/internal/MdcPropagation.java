package io.confluent.parallelconsumer.internal;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import io.confluent.parallelconsumer.ParallelConsumerOptions;
import org.slf4j.MDC;

import java.util.Map;

/**
 * Carries the SLF4J {@link MDC} (Mapped Diagnostic Context) across the thread boundaries that Parallel Consumer puts
 * between the caller and their user function.
 * <p>
 * Without this, a caller who has established diagnostic context - a {@code trace_id}, a {@code request_id}, a tenant -
 * loses all of it the moment a record crosses into the worker pool, and again crossing into the Vert.x event loop or a
 * Reactor scheduler. Logs emitted from inside the user function then cannot be correlated back to the originating
 * request.
 * <p>
 * Two operations, always used as a pair:
 * <ol>
 *     <li>{@link #capture()} on the thread that <em>has</em> the context (the caller of
 *     {@link io.confluent.parallelconsumer.ParallelStreamProcessor#poll}, or the controller at submit time).</li>
 *     <li>{@link #enter(Map)} on the thread that <em>runs</em> the work, in a try-with-resources block, so the context
 *     is torn down again when the work finishes.</li>
 * </ol>
 * <p>
 * <b>The teardown is the important half.</b> Worker threads are pooled and long-lived: a thread that keeps the previous
 * task's {@code trace_id} produces logs that are actively misleading - worse than no context at all. {@link #enter(Map)}
 * therefore always restores whatever context the thread had when the task started, which also cleans up any
 * {@link MDC#put} the user function itself did.
 * <p>
 * <b>Precedence.</b> This class only installs the caller's map; Parallel Consumer's own keys
 * ({@link AbstractParallelEoSStreamProcessor#MDC_INSTANCE_ID}, {@code offset}) are applied <em>on top</em> by the call
 * sites, so PC's keys win a collision. PC's keys are part of its logging contract and are what its own log lines are
 * read by - a caller key shadowing {@code offset} would silently corrupt PC's own diagnostics.
 * <p>
 * <b>Cost.</b> {@link MDC#getCopyOfContextMap()} returns {@code null}, allocating nothing, when the context is empty -
 * which is the case for every caller who never touches MDC, since PC's own {@code pcId} key is only set when
 * {@link AbstractParallelEoSStreamProcessor#setMyId} has been used. So the default configuration adds no allocation to
 * the per-work-item path, only a null check.
 *
 * @author Antony Stubbs
 * @see ParallelConsumerOptions#isPropagateMdc()
 */
public class MdcPropagation {

    /**
     * Undoes an {@link #enter(Map)}, restoring the diagnostic context the thread had beforehand.
     * <p>
     * Declared to not throw, so it can be used in try-with-resources without forcing callers to handle a checked
     * exception.
     */
    @FunctionalInterface
    public interface Scope extends AutoCloseable {
        @Override
        void close();
    }

    /**
     * Used when the thread had no context to begin with - clearing is then the correct restore, and needs no captured
     * state, so a single shared instance suffices.
     */
    private static final Scope CLEAR_ON_EXIT = MDC::clear;

    /**
     * Used when propagation is switched off - leaves the thread's context entirely alone.
     */
    private static final Scope NO_OP = () -> {
    };

    private final boolean enabled;

    public MdcPropagation(boolean enabled) {
        this.enabled = enabled;
    }

    /**
     * @return a snapshot of the calling thread's diagnostic context, or {@code null} if there is nothing to propagate
     *         (empty context, or propagation disabled). {@code null} is the normal, zero-allocation case.
     */
    public Map<String, String> capture() {
        if (!enabled) {
            return null;
        }
        // returns null (not an empty map) when the context is empty - no allocation for callers who never use MDC
        return MDC.getCopyOfContextMap();
    }

    /**
     * Installs {@code captured} on the current thread for the duration of the returned {@link Scope}, then restores
     * whatever was there before.
     * <p>
     * Safe to call with {@code null} (the {@link #capture()} empty-context result): the scope is still established, so
     * that anything the wrapped code puts into the MDC is cleaned up rather than left on a pooled thread.
     *
     * @param captured a context snapshot from {@link #capture()}, may be {@code null}
     * @return the scope to close when the work is done - never {@code null}
     */
    public Scope enter(Map<String, String> captured) {
        if (!enabled) {
            return NO_OP;
        }
        // null when the thread's context is empty - the overwhelmingly common case for a pooled worker
        final Map<String, String> previous = MDC.getCopyOfContextMap();
        if (captured != null && !captured.isEmpty()) {
            // MDC adapters copy the map they are given, so sharing one snapshot across tasks is safe
            MDC.setContextMap(captured);
        }
        return previous == null
                ? CLEAR_ON_EXIT
                : () -> MDC.setContextMap(previous);
    }

    /**
     * Adopts {@code captured} on the current thread for the rest of that thread's life, without a restore scope.
     * <p>
     * For PC's own single-purpose, never-pooled threads (the controller and the broker poller): they exist to serve
     * exactly one PC instance, so there is no later task to leak into, and the context should be visible on every line
     * they log.
     *
     * @param captured a context snapshot from {@link #capture()}, may be {@code null}
     */
    public void adopt(Map<String, String> captured) {
        if (enabled && captured != null && !captured.isEmpty()) {
            MDC.setContextMap(captured);
        }
    }

}
