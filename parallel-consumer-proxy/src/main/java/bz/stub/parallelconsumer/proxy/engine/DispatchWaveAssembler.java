package bz.stub.parallelconsumer.proxy.engine;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.proxy.protocol.v1.Dispatch;
import bz.stub.parallelconsumer.state.ShardKey;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static bz.stub.parallelconsumer.internal.utils.StringUtils.msg;

/**
 * Coalesces per-record dispatches into waves - several records handed to the transport at once (R50) - and
 * emits a wave when the size cap is reached, when the coalescing window since the wave's first record elapses,
 * or when {@link #flush()} is called, whichever comes first.
 * <p>
 * <b>It holds no count of its own.</b> The number of records it can ever hold is bounded by KTD6's in-flight
 * target, which the control loop already enforces every pass ({@code target - numberRecordsOutForProcessing});
 * the size cap the engine passes is that same derived target, never a second accumulator - which is also how
 * R49 is satisfied.
 * <p>
 * {@link #flush()} exists so a lone record is never held for the full window: the engine calls it from a
 * control-loop-end hook, so anything offered during a loop pass is emitted by that pass's end. The window timer
 * is the backstop that keeps this class correct standalone, not the primary emission path.
 * <p>
 * <b>The distinct-shard assertion (KTD10, AE22):</b> under restricted ordering
 * ({@code options.getOrdering() != UNORDERED} - the body of core's private
 * {@code ProcessingShard#isOrderRestricted()}), {@code ProcessingShard} permits at most one in-flight record
 * per shard, so a wave containing two records of one shard is evidence of an engine bug and {@link #offer} throws.
 * Under {@code UNORDERED} a shard is a partition and many of its records are legitimately in flight at once, so
 * the assertion is not applied - applying it would reject a supported configuration.
 *
 * @author Antony Stubbs
 */
@Slf4j
class DispatchWaveAssembler implements AutoCloseable {

    private final boolean orderingRestricted;
    private final int sizeCap;
    private final Duration coalescingWindow;
    private final Consumer<List<Dispatch>> sink;

    private final ScheduledExecutorService windowTimer;

    // all three guarded by `this`; emission happens outside the lock so a blocking sink cannot deadlock an offer
    private final List<Dispatch> pending = new ArrayList<>();
    private final Set<ShardKey> pendingShards = new HashSet<>();
    private ScheduledFuture<?> scheduledWindowFlush;

    DispatchWaveAssembler(boolean orderingRestricted, int sizeCap, Duration coalescingWindow,
                          Consumer<List<Dispatch>> sink) {
        if (sizeCap < 1) {
            throw new IllegalArgumentException(msg("wave size cap must be at least 1, got {}", sizeCap));
        }
        this.orderingRestricted = orderingRestricted;
        this.sizeCap = sizeCap;
        this.coalescingWindow = coalescingWindow;
        this.sink = sink;
        this.windowTimer = Executors.newSingleThreadScheduledExecutor(runnable -> {
            var thread = new Thread(runnable, "pc-proxy-wave-window");
            thread.setDaemon(true);
            return thread;
        });
    }

    /**
     * Adds one record to the wave being assembled, keyed by the shard identity the engine computed from its
     * ordering configuration.
     */
    void offer(ShardKey shard, Dispatch dispatch) {
        List<Dispatch> wave = null;
        synchronized (this) {
            if (orderingRestricted && !pendingShards.add(shard)) {
                throw new IllegalStateException(msg(
                        "Wave already carries a record of shard {} - under restricted ordering core permits one "
                                + "in-flight record per shard, so a second in one wave is an engine bookkeeping bug "
                                + "(arriving record: {})",
                        shard, dispatch.getToken().getRecordId()));
            }
            pending.add(dispatch);
            if (pending.size() >= sizeCap) {
                wave = takeWaveLocked();
            } else if (pending.size() == 1) {
                scheduledWindowFlush = windowTimer.schedule(this::flush,
                        coalescingWindow.toNanos(), TimeUnit.NANOSECONDS);
            }
        }
        emit(wave);
    }

    /** Emits whatever is pending, now. A no-op when nothing is - safe to call every control-loop pass. */
    void flush() {
        List<Dispatch> wave;
        synchronized (this) {
            wave = pending.isEmpty() ? null : takeWaveLocked();
        }
        emit(wave);
    }

    private List<Dispatch> takeWaveLocked() {
        var wave = List.copyOf(pending);
        pending.clear();
        pendingShards.clear();
        if (scheduledWindowFlush != null) {
            scheduledWindowFlush.cancel(false);
            scheduledWindowFlush = null;
        }
        return wave;
    }

    private void emit(List<Dispatch> wave) {
        if (wave == null) {
            return;
        }
        log.debug("Emitting wave of {} record(s)", wave.size());
        sink.accept(wave);
    }

    /** Emits anything still pending, then stops the window timer. */
    @Override
    public void close() {
        flush();
        windowTimer.shutdownNow();
    }
}
