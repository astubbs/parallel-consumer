package bz.stub.parallelconsumer.integrationTests.utils;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Captures submissions instead of running them, so the window where a task is queued but not yet
 * executing stays observable - the window {@link ManagedPCInstance}'s single-flight start guard
 * exists to close.
 */
public class RecordingExecutor extends AbstractExecutorService {

    private final List<Runnable> tasks = new java.util.concurrent.CopyOnWriteArrayList<>();

    /** Tasks submitted and not yet drained by {@link #runAll()}. */
    public List<Runnable> getTasks() {
        return Collections.unmodifiableList(tasks);
    }

    public void runAll() {
        List<Runnable> toRun = new ArrayList<>(tasks);
        tasks.clear();
        toRun.forEach(Runnable::run);
        // submit() wraps each task in a FutureTask, which captures a throwable instead of
        // propagating it. Drain the outcomes, or a task that blew up somewhere it should never have
        // reached is indistinguishable from one that returned cleanly - which would make a caller's
        // "it aborted" assertion unfalsifiable.
        for (Runnable task : toRun) {
            if (task instanceof Future) {
                try {
                    ((Future<?>) task).get();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError("interrupted draining a queued task", e);
                } catch (ExecutionException e) {
                    throw new AssertionError("queued task threw instead of returning cleanly", e.getCause());
                }
            }
        }
    }

    @Override
    public void execute(Runnable command) {
        tasks.add(command);
    }

    @Override
    public void shutdown() {
    }

    @Override
    public List<Runnable> shutdownNow() {
        return Collections.emptyList();
    }

    @Override
    public boolean isShutdown() {
        return false;
    }

    @Override
    public boolean isTerminated() {
        return false;
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) {
        return true;
    }
}
