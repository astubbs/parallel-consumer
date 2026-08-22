package bz.stub.parallelconsumer.internal;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.LongAdder;

/**
 * The worker pool's queue depth and active count, derived by conservation from counters Parallel Consumer keeps
 * itself rather than read off the executor.
 *
 * <pre>
 *     queued = submitted - started - neverStarted      (replaces ThreadPoolExecutor.getQueue().size())
 *     active = started   - finished                    (replaces ThreadPoolExecutor.getActiveCount())
 * </pre>
 * <p>
 * <b>Why this exists.</b> Those two {@link ThreadPoolExecutor} methods drive the pressure system
 * ({@code isPoolQueueLow}, {@code checkPipelinePressure}, {@link DynamicLoadFactor}), the shutdown diagnostics, and
 * the {@code pool active: {} queued: {}} log line. A virtual-thread-per-task executor is not a
 * {@link ThreadPoolExecutor} and exposes neither, so under
 * {@link bz.stub.parallelconsumer.ParallelConsumerOptions#isUseVirtualThreads()} there is nothing to read. Counting
 * here serves both modes from one code path, with no {@code instanceof} fork - and on the platform path the
 * executor's own figures remain available as an independent oracle to test these against, which a fork returning a
 * hardcoded zero for virtual threads could never have.
 * <p>
 * <b>A task is a batch, not a record.</b> {@code getQueue().size()} counted submitted {@code Runnable}s, and one
 * submission carries one batch, so these counts mean exactly what they replaced. Records are counted elsewhere, by
 * {@code WorkManager#getNumberRecordsOutForProcessing()}.
 *
 * <h2>Every path that can move a counter</h2>
 * <p>
 * Nothing here is ever decremented, so there is no removal site with a predicate to get wrong - the defect shape
 * behind {@code docs/solutions/logic-errors/counter-clamp-hid-a-conditional-decrement-bug-2026-08-21.md} and the
 * clamp it removed. The complete enumeration, which is the point of this class:
 *
 * <table>
 *     <caption>Counter transitions</caption>
 *     <tr><th>Counter</th><th>Moved by</th></tr>
 *     <tr><td>{@code submitted}</td>
 *         <td>{@link #onSubmitting()}, called immediately <em>before</em> handing a task to the executor. Before,
 *             not after: a virtual thread can begin running before {@code submit()} returns, and incrementing
 *             afterwards would let {@code started} overtake {@code submitted} and make the derived queue depth
 *             negative.</td></tr>
 *     <tr><td>{@code neverStarted}</td>
 *         <td>{@link #onSubmitRejected()}, when {@code submit()} threw - the platform pool's
 *             {@link ThreadPoolExecutor.AbortPolicy}, or any executor rejecting after shutdown. The task will never
 *             run, so nothing else will ever account for it.</td></tr>
 *     <tr><td>{@code neverStarted}</td>
 *         <td>{@link #onTasksDiscarded(long)}, for tasks drained out of the executor's queue at close, and for the
 *             list {@code shutdownNow()} hands back. Both are shutdown paths that today return zero or near-zero -
 *             counted anyway, because an unaccounted path is a drift path whether or not it fires.</td></tr>
 *     <tr><td>{@code started}</td>
 *         <td>{@link #onTaskStarted()}, the first statement inside the submitted task.</td></tr>
 *     <tr><td>{@code finished}</td>
 *         <td>{@link #onTaskFinished()}, in that task's outermost {@code finally} - so it covers a normal return, a
 *             user-function exception, and the {@code InterruptedException} that {@code shutdownNow()} delivers.
 *             {@code finally} also runs for an {@link Error}; only JVM exit skips it, and that is terminal.</td></tr>
 * </table>
 * <p>
 * The relationship between them is an invariant a test can assert directly once the pool is quiet:
 * {@code submitted == started + neverStarted} and {@code started == finished}. See
 * {@link #getSubmittedTotal()} and its siblings.
 *
 * @author Antony Stubbs
 * @see bz.stub.parallelconsumer.state.RecordPopulation the same conservation shape, applied to records in shards
 */
public class UserFunctionTaskAccounting {

    private final LongAdder submitted = new LongAdder();

    private final LongAdder started = new LongAdder();

    private final LongAdder neverStarted = new LongAdder();

    private final LongAdder finished = new LongAdder();

    /**
     * A task is about to be handed to the executor. Must be called before the submit, not after - see the class
     * javadoc.
     */
    void onSubmitting() {
        submitted.increment();
    }

    /**
     * The submit threw, so the task will never run.
     */
    void onSubmitRejected() {
        neverStarted.increment();
    }

    /**
     * Tasks that were accepted by the executor but will never run: drained from its queue on shutdown, or handed
     * back by {@code shutdownNow()}.
     */
    void onTasksDiscarded(long count) {
        if (count > 0) {
            neverStarted.add(count);
        }
    }

    /**
     * First statement inside the submitted task.
     */
    void onTaskStarted() {
        started.increment();
    }

    /**
     * Outermost {@code finally} of the submitted task.
     */
    void onTaskFinished() {
        finished.increment();
    }

    /**
     * @return tasks handed to the executor that have not begun running - the figure
     *         {@code ThreadPoolExecutor.getQueue().size()} used to supply
     */
    public int getQueued() {
        // Subtrahends first, for the reason RecordPopulation#getInSystem states: every counter here only
        // increases, and no task can start (or be discarded) without having been submitted first, so a start
        // observed here always has its submission already committed. Reading this way round makes the difference
        // non-negative by construction rather than by clamp.
        long startedSoFar = started.sum();
        long neverStartedSoFar = neverStarted.sum();
        long submittedSoFar = submitted.sum();
        return (int) Math.min(Integer.MAX_VALUE, submittedSoFar - startedSoFar - neverStartedSoFar);
    }

    /**
     * @return tasks currently running the user's function - the figure {@code ThreadPoolExecutor.getActiveCount()}
     *         used to supply
     */
    public int getActive() {
        long finishedSoFar = finished.sum();
        long startedSoFar = started.sum();
        return (int) Math.min(Integer.MAX_VALUE, startedSoFar - finishedSoFar);
    }

    /**
     * @return running total, for asserting on conservation in tests
     */
    public long getSubmittedTotal() {
        return submitted.sum();
    }

    /**
     * @return running total, for asserting on conservation in tests
     */
    public long getStartedTotal() {
        return started.sum();
    }

    /**
     * @return running total, for asserting on conservation in tests
     */
    public long getNeverStartedTotal() {
        return neverStarted.sum();
    }

    /**
     * @return running total, for asserting on conservation in tests
     */
    public long getFinishedTotal() {
        return finished.sum();
    }

    @Override
    public String toString() {
        return "queued=" + getQueued() + ", active=" + getActive()
                + " (submitted=" + submitted.sum() + ", started=" + started.sum()
                + ", neverStarted=" + neverStarted.sum() + ", finished=" + finished.sum() + ")";
    }
}
