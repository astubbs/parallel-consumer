package io.confluent.parallelconsumer.streams;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.internal.EpochAndRecordsMap;
import io.confluent.parallelconsumer.internal.PCModule;
import io.confluent.parallelconsumer.state.WorkContainer;
import io.confluent.parallelconsumer.state.WorkManager;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;

import java.io.Closeable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * One Parallel Consumer {@code WorkManager}, and the worker pool that drains it, standing in for a
 * {@code StreamTask}'s {@code PartitionGroup} and the StreamThread that used to walk it.
 * <p>
 * The seam is deliberately narrow. Everything that needs Kafka Streams' package-private internals -
 * deserialisation, timestamp extraction, finding the source node, running the processor chain - stays in the
 * patched {@code StreamTask}, handed over as an opaque {@link WorkPreparer}. Everything that is Parallel
 * Consumer's - assignment lifecycle, sharding, ordering, in-flight accounting - lives here, in fork-original
 * code that is tracked in the repository like any other source file.
 *
 * <h2>Threading contract, which is not optional</h2>
 * {@code WorkManager} is not thread-safe. In real PC, every call into it comes from the single controller
 * thread; workers report back through a mailbox that the controller drains. This class keeps that discipline
 * exactly:
 * <ul>
 *   <li>{@link #registerRecords} and {@link #dispatchAvailable} are called <b>only from the StreamThread</b>
 *       (from {@code addRecords} and {@code process} respectively), and only they touch the WorkManager.</li>
 *   <li>Workers never call the WorkManager. They set the outcome on their own {@code WorkContainer} and drop
 *       it into {@link #completed}, which the StreamThread drains at the top of the next
 *       {@link #dispatchAvailable}. That mirrors PC's own {@code workMailBox}.</li>
 * </ul>
 *
 * <h2>Decisions taken here, and what they cost</h2>
 * <ul>
 *   <li><b>KEY ordering, explicitly.</b> This is the whole reason the seam is interesting: a KEY-ordered
 *       shard hands out at most one record per key at a time, so a Streams topology keeps its per-key
 *       sequencing while distinct keys run concurrently. Set explicitly rather than left to the default so
 *       that a future default change cannot silently invalidate the result.</li>
 *   <li><b>A stub {@code Consumer}, not the Streams consumer.</b> {@code ParallelConsumerOptions.validate()}
 *       demands a Consumer, and although nothing here calls {@code validate()}, PC's
 *       {@code onPartitionsAssigned} really does use one: it calls {@code consumer.committed(...)} to
 *       bootstrap each partition's completed-offset map. Handing it the live Streams consumer would be worse
 *       than useless - a group that ran stock Streams carries <em>Streams-format</em> metadata in its
 *       commits, and PC's bootstrap would try to decode that as PC's incomplete-offset payload and fail
 *       under the default {@code invalidOffsetMetadataPolicy} (FAIL). A {@link MockConsumer} with no
 *       committed offsets gives every partition a clean state and every record is accepted.
 *       <p>
 *       <b>Since U9 the group's commits really do carry PC's data</b> - {@link #collectCommitData} feeds the
 *       patched {@code StreamTask}'s committable offsets - and the mock bootstrap stays sound even so:
 *       PC's bootstrap truncation ({@code PartitionState.maybeTruncateBelowOrAbove}) aligns the frontier to
 *       the first record Streams actually polls after a resume, and only <em>dirty</em> partitions are ever
 *       collected, so a first commit from a fresh dispatcher cannot regress the group's committed offset.
 *       What the mock costs is read-back: PC starts blank, so records a previous run completed beyond the
 *       frontier are replayed rather than skipped - a permitted at-least-once duplicate, recorded in the
 *       plan as the follow-up.</li>
 *   <li><b>Retries disabled.</b> PC's answer to a failure is to re-dispatch the record. Here that would
 *       re-run the entire processor chain, including {@code forward()} calls that already emitted records
 *       downstream - duplicates stock Streams never produces, since it surfaces the exception to the
 *       uncaught-exception handler instead. The retry delay is set far beyond any run's lifetime, so a
 *       failed record is never handed out again. Under KEY ordering that leaves its key's shard blocked
 *       while every other key keeps flowing, and {@link #pollFailure()} lets the StreamThread surface the
 *       exception the way stock does.</li>
 * </ul>
 *
 * @author Antony Stubbs
 * @see PcDispatchSwitch
 * @see PcDispatchCounters
 */
@Slf4j
public class PcTaskDispatcher implements Closeable {

    /**
     * Turns a raw record into the work to run on a worker thread.
     * <p>
     * {@link #prepare} runs on the <b>StreamThread</b>, inside {@link #dispatchAvailable}, and the
     * {@link Runnable} it returns runs on a <b>worker</b>. That split is load-bearing: deserialisation and
     * timestamp extraction go through Kafka Streams' {@code RecordQueue}, which is no more thread-safe than
     * anything else in {@code processor.internals}, so it has to happen while we are still single-threaded.
     */
    public interface WorkPreparer {
        /**
         * @return the chain execution to run on a worker, or null if the record was dropped during
         *         preparation (a bad timestamp, say) and there is nothing to run - it still counts as
         *         consumed.
         */
        Runnable prepare(ConsumerRecord<byte[], byte[]> record);
    }

    /**
     * Signals the outcome of one record whose completion is deferred past the worker call. Safe to call from
     * any thread, exactly once.
     *
     * @see DeferringWorkPreparer
     */
    public interface CompletionHandle {
        /** The record is durable. Only now does it count as complete, and only now can the frontier pass it. */
        void succeeded();

        /** The record failed durably. Treated exactly as a throw from the worker call would have been. */
        void failed(Throwable cause);
    }

    /**
     * A {@link WorkPreparer} for which returning from the worker call is <b>not</b> a completion claim.
     * <p>
     * The default seam completes a record the instant its {@link Runnable} returns, which is right when the
     * work is the processing. It is wrong when the callee only <em>buffers</em> - a Kafka Connect sink task
     * accumulating inside {@code put()} being the case this exists for - because the record is then complete
     * in PC's accounting while still being only in the sink's memory, and the frontier passes an offset no
     * one has durably written.
     * <p>
     * This is not a new shape for the codebase: {@code ExternalEngine.addToMailBoxOnUserFunctionSuccess} is
     * deliberately a no-op for async work, and the Vert.x module completes the {@code WorkContainer} later
     * from the Vert.x event loop. Same idea, expressed through this dispatcher's own seam.
     * <p>
     * <b>Do not block a worker waiting for durability instead.</b> {@link #dispatchAvailable} computes
     * capacity as {@code poolSize - inFlight}, and {@code inFlight} only falls when the worker call returns,
     * so waiting inside the {@link Runnable} stalls the pump at capacity - and, for a sink that flushes on a
     * timer, deadlocks against the very flush it is waiting on. Deferring the <em>signal</em> costs nothing;
     * the worker still returns immediately and its slot is freed.
     */
    public interface DeferringWorkPreparer extends WorkPreparer {

        /**
         * @param handle the completion this record's outcome must eventually be reported through - exactly
         *               once, from any thread
         * @return the work to run on a worker, or null if the record was dropped during preparation (which
         *         still counts as consumed, and completes it immediately - the handle is then unused)
         */
        Runnable prepare(ConsumerRecord<byte[], byte[]> record, CompletionHandle handle);

        /**
         * Never called on this path; {@link #prepare(ConsumerRecord, CompletionHandle)} replaces it. Present
         * only because the dispatcher's field is typed to the parent interface.
         */
        @Override
        default Runnable prepare(ConsumerRecord<byte[], byte[]> record) {
            throw new UnsupportedOperationException(
                    "a DeferringWorkPreparer must be driven through prepare(record, handle)");
        }
    }

    /**
     * Long enough that no run outlives it. See the retry note in the class javadoc - this is how "disabled"
     * is expressed, because PC has no "never retry" setting.
     */
    static final Duration RETRIES_DISABLED_DELAY = Duration.ofDays(3650);

    /**
     * The thread currently inside {@link WorkManager} through this dispatcher, or null.
     *
     * <p>{@code WorkManager} and its partition state are not thread-safe, and the cost of getting that wrong
     * is silent: a second thread calling in corrupts offset bookkeeping without throwing, and the damage
     * surfaces later as a commit covering work that never completed. This makes the "one at a time" prose
     * on the methods below into a check.
     *
     * <p><b>Occupancy, not identity - and that correction was earned.</b> This guard first read
     * {@code Thread.currentThread() == ownerThread}, pinned to the constructing thread, and Kafka Streams
     * refuted it: {@code DefaultStateUpdater.maybeCheckpointTasks} calls {@code task.maybeCheckpoint(false)}
     * from its own {@code StateUpdater-N} thread, which reaches {@code commitNeeded()} and so this class.
     * Streams does not keep one thread per task; it transfers <em>exclusive ownership</em> of a task between
     * the StreamThread and the state-updater thread. A task handed over is removed from the StreamThread's
     * registry first ({@code TaskManager.handleReassignedActiveTask}: {@code tasks.removeTask(task)} then
     * {@code stateUpdater.add(task)}), and comes back only through
     * {@code stateUpdater.drainRestoredActiveTasks} followed by {@code tasks.addTask(task)} in
     * {@code transitRestoredTaskToRunning} - while everything the StreamThread processes and commits
     * iterates {@code tasks.activeTasks()}. So the mutual exclusion holds; the fixed identity never did.
     *
     * <p>Deliberately a detector rather than a lock. Streams already provides the exclusion, and taking a
     * lock here would add a second ordering to reason about on the dispatch path; what was missing was a way
     * to hear about it loudly if that guarantee ever stops holding.
     */
    private final AtomicReference<Thread> insideWorkManager = new AtomicReference<>();

    private final Set<TopicPartition> inputPartitions;

    @Getter
    private final WorkManager<byte[], byte[]> workManager;

    private final ExecutorService workerPool;

    @Getter
    private final int poolSize;

    /**
     * Workers' outbox. Drained by the StreamThread only - see the threading contract above.
     */
    private final Queue<WorkContainer<byte[], byte[]>> completed = new ConcurrentLinkedQueue<>();

    private final AtomicInteger inFlight = new AtomicInteger();

    private final AtomicReference<Throwable> firstFailure = new AtomicReference<>();

    /**
     * How many records the last {@link #dispatchAvailable} consumed - dispatched to the pool, dropped, or
     * failed during preparation - or -1 before the first one.
     * <p>
     * This, and not "does PC still hold records", is what defines {@link #isQuiescent()}. With retries
     * disabled a failed record's KEY shard stays blocked forever, and the records queued behind it stay
     * <em>available</em> in PC's accounting even though PC will never hand them out - so asking the
     * WorkManager whether work is waiting reports a permanently busy dispatcher and every wait times out.
     * Asking instead "did the last pump produce anything, and is anything still running" answers the question
     * actually being asked: is there any point pumping again.
     */
    private volatile int lastDispatchCount = -1;

    private final AtomicLong recordsOffered = new AtomicLong();
    private final AtomicLong recordsAccepted = new AtomicLong();
    private final AtomicLong recordsDispatched = new AtomicLong();
    private final AtomicLong recordsSucceeded = new AtomicLong();
    private final AtomicLong recordsFailed = new AtomicLong();

    private volatile boolean closed;

    /**
     * @return a dispatcher, or null when the switch is off - null being the patched {@code StreamTask}'s
     *         signal to keep using its own {@code PartitionGroup}.
     */
    public static PcTaskDispatcher createIfEnabled(final String taskName, final Set<TopicPartition> inputPartitions) {
        if (!PcDispatchSwitch.isEnabled()) {
            return null;
        }
        return new PcTaskDispatcher(taskName, inputPartitions, PcDispatchSwitch.getPoolSize());
    }

    public PcTaskDispatcher(final String taskName, final Set<TopicPartition> inputPartitions, final int poolSize) {
        this.inputPartitions = new LinkedHashSet<>(inputPartitions);
        this.poolSize = poolSize;

        ParallelConsumerOptions<byte[], byte[]> options = ParallelConsumerOptions.<byte[], byte[]>builder()
                .consumer(new MockConsumer<byte[], byte[]>(OffsetResetStrategy.NONE))
                .ordering(ParallelConsumerOptions.ProcessingOrder.KEY)
                .maxConcurrency(poolSize)
                .defaultMessageRetryDelay(RETRIES_DISABLED_DELAY)
                .build();

        PCModule<byte[], byte[]> module = new PCModule<>(options);
        this.workManager = module.workManager();

        // Without this, nothing works and nothing says so. WorkManager is a ConsumerRebalanceListener, but
        // Kafka Streams owns the consumer, so no rebalance callback ever reaches PC. Skip it and
        // PartitionStateManager.getPartitionState returns null (NPE inside maybeRegisterNewRecordAsWork),
        // while EpochAndRecordsMap quietly drops every partition whose epoch is null. The second failure is
        // the dangerous one: zero records registered, no exception, a topology that just looks idle.
        this.workManager.onPartitionsAssigned(this.inputPartitions);

        AtomicInteger threadCounter = new AtomicInteger();
        ThreadFactory threadFactory = runnable -> {
            Thread thread = new Thread(runnable, "pc-streams-" + taskName + "-" + threadCounter.getAndIncrement());
            thread.setDaemon(true);
            return thread;
        };
        this.workerPool = Executors.newFixedThreadPool(poolSize, threadFactory);

        ACTIVE.add(this);
        log.info("PC dispatch active for task {} over {} with a pool of {}, KEY ordering, retries disabled",
                taskName, this.inputPartitions, poolSize);
    }

    /**
     * Hand a poll batch to PC instead of to the partition group. Called from the StreamThread only.
     */
    public void registerRecords(final TopicPartition partition, final Iterable<ConsumerRecord<byte[], byte[]>> records) {
        List<ConsumerRecord<byte[], byte[]>> batch = new ArrayList<>();
        for (ConsumerRecord<byte[], byte[]> record : records) {
            batch.add(record);
        }
        if (batch.isEmpty()) {
            return;
        }

        enterWorkManager("registerRecords");
        try {
            Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> byPartition = new HashMap<>();
            byPartition.put(partition, batch);
            EpochAndRecordsMap<byte[], byte[]> epochTagged =
                    new EpochAndRecordsMap<>(new ConsumerRecords<>(byPartition), workManager.getPm());

            recordsOffered.addAndGet(batch.size());
            recordsAccepted.addAndGet(epochTagged.count());
            PcDispatchCounters.onOfferedToWorkManager(batch.size());
            PcDispatchCounters.onAcceptedByWorkManager(epochTagged.count());
            if (epochTagged.count() != batch.size()) {
                // Loud, because the thing being guarded against is silence.
                log.error("PC dropped {} of {} records for {} for want of a partition-assignment epoch - " +
                                "onPartitionsAssigned was not driven for this partition",
                        batch.size() - epochTagged.count(), batch.size(), partition);
            }

            workManager.registerWork(epochTagged);
        } finally {
            leaveWorkManager();
        }
    }

    /**
     * Take whatever PC will hand out, prepare each record on this thread, and run it on a worker. Called from
     * the StreamThread only.
     *
     * @return how many records were CONSUMED from the WorkManager this time round - dispatched to the pool,
     *         dropped during preparation, or failed at preparation. Not merely pool submissions: the patched
     *         {@code process()} returns this as its progress signal, and stock's contract is "did the task
     *         make progress", which a consumed-by-drop record satisfies. Counting only pool submissions made
     *         {@code process()} report false after consuming a batch of corrupted records - a lie that
     *         StreamTaskTest catches with assertTrue(task.process(...)) and that stock's TaskExecutor paces
     *         on.
     */
    public int dispatchAvailable(final WorkPreparer preparer) {
        enterWorkManager("dispatchAvailable");
        try {
            return dispatchAvailableExclusively(preparer);
        } finally {
            leaveWorkManager();
        }
    }

    private int dispatchAvailableExclusively(final WorkPreparer preparer) {
        drainCompletions();

        if (closed) {
            lastDispatchCount = 0;
            return 0;
        }

        // Loop-invariant: `preparer` is fixed for this call, so ask once rather than once per record.
        final boolean deferred = preparer instanceof DeferringWorkPreparer;

        // Never ask for more than the pool can start (checked per loop pass below): PC would happily hand
        // out its full target and the surplus would sit in the executor's queue marked in-flight, which
        // inflates the concurrency being measured and delays every completion behind it.

        // The pump loops while preparation consumes records SYNCHRONOUSLY (corrupted or dropped records:
        // completed on this thread, no worker involved). Under KEY ordering a synchronously-consumed
        // record's key-mate only becomes available once that completion is fed back, and deferring the
        // feed-back to the next pump would stall the key by a full poll cycle - a poison pill would hold up
        // its whole key for ~poll.ms. Stock consumes such records inline, so this loop restores parity.
        // Terminates: every iteration either consumes at least one record from a finite backlog or exits.
        int consumed = 0;
        while (true) {
            int capacity = poolSize - inFlight.get();
            if (capacity < 1) {
                break;
            }
            List<WorkContainer<byte[], byte[]>> available = workManager.getWorkIfAvailable(capacity);
            int syncCompleted = 0;
            for (WorkContainer<byte[], byte[]> work : available) {
                consumed++;
                // Held rather than discarded: runOnWorker reports a throw through the SAME handle, so a
                // preparer that has already failed the record cannot be counted a second time.
                final CompletionHandle handle = deferred ? handleFor(work) : null;
                Runnable chainExecution;
                try {
                    chainExecution = deferred
                            ? ((DeferringWorkPreparer) preparer).prepare(work.getCr(), handle)
                            : preparer.prepare(work.getCr());
                } catch (RuntimeException e) {
                    // Preparation failed on the StreamThread - deserialisation, most likely. Treat it exactly
                    // as a processing failure so the record does not vanish from PC's accounting.
                    recordFailure(work, e);
                    syncCompleted++;
                    continue;
                }

                if (chainExecution == null) {
                    // Dropped during preparation - consumed, nothing to run.
                    work.onUserFunctionSuccess();
                    completed.add(work);
                    syncCompleted++;
                    continue;
                }

                inFlight.incrementAndGet();
                recordsDispatched.incrementAndGet();
                PcDispatchCounters.onDispatchedToPool();
                workerPool.execute(() -> runOnWorker(work, chainExecution, handle));
            }
            if (syncCompleted == 0) {
                break;
            }
            // Feed the synchronous outcomes back so their key-mates become available to this same pump.
            drainCompletions();
        }
        lastDispatchCount = consumed;
        return consumed;
    }

    /**
     * @param handle non-null only for a {@link DeferringWorkPreparer}, whose return says "buffered" rather
     *               than "done" - completing here would let the frontier pass an offset nothing has durably
     *               written. A throw still ends the record, but is reported <em>through the handle</em> so
     *               that a preparer which already failed it on the way out cannot have it counted twice.
     */
    private void runOnWorker(final WorkContainer<byte[], byte[]> work, final Runnable chainExecution,
                             final CompletionHandle handle) {
        try {
            chainExecution.run();
            if (handle == null) {
                recordSuccess(work);
            }
        } catch (Throwable t) {
            if (handle == null) {
                recordFailure(work, t);
            } else {
                handle.failed(t);
            }
        } finally {
            // Unconditional, deferred or not: this measures POOL OCCUPANCY, and the worker is genuinely
            // free. Making it wait for durability is what would stall the pump.
            inFlight.decrementAndGet();
        }
    }

    /**
     * The completion a {@link DeferringWorkPreparer} reports through. Feeding {@link #completed} is what
     * makes it safe off the owner thread - it is a concurrent queue drained by the owner in
     * {@link #drainCompletions()}, which is the same route the Vert.x module's mailbox uses.
     */
    private CompletionHandle handleFor(final WorkContainer<byte[], byte[]> work) {
        final AtomicBoolean reported = new AtomicBoolean();
        return new CompletionHandle() {
            @Override
            public void succeeded() {
                if (reported.compareAndSet(false, true)) {
                    recordSuccess(work);
                }
            }

            @Override
            public void failed(final Throwable cause) {
                if (reported.compareAndSet(false, true)) {
                    recordFailure(work, cause);
                }
            }
        };
    }

    private void recordSuccess(final WorkContainer<byte[], byte[]> work) {
        work.onUserFunctionSuccess();
        recordsSucceeded.incrementAndGet();
        PcDispatchCounters.onCompletedSuccessfully();
        completed.add(work);
    }

    private void recordFailure(final WorkContainer<byte[], byte[]> work, final Throwable cause) {
        work.onUserFunctionFailure(cause);
        recordsFailed.incrementAndGet();
        PcDispatchCounters.onFailed();
        firstFailure.compareAndSet(null, cause);
        completed.add(work);
    }

    /**
     * Feed worker outcomes back to PC. StreamThread only - {@code handleFutureResult} mutates shard and
     * partition state and PC's in-flight counter, none of which tolerate concurrent callers.
     */
    private void drainCompletions() {
        WorkContainer<byte[], byte[]> work;
        while ((work = completed.poll()) != null) {
            workManager.handleFutureResult(work);
        }
    }

    /**
     * The first processing failure since the last call, cleared as it is returned, so the caller can surface
     * it the way stock Kafka Streams surfaces one - out of the thread that drives processing.
     *
     * @return the failure, or null
     */
    public Throwable pollFailure() {
        return firstFailure.getAndSet(null);
    }

    /**
     * The frontier and its encoded holes, for every dirty partition - what the consumer-group commit should
     * carry. Whichever thread currently owns the task, and only one at a time - see
     * {@link #insideWorkManager}.
     * <p>
     * Completions are drained first, so work that has already finished is folded into the answer - but
     * nothing waits: records still in flight stay incomplete, which is precisely what keeps the frontier
     * below them. A commit-time drain would reintroduce the head-of-line stall this module exists to remove.
     * <p>
     * Collection does not clear anything. PC's dirty state clears only on {@link #onCommitSuccess}, so a
     * commit that fails after collection simply leaves the partition dirty and the next collection returns
     * the same (or newer) data - nothing is stranded.
     */
    public Map<TopicPartition, OffsetAndMetadata> collectCommitData() {
        enterWorkManager("collectCommitData");
        try {
            drainCompletions();
            return workManager.collectCommitDataForDirtyPartitions();
        } finally {
            leaveWorkManager();
        }
    }

    /**
     * Whether a commit is worth attempting: completed work exists that no successful commit has covered.
     * This is PC's own dirty flag, not a parallel copy of it - see KTD-S7's grain.
     * <p>
     * <b>Not StreamThread-only</b>, unlike the rest of this surface: Kafka Streams also reaches it from the
     * state-updater thread, via {@code DefaultStateUpdater.maybeCheckpointTasks} to {@code maybeCheckpoint}
     * to {@code commitNeeded()}, for tasks it currently owns. That is exclusive, not concurrent - see
     * {@link #insideWorkManager} for why, and for the guard that says so if it ever stops being true.
     */
    public boolean hasCommitDataOutstanding() {
        enterWorkManager("hasCommitDataOutstanding");
        try {
            drainCompletions();
            return workManager.isDirty();
        } finally {
            leaveWorkManager();
        }
    }

    /**
     * The other half of the commit protocol: report a <em>successful</em> commit back, so PC can mark the
     * covered work clean. The only caller of PC's {@code setClean} - skip this and every partition stays
     * dirty forever, which turns "commit when needed" into "commit every interval, unconditionally".
     * StreamThread only.
     */
    public void onCommitSuccess(final Map<TopicPartition, OffsetAndMetadata> committed) {
        enterWorkManager("onCommitSuccess");
        try {
            workManager.onOffsetCommitSuccess(committed);
        } finally {
            leaveWorkManager();
        }
    }

    /**
     * Claims sole occupancy of {@link WorkManager} for the calling thread, failing fast if another thread is
     * already inside. Every caller must pair this with {@link #leaveWorkManager()} in a {@code finally}.
     *
     * <p>Deliberately an {@link IllegalStateException} naming both threads rather than an assertion: an
     * assertion disappears without {@code -ea}, and this is exactly the class of mistake that produces no
     * symptom at the call site. The guarded methods do not nest, so occupancy needs no depth count.
     */
    private void enterWorkManager(final String method) {
        final Thread current = Thread.currentThread();
        // compareAndSet, not get-then-set. A read followed by a separate write leaves a window in which two
        // threads both observe null, both pass, and both proceed inside - the detector staying silent at
        // exactly the moment it is needed. Claiming the slot atomically closes it.
        if (insideWorkManager.compareAndSet(null, current)) {
            return;
        }
        final Thread holder = insideWorkManager.get();
        if (holder != current) {
            throw new IllegalStateException(String.format(
                    "%s touches WorkManager, which admits one thread at a time, but '%s' is already inside it; "
                            + "this call came from '%s'", method,
                    holder == null ? "another thread" : holder.getName(), current.getName()));
        }
    }

    private void leaveWorkManager() {
        insideWorkManager.set(null);
    }

    /** Whether either close path has run. A closed dispatcher hands out no work and accepts no more. */
    public boolean isClosed() {
        return closed;
    }

    public int getInFlightCount() {
        return inFlight.get();
    }

    /** Records this dispatcher handed to {@code registerWork}. */
    public long getRecordsOffered() {
        return recordsOffered.get();
    }

    /** Records that survived {@code EpochAndRecordsMap}'s epoch filter. A shortfall is the silent drop. */
    public long getRecordsAccepted() {
        return recordsAccepted.get();
    }

    /** This dispatcher's share of the dispatch marker - see {@link PcDispatchCounters}. */
    public long getRecordsDispatched() {
        return recordsDispatched.get();
    }

    public long getRecordsSucceeded() {
        return recordsSucceeded.get();
    }

    public long getRecordsFailed() {
        return recordsFailed.get();
    }

    /**
     * True when the last pump produced nothing, nothing is running, and no outcome is waiting to be reported
     * back to PC - which is to say, there is no point pumping again.
     * <p>
     * Deliberately <em>not</em> "PC holds no more records". With retries disabled a failed record blocks its
     * KEY shard permanently while the records behind it stay available in PC's counters, so that definition
     * reports a busy dispatcher forever and turns a handled failure into a hang.
     */
    public boolean isQuiescent() {
        return lastDispatchCount == 0 && inFlight.get() == 0 && completed.isEmpty();
    }

    /**
     * Pump until quiescent or out of time. Only safe from the thread that owns the WorkManager.
     *
     * @return true if quiescence was reached
     */
    public boolean pumpUntilQuiescent(final WorkPreparer preparer, final Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            dispatchAvailable(preparer);
            if (isQuiescent()) {
                return true;
            }
            try {
                Thread.sleep(2);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        drainCompletions();
        return isQuiescent();
    }

    /**
     * Every dispatcher currently alive in this JVM. Exists so a test can reach dispatchers buried inside
     * running {@code StreamTask}s to {@link #abortClose()} them - the crash-injection surface for the R10
     * kill-restart proof. Registered at construction, removed on either close path.
     * <p>
     * <b>Invariant for callers: any test that constructs a dispatcher must run {@code @Isolated}</b>, because
     * {@link #abortAllActive()} reaches every live instance in this JVM - including one belonging to a
     * concurrently running test class. This module inherits concurrent test execution from core's test jar,
     * so that isolation is load-bearing rather than decorative.
     */
    private static final Set<PcTaskDispatcher> ACTIVE = ConcurrentHashMap.newKeySet();

    /** Crash-injection for tests: {@link #abortClose()} every live dispatcher in the JVM. */
    public static void abortAllActive() {
        for (PcTaskDispatcher dispatcher : ACTIVE) {
            dispatcher.abortClose();
        }
    }

    /**
     * A crash, not a shutdown: no drain, no completion feed-back, no revocation, workers interrupted
     * immediately. Exists for the kill-restart proof (R10) - the orderly {@link #close()} path drains via
     * the patched {@code suspend()} and commits on the way down, which would hand a simulated crash exactly
     * the repair pass a real one never gets, and park each test repetition on the pool-termination wait.
     */
    public void abortClose() {
        if (closed) {
            return;
        }
        closed = true;
        ACTIVE.remove(this);
        workerPool.shutdownNow();
        log.info("PC dispatch ABORTED over {} - simulating a crash, nothing drained, nothing reported",
                inputPartitions);
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        ACTIVE.remove(this);
        workerPool.shutdown();
        try {
            if (!workerPool.awaitTermination(30, TimeUnit.SECONDS)) {
                log.warn("PC dispatch worker pool did not drain within 30s; forcing shutdown");
                workerPool.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            workerPool.shutdownNow();
        }
        // Touches WorkManager without claiming occupancy, unlike the five methods that do. Deliberate: this
        // runs after the pool has drained, and a teardown that threw because someone else was mid-call would
        // turn an orderly close into a leak. The asymmetry is the trade, not an oversight - if a caller can
        // reach close() concurrently with dispatch, the guard will not be the thing that tells you.
        drainCompletions();
        workManager.onPartitionsRevoked(new ArrayList<>(inputPartitions));
        log.info("PC dispatch closed over {}", inputPartitions);
    }
}
