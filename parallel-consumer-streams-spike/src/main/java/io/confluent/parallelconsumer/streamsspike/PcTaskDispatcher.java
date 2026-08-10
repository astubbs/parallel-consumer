package io.confluent.parallelconsumer.streamsspike;

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
     * Long enough that no run outlives it. See the retry note in the class javadoc - this is how "disabled"
     * is expressed, because PC has no "never retry" setting.
     */
    static final Duration RETRIES_DISABLED_DELAY = Duration.ofDays(3650);

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
     * How many records the last {@link #dispatchAvailable} handed to the pool, or -1 before the first one.
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
            Thread thread = new Thread(runnable, "pc-streams-spike-" + taskName + "-" + threadCounter.getAndIncrement());
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
        drainCompletions();

        if (closed) {
            lastDispatchCount = 0;
            return 0;
        }

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
                Runnable chainExecution;
                try {
                    chainExecution = preparer.prepare(work.getCr());
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
                workerPool.execute(() -> runOnWorker(work, chainExecution));
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

    private void runOnWorker(final WorkContainer<byte[], byte[]> work, final Runnable chainExecution) {
        try {
            chainExecution.run();
            work.onUserFunctionSuccess();
            recordsSucceeded.incrementAndGet();
            PcDispatchCounters.onCompletedSuccessfully();
            completed.add(work);
        } catch (Throwable t) {
            recordFailure(work, t);
        } finally {
            inFlight.decrementAndGet();
        }
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
     * carry. StreamThread only, like every WorkManager touch.
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
        drainCompletions();
        return workManager.collectCommitDataForDirtyPartitions();
    }

    /**
     * Whether a commit is worth attempting: completed work exists that no successful commit has covered.
     * StreamThread only. This is PC's own dirty flag, not a parallel copy of it - see KTD-S7's grain.
     */
    public boolean hasCommitDataOutstanding() {
        drainCompletions();
        return workManager.isDirty();
    }

    /**
     * The other half of the commit protocol: report a <em>successful</em> commit back, so PC can mark the
     * covered work clean. The only caller of PC's {@code setClean} - skip this and every partition stays
     * dirty forever, which turns "commit when needed" into "commit every interval, unconditionally".
     * StreamThread only.
     */
    public void onCommitSuccess(final Map<TopicPartition, OffsetAndMetadata> committed) {
        workManager.onOffsetCommitSuccess(committed);
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
        drainCompletions();
        workManager.onPartitionsRevoked(new ArrayList<>(inputPartitions));
        log.info("PC dispatch closed over {}", inputPartitions);
    }
}
