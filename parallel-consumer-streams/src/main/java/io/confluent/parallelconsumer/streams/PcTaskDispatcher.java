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
 * <h3>The commit surface really is driven from two threads</h3>
 * The three commit methods once said "StreamThread only" in their javadoc, and {@link #ownerThread} turned
 * that comment into a check. The comment was false, and the check is what proved it: Kafka Streams'
 * {@code DefaultStateUpdater} calls {@code StreamTask.maybeCheckpoint} <b>from its own thread</b> for every
 * task it is restoring, and the patched {@code maybeCheckpoint} asks whether a commit is outstanding before
 * it refreshes changelog offsets. The honest model, established by walking every caller in
 * {@code kafka-streams} 3.9.2:
 * <ul>
 *   <li>{@link #hasCommitDataOutstanding()} - <b>any thread</b>. The StreamThread reaches it through
 *       {@code prepareCommit}, {@code validateClean} and {@code commitNeeded()}; the state-updater thread
 *       reaches it through {@code maybeCheckpoint} on a RESTORING task. It is therefore written as a genuine
 *       query - it reads two counters and touches neither {@code WorkManager} nor the mailbox.</li>
 *   <li>{@link #collectCommitData()} and {@link #onCommitSuccess} - <b>owner thread only</b>, still enforced
 *       by {@link #assertOwnerThread}. Both reach {@code WorkManager}, and Streams only reaches them through
 *       {@code StreamTask.prepareCommit} and {@code StreamTask.updateCommittedOffsets}, which the state
 *       updater cannot call: it holds tasks as {@code ReadOnlyTask}, whose {@code prepareCommit} throws and
 *       whose {@code commitNeeded} throws for an active task. {@code maybeCheckpoint} is the one method it
 *       calls on the real object.</li>
 *   <li>{@link #registerRecords} and {@link #dispatchAvailable} - StreamThread, and unguarded because
 *       {@code addRecords} and {@code process} are hot-path. One known exception, out of scope here and not
 *       reachable by default: with Streams' private {@code __processing.threads.enabled__} config on,
 *       {@code DefaultTaskExecutor} calls {@code task.process} from its own thread, which would drive
 *       {@link #dispatchAvailable} off the owner thread.</li>
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
 * @see PcDispatchSettings for the per-instance decision of whether a task gets one of these at all
 * @see PcDispatchSwitch for the JVM-wide fallback that decision falls through to
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

    /**
     * The thread that constructed this dispatcher, and the only one allowed to touch {@link WorkManager}
     * through it.
     *
     * <p>Several methods here already document "StreamThread only" in prose. This turns that comment into
     * a check, because the cost of getting it wrong is silent: {@code WorkManager} and its partition state
     * are not thread-safe, so a second thread calling in corrupts offset bookkeeping without throwing, and
     * the damage surfaces later as a commit that covers work which never completed.
     *
     * <p>Worth the guard specifically now that the commit surface exists. {@link #collectCommitData} and
     * {@link #onCommitSuccess} reach {@code WorkManager} directly and sit exactly where a caller wiring up
     * its own commit path is most likely to call from a commit or scheduler thread rather than the owner.
     *
     * <p>{@link #hasCommitDataOutstanding()} was guarded too, and is not any more - not because the rule was
     * relaxed for it, but because the method no longer breaks it. Kafka Streams asks that question from the
     * state-updater thread (see the class javadoc), so the answer had to stop coming from a mailbox drain.
     */
    private final Thread ownerThread;

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
     * Successful completions <b>published</b> into {@link #completed}, counted by whichever thread publishes
     * one, before it publishes it. With {@link #successesCommitted} this is what lets
     * {@link #hasCommitDataOutstanding()} answer from any thread without touching {@link WorkManager}.
     * <p>
     * Not a second copy of PC's dirty flag that could drift out of step with it: this dispatcher owns both of
     * that flag's edges. PC turns a partition dirty on exactly the successes that pass through
     * {@link #completed} ({@code PartitionState.onSuccess}) and clean on exactly the acknowledgements that
     * pass through {@link #onCommitSuccess} ({@code PartitionState.onOffsetCommitSuccess}). Counting those two
     * events is the same information as the flag, published where a second thread may read it safely.
     * <p>
     * Counted at publication rather than at drain on purpose: the whole point is that a worker finishing must
     * make the answer true <em>immediately</em>, with no owner-thread drain in between. A count taken at drain
     * would be thread-safe and wrong, which is worse than the crash it replaced.
     */
    private final AtomicLong successesPublished = new AtomicLong();

    /**
     * Successes {@link #drainCompletions} has fed back to PC. Owner thread only, so a plain field.
     * <p>
     * Counted at the drain rather than assumed equal to {@link #successesPublished}, so that a collection can
     * mark covered exactly what it collected: a worker publishing between the drain and the collection is not
     * in the collected map, and must stay outstanding.
     */
    private long successesDrained;

    /** {@link #successesDrained} as of the last {@link #collectCommitData()}. Owner thread only. */
    private long successesCollected;

    /**
     * The completed work a successful commit has actually covered: {@link #successesCollected} as of the last
     * {@link #onCommitSuccess}. Volatile because {@link #hasCommitDataOutstanding()} reads it from whichever
     * thread asks; only the owner thread ever writes it.
     */
    private volatile long successesCommitted;

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
     * The per-instance entry point, and the one the patched {@code StreamTask} uses.
     *
     * @param streamsConfigs this instance's configuration, as {@code ProcessorContext#appConfigs()} returns
     *                       it. Null or empty means the instance said nothing, and the decision falls
     *                       through to the JVM-wide {@link PcDispatchSwitch}.
     * @return a dispatcher, or null when dispatch is off for this instance - null being the patched
     *         {@code StreamTask}'s signal to keep using its own {@code PartitionGroup}.
     * @throws IllegalArgumentException if a PC dispatch property is set to something unintelligible. Thrown
     *                                  here rather than defaulted, because the property that turns the seam
     *                                  off is the last place a typo may pass silently.
     */
    public static PcTaskDispatcher createIfEnabled(final String taskName,
                                                   final Set<TopicPartition> inputPartitions,
                                                   final Map<String, ?> streamsConfigs) {
        final PcDispatchSettings settings = PcDispatchSettings.resolve(streamsConfigs);
        if (!settings.isEnabled()) {
            return null;
        }
        return new PcTaskDispatcher(taskName, inputPartitions, settings.getPoolSize());
    }

    /**
     * For callers with no {@code StreamsConfig} to hand - the decision comes from the JVM-wide
     * {@link PcDispatchSwitch} alone.
     *
     * @return a dispatcher, or null when the switch is off
     */
    public static PcTaskDispatcher createIfEnabled(final String taskName, final Set<TopicPartition> inputPartitions) {
        return createIfEnabled(taskName, inputPartitions, null);
    }

    public PcTaskDispatcher(final String taskName, final Set<TopicPartition> inputPartitions, final int poolSize) {
        this.inputPartitions = new LinkedHashSet<>(inputPartitions);
        this.poolSize = poolSize;
        this.ownerThread = Thread.currentThread();

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
                    // Dropped during preparation - consumed, nothing to run. Still a success as far as PC is
                    // concerned, and so still commit-outstanding, hence publishSuccess and not a bare add.
                    work.onUserFunctionSuccess();
                    publishSuccess(work);
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
            publishSuccess(work);
        } catch (Throwable t) {
            recordFailure(work, t);
        } finally {
            inFlight.decrementAndGet();
        }
    }

    /**
     * Publish a successful outcome to the owner thread, and make it visible to
     * {@link #hasCommitDataOutstanding()} in the same movement.
     * <p>
     * The count goes up <b>before</b> the container is queued, never after: the invariant the commit query
     * rests on is that nothing sits in {@link #completed} uncounted. Reversed, a completion could be drained
     * and folded into PC while the counter still said nothing had happened.
     * <p>
     * The single publication point for successes is the point. Failures deliberately do not come through here
     * - see {@link #recordFailure}.
     */
    private void publishSuccess(final WorkContainer<byte[], byte[]> work) {
        successesPublished.incrementAndGet();
        completed.add(work);
    }

    /**
     * A failure is published to the owner thread like a success, but is <b>not</b> counted towards
     * {@link #hasCommitDataOutstanding()}, because it does not make a commit worth attempting: PC leaves the
     * offset incomplete and {@code PartitionState.onFailure} is a no-op, so the partition never turns dirty.
     * <p>
     * That asymmetry is why the query counts successes and not the mailbox's size. Counting the mailbox would
     * report a commit outstanding after a poison pill - forever, since retries are disabled and the record
     * never succeeds - and {@code validateClean} would turn that into a spurious {@code TaskMigratedException}
     * on an otherwise clean close.
     */
    private void recordFailure(final WorkContainer<byte[], byte[]> work, final Throwable cause) {
        work.onUserFunctionFailure(cause);
        recordsFailed.incrementAndGet();
        PcDispatchCounters.onFailed();
        firstFailure.compareAndSet(null, cause);
        completed.add(work);
    }

    /**
     * Feed worker outcomes back to PC. Owner thread only - {@code handleFutureResult} mutates shard and
     * partition state and PC's in-flight counter, none of which tolerate concurrent callers. This is the one
     * place that reaches PC on behalf of another thread's work, which is why every caller of it carries the
     * owner-thread rule, and why {@link #hasCommitDataOutstanding()} is no longer one of them.
     */
    private void drainCompletions() {
        WorkContainer<byte[], byte[]> work;
        while ((work = completed.poll()) != null) {
            // Read the outcome before handing the container over - handleFutureResult ends the flight and
            // hands the container to shard and partition state, and nothing promises it stays readable.
            final boolean succeeded = work.isUserFunctionSucceeded();
            workManager.handleFutureResult(work);
            if (succeeded) {
                successesDrained++;
            }
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
     * carry. Owner thread only, like every WorkManager touch: Streams reaches this only through
     * {@code StreamTask.prepareCommit}, which the state updater cannot call.
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
        assertOwnerThread("collectCommitData");
        drainCompletions();
        // What this collection covers is what the drain just folded in - the DRAINED count, never the
        // published one. A worker that publishes from here on is not in the map below, so pinning the
        // published count instead would let onCommitSuccess mark its record covered by a commit that never
        // carried it, and the record would be lost on the next crash.
        successesCollected = successesDrained;
        return workManager.collectCommitDataForDirtyPartitions();
    }

    /**
     * Whether a commit is worth attempting: completed work exists that no successful commit has covered.
     * <p>
     * <b>Callable from any thread</b>, alone on this surface, and the only reason it may be is that it is a
     * genuine query - it compares two counters and touches neither {@link WorkManager} nor the completion
     * mailbox, so it mutates nothing and races nothing. It has to be: Kafka Streams' {@code DefaultStateUpdater}
     * asks a restoring task this question from its own thread, through {@code StreamTask.maybeCheckpoint}.
     * While the answer came from a mailbox drain, that call reached non-thread-safe {@code WorkManager} and
     * partition state from a second thread, concurrently with the StreamThread.
     * <p>
     * The drain stays where it belongs, on the owner-thread paths - {@link #dispatchAvailable} every pump, and
     * {@link #collectCommitData()} immediately before it reads the frontier - so removing it from here strands
     * nothing. Kafka Streams always follows a true answer here with {@code prepareCommit}, which collects, and
     * collection drains.
     * <p>
     * Still PC's dirty question rather than a parallel answer to it (KTD-S7's grain): see
     * {@link #successesPublished} for why counting these two events is the same information as PC's flag, and
     * why the count is taken at publication rather than at drain.
     */
    public boolean hasCommitDataOutstanding() {
        return successesPublished.get() > successesCommitted;
    }

    /**
     * The other half of the commit protocol: report a <em>successful</em> commit back, so PC can mark the
     * covered work clean. The only caller of PC's {@code setClean} - skip this and every partition stays
     * dirty forever, which turns "commit when needed" into "commit every interval, unconditionally".
     * Owner thread only.
     * <p>
     * Marks covered only what {@link #collectCommitData()} last collected, never what has completed since.
     * That is the same window PC protects internally with
     * {@code PartitionState.stateChangedSinceCommitStart}: a worker finishing between collection and this
     * acknowledgement was not in the committed map, so it stays outstanding and the next cycle commits it.
     */
    public void onCommitSuccess(final Map<TopicPartition, OffsetAndMetadata> committed) {
        assertOwnerThread("onCommitSuccess");
        workManager.onOffsetCommitSuccess(committed);
        successesCommitted = successesCollected;
    }

    /**
     * Fails fast when a {@link WorkManager}-touching method is called off the owning thread.
     *
     * <p>Deliberately an {@link IllegalStateException} naming both threads rather than an assertion: an
     * assertion disappears without {@code -ea}, and this is exactly the class of mistake that produces no
     * symptom at the call site.
     */
    private void assertOwnerThread(final String method) {
        final Thread current = Thread.currentThread();
        if (current != ownerThread) {
            throw new IllegalStateException(String.format(
                    "%s touches WorkManager and is owner-thread-only, but was called from '%s'; the owner is '%s'",
                    method, current.getName(), ownerThread.getName()));
        }
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
        drainCompletions();
        workManager.onPartitionsRevoked(new ArrayList<>(inputPartitions));
        log.info("PC dispatch closed over {}", inputPartitions);
    }
}
