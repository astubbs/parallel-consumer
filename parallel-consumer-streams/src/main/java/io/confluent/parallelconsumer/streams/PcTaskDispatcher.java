package io.confluent.parallelconsumer.streams;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.internal.EpochAndRecordsMap;
import io.confluent.parallelconsumer.internal.PCModule;
import io.confluent.parallelconsumer.state.PartitionState;
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
import java.util.Collections;
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
 * <h2>The thread model, stated rather than assumed (astubbs#255, U10)</h2>
 * "One owner thread per task" is <b>not</b> the model, and believing it was cost a production-shaped defect.
 * Kafka Streams drives a task from more than one thread by design: {@code DefaultStateUpdater} calls
 * {@code StreamTask.maybeCheckpoint} from its own thread for restoring and standby tasks. So this class has
 * two surfaces, and which one a method belongs to is a decision, not an accident:
 *
 * <table border="1">
 *   <caption>Who may call what</caption>
 *   <tr><th>Surface</th><th>Methods</th><th>Callable from</th><th>Why</th></tr>
 *   <tr>
 *     <td><b>Mutating, guard enforced</b></td>
 *     <td>{@link #collectCommitData}, {@link #onCommitSuccess}, {@link #hasCommitDataOutstanding},
 *         {@link #updatePartitions}</td>
 *     <td><b>The owner thread only</b>, and {@code assertOwnerThread} throws otherwise</td>
 *     <td>Each reaches {@code WorkManager}, its shards or its partition state, none of which is
 *         thread-safe. A second caller corrupts offset bookkeeping without throwing. These are the ones a
 *         caller wiring up its own commit path is most likely to reach from the wrong thread.</td>
 *   </tr>
 *   <tr>
 *     <td><b>Mutating, convention only</b></td>
 *     <td>{@link #registerRecords}, {@link #dispatchAvailable}, {@link #pumpUntilQuiescent},
 *         {@link #pollFailure}, {@link #close}, {@link #abortClose}</td>
 *     <td>The owner thread only, <b>unenforced</b></td>
 *     <td>Just as unsafe off-thread as the row above, but reached only from Kafka's own single-threaded
 *         call sites ({@code addRecords}, {@code process}, {@code suspend}, task teardown) rather than
 *         from anything an integrator wires up. Adding the guard here is a behaviour change with real
 *         blast radius - it would turn today's silent misuse into a thrown exception mid-run - so it is
 *         deliberately left as a separate, measured decision rather than smuggled in with this one.
 *         <b>Do not read this row as "safe from any thread".</b></td>
 *   </tr>
 *   <tr>
 *     <td><b>Read-only</b></td>
 *     <td>{@link #hasUncommittedWork}, {@link #isClosed}, {@link #getInFlightCount},
 *         {@link #hasPendingCompletions}, {@link #isQuiescent},
 *         {@link #getBufferedRecordCount(TopicPartition)}, {@link #getBufferedRecordCount()},
 *         the counters</td>
 *     <td><b>Any thread</b></td>
 *     <td>Answered from atomics, concurrent collections and volatiles. Never touches {@code WorkManager}.
 *         This is what the state updater is allowed to ask, and the reason it may ask it.</td>
 *   </tr>
 * </table>
 *
 * <p>The rule that keeps the two apart: <b>a question is not allowed to mutate.</b> A query that drained the
 * completion mailbox "just to be accurate" is how a field read became a cross-thread write.
 *
 * <p>Which thread is the owner is itself mutable - see {@link #bindToCurrentThread()}. <b>Nothing in
 * production calls it yet</b>, and saying otherwise would send the next reader hunting for a call site that
 * does not exist: in Kafka 3.9.2 a reassigned task is closed and rebuilt rather than handed to another
 * thread, so the constructor's bind is the only one that happens. It is here because the construction-time
 * capture was an unstated assumption rather than a decision, and the cross-thread hazard that actually bit
 * (the state updater) is handled by the read-only surface above, not by rebinding.
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

    /**
     * The thread currently allowed to touch {@link WorkManager} through this dispatcher.
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
     * <p><b>Bound at hand-off, not at construction, and this used to be the other way round</b>
     * (astubbs#255, U10). Capturing the constructing thread is correct only while a {@code StreamTask} is
     * created and driven by one StreamThread forever. A task object that outlives one thread assignment -
     * recycled, or reassigned by a rebalance - then carries a stale owner, and the guard throws
     * {@link IllegalStateException} on a <em>legitimate</em> call from its new thread. The constructor
     * still binds, so nothing changes for the common case; {@link #bindToCurrentThread()} is what makes
     * re-assignment expressible instead of fatal.
     *
     * <p>Volatile because the binding thread and the reading thread are by definition different ones
     * during a hand-off.
     */
    private volatile Thread ownerThread;

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

    private final AtomicReference<Failure> firstFailure = new AtomicReference<>();

    /**
     * Set by the first {@link #recordFailure}, never cleared while this dispatcher is live. See
     * {@link #hasPendingFailure()} for why this is a separate field from {@link #firstFailure} rather than a
     * null check on it.
     * <p>
     * Volatile: written by whichever worker fails first, read by the owner thread on every pump.
     */
    private volatile boolean failureSeen;

    /**
     * PC's dirty state, published by the owner thread for readers that must not touch {@link WorkManager}.
     *
     * <p><b>Not a parallel copy of the run state, and the distinction matters</b> - this repository has a
     * recorded silent-stall defect caused by exactly that anti-pattern. It is a <em>publication</em> of a
     * single authoritative value: written only by the owner thread, only immediately after that thread has
     * changed the value it mirrors ({@link #drainCompletions}, {@link #onCommitSuccess},
     * {@link #updatePartitions}), and never consulted to decide whether to dispatch. Nothing derives from it
     * that is not also derivable from {@code workManager.isDirty()} on the owner thread.
     *
     * <p>It exists because {@link #hasUncommittedWork()} has to answer from another thread - see that
     * method for the {@code DefaultStateUpdater} call path that makes this non-negotiable.
     */
    private volatile boolean pcDirty;

    /**
     * How many records this dispatcher holds for each partition that <b>no worker has started</b>. The
     * owner thread's copy; {@link #publishedBufferedCounts} is what everyone else reads.
     *
     * <p><b>"Buffered" means accepted-and-not-yet-handed-out, not accepted-and-not-yet-completed</b>, and
     * that is the whole design (astubbs#255, U14). Kafka Streams' {@code RecordQueue.size()} counts records
     * {@code nextRecord()} has not yet returned; because stock processes synchronously, that is the same as
     * "not yet started". This is the faithful analogue. The two definitions differ by the in-flight set,
     * which is bounded by {@link #poolSize}, so the one chosen here is both what stock means and the only
     * unbounded quantity of the two.
     *
     * <p><b>Deliberately NOT derived from {@code PartitionState.getNumberOfIncompleteOffsets()}</b>, which
     * was the tempting DRY source - PC's own per-partition number, no new state. It counts a <em>failed</em>
     * record forever, because with retries disabled a failed record never leaves {@code incompleteOffsets},
     * so backpressure built on it would pause that partition permanently. That is the same trap
     * {@link #hasUncommittedWork()} documents avoiding, and repeating it would be repeating a recorded
     * defect.
     *
     * <p>Mutated at exactly two places, both on the owner thread: {@link #registerRecords} adds what PC
     * actually took on, and {@link #dispatchAvailable} subtracts one per record consumed. See
     * {@link #registerRecords} for why the increment is measured rather than counted.
     *
     * <p>Records queued behind a <em>failed</em> record on the same key stay counted, and that is correct
     * rather than a trap: they genuinely are in memory and will never be handed out. Their partition stays
     * paused, and the task is already dying, because the failure surfaces through {@link #pollFailure()}.
     */
    private final Map<TopicPartition, Integer> bufferedByPartition = new HashMap<>();

    /**
     * {@link #bufferedByPartition}, published for readers that are not the owner thread.
     *
     * <p>The same publication pattern as {@link #pcDirty}, for the same reason and under the same rule:
     * written only by the owner thread, only immediately after that thread has changed the map it mirrors,
     * and never consulted to decide whether to dispatch. An immutable snapshot rather than a shared mutable
     * map, so a reader can never observe a half-updated view.
     *
     * <p>It is a publication rather than a convenience because the question genuinely gets asked from
     * another thread: the memory-bound proof samples occupancy from a watcher thread while the run is in
     * flight, which is exactly the shape the class javadoc's read-only surface exists to permit.
     *
     * <p><b>Not folded into one snapshot object with {@link #pcDirty}.</b> One coherent record carrying
     * every published field would be better than several independent volatiles, and is the right end state
     * once the parallel branches land - but a sibling unit is editing this class for a stream-time
     * low-water mark, and two independent redesigns of the publication mechanism would cost more than they
     * save.
     */
    private volatile Map<TopicPartition, Integer> publishedBufferedCounts = Collections.emptyMap();

    /**
     * How many times {@link #dispatchAvailable} consumed a record with no buffered count left to decrement.
     * Owner thread only, and exposed so a test can assert the accounting balanced rather than merely that
     * nothing crashed. Non-zero is expected after a revocation and suspicious otherwise.
     */
    private int bufferedUnderflows;

    /**
     * The condition the StreamThread waits on instead of sitting out the rest of {@code poll.ms}. Bound
     * alongside {@link #ownerThread}, so the signal this dispatcher's workers raise is always the one its
     * current owner waits on. See {@link PcWorkSignal} for what happens if that ever stops being true
     * (nothing bad: the thread reverts to the stock poll).
     *
     * <p>Re-bound by {@link #bindToCurrentThread()} rather than fixed at construction, because the guard
     * and the signal are <b>one seam</b>: move the owner without moving the signal and you get a dispatcher
     * whose guard admits the new thread while its wake still goes to the old one, which is a stall rather
     * than an exception and therefore worse.
     *
     * <p>Volatile for the same reason as {@link #ownerThread} - worker threads read it on every outcome.
     */
    private volatile PcWorkSignal workSignal;

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

        // Bound LAST, so that a StreamThread can never see a half-built dispatcher through the gate.
        bindToCurrentThread();

        ACTIVE.add(this);
        log.info("PC dispatch active for task {} over {} with a pool of {}, KEY ordering, retries disabled",
                taskName, this.inputPartitions, poolSize);
    }

    /**
     * Hand this dispatcher to the calling thread: it becomes the only thread allowed on the commit surface,
     * and the thread whose {@link PcWorkSignal} this dispatcher's workers raise.
     * <p>
     * Called from the constructor, and again wherever a {@code StreamTask} is handed to a different thread.
     * Both bindings move together on purpose - see {@link #workSignal} for why splitting them converts a
     * loud exception into a silent stall.
     * <p>
     * Idempotent: re-binding to the thread that already owns this dispatcher does nothing, which matters
     * because Kafka calls the task-assignment paths on every rebalance whether or not the owner changed.
     *
     * @throws IllegalStateException if the dispatcher is already closed - a closed dispatcher hands out no
     *                               work, so binding one to a live thread can only be a wiring mistake, and
     *                               the resulting silence is exactly the failure the revive guard exists for
     */
    public void bindToCurrentThread() {
        if (closed) {
            throw new IllegalStateException(
                    "Cannot bind a closed PC dispatcher over " + inputPartitions + " to thread '"
                            + Thread.currentThread().getName() + "' - it would accept records without ever "
                            + "dispatching them");
        }
        final Thread newOwner = Thread.currentThread();
        final Thread previousOwner = ownerThread;
        if (previousOwner == newOwner) {
            return;
        }
        if (previousOwner != null) {
            // Leave the old owner's signal, or its gate keeps reporting work for a dispatcher it no longer
            // drives, and that thread takes the split-wait branch forever.
            workSignal.deregister(this);
            log.info("PC dispatch over {} re-bound from thread '{}' to '{}'",
                    inputPartitions, previousOwner.getName(), newOwner.getName());
        }
        this.workSignal = PcWorkSignal.registerForCurrentThread(this);
        this.ownerThread = newOwner;
    }

    /**
     * Hand a poll batch to PC instead of to the partition group. Called from the StreamThread only.
     * <p>
     * Also raises this partition's {@link #bufferedByPartition} count, by <b>the number PC actually took
     * on</b> rather than the number offered or the number that survived the epoch filter. The difference is
     * load-bearing: PC can refuse a record after the epoch filter - bootstrap truncation does exactly that,
     * and it is observed against the mock consumer this dispatcher builds - and a refused record is never
     * handed out, so counting it would leave the count permanently high and pause the partition forever.
     * Measuring the delta in PC's own incomplete-offset count across {@code registerWork} asks PC what it
     * accepted instead of predicting it.
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

        final int willBeQueued = countRecordsPcWillQueue(partition, epochTagged);
        workManager.registerWork(epochTagged);
        if (willBeQueued > 0) {
            bufferedByPartition.merge(partition, willBeQueued, Integer::sum);
            publishBufferedCounts();
        }
    }

    /**
     * How many of {@code batch}'s records PC will turn into work for {@code partition} - those it has not
     * already completed. Owner thread only: it reads {@link WorkManager}'s partition state.
     *
     * <p><b>Counted in records, because the decrement is in records.</b> The obvious implementation - take
     * the delta in {@code getNumberOfIncompleteOffsets()} across {@code registerWork} - is wrong, and wrong
     * in the dangerous direction. That map is keyed by <em>offset</em> while
     * {@link #dispatchAvailable} decrements once per {@code WorkContainer}, and the two are not the same
     * count: two records sharing an offset produce two containers but one map entry, and the truncation
     * {@code registerWork} performs can shrink the map while containers are being added. Every such
     * mismatch leaves the count permanently <em>low</em>, and a count that is too low stops the pause ever
     * firing again for that partition - the memory bound silently absent, with nothing to say so.
     *
     * <p>Asking PC per record instead keeps both sides in the same unit, and asks the same question
     * {@code registerWork} is about to ask itself.
     */
    private int countRecordsPcWillQueue(final TopicPartition partition,
                                        final EpochAndRecordsMap<byte[], byte[]> batch) {
        final PartitionState<byte[], byte[]> state = workManager.getPm().getPartitionState(partition);
        final EpochAndRecordsMap<byte[], byte[]>.RecordsAndEpoch entry = batch.records(partition);
        if (state == null || entry == null) {
            return 0;
        }
        int queued = 0;
        for (final ConsumerRecord<byte[], byte[]> record : entry.getRecords()) {
            if (!state.isRecordPreviouslyCompleted(record)) {
                queued++;
            }
        }
        return queued;
    }

    /**
     * Republish {@link #bufferedByPartition} for the cross-thread readers. <b>Owner thread only</b>, and
     * called immediately after every change to the map it mirrors - the same rule as
     * {@link #publishDirtyState()}.
     */
    private void publishBufferedCounts() {
        publishedBufferedCounts = Collections.unmodifiableMap(new HashMap<>(bufferedByPartition));
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

        // A failure the StreamThread has not yet surfaced stops further work being handed out (U14). The
        // drain above still ran, deliberately: those outcomes have to reach PC's accounting or the commit
        // frontier is wrong on the way down. In-flight records are left to finish rather than interrupted -
        // a worker cancelled mid-chain leaves a half-forwarded record, which is worse than letting it
        // complete, and abandoning rather than interrupting is the policy PC already applies at revocation.
        //
        // Returning 0 marks the dispatcher quiescent once the pool empties, which is what suspend() needs to
        // drain rather than sit out its full timeout.
        if (hasPendingFailure()) {
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
                // The record has left the buffer the moment PC hands it out, whatever happens to it next -
                // dispatched, dropped during preparation, or failed at preparation. Decremented here, beside
                // the `consumed` it must always agree with, rather than on each of the three branches below,
                // because three call sites is how they would silently stop agreeing.
                //
                // computeIfPresent, so the count can never go negative and a container handed out for a
                // partition that has since been revoked cannot resurrect an entry for it. Underflow is
                // COUNTED AND LOGGED rather than silently clamped: a count that drifts low stops the pause
                // ever firing for that partition, which is this unit's whole purpose failing with no
                // symptom. It is expected exactly once per revoked partition with work still in PC's shards,
                // so it is a warning to correlate rather than an error on its own.
                // Underflow is decided BEFORE the decrement, not from its result: the legitimate last
                // decrement removes the entry, so computeIfPresent returns null for "counted down to zero"
                // and for "was never there" alike, and reading the result cannot tell them apart.
                final TopicPartition consumedFrom = work.getTopicPartition();
                if (!bufferedByPartition.containsKey(consumedFrom) && bufferedUnderflows++ == 0) {
                    log.warn("PC dispatch consumed a record for {} with no buffered count to decrement - "
                                    + "expected after that partition is revoked, a backpressure accounting "
                                    + "drift otherwise. Further occurrences are counted, not logged.",
                            consumedFrom);
                }
                bufferedByPartition.computeIfPresent(consumedFrom,
                        (partition, held) -> held <= 1 ? null : held - 1);
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
        if (consumed > 0) {
            publishBufferedCounts();
        }
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
            // ORDER MATTERS, and it looks arbitrary: the signal goes AFTER the decrement, not with the
            // completion above. Signal first and the woken StreamThread drains the completion, then computes
            // capacity = poolSize - inFlight against a count this thread has not yet decremented, dispatches
            // nothing, and parks again - a full poll-budget stall, microseconds wide, that would never
            // reproduce on demand.
            // Raised on the failure path too, via this finally: a failed record frees a pool slot exactly like
            // a successful one, and the failure branch is the one a later refactor forgets.
            //
            // KNOWN RESIDUAL, and this ordering narrows it rather than closing it. A StreamThread that
            // reaches PcWorkSignal's wait between the add and the decrement above leaves on the pending
            // outcome without any signal, and still reads the stale count. It only bites at poolSize 1,
            // where the stale read means capacity 0 and the next pass waits a poll budget with a
            // dispatchable record in hand; at poolSize >= 2 the spare slot absorbs it. Closing it properly
            // means splitting this counter in two - a pool-capacity count the worker owns and decrements
            // BEFORE enqueuing, and an outstanding count the StreamThread owns and decrements as it drains -
            // so that neither question is ever answered from the other's window. Deliberately not done here:
            // it re-keys every reader of getInFlightCount(), and this defect is bounded and non-hanging.
            workSignal.signalWorkAvailable();
        }
    }

    private void recordFailure(final WorkContainer<byte[], byte[]> work, final Throwable cause) {
        work.onUserFunctionFailure(cause);
        recordsFailed.incrementAndGet();
        PcDispatchCounters.onFailed();
        firstFailure.compareAndSet(null, new Failure(cause, work.getCr()));
        // Set AFTER firstFailure, so an owner thread that sees the bar closed can always find the failure
        // behind it. The reverse order leaves a window where dispatch is barred but pollFailure() returns
        // null, which reads as a dispatcher that has silently stopped working.
        failureSeen = true;
        completed.add(work);
    }

    /**
     * A worker's exception, together with the record that caused it (astubbs#255, U14).
     *
     * <p>The record travels with the throwable because stock Kafka Streams' {@code handleException} names
     * the topic, partition and offset of the failing record in the message it wraps, and a PC-dispatched
     * failure that arrives without them is strictly harder to diagnose than the stock one it replaces - the
     * thing being surfaced is the failure of <em>one record out of poolSize running concurrently</em>, so
     * "which one" is the first question anyone asks.
     */
    public static final class Failure {

        private final Throwable cause;

        private final ConsumerRecord<byte[], byte[]> record;

        Failure(final Throwable cause, final ConsumerRecord<byte[], byte[]> record) {
            this.cause = cause;
            this.record = record;
        }

        public Throwable getCause() {
            return cause;
        }

        public String getTopic() {
            return record.topic();
        }

        public int getPartition() {
            return record.partition();
        }

        public long getOffset() {
            return record.offset();
        }
    }

    /**
     * Feed worker outcomes back to PC. StreamThread only - {@code handleFutureResult} mutates shard and
     * partition state and PC's in-flight counter, none of which tolerate concurrent callers.
     */
    private void drainCompletions() {
        WorkContainer<byte[], byte[]> work;
        // PEEK, handle, publish, THEN remove - and the order is the whole point. Polling first takes the
        // outcome out of `completed` before `pcDirty` has been republished, which opens a window where a
        // foreign reader sees an empty queue, a zero in-flight count and a stale-false pcDirty all at once,
        // and hasUncommittedWork() answers "nothing outstanding" for work that is very much outstanding.
        // That is the one direction that guarantee may not fail in. Leaving the item on the queue until
        // after the publish means it is always visible through one term or the other.
        while ((work = completed.peek()) != null) {
            try {
                workManager.handleFutureResult(work);
            } finally {
                // Publish before the remove, on the thread that just changed the value being published. See
                // pcDirty for why this is a publication rather than a shadow.
                //
                // In a finally, so a throwing outcome cannot WEDGE the dispatcher. Peeking before removing is
                // what closes the publication race, but it also means a head-of-queue item that throws would
                // be re-attempted by every future drain - from process(), from every commit, from close() -
                // and re-throw forever. Removing unconditionally turns a permanent wedge back into a single
                // failed outcome, which is the lesser of the two and the behaviour the poll-first version had.
                publishDirtyState();
                completed.poll();
            }
        }
    }

    /**
     * Re-read PC's authoritative dirty state and publish it. <b>Owner thread only</b> - it reads
     * {@link WorkManager}, which is exactly what the readers of {@link #pcDirty} are forbidden from doing.
     */
    private void publishDirtyState() {
        pcDirty = workManager.isDirty();
    }

    /**
     * The first processing failure since the last call, cleared as it is returned, so the caller can surface
     * it the way stock Kafka Streams surfaces one - out of the thread that drives processing.
     *
     * @return the failure and the record that caused it, or null
     */
    public Failure pollFailure() {
        return firstFailure.getAndSet(null);
    }

    /**
     * Whether this dispatcher has seen a processing failure that has not been made good.
     * <p>
     * {@link #dispatchAvailable} gates on this so that a known failure stops further work being handed out
     * (astubbs#255, U14). Stock Kafka Streams stops processing at the throw; here the throw happens on a
     * worker and reaches the StreamThread a pump later, so without this gate the dispatcher keeps handing
     * out records for a whole poll budget after it already knows the task is going to die.
     * <p>
     * <b>Sticky, and NOT the same question as "is a failure waiting to be surfaced".</b> That distinction is
     * the whole point, and getting it wrong reopens the hole this gate exists to close.
     * {@link #pollFailure()} clears {@link #firstFailure} as it hands the failure over - so a gate that read
     * only that field would be open again the instant the StreamThread took the exception, and
     * {@code StreamTask.suspend()}'s {@link #pumpUntilQuiescent} - which runs immediately afterwards, on the
     * way down - would dispatch the <em>entire remaining backlog</em> of a task that is already dying. The
     * bound would then be "everything PC holds" rather than "what was already running", which is no bound at
     * all.
     * <p>
     * Cleared only by {@link #close()} or {@link #abortClose()}, because those are the only points at which
     * this dispatcher stops being the one that failed. A revived task is refused outright by the patched
     * {@code StreamTask.revive()}, so there is no path that needs it cleared while still live.
     * <p>
     * A question, so it does not mutate.
     */
    public boolean hasPendingFailure() {
        return failureSeen;
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
        assertOwnerThread("collectCommitData");
        drainCompletions();
        return workManager.collectCommitDataForDirtyPartitions();
    }

    /**
     * Whether a commit is worth attempting: completed work exists that no successful commit has covered.
     * StreamThread only. This is PC's own dirty flag, not a parallel copy of it - see KTD-S7's grain.
     */
    public boolean hasCommitDataOutstanding() {
        assertOwnerThread("hasCommitDataOutstanding");
        drainCompletions();
        return workManager.isDirty();
    }

    /**
     * Whether <b>any</b> work this dispatcher has taken on is not yet covered by a successful commit -
     * including work still running.
     *
     * <h2>Why this is a second question and not a synonym</h2>
     * {@link #hasCommitDataOutstanding} asks "is a commit worth attempting", which only completed work can
     * answer. This asks "is it safe to walk away", which in-flight work also answers, and <b>on the stock
     * path those are the same question</b> - processing is synchronous, so a record is either done or not
     * yet started and there is no third state. Asynchronous dispatch creates that third state, and
     * {@code StreamTask.validateClean()} is exactly the caller that must see it: a clean close while
     * records are still inside the processor chain otherwise succeeds silently, where Kafka's contract is
     * to throw {@code TaskMigratedException} so the TaskManager closes the task dirty instead
     * (astubbs#255, U10).
     *
     * <h2>CALLABLE FROM ANY THREAD, and that is a requirement rather than a convenience</h2>
     * <b>This must not drain, must not touch {@link WorkManager}, and must not be owner-thread-guarded.</b>
     * Kafka Streams' {@code DefaultStateUpdater} calls {@code StreamTask.maybeCheckpoint} <em>from its own
     * thread</em> for restoring and standby tasks, and {@code maybeCheckpoint} gates on this question. Under
     * stock that gate is a plain {@code boolean} field read, which is why nobody had to think about it;
     * routing it through a draining call turned a field read into concurrent mutation of PC's shard and
     * partition state from a second thread. That produced
     * {@code IllegalStateException: ... owner-thread-only, but was called from '...-StateUpdater-1'} - the
     * guard doing its job, on a hazard that had been silent since the gate was introduced.
     *
     * <p>So the answer is assembled from three sources that are each safe to read concurrently: an
     * {@code AtomicInteger}, a {@code ConcurrentLinkedQueue}, and {@link #pcDirty}, a volatile the owner
     * thread publishes whenever it changes PC's authoritative dirty state. The drain still happens - in
     * {@link #collectCommitData}, on the owner thread, which is where a commit actually needs it.
     *
     * <h2>What is deliberately NOT counted: records PC is still holding</h2>
     * Not "does the WorkManager still have records". With retries disabled a failed record blocks its KEY
     * shard permanently and the records queued behind it stay <em>available</em> in PC's accounting
     * forever, so that definition would make a task with one poison pill impossible to ever close clean -
     * the same trap {@link #isQuiescent()} exists to avoid. A failed record is not lost by closing: it was
     * never completed, so the frontier never rose over it and whoever owns the partition next re-reads it.
     *
     * <p>The three terms leave no gap for a live record to hide in, and <b>two orderings are what close
     * that gap</b> - neither is incidental. A worker enqueues its outcome <em>before</em> it decrements the
     * in-flight count, so a record is never missing from both at once. And {@link #drainCompletions} leaves
     * an outcome on the queue until <em>after</em> it has republished {@link #pcDirty}, so a record is never
     * missing from both the queue and the dirty flag at once. A reader can therefore be stale in the
     * direction of "yes, work exists" for a few microseconds, and never in the direction of "no, nothing
     * outstanding" - which is the only direction that would be unsafe.
     */
    public boolean hasUncommittedWork() {
        // TERM ORDER IS LOAD-BEARING: pcDirty is read LAST, and reading it first is a real bug.
        // drainCompletions publishes pcDirty before it removes the outcome from the queue, so a reader that
        // samples pcDirty EARLY (before the publish) and the queue LATE (after the removal) straddles the
        // whole transition and sees false from both - the one answer this method may never give. Reading the
        // queue first and pcDirty last makes that impossible: an empty queue means the removal already
        // happened, the removal follows the publish, so the pcDirty read that comes after it cannot be stale.
        return inFlight.get() > 0 || hasPendingCompletions() || pcDirty;
    }

    /**
     * Track a change to this task's partition assignment, so PC's assignment and epoch bookkeeping follows
     * a cooperative rebalance instead of staying on the set the dispatcher was built with. StreamThread
     * only.
     * <p>
     * <b>Revoke before assign, which is Parallel Consumer's own order</b>
     * ({@code AbstractParallelEoSStreamProcessor.onPartitionsRevoked} commits, then truncates). Revoking is
     * what bumps the partition-assignment epoch, and the epoch is the whole safety mechanism here: an
     * outcome that arrives for a partition revoked while its record was in flight is recognised as stale by
     * {@code WorkManager.handleFutureResult} and dropped instead of advancing a frontier the new owner is
     * now responsible for. <b>In-flight work on a revoked partition is abandoned, not awaited</b> - which is
     * the at-least-once trade PC already makes, rather than a new policy invented here.
     * <p>
     * A no-op update revokes nothing. Kafka calls the assignment paths on every rebalance whether or not
     * the set changed, and a spurious epoch bump would strand live work for no reason.
     *
     * @param newPartitions the task's input partitions after the rebalance
     */
    public void updatePartitions(final Set<TopicPartition> newPartitions) {
        assertOwnerThread("updatePartitions");

        final List<TopicPartition> revoked = new ArrayList<>();
        for (TopicPartition held : inputPartitions) {
            if (!newPartitions.contains(held)) {
                revoked.add(held);
            }
        }
        final List<TopicPartition> assigned = new ArrayList<>();
        for (TopicPartition wanted : newPartitions) {
            if (!inputPartitions.contains(wanted)) {
                assigned.add(wanted);
            }
        }
        if (revoked.isEmpty() && assigned.isEmpty()) {
            return;
        }

        if (!revoked.isEmpty()) {
            workManager.onPartitionsRevoked(revoked);
            inputPartitions.removeAll(revoked);
            // Records PC was holding for a revoked partition are gone with it - the new owner re-reads them.
            // Leaving their count behind would report a buffer for a partition this task does not have, and
            // since the count is what pauses a partition, a stale entry survives a rebalance as a partition
            // paused by a task that no longer owns it.
            revoked.forEach(bufferedByPartition::remove);
        }
        if (!assigned.isEmpty()) {
            workManager.onPartitionsAssigned(assigned);
            inputPartitions.addAll(assigned);
        }
        // A revocation discards that partition's completed-but-uncommitted work, so the dirty answer can
        // change here without any completion being drained.
        publishDirtyState();
        publishBufferedCounts();
        log.info("PC dispatch partitions updated: revoked {}, assigned {}, now over {}",
                revoked, assigned, inputPartitions);
    }

    /**
     * The other half of the commit protocol: report a <em>successful</em> commit back, so PC can mark the
     * covered work clean. The only caller of PC's {@code setClean} - skip this and every partition stays
     * dirty forever, which turns "commit when needed" into "commit every interval, unconditionally".
     * StreamThread only.
     */
    public void onCommitSuccess(final Map<TopicPartition, OffsetAndMetadata> committed) {
        assertOwnerThread("onCommitSuccess");
        workManager.onOffsetCommitSuccess(committed);
        publishDirtyState();
    }

    /**
     * Fails fast when a {@link WorkManager}-touching method is called off the owning thread.
     *
     * <p>Deliberately an {@link IllegalStateException} naming both threads rather than an assertion: an
     * assertion disappears without {@code -ea}, and this is exactly the class of mistake that produces no
     * symptom at the call site.
     *
     * <p>The message points at {@link #bindToCurrentThread()} because since U10 an off-owner call has two
     * distinct causes - a genuinely wrong thread, and a task handed to a new thread without being re-bound -
     * and they need different fixes.
     */
    private void assertOwnerThread(final String method) {
        final Thread current = Thread.currentThread();
        final Thread owner = ownerThread;
        if (current != owner) {
            throw new IllegalStateException(String.format(
                    "%s touches WorkManager and is owner-thread-only, but was called from '%s'; the owner is "
                            + "'%s'. If this task was handed to a new thread, bindToCurrentThread() on that "
                            + "thread is what moves ownership.",
                    method, current.getName(), owner == null ? "<unbound>" : owner.getName()));
        }
    }

    /** Whether either close path has run. A closed dispatcher hands out no work and accepts no more. */
    public boolean isClosed() {
        return closed;
    }

    public int getInFlightCount() {
        return inFlight.get();
    }

    /**
     * How many records this dispatcher holds for {@code partition} that no worker has started - Kafka
     * Streams' {@code RecordQueue.size()} for the PC path, and what the patched {@code StreamTask} compares
     * against {@code buffered.records.per.partition} to decide whether to pause the consumer.
     * <p>
     * <b>Callable from any thread</b>, answered from {@link #publishedBufferedCounts}. See
     * {@link #bufferedByPartition} for what "buffered" means here and why it is not PC's incomplete-offset
     * count.
     */
    public int getBufferedRecordCount(final TopicPartition partition) {
        return publishedBufferedCounts.getOrDefault(partition, 0);
    }

    /**
     * {@link #getBufferedRecordCount(TopicPartition)} summed over every partition - the PC path's answer for
     * {@code StreamTask.numBuffered()}, and so for the {@code active-buffer-count} metric. Any thread.
     */
    public int getBufferedRecordCount() {
        int total = 0;
        for (Integer held : publishedBufferedCounts.values()) {
            total += held;
        }
        return total;
    }

    /** Test seam: see {@link #bufferedUnderflows}. Owner thread only. */
    int getBufferedUnderflowCount() {
        return bufferedUnderflows;
    }

    /**
     * Whether a worker outcome is sitting in the mailbox waiting for the StreamThread to feed it back to PC.
     * <p>
     * This is {@link PcWorkSignal}'s wake predicate, and it is level-triggered on purpose - the queue is
     * drained only by the StreamThread inside {@link #dispatchAvailable}, so a StreamThread that is waiting is
     * by definition not draining, and a completion enqueued at any point before or during the wait is still
     * here to be seen. That is what makes a lost wakeup impossible without any extra state.
     */
    public boolean hasPendingCompletions() {
        return !completed.isEmpty();
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

    /**
     * Test seam: how many dispatchers are live in this JVM.
     * <p>
     * Exists because a leaked dispatcher has no other observable signature - it keeps running, keeps its
     * pool, and says nothing. The pool and the {@link PcWorkSignal} registration are each checkable on their
     * own; this is the third, and a teardown path that forgets to close is only caught by all three.
     */
    static int activeDispatcherCount() {
        return ACTIVE.size();
    }

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
        // Deregistered on BOTH close paths: a closed dispatcher that still answered PcWorkSignal's gate would
        // keep its StreamThread on the split-wait branch for work that can never complete.
        workSignal.deregister(this);
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
        workSignal.deregister(this);
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
        // The revoke is the LAST thing that changes PC's dirty answer, and it happens after the drain above
        // published it - so without this the flag stays true for a dispatcher that owns nothing and will
        // never commit again. Kafka's shouldClearCommitStatusesInCloseDirty caught exactly that: it asserts
        // commitNeeded() is false once the task has closed dirty.
        publishDirtyState();
        // Same reason as the revoke in updatePartitions: a closed dispatcher holds nothing, and a count left
        // behind here would keep a partition paused after the task that paused it has gone.
        bufferedByPartition.clear();
        publishBufferedCounts();
        log.info("PC dispatch closed over {}", inputPartitions);
    }
}
