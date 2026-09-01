package bz.stub.parallelconsumer.state;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.internal.PCModuleTestEnv;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.jetbrains.lincheck.datastructures.IntGen;
import org.jetbrains.lincheck.datastructures.Operation;
import org.jetbrains.lincheck.datastructures.Param;
import org.jetbrains.lincheck.datastructures.StressOptions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import pl.tlinkowski.unij.api.UniLists;

import java.util.Collections;
import java.util.List;

/**
 * REGRESSION DETECTOR, not a calibration - and it is the first in this lane that way round, so the difference
 * is worth stating. {@link ShardManagerLincheckTest}, {@link PartitionStateLincheckTest} and
 * {@link WorkManagerLincheckTest} all assert that Lincheck FINDS a violation, because each is pointed at a
 * defect master still carries. {@link RetryQueue}'s is fixed, so this harness asserts the opposite: no
 * interleaving of the queue's operations may produce a result no sequential order could.
 * <p>
 * <b>What it guards.</b> {@code removeAll} once opened with a fast path reading the shared {@code unique} map
 * off-lock; the JMM permits a stale {@code true} there, so the method could return {@code false} having
 * removed nothing and leave a container in the retry queue while it was also in flight. The fix guards on the
 * caller's own list instead. Nothing below names that seam - the operations are just what the two real threads
 * do to the queue - so this covers the whole locked API, not one line of it.
 * <p>
 * <b>Both halves of that were MEASURED with control arms, because a harness asserting "no violation" is the
 * exact shape of a false green this lane exists to avoid.</b> Each arm broke one thing in {@link RetryQueue},
 * ran this test unchanged, and the class was restored byte-identical afterwards:
 * <ul>
 *   <li><b>It has teeth.</b> Dropping the write lock from {@code add} and {@code remove} - predicted red -
 *       failed in under 30s on a two-actor scenario, {@code dropOne(0): true} beside
 *       {@code dropTaken(0): true}: two operations both claiming to have removed the same container, which no
 *       sequential order permits.</li>
 *   <li><b>It cannot see the bug that was actually fixed here</b> - predicted green, and green. Restoring the
 *       historical {@code unique.isEmpty()} guard, read off-lock, produced no violation. That is not a gap in
 *       the bounds: with perfect visibility that guard IS linearizable, because an empty read that was true
 *       at some instant justifies removing nothing at that instant. Its defect is that the read can be STALE,
 *       and staleness is a memory-model question - jcstress owns that half, and the {@code jcstress-poc}
 *       probe module is where it is measured.</li>
 * </ul>
 * So what this closes is the linearizability question: whether the read/write lock discipline across
 * {@code add}, {@code remove}, {@code removeAll}, {@code contains} and {@code isEmpty} can be broken by
 * ordinary interleaving - which is what a future edit "simplifying" the locking would break first, and what
 * arm one shows this catches.
 * <p>
 * <b>STRESS only</b>, for the reason already settled in this lane: the queue keys itself with
 * {@code WorkContainerSortKey}, a Lombok {@code @EqualsAndHashCode(callSuper = true)} value type, and Lincheck
 * 3.7 rewrites its {@code super.hashCode()} into a virtual self-call that recurses to StackOverflowError.
 * {@link LincheckSuperHashCodeProbeTest} is the tripwire that fires when that is fixed.
 * <p>
 * Written against target 7 of {@code docs/inflight/test-lincheck-lane-open-items.md} ("RetryQueue is not
 * modelled at all"); the untested behaviour it does not reach is inventoried in
 * {@code docs/inflight/test-retry-queue-behaviour-untested.md}.
 *
 * @author Antony Stubbs
 * @see RetryQueueTest for the single-threaded return-value contract of {@code removeAll}
 */
@Tag("lincheck")
@Param(name = "offset", gen = IntGen.class, conf = "0:1")
public class RetryQueueLincheckTest {

    private static final String TOPIC = "lincheck-topic";

    private static final int PARTITION = 0;

    private static final long EPOCH = 0L;

    /**
     * Built ONCE for the whole class rather than per instance, and that is not just a speed choice.
     * <p>
     * Lincheck constructs a fresh test instance for every invocation, and a {@link PCModuleTestEnv} drags a
     * whole metrics stack up with it; more importantly, first-time classloading inside a scenario is the trap
     * {@link ShardManagerLincheckTest} records - the model checker cannot tell it from a spin loop. The
     * containers are safe to share because {@link RetryQueue} only ever READS a container's topic, partition,
     * offset and retry-due time; none of these has ever failed, so every retry-due time is
     * {@link java.time.Instant#MIN} and the ordering is fully determined by the offset.
     */
    private static final PCModuleTestEnv MODULE = new PCModuleTestEnv();

    private static final List<WorkContainer<String, String>> CONTAINERS = UniLists.of(
            newContainer(0L),
            newContainer(1L));

    /**
     * The {@code removeAll} arguments, one singleton list per offset, built up front for the same reason.
     * These stand for what {@code ProcessingShard.getWorkIfAvailable} passes: a freshly built list the caller
     * shares with nobody.
     */
    private static final List<List<WorkContainer<String, String>>> SINGLETON_REMOVALS = UniLists.of(
            UniLists.of(CONTAINERS.get(0)),
            UniLists.of(CONTAINERS.get(1)));

    private static WorkContainer<String, String> newContainer(long offset) {
        return new WorkContainer<>(EPOCH, new ConsumerRecord<>(TOPIC, PARTITION, offset, "key-" + offset, "value"), MODULE);
    }

    private final RetryQueue retryQueue = new RetryQueue();

    public RetryQueueLincheckTest() {
        // Initial state: one unit already queued for retry, i.e. the ordinary state of a consumer that has
        // had a failure. An empty queue would let every operation take its trivial path.
        retryQueue.add(CONTAINERS.get(0));
    }

    /**
     * Control thread: the user function threw, so the work is scheduled for retry.
     */
    @Operation
    public boolean queueForRetry(@Param(name = "offset") int offset) {
        return retryQueue.add(CONTAINERS.get(offset));
    }

    /**
     * Control thread: a single container leaves the queue - the path {@code ShardManager} takes when a stale
     * container is swept.
     */
    @Operation
    public boolean dropOne(@Param(name = "offset") int offset) {
        return retryQueue.remove(CONTAINERS.get(offset));
    }

    /**
     * Control thread: work was taken for the pool, so the containers it took leave the queue. This is the
     * production caller, and it runs on every work request per shard.
     */
    @Operation
    public boolean dropTaken(@Param(name = "offset") int offset) {
        return retryQueue.removeAll(SINGLETON_REMOVALS.get(offset));
    }

    /**
     * The same call when the shard took no previously-failed work - i.e. the fast path, which returns without
     * taking the write lock at all. It is a separate operation because "returns early" is exactly the leg that
     * has to keep agreeing with the locked one.
     */
    @Operation
    public boolean dropNothingTaken() {
        return retryQueue.<String, String>removeAll(Collections.emptyList());
    }

    /**
     * Reader: does the queue still hold this work? Feeds the shard's decision not to re-queue.
     */
    @Operation
    public boolean stillQueued(@Param(name = "offset") int offset) {
        return retryQueue.contains(CONTAINERS.get(offset));
    }

    /**
     * Reader: the emptiness question the deleted fast path used to ask of shared state.
     */
    @Operation
    public boolean queueIsEmpty() {
        return retryQueue.isEmpty();
    }

    /**
     * Bounds. Unlike the violation-expecting harnesses in this lane, every iteration here runs to completion -
     * a run that finds nothing cannot stop early - so the bound is a straight cost, and there is no hit rate to
     * price on the gating machine. 100 x 1,000 keeps it inside the lane's existing whole-lane budget while
     * covering every pairing of the six operations several times over.
     * <p>
     * The assertion is Lincheck's own: {@code check} throws and fails this test, carrying the interleaving,
     * the moment a result appears that no sequential order of the same operations could produce. Do not
     * convert a red here into a wider bound or a retry - on this class it means the locking has been broken.
     */
    @Test
    void stressFindsNoWayToBreakTheQueuesLocking() {
        new StressOptions()
                .threads(2)
                .actorsPerThread(3)
                .actorsBefore(0)
                .actorsAfter(0)
                .iterations(100)
                .invocationsPerIteration(1_000)
                .check(getClass());
    }
}
