package bz.stub.parallelconsumer.state;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.internal.EpochAndRecordsMap;
import bz.stub.parallelconsumer.internal.PCModuleTestEnv;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.jetbrains.lincheck.datastructures.Operation;
import org.jetbrains.lincheck.datastructures.StressOptions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import pl.tlinkowski.unij.api.UniLists;
import pl.tlinkowski.unij.api.UniMaps;

import java.util.List;

import static com.google.common.truth.Truth.assertThat;

/**
 * CALIBRATION for the worst of the three settled torn-read candidates: {@code WorkManager.handleFutureResult}
 * makes its staleness check and its subsequent acting reads through two separate {@code partitionStates.get(tp)}
 * lookups, so a rebalance landing between them lets a result that passed the check act on a different state
 * object than the one it was checked against.
 * <p>
 * Two operations, both real: a unit of in-flight work returns from the user function, and the broker revokes
 * and reassigns the partition. Nothing here says they interact.
 * <p>
 * <b>What makes the tear observable.</b> Not the failure-path harm - a stale container orphaned in the retry
 * queue is a leak, invisible to any return value, and stating that plainly is part of the calibration result.
 * The success path IS observable: acting on the reassigned state calls {@code PartitionState.onSuccess} for an
 * offset that state has never seen, tripping its {@code assert}. Assertions are on because surefire enables
 * them by default, and Lincheck compares a thrown exception against the sequential specification - where
 * neither order throws, since a revoke that lands BEFORE the check bumps the epoch and the result is dropped.
 * <p>
 * <b>Reuses the repo's existing pattern</b> of driving {@link WorkManager} through Kafka's own
 * {@link MockConsumer} rather than a Mockito mock, as {@code WorkManagerTest} does - which also keeps
 * Mockito's generated classes out of the instrumented object graph.
 * <p>
 * <b>Expected on this tree</b>: a violation, because master carries the defect unfixed. astubbs#346 carries
 * the fix, and when it lands this test goes red and must be inverted - see {@link ShardManagerLincheckTest}.
 * <p>
 * STRESS only: {@code ShardKey} is unavoidable here (registering work goes through the shard manager), and
 * Lincheck 3.7 cannot model-check a Lombok {@code callSuper = true} value type - see
 * {@link LincheckSuperHashCodeProbeTest}.
 *
 * @author Antony Stubbs
 */
@Tag("lincheck")
public class WorkManagerLincheckTest {

    private static final String TOPIC = "lincheck-topic";

    private static final TopicPartition TP = new TopicPartition(TOPIC, 0);

    private static final long FIRST_EPOCH = 0L;

    private final PCModuleTestEnv module = newModule();

    private final WorkManager<String, String> wm = module.workManager();

    /**
     * One unit of work, taken and completed by the user function - i.e. exactly the state a container is in
     * when {@code handleFutureResult} receives it.
     */
    private final WorkContainer<String, String> inFlight = takeOneUnitOfWork();

    private static PCModuleTestEnv newModule() {
        var options = ParallelConsumerOptions.<String, String>builder()
                .consumer(new MockConsumer<>(OffsetResetStrategy.EARLIEST))
                .build();
        return new PCModuleTestEnv(options);
    }

    private WorkContainer<String, String> takeOneUnitOfWork() {
        wm.onPartitionsAssigned(UniLists.of(TP));
        var record = new ConsumerRecord<>(TOPIC, 0, 0L, "a-key", "a-value");
        wm.registerWork(new EpochAndRecordsMap<>(
                new ConsumerRecords<>(UniMaps.of(TP, UniLists.of(record))), wm.getPm()));

        List<WorkContainer<String, String>> taken = wm.getWorkIfAvailable(1);
        WorkContainer<String, String> work = taken.get(0);
        work.onUserFunctionSuccess();
        return work;
    }

    /**
     * Control thread: a completed unit of work comes back from the worker pool.
     */
    @Operation
    public void completeWork() {
        wm.handleFutureResult(inFlight);
    }

    /**
     * Broker-poll thread: the group rebalances - this partition is taken away and handed back. One operation
     * rather than two because that is how a rebalance arrives, as a single callback pair inside one
     * {@code consumer.poll}.
     * <p>
     * {@code nonParallelGroup} keeps two of these out of different threads, and that is FIDELITY rather than
     * convenience: {@link org.apache.kafka.clients.consumer.ConsumerRebalanceListener} callbacks are invoked
     * by the consumer from inside a single {@code poll}, so two overlapping rebalances is a state production
     * cannot reach. Left parallel, the harness reports a real but different defect first - two concurrent
     * rebalances throw {@code ArrayIndexOutOfBoundsException} out of the unsynchronised per-partition counter
     * maps - and Lincheck stops at the first violation, so the checkpoint-3 tear is never reached. That
     * defect is recorded in {@code docs/inflight/bug-pcmetrics-registered-meters-is-a-plain-arraylist.md}
     * rather than deleted along with the scenario that found it.
     */
    @Operation(nonParallelGroup = "rebalance")
    public void revokeAndReassign() {
        wm.onPartitionsRevoked(UniLists.of(TP));
        wm.onPartitionsAssigned(UniLists.of(TP));
    }

    @Test
    void stressRediscoversTheCheckpointThreeTear() {
        // 1,000 iterations, and the number is measured rather than chosen. A rebalance is expensive next
        // to a mailbox handoff, so the tear's window is a small fraction of each invocation, and 200 - the
        // first bound committed here, chosen on three green runs - missed 2 runs in 8 on the reviewer's
        // machine. A miss is a flake, and a flake fails this build with no retry, by design.
        //
        // HOW THE BOUND WAS PRICED. Running the harness at a deliberately starved iterations(25) makes it
        // miss most of the time, and the miss fraction gives the per-iteration probability, which then
        // prices every bound at once. 48 runs on one machine - 14 hits in 32 at 25, 8 of 8 at 200, 8 of 8
        // at 1,000 - fit a per-iteration hit probability of 2.33% (95% profile-likelihood interval
        // 1.38-3.72%). Treating each iteration as an independent trial is not just assumed: that fit
        // predicts 17.7 misses out of 32 at 25 against 18 observed, and 0.07 out of 8 at 200 against 0.
        //
        // THE NUMBER IS A RANGE, AND IT IS MACHINE-DEPENDENT. Those 48 runs put this machine's miss rate at
        // 1,000 below 1e-8%. The reviewer's machine is 3.4x slower to find the tear (0.69%/iteration from
        // its 2-in-8; a likelihood-ratio test rejects the two being equal, p=0.011), and on ITS estimate
        // 1,000 misses about 1 run in 1,000. Only 8 runs exist from that machine, so the pessimistic end of
        // its interval is far weaker - roughly 1 in 14 - and no bound stated here should be read as more
        // precise than that. docs/inflight/test-lincheck-lane-open-items.md carries what would settle it.
        //
        // WHY 1,000 RATHER THAN THE SMALLEST SUFFICIENT NUMBER. The extra iterations are free where it
        // counts: Lincheck stops at the FIRST violation, so a run that finds the tear never reaches them -
        // measured mean 11.1s at 1,000 against 12.9s at 200, i.e. no increase at all. Only the run that was
        // going to fail gets longer, at a measured ~0.14s per iteration, so an exhaust costs ~2.4min here
        // against ~33s at 200. That run is either the flake this removes, or the designed inversion below.
        //
        // Do NOT tune this by lowering it back, by adding a retry, or by weakening the assertion: this
        // harness asserting that a violation EXISTS is the calibration itself.
        var options = new StressOptions()
                .threads(2)
                .actorsPerThread(1)
                .actorsBefore(0)
                .actorsAfter(0)
                .iterations(1_000)
                .invocationsPerIteration(5_000);
        String report = LincheckHarness.runExpectingViolation("WorkManager / stress", options, getClass());
        assertThat(report).contains("completeWork");
    }

    /**
     * The epoch the partition starts on, asserted so that a change to the assignment bookkeeping cannot
     * silently turn the setup above into something that no longer models a first assignment.
     */
    @Test
    void setupPutsTheWorkOnTheFirstEpoch() {
        assertThat(inFlight.getEpoch()).isEqualTo(FIRST_EPOCH);
    }
}
