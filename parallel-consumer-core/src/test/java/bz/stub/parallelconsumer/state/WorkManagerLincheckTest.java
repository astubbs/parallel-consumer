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
        // 200 iterations, not 50. At 50 this found the tear in two runs out of three: a rebalance is
        // expensive next to a mailbox handoff, so the window is a small fraction of each invocation, and the
        // budget has to buy enough scenarios to land in it. Still seconds - the whole harness is one
        // partition and one record.
        var options = new StressOptions()
                .threads(2)
                .actorsPerThread(1)
                .actorsBefore(0)
                .actorsAfter(0)
                .iterations(200)
                .invocationsPerIteration(5_000);
        String report = LincheckHarness.runExpectingViolation("WorkManager / stress",
                LincheckHarness.check(options, this.getClass()));
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
