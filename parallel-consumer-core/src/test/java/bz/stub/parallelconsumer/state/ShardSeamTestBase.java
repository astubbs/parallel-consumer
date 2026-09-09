package bz.stub.parallelconsumer.state;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.internal.PCModuleTestEnv;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.mockito.ArgumentMatchers;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.Mockito.doAnswer;

/**
 * The single-threaded fixture for driving a {@link ProcessingShard} against another thread's action landing at an
 * exact instruction.
 * <p>
 * <b>The seam is {@link PartitionStateManager#getPartitionState(WorkContainer)}</b>, which
 * {@link ProcessingShard} reaches on every staleness question it asks - inside {@link ProcessingShard#addWorkContainer}
 * and inside {@link ProcessingShard#removeStaleWorkContainersFromShard}. Making the collaborator run something on the
 * way past is what lets one thread reproduce an interleaving deterministically, instead of racing for it and
 * reporting a rate.
 * <p>
 * <b>A seam stands in for the other thread; it is not a fixture the product knows about.</b> If a call stops
 * happening where a test assumes, the test stops exercising its window and starts passing vacuously - so every
 * subclass asserts its own precondition that the seam fired, rather than trusting this class.
 * <p>
 * Shared by {@link ShardPopulationRaceTest} and {@link ShardStaleSweepReplacementEvictionTest}: both drive the same
 * seam at the same class, and a second copy of the arming mechanism is how two of them drift apart.
 *
 * @author Antony Stubbs
 */
abstract class ShardSeamTestBase {

    static final String TOPIC = "topic";

    static final TopicPartition TP = new TopicPartition(TOPIC, 0);

    final ModelUtils mu = new ModelUtils();

    final PCModuleTestEnv module = mu.getModule();

    final WorkManager<String, String> wm = module.workManager();

    /**
     * Makes the collaborator run {@code action} the next time the shard asks it whether a container is stale, and
     * only then - the sweeps an action drives ask the same question themselves, so a re-arming seam would recurse.
     */
    void onNextStalenessCheck(PartitionStateManager<String, String> seam, Runnable action) {
        var armed = new AtomicBoolean(true);
        doAnswer(invocation -> {
            Object state = invocation.callRealMethod();
            if (armed.compareAndSet(true, false)) {
                action.run();
            }
            return state;
        }).when(seam).getPartitionState(ArgumentMatchers.<WorkContainer<String, String>>any());
    }

    ProcessingShard<String, String> shardWith(PartitionStateManager<String, String> pm,
                                              RecordPopulation population,
                                              ConsumerRecord<String, String> record) {
        return new ProcessingShard<>(ShardKey.of(record, module.options().getOrdering()),
                module.options(), pm, population, new DispatchScanMeter());
    }

    ConsumerRecord<String, String> recordAt(long offset) {
        return new ConsumerRecord<>(TOPIC, 0, offset, "a-key", "value-" + offset);
    }
}
