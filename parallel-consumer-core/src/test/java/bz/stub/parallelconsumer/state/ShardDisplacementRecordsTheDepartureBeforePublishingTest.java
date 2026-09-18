package bz.stub.parallelconsumer.state;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import pl.tlinkowski.unij.api.UniLists;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

/**
 * The displacement door of astubbs#178, at the instruction that matters: {@link ProcessingShard#addWorkContainer}
 * must record the displaced container as an in-flight departure BEFORE it publishes the replacement, because a scan
 * can land between the two.
 * <p>
 * <b>The interleaving.</b> {@code workMap.put(offset, fresh)} makes the fresh container resident, and resident is
 * selectable. The first version of the fix recorded the displaced container in {@code inFlightDepartures} inside
 * {@code retire(displaced)}, several statements after the put - so a scan arriving in that gap saw no departure
 * and a takeable fresh container, and handed it out while the displaced one was still executing. Reported on the
 * astubbs/parallel-consumer#517 review. {@code KeyOrderAcrossRebalanceTest}'s displacement arm cannot see it: it
 * runs {@code addWorkContainer} to completion before it scans.
 * <p>
 * <b>Forced, not raced for.</b> The scan runs from a hook on {@link RecordPopulation#onRetired()}, which the
 * displacement branch reaches exactly once, after the put and before the (old) record - the seam is the shard's own
 * collaborator, as {@link ShardSeamTestBase} does with the partition-state manager. Single-threaded, so the
 * verdict is the code's ordering and nothing else. On the shipped engine both sides are the controller thread
 * and this gap is unreachable; the direct-pull engine (astubbs#361) scans from worker threads, and nothing checks
 * the confinement either way - which is why the ordering, not the confinement, is what this pins.
 *
 * @author Antony Stubbs
 * @see ShardSeamTestBase
 */
class ShardDisplacementRecordsTheDepartureBeforePublishingTest extends ShardSeamTestBase {

    private static final long CONTESTED_OFFSET = 7L;

    @Test
    void aScanLandingBetweenThePublicationAndTheRetireTakesNothing() {
        RecordPopulation population = spy(new RecordPopulation());
        ConsumerRecord<String, String> record = recordAt(CONTESTED_OFFSET);
        ProcessingShard<String, String> shard = shardWith(wm.getPm(), population, record);

        wm.onPartitionsAssigned(UniLists.of(TP));
        long firstEpoch = wm.getPm().getEpochOfPartition(TP);
        wm.onPartitionsRevoked(UniLists.of(TP));
        wm.onPartitionsAssigned(UniLists.of(TP));
        long currentEpoch = wm.getPm().getEpochOfPartition(TP);
        assertWithMessage("PRECONDITION: the rebalance moved the epoch").that(currentEpoch).isGreaterThan(firstEpoch);

        // a stale resident whose worker is still inside the user function
        var displaced = new WorkContainer<>(firstEpoch, record, module);
        assertThat(displaced.onQueueingForExecution()).isTrue();
        shard.plantResident(displaced);

        var fresh = new WorkContainer<>(currentEpoch, record, module);
        var takenInsideTheGap = new ArrayList<WorkContainer<String, String>>();
        var hookFired = new AtomicBoolean();

        // THE INTERLEAVING: the scan lands after the put has published `fresh` and before the displacement
        // branch has finished with `displaced`. onRetired is the first thing retire(displaced) does, so a
        // scan run from it observes exactly the state a concurrent scanner would in that gap.
        doAnswer(invocation -> {
            if (hookFired.compareAndSet(false, true)) {
                takenInsideTheGap.addAll(shard.getWorkIfAvailable(10, new RetryQueue()));
            }
            return invocation.callRealMethod();
        }).when(population).onRetired();

        shard.addWorkContainer(fresh);

        assertWithMessage("PRECONDITION: the scan must have run inside the displacement branch, or this test "
                + "exercised nothing").that(hookFired.get()).isTrue();
        assertWithMessage("PRECONDITION: the displacement branch fired - the fresh container holds the offset")
                .that(shard.getWorkContainerAtOffset(CONTESTED_OFFSET).orElse(null)).isSameInstanceAs(fresh);
        assertWithMessage("PRECONDITION: the displaced container is still executing")
                .that(displaced.isInFlight()).isTrue();

        assertWithMessage("KEY ORDER VIOLATED in the gap between publishing the replacement and recording the "
                + "displaced departure: a scan there was handed the fresh container while the displaced one was "
                + "still running (astubbs#178, second door, astubbs/parallel-consumer#517 review)")
                .that(takenInsideTheGap).isEmpty();

        // and the departure clears the ordinary way
        assertThat(shard.getWorkIfAvailable(10, new RetryQueue())).isEmpty();
        displaced.endFlight();
        List<WorkContainer<String, String>> takenAfterwards = shard.getWorkIfAvailable(10, new RetryQueue());
        assertThat(takenAfterwards).hasSize(1);
        assertThat(takenAfterwards.get(0)).isSameInstanceAs(fresh);
        assertWithMessage("the accounting half is untouched by the ordering half")
                .that(shard.getCountOfWorkAwaitingSelection()).isEqualTo(shard.countSelectionClaimedByScan());
    }
}
