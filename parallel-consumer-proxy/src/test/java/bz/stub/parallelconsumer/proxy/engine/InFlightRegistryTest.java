package bz.stub.parallelconsumer.proxy.engine;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.state.WorkContainer;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.HashSet;
import java.util.Set;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

/**
 * The registry's collision policy, which is the difference between a rebalance costing one redelivery and a
 * rebalance blocking a shard forever - and the compare-and-claim that makes two threads racing on one token
 * resolve to a single winner.
 * <p>
 * Containers are mocks here deliberately: the policy is about <em>which</em> container an entry holds and
 * whether its partition generation has moved on, and both are inputs, not behaviours to simulate.
 *
 * @author Antony Stubbs
 */
class InFlightRegistryTest {

    private static final String RECORD_ID = "topic/0/0";

    private final Set<WorkContainer<byte[], byte[]>> staleContainers = new HashSet<>();

    private final InFlightRegistry registry = new InFlightRegistry(staleContainers::contains);

    @SuppressWarnings("unchecked")
    private static WorkContainer<byte[], byte[]> container() {
        return mock(WorkContainer.class);
    }

    private static InFlightRegistry.InFlight entry(WorkContainer<byte[], byte[]> wc, long epoch) {
        return new InFlightRegistry.InFlight(wc, null, epoch, Instant.MAX);
    }

    @Test
    void anOrdinaryRegistrationDisplacesNothing() {
        var entry = entry(container(), 1);

        assertThat(registry.register(RECORD_ID, entry)).isEmpty();
        assertThat(registry.peek(RECORD_ID)).hasValue(entry);
    }

    /**
     * The bug this policy exists for: a rebalance leaves the old generation's entry behind, and the record's
     * redelivery - a FRESH container for the same topic-partition-offset - must replace it rather than throw
     * into core's user-function catch block, which would error-retry the record forever and block its shard.
     */
    @Test
    void aRedeliveryStrandedByARebalanceReplacesTheEntryAndHandsTheOldOneBack() {
        var stranded = container();
        var strandedEntry = entry(stranded, 1);
        registry.register(RECORD_ID, strandedEntry);
        staleContainers.add(stranded); // the partition was revoked under it

        var redelivery = entry(container(), 1);
        var displaced = registry.register(RECORD_ID, redelivery);

        assertWithMessage("the displaced entry must come back for its caller to return to scheduling")
                .that(displaced).hasValue(strandedEntry);
        assertThat(registry.peek(RECORD_ID)).hasValue(redelivery);
    }

    /**
     * The discriminator is container identity, not equality: {@code WorkContainer} equality is
     * topic/partition/offset, so a redelivery of a stranded record is EQUAL to the entry it collides with,
     * and an {@code equals}-based test would read this case as the double-flight bug.
     */
    @Test
    void aDifferentContainerForOneRecordIsAReplacementEvenBeforeItLooksStale() {
        var first = entry(container(), 1);
        registry.register(RECORD_ID, first);

        var displaced = registry.register(RECORD_ID, entry(container(), 2));

        assertThat(displaced).hasValue(first);
    }

    /**
     * The loud throw is kept for the case it was written for: the same live container registered twice is two
     * deliveries of one record in flight at once, which core's scheduling makes impossible - an engine
     * bookkeeping bug, not a rebalance.
     */
    @Test
    void theSameLiveContainerRegisteredTwiceIsStillTheLoudBug() {
        var wc = container();
        registry.register(RECORD_ID, entry(wc, 1));

        var thrown = assertThrows(IllegalStateException.class,
                () -> registry.register(RECORD_ID, entry(wc, 2)));

        assertThat(thrown).hasMessageThat().contains("in flight at once");
        assertWithMessage("the refused registration must not have displaced the live one")
                .that(registry.peek(RECORD_ID).orElseThrow().capturedEpoch()).isEqualTo(1);
    }

    /** A claim is conditional on the entry still being the registered one - the peek-then-claim race's fence. */
    @Test
    void claimingAnEntryThatHasSinceBeenReplacedLosesTheRace() {
        var peeked = entry(container(), 1);
        registry.register(RECORD_ID, peeked);
        var replacement = entry(container(), 2);
        registry.register(RECORD_ID, replacement);

        assertThat(registry.claim(RECORD_ID, peeked)).isEmpty();
        assertWithMessage("the live delivery must survive a lost claim untouched")
                .that(registry.peek(RECORD_ID)).hasValue(replacement);
    }

    @Test
    void theSnapshotIsAPictureAndNotAView() {
        registry.register(RECORD_ID, entry(container(), 1));
        var snapshot = registry.snapshot();

        registry.unregister(RECORD_ID);

        assertThat(snapshot).hasSize(1);
        assertThat(registry.snapshot()).isEmpty();
    }
}
