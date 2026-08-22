package bz.stub.parallelconsumer.state;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.FakeRuntimeException;
import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.internal.EpochAndRecordsMap;
import bz.stub.parallelconsumer.internal.PCModuleTestEnv;
import bz.stub.parallelconsumer.state.WorkContainer.ExecutionState;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import pl.tlinkowski.unij.api.UniLists;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The claim on a record is one atomic transition, and every other move a record makes is one too.
 * <p>
 * WHY THIS EXISTS, and it is a real defect rather than a hypothetical. Selection used to read
 * {@code isAvailableToTakeAsWork() && onQueueingForExecution()}: the first evaluated three terms - not in
 * flight, no success verdict, retry delay passed - and the second re-validated <b>only the first</b>, with a
 * compare-and-set on an {@code AtomicBoolean}. That makes the <em>claim</em> atomic but not the
 * <em>decision</em>. Under the direct-pull engine, where every worker selects work concurrently, a puller
 * whose availability decision predated another puller's completion could still win the boolean CAS on an
 * already-completed record - and the claim then <em>cleared</em> the success verdict, erasing the one term
 * that would have refused it. The record was delivered twice and its offset committed twice; with assertions
 * enabled the second completion surfaces as an {@code AssertionError} from
 * {@link PartitionState#onSuccess(long)}.
 * <p>
 * {@link #aClaimDecidedBeforeAnotherPullerCompletedTheRecordIsRefused()} is that interleaving, played out by
 * hand with no threads at all. The concurrent reproduction of the same defect needed of the order of 14
 * million record completions to land four occurrences, so it is a soak and not a gate; this runs in
 * milliseconds and is exact. Full diagnosis, the reproduction numbers and the refuted predictions are in
 * {@code docs/inflight/bug-direct-pull-claim-is-check-then-act.md}.
 * <p>
 * THE FIX THIS PINS is {@link ExecutionState}: one atomic field carrying both the flight and the verdict, so
 * a claim is a single compare-and-set from a state that was <em>observed</em> to be claimable. The check IS
 * the act. A claim attempted against a record that has since succeeded finds {@link ExecutionState#SUCCEEDED}
 * rather than the {@code false} of a boolean that no longer answers the question that was asked.
 *
 * @author Antony Stubbs
 * @see WorkContainer#onQueueingForExecution()
 * @see ExecutionState
 */
@Slf4j
class WorkClaimStateMachineTest {

    static final String TOPIC = "claim-state-topic";
    static final TopicPartition TP = new TopicPartition(TOPIC, 0);

    PCModuleTestEnv module;
    WorkManager<String, String> wm;

    void setup(ParallelConsumerOptions.ProcessingOrder ordering) {
        module = new PCModuleTestEnv(ParallelConsumerOptions.<String, String>builder()
                .ordering(ordering)
                .build());
        wm = module.workManager();
        wm.onPartitionsAssigned(UniLists.of(TP));
    }

    void register(int fromOffset, int count) {
        List<ConsumerRecord<String, String>> recs = new ArrayList<>(count);
        for (int i = fromOffset; i < fromOffset + count; i++) {
            recs.add(new ConsumerRecord<>(TOPIC, 0, i, "the-key", "value-" + i));
        }
        Map<TopicPartition, List<ConsumerRecord<String, String>>> m = new HashMap<>();
        m.put(TP, recs);
        wm.registerWork(new EpochAndRecordsMap<>(new ConsumerRecords<>(m), wm.getPm()));
    }

    /**
     * The container the shard is holding at an offset, reached the way a concurrent scanner reaches it - through
     * the shard - so the test holds the same reference a losing puller would still be holding after the winner
     * completed the record and it left the shard.
     */
    WorkContainer<String, String> containerInShardAt(long offset) {
        var sm = wm.getSm();
        var shard = sm.getShard(sm.computeShardKey(new ConsumerRecord<>(TOPIC, 0, offset, "the-key", "v")));
        assertWithMessage("the shard for offset %s must exist for this test to prove anything", offset)
                .that(shard.isPresent()).isTrue();
        var wc = shard.get().getWorkContainerAt(offset);
        assertWithMessage("the shard must still hold offset %s", offset).that(wc).isNotNull();
        return wc;
    }

    // -----------------------------------------------------------------------------------------------------
    // The defect, deterministically
    // -----------------------------------------------------------------------------------------------------

    /**
     * THE INTERLEAVING, step by step, with no threads - each step is a plain method call in program order:
     * <ol>
     *   <li>Puller <b>A</b> scans the shard and evaluates availability for offset 0: true. A is descheduled
     *       here, between its check and its claim.</li>
     *   <li>Puller <b>B</b> evaluates the same guard, wins, and takes offset 0.</li>
     *   <li>The control thread runs B's verdict: success, then {@link WorkManager#handleFutureResult}. That
     *       ends the flight, removes the offset from the incomplete set and the container from the shard.</li>
     *   <li>A resumes and performs the second half of its guard - the claim.</li>
     * </ol>
     * Under the old boolean CAS step 4 returned {@code true}, because step 3 had reset the flag; the claim then
     * cleared the success verdict, and feeding the container back through {@code handleFutureResult} tripped
     * {@link PartitionState#onSuccess(long)}'s assert on an offset that was already gone. With one atomic state
     * the claim compares against {@link ExecutionState#SUCCEEDED} and is refused.
     * <p>
     * A holds the container reference from its own iteration, which is why moving the flight-end after the
     * shard removal does not close this - see the control arm recorded in the diagnosis note.
     */
    @Test
    void aClaimDecidedBeforeAnotherPullerCompletedTheRecordIsRefused() {
        setup(ParallelConsumerOptions.ProcessingOrder.UNORDERED);
        register(0, 1);

        // 1. puller A decides, and is descheduled holding the container
        var containerHeldByA = containerInShardAt(0L);
        boolean aSawItAvailable = containerHeldByA.isAvailableToTakeAsWork();
        assertWithMessage("A must have seen it available, or the interleaving under test never starts")
                .that(aSawItAvailable).isTrue();

        // 2. puller B takes it
        var takenByB = wm.getWorkIfAvailable(1);
        assertThat(takenByB).hasSize(1);
        assertThat(takenByB.get(0)).isSameInstanceAs(containerHeldByA);

        // 3. the control thread completes B's delivery
        takenByB.get(0).onUserFunctionSuccess();
        wm.handleFutureResult(takenByB.get(0));
        assertWithMessage("the record really did complete, so there is nothing left to deliver")
                .that(wm.getNumberOfIncompleteOffsets()).isEqualTo(0L);

        // 4. A resumes and attempts the claim it decided on in step 1
        boolean aWonTheClaim = containerHeldByA.onQueueingForExecution();

        assertWithMessage("P1 - a claim whose availability decision predates another puller's completion must "
                + "be REFUSED. Winning it here delivers an already-committed record a second time, and in "
                + "production - where assertions are off - the only surviving evidence is the duplicate.")
                .that(aWonTheClaim).isFalse();
        assertWithMessage("P1b - and the record must still say it succeeded. The old claim cleared the verdict, "
                + "erasing the very term that should have refused it.")
                .that(containerHeldByA.isUserFunctionSucceeded()).isTrue();
        assertWithMessage("the delivery count must not move for a claim that was refused")
                .that(containerHeldByA.getDeliveryCount()).isEqualTo(1L);
    }

    /**
     * P2 - the observable consequence of losing the test above, asserted separately so that a regression tells
     * you which half broke. A second delivery is returned as a second success, and
     * {@link PartitionState#onSuccess(long)} removes an offset that is already gone.
     * <p>
     * Written as "if the claim were ever won, this is what follows", so it exercises the return path rather than
     * merely restating the claim assertion.
     */
    @Test
    void asecondSuccessfulReturnForAnAlreadyCompletedRecordWouldTripTheIncompleteOffsetsAssert() {
        setup(ParallelConsumerOptions.ProcessingOrder.UNORDERED);
        register(0, 1);

        var wc = containerInShardAt(0L);
        var taken = wm.getWorkIfAvailable(1);
        assertThat(taken).hasSize(1);
        wc.onUserFunctionSuccess();
        wm.handleFutureResult(wc);

        // the record is now SUCCEEDED and out of the shard. Force the second return the lost claim would have
        // produced, without going through the claim - so this test still means something if the claim changes.
        wc.onUserFunctionSuccess();

        assertWithMessage("assertions must be enabled for this test to prove anything - run with -ea")
                .that(WorkClaimStateMachineTest.class.desiredAssertionStatus()).isTrue();
        assertThatThrownBy(() -> wm.handleFutureResult(wc))
                .as("a second success for an offset already removed from the incomplete set is the recorded "
                        + "sighting: PartitionState#onSuccess asserts the removal actually removed something")
                .isInstanceOf(AssertionError.class);
    }

    /**
     * The in-flight control from the same proof: a claim attempted while the record is still out at a worker is
     * refused too. Without this, a fix that simply refused every second claim would look correct.
     */
    @Test
    void aClaimOnARecordStillInFlightIsRefused() {
        setup(ParallelConsumerOptions.ProcessingOrder.UNORDERED);
        register(0, 1);

        var wc = containerInShardAt(0L);
        assertThat(wm.getWorkIfAvailable(1)).hasSize(1);

        assertWithMessage("P3 - a record already out at a worker may not be claimed again")
                .that(wc.onQueueingForExecution()).isFalse();
        assertThat(wc.getDeliveryCount()).isEqualTo(1L);
    }

    // -----------------------------------------------------------------------------------------------------
    // The transitions the state machine makes explicit
    // -----------------------------------------------------------------------------------------------------

    @Test
    void aFreshRecordIsAvailableAndAClaimTakesItInFlight() {
        setup(ParallelConsumerOptions.ProcessingOrder.UNORDERED);
        register(0, 1);

        var wc = containerInShardAt(0L);
        assertThat(wc.getExecutionState()).isEqualTo(ExecutionState.AVAILABLE);
        assertThat(wc.isAvailableToTakeAsWork()).isTrue();

        assertThat(wc.onQueueingForExecution()).isTrue();
        assertThat(wc.getExecutionState()).isEqualTo(ExecutionState.IN_FLIGHT);
        assertThat(wc.getDeliveryCount()).isEqualTo(1L);
        assertThat(wc.isAvailableToTakeAsWork()).isFalse();
    }

    /**
     * The verdict is recorded by the thread that ran the user function, which still holds the record - so a
     * verdict does not end the flight, and the container stays in flight until the controller returns it.
     * Getting that wrong would let the revocation sweep treat an outstanding record as if it were parked.
     */
    @Test
    void averdictIsRecordedWithoutEndingTheFlight() {
        setup(ParallelConsumerOptions.ProcessingOrder.UNORDERED);
        register(0, 2);

        var succeeding = containerInShardAt(0L);
        var failing = containerInShardAt(1L);
        assertThat(wm.getWorkIfAvailable(2)).hasSize(2);

        succeeding.onUserFunctionSuccess();
        assertThat(succeeding.getExecutionState()).isEqualTo(ExecutionState.IN_FLIGHT_SUCCEEDED);
        assertThat(succeeding.isInFlight()).isTrue();

        failing.onUserFunctionFailure(new FakeRuntimeException("deliberate"));
        assertThat(failing.getExecutionState()).isEqualTo(ExecutionState.IN_FLIGHT_FAILED);
        assertThat(failing.isInFlight()).isTrue();
    }

    /**
     * SUCCEEDED is terminal. This is the illegal transition that the whole state machine exists to make
     * assertable - and it is exactly the one the old boolean could not express, because "not in flight" and
     * "already succeeded" were two different fields.
     */
    @Test
    void aClaimFromSucceededIsRefusedForever() {
        setup(ParallelConsumerOptions.ProcessingOrder.UNORDERED);
        register(0, 1);

        var wc = containerInShardAt(0L);
        assertThat(wm.getWorkIfAvailable(1)).hasSize(1);
        wc.onUserFunctionSuccess();
        wm.handleFutureResult(wc);

        assertThat(wc.getExecutionState()).isEqualTo(ExecutionState.SUCCEEDED);
        assertThat(wc.isAvailableToTakeAsWork()).isFalse();
        assertWithMessage("a succeeded record can never be claimed again, however often it is asked")
                .that(wc.onQueueingForExecution()).isFalse();
        assertThat(wc.onQueueingForExecution()).isFalse();
        assertThat(wc.getExecutionState()).isEqualTo(ExecutionState.SUCCEEDED);
        assertThat(wc.getDeliveryCount()).isEqualTo(1L);
    }

    /**
     * FAILED is not terminal, but it is not immediately claimable either: the retry delay is time, and it lives
     * outside the state because no transition can be scheduled to fire when a clock passes a point.
     */
    @Test
    void aFailedRecordIsRefusedUntilItsRetryDelayPassesAndThenClaimable() {
        setup(ParallelConsumerOptions.ProcessingOrder.UNORDERED);
        register(0, 1);

        var wc = containerInShardAt(0L);
        assertThat(wm.getWorkIfAvailable(1)).hasSize(1);
        wc.onUserFunctionFailure(new FakeRuntimeException("deliberate"));
        wm.handleFutureResult(wc);

        assertThat(wc.getExecutionState()).isEqualTo(ExecutionState.FAILED);
        assertWithMessage("still inside its retry delay, so not claimable - the state alone does not decide")
                .that(wc.isAvailableToTakeAsWork()).isFalse();
        assertThat(wc.onQueueingForExecution()).isFalse();
        assertThat(wc.getDeliveryCount()).isEqualTo(1L);

        module.getMutableClock().add(module.options().getDefaultMessageRetryDelay().plus(Duration.ofSeconds(1)));

        assertThat(wc.isAvailableToTakeAsWork()).isTrue();
        assertThat(wc.onQueueingForExecution()).isTrue();
        assertThat(wc.getExecutionState())
                .isEqualTo(ExecutionState.IN_FLIGHT);
        assertWithMessage("the retry is a second delivery, and it carries no verdict from the first")
                .that(wc.getDeliveryCount()).isEqualTo(2L);
        assertThat(wc.getMaybeUserFunctionSucceeded()).isEmpty();
        assertWithMessage("the failure history survives the redelivery - the attempt happened")
                .that(wc.getNumberOfFailedAttempts()).isEqualTo(1);
    }

    /**
     * Fail, retry, then be abandoned mid-flight: the one path that crosses every transition the state machine
     * has. {@code docs/inflight/next-open-items-from-the-perf-session.md} records retry and abandonment as
     * covered by nothing at all, which is why this is written as one walk rather than three fragments.
     * <p>
     * Abandonment returns the record to {@link ExecutionState#AVAILABLE} rather than to
     * {@link ExecutionState#FAILED}: a record whose holder went away was never attempted to a conclusion, so it
     * has earned no retry delay - and, critically, it must not still be carrying the FAILED verdict of its
     * <em>previous</em> delivery, which would route it down the failure path a second time.
     */
    @Test
    void aRecordThatFailsThenRetriesThenIsAbandonedCrossesEveryState() {
        setup(ParallelConsumerOptions.ProcessingOrder.UNORDERED);
        register(0, 1);

        var wc = containerInShardAt(0L);

        // AVAILABLE -> IN_FLIGHT
        assertThat(wm.getWorkIfAvailable(1)).hasSize(1);
        assertThat(wc.getExecutionState()).isEqualTo(ExecutionState.IN_FLIGHT);

        // IN_FLIGHT -> IN_FLIGHT_FAILED -> FAILED
        wc.onUserFunctionFailure(new FakeRuntimeException("deliberate"));
        assertThat(wc.getExecutionState()).isEqualTo(ExecutionState.IN_FLIGHT_FAILED);
        wm.handleFutureResult(wc);
        assertThat(wc.getExecutionState()).isEqualTo(ExecutionState.FAILED);

        // FAILED -> IN_FLIGHT, once the delay has passed
        module.getMutableClock().add(module.options().getDefaultMessageRetryDelay().plus(Duration.ofSeconds(1)));
        var retried = wm.getWorkIfAvailable(1);
        assertThat(retried).hasSize(1);
        assertThat(retried.get(0)).isSameInstanceAs(wc);
        assertThat(wc.getExecutionState()).isEqualTo(ExecutionState.IN_FLIGHT);
        long secondDelivery = wc.getDeliveryCount();
        assertThat(secondDelivery).isEqualTo(2L);

        // IN_FLIGHT -> AVAILABLE, by abandonment
        wc.markAbandoned(secondDelivery);
        wm.handleFutureResult(wc);
        assertThat(wc.getExecutionState()).isEqualTo(ExecutionState.AVAILABLE);
        assertWithMessage("an abandoned record earns no retry delay, even after a prior failure, so it is "
                + "selectable at once")
                .that(wc.isAvailableToTakeAsWork()).isTrue();
        assertWithMessage("and it carries no verdict - the FAILED verdict of the first delivery must not "
                + "survive into the third")
                .that(wc.getMaybeUserFunctionSucceeded()).isEmpty();
        assertThat(wc.getNumberOfFailedAttempts()).isEqualTo(1);
        assertThat(wm.getNumberRecordsOutForProcessing()).isEqualTo(0);

        // AVAILABLE -> IN_FLIGHT -> IN_FLIGHT_SUCCEEDED -> SUCCEEDED
        assertThat(wm.getWorkIfAvailable(1)).hasSize(1);
        assertThat(wc.getDeliveryCount()).isEqualTo(3L);
        wc.onUserFunctionSuccess();
        wm.handleFutureResult(wc);
        assertThat(wc.getExecutionState()).isEqualTo(ExecutionState.SUCCEEDED);
        assertThat(wm.getNumberOfIncompleteOffsets()).isEqualTo(0L);
    }
}
