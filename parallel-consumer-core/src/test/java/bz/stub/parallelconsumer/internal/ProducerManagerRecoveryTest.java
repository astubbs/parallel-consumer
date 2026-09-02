package bz.stub.parallelconsumer.internal;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.PollContextInternal;
import bz.stub.parallelconsumer.internal.utils.BlockedThreadAsserter;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Meter;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TransactionalIdAuthorizationException;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static bz.stub.parallelconsumer.ParallelConsumerOptions.CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER;
import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

/**
 * The manager's side of recovery in isolation (KTD4, KTD7, R15): what the produce lock does while no producer
 * exists, how the outage ends, and what the replacement sequence tolerates.
 */
@Timeout(60)
class ProducerManagerRecoveryTest {

    private PCModuleTestEnv module;
    private ProducerWrapper<String, String> initial;
    private ProducerManager<String, String> manager;
    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    /** Every replacement the source built, in order, so a test can ask what became of a rejected one. */
    private final List<ProducerWrapper<String, String>> built = new ArrayList<>();
    /** Applied to each replacement as it is built, before the manager sees it; keyed by build index. */
    private final Map<Integer, Consumer<ProducerWrapper<String, String>>> onBuild = new HashMap<>();
    /** When set, the source throws this instead of building - a factory that fails outright. */
    private volatile RuntimeException sourceFailure;

    @BeforeEach
    void setUp() {
        manager = managerOn(registry);
    }

    private ProducerManager<String, String> managerOn(SimpleMeterRegistry meterRegistry) {
        module = new PCModuleTestEnv(ParallelConsumerOptions.<String, String>builder()
                .meterRegistry(meterRegistry)
                .commitMode(PERIODIC_TRANSACTIONAL_PRODUCER)
                .commitLockAcquisitionTimeout(Duration.ofMillis(500))
                .produceLockAcquisitionTimeout(Duration.ofMillis(200))
                .build());
        initial = module.producerWrap();
        // a fresh wrapper per build, as the module's configuration path gives; the test env memoises its own spy
        var source = new ReplacementProducerSource<String, String>(this::buildReplacement, "pc-4-test-id");
        var pm = new ProducerManager<>(initial, module.consumerManager(), module.workManager(), module.options(), Optional.of(source));
        pm.recoveryBackoffInitial = Duration.ofMillis(100);
        return pm;
    }

    private ProducerWrapper<String, String> buildReplacement() {
        if (sourceFailure != null) {
            throw sourceFailure;
        }
        ProducerWrapper<String, String> replacement = spy(new ProducerWrapper<>(module.options(), true, mock(org.apache.kafka.clients.producer.Producer.class)));
        Consumer<ProducerWrapper<String, String>> hook = onBuild.get(built.size());
        built.add(replacement);
        if (hook != null) {
            hook.accept(replacement);
        }
        return replacement;
    }

    /** The write-locked half as the control thread runs it: enter, (drain and) replay, release. */
    private void recoverPhaseA() throws InterruptedException {
        recoverPhaseA(0);
    }

    private void recoverPhaseA(int restoredByTheReplay) throws InterruptedException {
        manager.recordInvalidation(new ProducerFencedException("fenced"));
        assertThat(manager.beginReplacement()).isTrue();
        manager.replayCompleted(restoredByTheReplay);
        manager.releaseCommitLockAfterReplacement();
    }

    /**
     * Between entering the lock and the replay returning, the aborted transaction's work is in the ledger and
     * nowhere else. A pass that entered and threw before the replay must not build on its next pass - the commit
     * after that would trim the ledger for output the broker never saw - so the replay stays owed and the build is
     * deferred until a pass has completed it.
     */
    @Test
    void noReplacementIsBuiltWhileTheReplayIsStillOwed() throws Exception {
        manager.recordInvalidation(new ProducerFencedException("fenced"));
        assertThat(manager.beginReplacement()).isTrue();
        manager.releaseCommitLockAfterReplacement(); // the drain threw: replayCompleted was never reached
        assertThat(manager.isReplayOwed()).isTrue();

        var deferred = manager.completeReplacement();

        assertThat(deferred.getKind()).isEqualTo(ProducerManager.ReplacementOutcome.Kind.DEFERRED);
        assertWithMessage("nothing was built").that(built).isEmpty();
        assertThat(manager.isReplacing()).isTrue();
        assertWithMessage("paced like a failed build, not spun").that(manager.isRecoveryAttemptDue(Instant.now())).isFalse();

        // the next pass re-enters the lock and replays; only then may it build
        assertThat(manager.beginReplacement()).isTrue();
        manager.replayCompleted(0);
        manager.releaseCommitLockAfterReplacement();
        assertThat(manager.isReplayOwed()).isFalse();
        assertThat(manager.completeReplacement().getKind()).isEqualTo(ProducerManager.ReplacementOutcome.Kind.REPLACED);
    }

    /**
     * A worker dispatched before the replay put lower offsets back into its shard, parked through the outage,
     * must not produce ahead of them into the replacement's transaction: it is refused the lock and its batch
     * re-queues behind the restored work. Compared against the generation the control thread stamped at dispatch.
     */
    @Test
    void aWorkerDispatchedBeforeAReplayIsRefusedTheProduceLockAfterIt() throws Exception {
        var context = mock(PollContextInternal.class);
        doReturn(java.util.OptionalLong.of(manager.replayGeneration())).when(context).replayGenerationAtDispatch();
        manager.recordInvalidation(new ProducerFencedException("fenced"));
        assertThat(manager.beginReplacement()).isTrue();
        var outcome = new AtomicReference<Throwable>();

        var blocked = new BlockedThreadAsserter();
        blocked.assertUnblocksAfter(
                () -> {
                    try {
                        manager.beginProducing(context);
                    } catch (Throwable t) {
                        outcome.set(t);
                    }
                },
                () -> {
                    manager.replayCompleted(3); // the replay put three records back
                    manager.releaseCommitLockAfterReplacement();
                    assertThat(manager.completeReplacement().isTerminal()).isFalse();
                });

        assertThat(outcome.get()).isInstanceOf(ProducerInvalidatedException.class);
        assertThat(outcome.get()).hasMessageThat().contains("re-queues behind");
        assertWithMessage("the refused hold was released").that(manager.getProducerTransactionLock().getReadLockCount()).isEqualTo(0);
        assertWithMessage("a worker dispatched after the replay is not refused")
                .that(manager.replayGeneration()).isNotEqualTo(0L);
    }

    /**
     * The control arm of the check above: a replay that put nothing back moved no offset below any record in
     * flight, so a parked worker proceeds as before.
     */
    @Test
    void aParkedWorkerProceedsWhenTheReplayPutNothingBack() throws Exception {
        var context = mock(PollContextInternal.class);
        doReturn(java.util.OptionalLong.of(manager.replayGeneration())).when(context).replayGenerationAtDispatch();
        manager.recordInvalidation(new ProducerFencedException("fenced"));
        assertThat(manager.beginReplacement()).isTrue();
        var outcome = new AtomicReference<Throwable>();

        var blocked = new BlockedThreadAsserter();
        blocked.assertUnblocksAfter(
                () -> {
                    try {
                        manager.finishProducing(manager.beginProducing(context));
                    } catch (Throwable t) {
                        outcome.set(t);
                    }
                },
                () -> {
                    manager.replayCompleted(0);
                    manager.releaseCommitLockAfterReplacement();
                    assertThat(manager.completeReplacement().isTerminal()).isFalse();
                });

        assertThat(outcome.get()).isNull();
    }

    @Test
    void recordingAConditionSuspendsAvailabilityAtOnceOnTheDetectingThread() {
        assertThat(manager.isProducerAvailable()).isTrue();

        manager.recordInvalidation(new ProducerFencedException("fenced"));

        assertThat(manager.isProducerAvailable()).isFalse();
        assertThat(manager.isReplacing()).isTrue();
        assertThat(manager.isRecoveryAttemptDue(Instant.now())).isTrue();
    }

    /**
     * R15: the produce lock's bounded wait does not surface while the producer is being replaced - the worker parks
     * and proceeds once the replacement is published.
     */
    @Test
    void aWorkerReachingTheProduceLockDuringAnOutageWaitsForTheReplacementRatherThanTimingOut() throws Exception {
        recoverPhaseA();
        assertThat(manager.isReplacing()).isTrue();
        var produced = new AtomicBoolean(false);

        var blocked = new BlockedThreadAsserter();
        blocked.assertUnblocksAfter(
                () -> {
                    try {
                        // acquire and release on the same thread, as a worker does: the read lock is per-thread
                        var lock = manager.beginProducing(mock(PollContextInternal.class));
                        produced.set(manager.isProducerAvailable());
                        manager.finishProducing(lock);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                },
                () -> assertThat(manager.completeReplacement().isTerminal()).isFalse());

        assertWithMessage("the worker held a produce lock with the replacement published").that(produced.get()).isTrue();
        assertThat(manager.isProducerAvailable()).isTrue();
    }

    /**
     * R15: a shutdown during the outage releases the parked worker with the exception that fails its record.
     */
    @Test
    void aParkedWorkerIsReleasedWhenTheProcessorStopsRunning() throws Exception {
        var shuttingDown = new AtomicBoolean(false);
        manager.setSuspensionEndsWhen(shuttingDown::get);
        recoverPhaseA();
        var outcome = new AtomicReference<Throwable>();

        var blocked = new BlockedThreadAsserter();
        blocked.assertUnblocksAfter(
                () -> {
                    try {
                        manager.beginProducing(mock(PollContextInternal.class));
                    } catch (Throwable t) {
                        outcome.set(t);
                    }
                },
                () -> shuttingDown.set(true));

        assertThat(outcome.get()).isInstanceOf(ProducerInvalidatedException.class);
    }

    @Test
    void closingTheManagerDuringAnOutageReleasesTheParkedWorker() throws Exception {
        recoverPhaseA();
        var outcome = new AtomicReference<Throwable>();

        var blocked = new BlockedThreadAsserter();
        blocked.assertUnblocksAfter(
                () -> {
                    try {
                        manager.beginProducing(mock(PollContextInternal.class));
                    } catch (Throwable t) {
                        outcome.set(t);
                    }
                },
                () -> manager.close(Duration.ofSeconds(1)));

        assertThat(outcome.get()).isInstanceOf(ProducerInvalidatedException.class);
    }

    /**
     * KTD4: the write lock held by another thread across the whole wait defers the attempt instead of throwing.
     */
    @Test
    void aWriteLockHeldElsewhereForTheWholeWaitDefersRecoveryWithBackoff() throws Exception {
        manager.recordInvalidation(new ProducerFencedException("fenced"));
        var writer = new Thread(() -> {
            manager.getProducerTransactionLock().writeLock().lock();
            try {
                Thread.sleep(1500);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            } finally {
                manager.getProducerTransactionLock().writeLock().unlock();
            }
        }, "other-holder");
        writer.start();
        Thread.sleep(50);

        boolean entered = manager.beginReplacement();

        assertThat(entered).isFalse();
        assertThat(manager.isReplacing()).isTrue();
        assertWithMessage("the condition stays recorded for the next pass").that(manager.pendingInvalidation()).isPresent();
        assertWithMessage("not due again until the backoff has elapsed").that(manager.isRecoveryAttemptDue(Instant.now())).isFalse();
        assertThat(manager.timeUntilNextRecoveryAttempt(Instant.now())).isPresent();
        writer.join();
        assertThat(manager.beginReplacement()).isTrue();
        manager.releaseCommitLockAfterReplacement();
    }

    @Test
    void abortAndCloseOfTheInvalidatedProducerMayBothThrowAndRecoveryContinues() throws Exception {
        doReturn(true).when(initial).isTransactionOpen();
        doThrow(new ProducerFencedException("fenced")).when(initial).abortTransaction();
        doThrow(new IllegalStateException("close blew up")).when(initial).close(any(Duration.class));

        recoverPhaseA();
        var outcome = manager.completeReplacement();

        verify(initial).abortTransaction();
        verify(initial).close(any(Duration.class));
        assertThat(outcome.getKind()).isEqualTo(ProducerManager.ReplacementOutcome.Kind.REPLACED);
        assertThat(manager.isProducerAvailable()).isTrue();
        assertThat(manager.getProducerWrapper()).isNotSameInstanceAs(initial);
    }

    @Test
    void aSuccessfulCommitResetsTheConsecutiveRecoveryCountAndTheNextRecoveryIsNotPaced() throws Exception {
        recoverPhaseA();
        assertThat(manager.completeReplacement().getKind()).isEqualTo(ProducerManager.ReplacementOutcome.Kind.REPLACED);
        assertThat(manager.getConsecutiveRecoveriesWithoutCommit()).isEqualTo(1);

        // a second condition with no commit between is paced by the backoff
        manager.recordInvalidation(new ProducerFencedException("fenced again"));
        assertThat(manager.isRecoveryAttemptDue(Instant.now())).isFalse();
        assertThat(manager.beginReplacement()).isTrue(); // the pacing gates the control loop, not the lock
        manager.replayCompleted(0);
        manager.releaseCommitLockAfterReplacement();
        assertThat(manager.completeReplacement().getKind()).isEqualTo(ProducerManager.ReplacementOutcome.Kind.REPLACED);
        assertThat(manager.getConsecutiveRecoveriesWithoutCommit()).isEqualTo(2);

        // R23 / R24 at the meter: two recoveries so far, both fenced, none committed since
        assertThat(registry.get("pc.producer.consecutive.recoveries").gauge().value()).isEqualTo(2.0);
        assertThat(registry.get("pc.producer.recoveries").tag("condition", "ProducerFencedException").counter().count()).isEqualTo(2.0);

        manager.preAcquireOffsetsToCommit();
        manager.commitOffsets(pl.tlinkowski.unij.api.UniMaps.of(), new org.apache.kafka.clients.consumer.ConsumerGroupMetadata("group"));
        manager.postCommit();

        assertThat(manager.getConsecutiveRecoveriesWithoutCommit()).isEqualTo(0);
        assertThat(registry.get("pc.producer.consecutive.recoveries").gauge().value()).isEqualTo(0.0);
        assertWithMessage("the counter is cumulative; only the gauge resets")
                .that(registry.get("pc.producer.recoveries").tag("condition", "ProducerFencedException").counter().count()).isEqualTo(2.0);
        manager.recordInvalidation(new ProducerFencedException("fenced a third time"));
        assertWithMessage("the first recovery of a new run is not paced").that(manager.isRecoveryAttemptDue(Instant.now())).isTrue();
    }

    /**
     * A replacement that was built but could not initialise is nobody's: the manager never published it, the
     * source has forgotten it, and each one leaked keeps a {@code KafkaProducer}'s network thread - one per attempt,
     * for as long as the coordinator stays unreachable.
     */
    @Test
    void aReplacementThatFailsToInitialiseIsClosedBeforeTheRetryIsScheduled() throws Exception {
        onBuild.put(0, replacement -> doThrow(new TimeoutException("coordinator unreachable")).when(replacement).initTransactions());
        recoverPhaseA();

        var outcome = manager.completeReplacement();

        assertThat(outcome.getKind()).isEqualTo(ProducerManager.ReplacementOutcome.Kind.DEFERRED);
        verify(built.get(0)).close(any(Duration.class));
        assertWithMessage("the rejected replacement was never published").that(manager.getProducerWrapper()).isNull();
        assertThat(manager.isReplacing()).isTrue();
    }

    @Test
    void aReplacementThatFailsTerminallyIsClosedToo() throws Exception {
        onBuild.put(0, replacement -> doThrow(new TransactionalIdAuthorizationException("denied")).when(replacement).initTransactions());
        recoverPhaseA();

        var outcome = manager.completeReplacement();

        assertThat(outcome.getKind()).isEqualTo(ProducerManager.ReplacementOutcome.Kind.TERMINAL);
        verify(built.get(0)).close(any(Duration.class));
        assertThat(manager.isProducerAvailable()).isFalse();
    }

    /**
     * A factory that breaks its contract does so on every rebuild - a caching factory caches every time - so
     * deferring is a retry loop for the life of the instance, each attempt logged as if the coordinator were merely
     * slow. The failure names the violation, not the wrapper it arrived in.
     */
    @Test
    void aFactoryContractViolationIsTerminalRatherThanRetriedForever() throws Exception {
        sourceFailure = new ProducerFactoryContractException("The ProducerFactory returned the producer it had already returned");
        recoverPhaseA();

        var outcome = manager.completeReplacement();

        assertThat(outcome.getKind()).isEqualTo(ProducerManager.ReplacementOutcome.Kind.TERMINAL);
        assertThat(outcome.getFailure()).hasMessageThat().contains(ProducerFactoryContractException.class.getName());
        assertWithMessage("the transactional id is named, for the operator").that(outcome.getFailure()).hasMessageThat().contains("pc-4-test-id");
        assertThat(manager.isProducerAvailable()).isFalse();
        assertThat(manager.isReplacing()).isFalse();
    }

    /**
     * The recovery counter is the user's MeterRegistry. Once the replacement is published it is in use, so a
     * registry that throws must not turn the outcome into a deferral that schedules a second rebuild against it.
     */
    @Test
    void aThrowingMeterRegistryDoesNotTurnACompletedReplacementIntoADeferredOne() throws Exception {
        var registryThatRejectsTheRecoveryCounter = new SimpleMeterRegistry() {
            @Override
            protected Counter newCounter(Meter.Id id) {
                if (id.getName().equals("pc.producer.recoveries")) {
                    throw new IllegalStateException("registry down");
                }
                return super.newCounter(id);
            }
        };
        manager = managerOn(registryThatRejectsTheRecoveryCounter);
        recoverPhaseA();

        var outcome = manager.completeReplacement();

        assertThat(outcome.getKind()).isEqualTo(ProducerManager.ReplacementOutcome.Kind.REPLACED);
        assertThat(manager.isProducerAvailable()).isTrue();
        assertThat(manager.getProducerWrapper()).isSameInstanceAs(built.get(0));
        assertWithMessage("the recovery counted, whatever the registry did").that(manager.getConsecutiveRecoveriesWithoutCommit()).isEqualTo(1);
    }

    @Test
    void aCommitDuringTheOutageUnwindsAsInvalidatedRatherThanTouchingAnAbsentProducer() throws Exception {
        recoverPhaseA();

        manager.preAcquireOffsetsToCommit();
        var thrown = assertThrows(ProducerInvalidatedException.class,
                () -> manager.commitOffsets(pl.tlinkowski.unij.api.UniMaps.of(), new org.apache.kafka.clients.consumer.ConsumerGroupMetadata("group")));
        manager.postCommit();

        assertThat(thrown).hasMessageThat().contains("no usable producer");
        assertThat(manager.isTransactionCommittingInProgress()).isFalse();
    }
}
