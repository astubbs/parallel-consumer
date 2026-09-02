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

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

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

    @BeforeEach
    void setUp() {
        module = new PCModuleTestEnv(ParallelConsumerOptions.<String, String>builder()
                .meterRegistry(registry)
                .commitMode(PERIODIC_TRANSACTIONAL_PRODUCER)
                .commitLockAcquisitionTimeout(Duration.ofMillis(500))
                .produceLockAcquisitionTimeout(Duration.ofMillis(200))
                .build());
        initial = module.producerWrap();
        // a fresh wrapper per build, as the module's configuration path gives; the test env memoises its own spy
        var source = new ReplacementProducerSource<String, String>(
                () -> spy(new ProducerWrapper<>(module.options(), true, mock(org.apache.kafka.clients.producer.Producer.class))),
                "pc-4-test-id");
        manager = new ProducerManager<>(initial, module.consumerManager(), module.workManager(), module.options(), Optional.of(source));
        manager.recoveryBackoffInitial = Duration.ofMillis(100);
    }

    private void recoverPhaseA() throws InterruptedException {
        manager.recordInvalidation(new ProducerFencedException("fenced"));
        assertThat(manager.beginReplacement()).isTrue();
        manager.releaseCommitLockAfterReplacement();
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
