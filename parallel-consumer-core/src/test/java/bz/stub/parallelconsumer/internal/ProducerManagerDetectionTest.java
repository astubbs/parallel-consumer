package bz.stub.parallelconsumer.internal;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import pl.tlinkowski.unij.api.UniMaps;

import java.time.Duration;
import java.util.Optional;

import static bz.stub.parallelconsumer.ParallelConsumerOptions.CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER;
import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;

/**
 * The commit path as a detection site (R8, R11, R19, KTD3): where PC can recover, the condition is recorded and the
 * commit unwinds through the lock release; where it cannot, the outcome is what it was before recovery existed.
 */
@Timeout(30)
class ProducerManagerDetectionTest {

    private PCModuleTestEnv module;
    private ProducerWrapper<String, String> wrapper;

    @BeforeEach
    void setUp() {
        module = new PCModuleTestEnv(ParallelConsumerOptions.<String, String>builder()
                .commitMode(PERIODIC_TRANSACTIONAL_PRODUCER)
                .commitLockAcquisitionTimeout(Duration.ofSeconds(5))
                .build());
        wrapper = module.producerWrap();
    }

    private ProducerManager<String, String> managerThatCanRecover() {
        var replacement = new ReplacementProducerSource<String, String>(() -> module.producerWrap(), "pc-4-test-id");
        return new ProducerManager<>(wrapper, module.consumerManager(), module.workManager(), module.options(), Optional.of(replacement));
    }

    private ProducerManager<String, String> managerOnTheInstancePath() {
        return new ProducerManager<>(wrapper, module.consumerManager(), module.workManager(), module.options(), Optional.empty());
    }

    @Test
    void onThePcBuiltPathAFencedSendOffsetsIsRecordedAndUnwindsThroughTheLockRelease() throws Exception {
        var fenced = new ProducerFencedException("fenced by another producer with the same transactionalId");
        doThrow(fenced).when(wrapper).sendOffsetsToTransaction(anyMap(), any(ConsumerGroupMetadata.class));
        var manager = managerThatCanRecover();
        assertThat(manager.canRecover()).isTrue();

        manager.preAcquireOffsetsToCommit();
        var thrown = assertThrows(ProducerInvalidatedException.class,
                () -> manager.commitOffsets(UniMaps.of(), new ConsumerGroupMetadata("group")));
        manager.postCommit(); // what AbstractOffsetCommitter's finally does

        assertThat(thrown).hasCauseThat().isSameInstanceAs(fenced);
        assertThat(manager.pendingInvalidation()).hasValue(fenced);
        assertWithMessage("the write lock is released on the way out, as for any other commit failure")
                .that(manager.isTransactionCommittingInProgress()).isFalse();
    }

    @Test
    void onThePcBuiltPathAnErrorStateWrapperFromCommitIsUnwrappedAndRecorded() throws Exception {
        var fenced = new ProducerFencedException("fenced");
        var errorState = new KafkaException("Cannot execute transactional method because we are in an error state", fenced);
        doThrow(errorState).when(wrapper).commitTransaction();
        var manager = managerThatCanRecover();

        manager.preAcquireOffsetsToCommit();
        var thrown = assertThrows(ProducerInvalidatedException.class,
                () -> manager.commitOffsets(UniMaps.of(), new ConsumerGroupMetadata("group")));
        manager.postCommit();

        assertThat(thrown).hasCauseThat().isSameInstanceAs(fenced);
        assertThat(manager.pendingInvalidation()).hasValue(fenced);
    }

    @Test
    void onlyTheFirstConditionIsRecordedUntilRecoveryClearsIt() throws Exception {
        var first = new ProducerFencedException("first");
        var second = new ProducerFencedException("second");
        var manager = managerThatCanRecover();

        manager.recordInvalidation(first);
        manager.recordInvalidation(second);

        assertThat(manager.pendingInvalidation()).hasValue(first);
    }

    /**
     * Covers AE5, the condition half: the instance path behaves as it did before recovery existed - a fenced
     * send-offsets is wrapped as an internal error that kills the control thread - and records nothing.
     */
    @Test
    void onTheInstancePathAFencedSendOffsetsStaysTheInternalErrorItWas() throws Exception {
        var fenced = new ProducerFencedException("fenced");
        doThrow(fenced).when(wrapper).sendOffsetsToTransaction(anyMap(), any(ConsumerGroupMetadata.class));
        var manager = managerOnTheInstancePath();
        assertThat(manager.canRecover()).isFalse();

        manager.preAcquireOffsetsToCommit();
        var thrown = assertThrows(PCInternalRuntimeException.class,
                () -> manager.commitOffsets(UniMaps.of(), new ConsumerGroupMetadata("group")));
        manager.postCommit();

        assertThat(thrown).isNotInstanceOf(ProducerInvalidatedException.class);
        assertThat(thrown).hasCauseThat().isSameInstanceAs(fenced);
        assertThat(manager.pendingInvalidation()).isEmpty();
    }

    @Test
    void onTheInstancePathAFencedCommitPropagatesRawAsBefore() throws Exception {
        var fenced = new ProducerFencedException("fenced");
        doThrow(fenced).when(wrapper).commitTransaction();
        var manager = managerOnTheInstancePath();

        manager.preAcquireOffsetsToCommit();
        var thrown = assertThrows(ProducerFencedException.class,
                () -> manager.commitOffsets(UniMaps.of(), new ConsumerGroupMetadata("group")));
        manager.postCommit();

        assertThat(thrown).isSameInstanceAs(fenced);
        assertThat(manager.pendingInvalidation()).isEmpty();
    }

    /**
     * Inherited from astubbs#262: a throwing abort used to skip closing the producer, leaking one per fenced
     * shutdown. Abort is swallowed and the close happens regardless.
     */
    @Test
    void closeStillClosesTheProducerWhenAbortThrows() {
        doReturn(false).when(wrapper).isTransactionReady();
        doThrow(new ProducerFencedException("fenced")).when(wrapper).abortTransaction();
        var manager = managerOnTheInstancePath();

        manager.close(Duration.ofSeconds(1));

        verify(wrapper).abortTransaction();
        verify(wrapper).close(any(Duration.class));
        assertWithMessage("the commit lock taken for the abort is released")
                .that(manager.isTransactionCommittingInProgress()).isFalse();
    }
}
