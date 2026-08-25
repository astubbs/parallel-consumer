package bz.stub.parallelconsumer.integrationTests;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.CommitFailureContext;
import bz.stub.parallelconsumer.CommitFailureHandler;
import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.ParallelConsumerOptions.CommitMode;
import bz.stub.parallelconsumer.ParallelEoSStreamProcessor;
import bz.stub.parallelconsumer.internal.PCModule;
import bz.stub.parallelconsumer.internal.ProducerManager;
import bz.stub.parallelconsumer.internal.ProducerWrapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.TimeoutException;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicBoolean;

import static bz.stub.parallelconsumer.integrationTests.utils.KafkaClientUtils.GroupOption.NEW_GROUP;
import static bz.stub.parallelconsumer.internal.utils.ThreadUtils.sleepOrFail;
import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static pl.tlinkowski.unij.api.UniSets.of;

/**
 * AE3 against a real broker (astubbs#317): in transactional (EOS) commit mode with a CONTINUE handler, an
 * extended commit-phase outage keeps PC alive across repeated exhausted budgets, forces the intake pause
 * (the {@code PAUSE_INTAKE} coercion - EOS CONTINUE always pauses), and once the outage heals the pending
 * transaction is recovered (complete-else-abort, KTD8) so a fresh transaction commits everything - no record
 * skipped, no terminal "previous fatal or abortable error" ever reached.
 * <p>
 * The outage is injected in a {@link ProducerWrapper} subclass (the injection seam
 * {@link TransactionTimeoutsTest} also uses): {@code commitTransaction} fails with the retriable
 * {@link TimeoutException} while the outage flag is up, and {@code isTransactionCompleting} reports
 * {@code true} for as long as the outage lasts - which is what a genuinely timed-out
 * {@code KafkaProducer#commitTransaction} looks like (its {@code TransactionManager} stays in the committing
 * state), and is load-bearing for fidelity: reporting {@code false} would instead trigger the legacy retry
 * branch that assumes the transaction completed between interrupt and retry. Meanwhile everything else -
 * polling, group membership, {@code beginTransaction}/{@code sendOffsetsToTransaction}, the heal-time abort
 * and the healed commit - runs against the real broker.
 * <p>
 * Deliberately consume-only ({@code poll}, not {@code pollAndProduce}): output records produced by work that
 * COMPLETED into a transaction that recovery later aborts are not re-produced - completed work has no replay
 * machinery, its source offsets stay dirty and commit with the next transaction while the aborted outputs
 * are gone for {@code read_committed} consumers. Recovery prefers completion precisely to keep that window
 * small, but a produce-flow version of this test cannot honestly assert "nothing lost" until that gap is
 * closed - it is reported as the abort lane's residual risk. Offsets-only transactions lose nothing on
 * abort, so this test's at-least-once assertions are real.
 *
 * @author Antony Stubbs
 * @see ProducerManager
 * @see CommitFailureHandler
 * @see CommitOutageKeepProcessingBoundedIT the consumer-sync-mode sibling (KTD6)
 */
@Slf4j
@Tag("transactions")
@Timeout(300)
class TransactionalCommitOutageContinueIT extends BrokerIntegrationTest<String, String> {

    private static final int PHASE_SIZE = 20;

    /** Pacing inside each failing {@code commitTransaction}, so a budget makes a handful of attempts, not thousands. */
    private static final Duration FAILING_COMMIT_PACING = Duration.ofMillis(100);

    private ParallelEoSStreamProcessor<String, String> pc;

    @AfterEach
    void closePc() {
        if (pc != null && !pc.isClosedOrFailed()) {
            pc.closeDontDrainFirst();
        }
    }

    @Test
    void transactionalContinueSurvivesCommitOutagePausesIntakeAndRecoversOnHeal() throws Exception {
        var commitOutage = new AtomicBoolean(false);
        var exhaustions = new ConcurrentLinkedQueue<CommitFailureContext>();
        var processedOffsets = new ConcurrentSkipListSet<Long>();

        setupTopic(getClass().getSimpleName());

        Consumer<String, String> consumer = getKcu().createNewConsumer(NEW_GROUP);
        String groupId = consumer.groupMetadata().groupId();

        CommitFailureHandler continueHandler = context -> {
            log.info("Commit budget exhausted (consecutive: {}) - continuing",
                    context.getConsecutiveExhaustedBudgets());
            exhaustions.add(context);
            return CommitFailureHandler.CommitFailureDecision.CONTINUE;
        };

        var options = ParallelConsumerOptions.<String, String>builder()
                .consumer(consumer)
                .producer(getKcu().createNewProducer(CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER))
                .commitMode(CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER)
                .commitInterval(Duration.ofMillis(500))
                .offsetCommitTimeout(Duration.ofSeconds(1)) // small budget: exhaustions come quickly
                .commitFailureHandler(continueHandler)
                // commitFailureContinueMode is left at its default: under transactional commit mode validate()
                // coerces it to PAUSE_INTAKE - the coercion is part of what this test covers
                .build();

        var module = new PCModule<>(options) {
            private ProducerWrapper<String, String> wrapper;

            /** Cached, as {@link PCModule#producerWrap()} caches - see {@link TransactionTimeoutsTest}'s note. */
            @Override
            protected ProducerWrapper<String, String> producerWrap() {
                if (wrapper == null) {
                    wrapper = new CommitOutageProducerWrapper(options(), commitOutage);
                }
                return wrapper;
            }
        };

        pc = new ParallelEoSStreamProcessor<>(module.options(), module);
        pc.subscribe(of(getTopic()));
        pc.poll(recordContexts -> recordContexts.forEach(recordContext ->
                processedOffsets.add(recordContext.offset())));

        // phase 1 - happy path: everything processes and commits transactionally
        produceMessages(PHASE_SIZE, "phase1-");
        Awaitility.await().atMost(Duration.ofSeconds(60)).untilAsserted(() ->
                assertThat(processedOffsets.size()).isEqualTo(PHASE_SIZE));
        Awaitility.await().atMost(Duration.ofSeconds(60)).untilAsserted(() ->
                assertThat(committedOffset(groupId)).isEqualTo((long) PHASE_SIZE));

        // phase 2 - outage: these records process (they are in flight before the first exhaustion engages the
        // pause) and their offsets go dirty, so every commit cadence attempts - and exhausts - a commit
        commitOutage.set(true);
        produceMessages(PHASE_SIZE, "phase2-");

        Awaitility.await().atMost(Duration.ofSeconds(60)).untilAsserted(() ->
                assertThat(exhaustions.size()).isAtLeast(1));

        // phase 3 - produced only AFTER an exhaustion has engaged the EOS-coerced PAUSE_INTAKE: none of these
        // may enter processing until a commit succeeds
        produceMessages(PHASE_SIZE, "phase3-");
        int exhaustionsBeforePauseWindow = exhaustions.size();
        Awaitility.await().atMost(Duration.ofSeconds(60)).untilAsserted(() ->
                assertThat(exhaustions.size()).isAtLeast(exhaustionsBeforePauseWindow + 2));

        long firstPhase3Offset = 2L * PHASE_SIZE;
        assertWithMessage("EOS CONTINUE must pause intake: records arriving after a budget exhaustion must not "
                + "enter processing while commits keep failing")
                .that(processedOffsets.tailSet(firstPhase3Offset)).isEmpty();

        // survival: repeated exhaust-continue cycles (each one resuming the pending complete-else-abort
        // recovery within its budget) - and no terminal previous-fatal-or-abortable classification
        assertThat(exhaustions.size()).isAtLeast(3);
        assertWithMessage("PC must still be running after repeated exhausted transactional commit budgets")
                .that(pc.isClosedOrFailed()).isFalse();

        // heal: recovery aborts the long-stuck transaction (the broker never saw its commit, so at heal it is
        // no longer 'completing'), a fresh transaction commits the dirty offsets, the pause releases, and
        // phase 3 processes - nothing lost, nothing skipped
        log.info("Healing after {} exhausted budgets, {} offsets processed", exhaustions.size(),
                processedOffsets.size());
        commitOutage.set(false);

        long total = 3L * PHASE_SIZE;
        Awaitility.await().atMost(Duration.ofSeconds(120)).untilAsserted(() ->
                assertThat(processedOffsets.size()).isEqualTo((int) total));
        Awaitility.await().atMost(Duration.ofSeconds(60)).untilAsserted(() ->
                assertThat(committedOffset(groupId)).isEqualTo(total));

        assertThat(pc.isClosedOrFailed()).isFalse();
        assertThat(pc.getFailureCause()).isNull();
    }

    private Long committedOffset(String groupId) throws Exception {
        Map<TopicPartition, OffsetAndMetadata> committed = getKcu().getAdmin()
                .listConsumerGroupOffsets(groupId)
                .partitionsToOffsetAndMetadata()
                .get();
        OffsetAndMetadata offsetAndMetadata = committed.get(new TopicPartition(getTopic(), 0));
        return offsetAndMetadata == null ? null : offsetAndMetadata.offset();
    }

    /**
     * A real {@link ProducerWrapper} over a real transactional {@code KafkaProducer}, whose commit phase - and
     * ONLY its commit phase - fails retriably while the outage flag is up. See the class javadoc for why
     * {@link #isTransactionCompleting()} must report {@code true} during the outage.
     */
    private static class CommitOutageProducerWrapper extends ProducerWrapper<String, String> {

        private final AtomicBoolean outage;

        CommitOutageProducerWrapper(ParallelConsumerOptions<String, String> options, AtomicBoolean outage) {
            super(options);
            this.outage = outage;
        }

        @Override
        public void commitTransaction() throws ProducerFencedException {
            if (outage.get()) {
                sleepOrFail(FAILING_COMMIT_PACING, "Interrupted while pacing a failing transactional commit");
                throw new TimeoutException("simulated transactional commit outage (test-controlled)");
            }
            super.commitTransaction();
        }

        @Override
        protected boolean isTransactionCompleting() {
            if (outage.get()) {
                return true;
            }
            return super.isTransactionCompleting();
        }
    }
}
