package bz.stub.parallelconsumer.internal;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.state.WorkContainer;
import bz.stub.parallelconsumer.state.WorkManager;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import pl.tlinkowski.unij.api.UniLists;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import static bz.stub.parallelconsumer.ParallelConsumerOptions.CommitMode.PERIODIC_CONSUMER_ASYNCHRONOUS;
import static bz.stub.parallelconsumer.internal.AsyncCommitterFixture.AsyncCommitRequest;
import static bz.stub.parallelconsumer.internal.AsyncCommitterFixture.commitsHandedToTheClient;
import static bz.stub.parallelconsumer.internal.AsyncCommitterFixture.consumerManagerMock;
import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

/**
 * What happens when the answer to one {@code commitAsync} arrives while a LATER request is still outstanding - the
 * case that exists at all only because deferring the clean-marking is what lets two requests be in flight at once.
 * <p>
 * <b>The committer decides none of it, which is the point.</b> It passes an acknowledgement straight through, whole;
 * each partition then recognises whether the offset acknowledged is the one IT last offered for commit and marks
 * itself clean only then. So this class drives the real chain - {@code ConsumerOffsetCommitter} over a real
 * {@link WorkManager} over real partition states - and asserts the two things an operator would see: <b>what reached
 * the broker</b>, and <b>what the next commit cycle would still send</b>. The rule in isolation, on one
 * {@code PartitionState}, is {@code PartitionStateAcknowledgedCommitOffsetTest}'s subject.
 * <p>
 * "Still dirty" is asserted as {@code collectCommitDataForDirtyPartitions()} - the consequence rather than the flag,
 * and the exact call whose emptiness was the original defect: with the partition marked clean on send, it returned
 * nothing from then on and no further commit was ever issued for those offsets.
 * <p>
 * <b>Why the {@code MockConsumer} scenario tests cannot reach any of this.</b> {@code MockConsumer.commitAsync}
 * answers inline, on the calling thread, before it returns - so a request is always already answered by the time the
 * next one is sent, and the overlap never exists. Reaching it needs the callback held, which needs it captured,
 * which is what the mocked {@link ConsumerManager} here is for.
 *
 * @author Antony Stubbs
 */
// SAME_THREAD for two independent reasons, either of which alone would require it: WorkContainer holds a STATIC
// module reference that PCModuleTestEnv writes (the same reason WorkManagerTest states), and this module runs a
// class's METHODS concurrently by default.
@Execution(ExecutionMode.SAME_THREAD)
class ConsumerOffsetCommitterOverlappingAsyncCommitTest {

    private static final String TOPIC = ConsumerOffsetCommitterOverlappingAsyncCommitTest.class.getSimpleName();

    private static final TopicPartition MOVES_ON = new TopicPartition(TOPIC, 0);

    /**
     * The partition that completes nothing between the two commit requests, so the second request re-offers it at
     * exactly the offset the first one did. It is what distinguishes a per-partition outcome from a whole-request
     * one, and the previous design carried a map in the committer to reach it.
     */
    private static final TopicPartition STANDS_STILL = new TopicPartition(TOPIC, 1);

    private final ConsumerManager<String, String> consumerMgr = consumerManagerMock();

    private PCModuleTestEnv module;

    private WorkManager<String, String> wm;

    private ConsumerOffsetCommitter<String, String> committer;

    private List<WorkContainer<String, String>> work;

    @BeforeEach
    void setup() {
        // UNORDERED stated rather than defaulted: every record must be takeable at once, so the test can choose
        // which offsets complete between the two commit requests
        var options = ParallelConsumerOptions.<String, String>builder()
                .commitMode(PERIODIC_CONSUMER_ASYNCHRONOUS)
                .ordering(ParallelConsumerOptions.ProcessingOrder.UNORDERED)
                .consumer(new MockConsumer<>(OffsetResetStrategy.EARLIEST))
                .build();
        module = new PCModuleTestEnv(options);
        wm = module.workManager();
        module.setWorkManager(wm);
        committer = new ConsumerOffsetCommitter<>(consumerMgr, wm, options);

        wm.onPartitionsAssigned(UniLists.of(MOVES_ON, STANDS_STILL));
        registerThreeRecordsOn(MOVES_ON);
        registerThreeRecordsOn(STANDS_STILL);
        work = wm.getWorkIfAvailable();
    }

    /**
     * The ordering that actually happens: the client answers in send order, but by then a newer request is already
     * outstanding.
     * <p>
     * Marking the partition clean on that answer would end the story at the older offset while the newer one is
     * unanswered - so if the newer request then failed, nothing would be dirty and nothing would re-commit. Recording
     * the offset is simply true and costs nothing, so both happen: the offset moves, the partition stays dirty, and
     * the newer request's own answer is what finishes it.
     */
    @Test
    void anOlderAnswerLeavesThePartitionDirtyForTheHigherOfferStillOutstanding() throws Exception {
        succeed(MOVES_ON, 0);
        commitCycle();
        succeed(MOVES_ON, 1);
        commitCycle();

        List<AsyncCommitRequest> sent = commitsHandedToTheClient(consumerMgr, 2);
        long olderOffer = sent.get(0).offsetFor(MOVES_ON);
        long newerOffer = sent.get(1).offsetFor(MOVES_ON);
        assertWithMessage("the second request must ask the broker for a higher offset, or this is not an overlap")
                .that(newerOffer).isGreaterThan(olderOffer);

        sent.get(0).answerWith(null);

        assertWithMessage("offer %s is still unanswered, so the next cycle must still be willing to send it",
                newerOffer)
                .that(offsetsTheNextCycleWouldSend()).containsExactly(MOVES_ON, newerOffer);

        // and the newest request's own acknowledgement is what ends it
        sent.get(1).answerWith(null);

        assertThat(offsetsTheNextCycleWouldSend()).isEmpty();
    }

    /**
     * The out-of-order pair: the newest request is acknowledged first, and the older answer arrives after it.
     * <p>
     * Nothing is outstanding above the newer offer by then, so that answer cleans - and the late one must apply
     * nothing at all. It is not the answer to the partition's latest offer, so it cannot clean; there is nothing left
     * to clean anyway, and it must not put the partition back into a state that re-commits an offset the broker has
     * already acknowledged.
     */
    @Test
    void anAnswerArrivingAfterAHigherOneAppliesNothing() throws Exception {
        succeed(MOVES_ON, 0);
        commitCycle();
        succeed(MOVES_ON, 1);
        commitCycle();

        List<AsyncCommitRequest> sent = commitsHandedToTheClient(consumerMgr, 2);

        sent.get(1).answerWith(null);
        assertThat(offsetsTheNextCycleWouldSend()).isEmpty();

        sent.get(0).answerWith(null);

        assertWithMessage("the late answer is to a superseded offer, so it must move nothing - including back into "
                + "a re-commit of offsets the broker has already acknowledged")
                .that(offsetsTheNextCycleWouldSend()).isEmpty();
    }

    /**
     * The mixed case, and the reason the outcome has to be per partition: one commit request carries every dirty
     * partition, and only the ones that completed more work move. The second request raises
     * {@link #MOVES_ON}'s offset and re-offers {@link #STANDS_STILL} at the same offset as the first, because nothing
     * new completed there.
     * <p>
     * The answer to the first request is therefore the answer to {@link #STANDS_STILL}'s latest offer and cleans it,
     * while {@link #MOVES_ON} stays dirty. A whole-request rule leaves the unsuperseded partition waiting for a
     * re-commit of an offset the broker has already acknowledged, every cycle, for as long as one partition of an
     * assignment outruns another. Nothing in the committer implements this: each partition made its own offer, so
     * each partition decides.
     */
    @Test
    void oneAnswerCleansThePartitionItIsTheLatestOfferForAndLeavesTheOtherDirty() throws Exception {
        succeed(MOVES_ON, 0);
        succeed(STANDS_STILL, 0);
        commitCycle();
        succeed(MOVES_ON, 1);
        commitCycle();

        List<AsyncCommitRequest> sent = commitsHandedToTheClient(consumerMgr, 2);
        assertWithMessage("the second request must re-offer the partition that stood still unchanged, or this is "
                + "not the mixed case")
                .that(sent.get(1).offsetFor(STANDS_STILL)).isEqualTo(sent.get(0).offsetFor(STANDS_STILL));
        long newerOffer = sent.get(1).offsetFor(MOVES_ON);
        assertWithMessage("the second request must raise the other partition, or this is not the mixed case")
                .that(newerOffer).isGreaterThan(sent.get(0).offsetFor(MOVES_ON));

        sent.get(0).answerWith(null);

        assertWithMessage("only the partition a newer offer is outstanding for may stay dirty")
                .that(offsetsTheNextCycleWouldSend()).containsExactly(MOVES_ON, newerOffer);
    }

    /**
     * One commit cycle exactly as the broker-poll thread runs it: collect what is dirty, hand it to the client, and
     * mark nothing - {@code commitOffsetsReturnsOnlyOnceAcknowledged()} is false in this mode, so the acknowledgement
     * is what moves state. Going through this rather than calling {@code commitOffsets} directly is what makes the
     * partition record the offset it OFFERED, which is the whole of the rule under test.
     */
    private void commitCycle() throws TimeoutException, InterruptedException {
        committer.retrieveOffsetsAndCommit();
    }

    /**
     * @return what a further commit cycle would ask the broker for, by partition - empty when nothing is dirty, which
     * is the state the original defect reached on SEND and could never leave
     */
    private Map<TopicPartition, Long> offsetsTheNextCycleWouldSend() {
        Map<TopicPartition, Long> offsets = new HashMap<>();
        wm.collectCommitDataForDirtyPartitions()
                .forEach((tp, meta) -> offsets.put(tp, meta.offset()));
        return offsets;
    }

    /**
     * Completes one record, which is what makes its partition dirty and raises the offset the next commit offers.
     */
    private void succeed(TopicPartition topicPartition, long recordOffset) {
        WorkContainer<String, String> container = work.stream()
                .filter(wc -> wc.getTopicPartition().equals(topicPartition) && wc.offset() == recordOffset)
                .findFirst()
                .orElseThrow(() -> new AssertionError("no work taken for " + topicPartition + " offset " + recordOffset));
        wm.onSuccessResult(container);
    }

    private void registerThreeRecordsOn(TopicPartition topicPartition) {
        List<ConsumerRecord<String, String>> records = new ArrayList<>();
        for (int recordOffset = 0; recordOffset < 3; recordOffset++) {
            records.add(new ConsumerRecord<>(topicPartition.topic(), topicPartition.partition(), recordOffset,
                    "key-" + recordOffset, "a-value"));
        }
        Map<TopicPartition, List<ConsumerRecord<String, String>>> batch = new HashMap<>();
        batch.put(topicPartition, records);
        wm.registerWork(new EpochAndRecordsMap<>(new ConsumerRecords<>(batch), wm.getPm()));
    }

}
