package bz.stub.parallelconsumer.state;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.FakeRuntimeException;
import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.ParallelConsumerOptions.ProcessingOrder;
import bz.stub.parallelconsumer.internal.EpochAndRecordsMap;
import bz.stub.parallelconsumer.internal.PCModuleTestEnv;
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

/**
 * Pins the property that a naive {@code UNORDERED} work queue destroys and no throughput benchmark would notice:
 * <b>a retried record must re-enter selection at its own offset, not behind the records that arrived after it.</b>
 *
 * <h2>Why this is a correctness property and not an aesthetic one</h2>
 *
 * PC commits the lowest incomplete offset and encodes the incomplete offsets ABOVE it into the commit metadata, so
 * the encoded payload grows with the <em>spread</em> of outstanding offsets, not with how many there are - and that
 * spread has a hard broker-side ceiling ({@code OffsetMapCodecManager.DefaultMaxMetadataSize}; the back-pressure
 * behaviour when it is hit is {@code OffsetEncodingBackPressureTest}'s subject). A wider spread also means more
 * replay after a crash, because everything above the committed frontier is redelivered.
 * <p>
 * The exact quantity the encoder pays for is
 * {@code getOffsetHighestSucceeded() - getOffsetHighestSequentialSucceeded()}, which is what
 * {@link #theEncodedOffsetSpreadStaysBoundedWhenRecordsFailAndRetry()} measures directly rather than reconstructing.
 *
 * <h2>What would break it</h2>
 *
 * Under {@code UNORDERED} a shard hands out available work with no ordering to enforce, so the container is free to
 * be any bag - and the obvious cheap ones are wrong here. <b>A plain FIFO keeps the happy path</b> (records enter in
 * poll order, which is offset order within a partition) <b>and breaks the retry path</b>: a record appended to the
 * tail is re-offered after every record that arrived while it was out, which widens exactly the spread above, on the
 * path that is already unhappy. A stack or a work-stealing deque is worse still, breaking the happy path too.
 * <p>
 * The shard therefore keeps an offset-sorted container, and re-entry is a keyed insert rather than an append. These
 * tests are what says so: the first fails outright against a tail-append queue, the second measures what that would
 * cost.
 *
 * @author Antony Stubbs
 * @see ProcessingShard
 * @see UnorderedAvailableQueueTest
 */
class UnorderedRetryOffsetOrderTest {

    static final String TOPIC = "unordered-retry-order-topic";
    static final TopicPartition TP = new TopicPartition(TOPIC, 0);

    PCModuleTestEnv module;
    WorkManager<String, String> wm;

    void setup(Duration retryDelay) {
        module = new PCModuleTestEnv(ParallelConsumerOptions.<String, String>builder()
                .ordering(ProcessingOrder.UNORDERED)
                .defaultMessageRetryDelay(retryDelay)
                .build());
        wm = module.workManager();
        wm.onPartitionsAssigned(UniLists.of(TP));
    }

    void register(long fromOffset, int count) {
        List<ConsumerRecord<String, String>> recs = new ArrayList<>(count);
        for (long i = fromOffset; i < fromOffset + count; i++) {
            recs.add(new ConsumerRecord<>(TOPIC, 0, i, "key-" + i, "value-" + i));
        }
        Map<TopicPartition, List<ConsumerRecord<String, String>>> m = new HashMap<>();
        m.put(TP, recs);
        wm.registerWork(new EpochAndRecordsMap<>(new ConsumerRecords<>(m), wm.getPm()));
    }

    PartitionState<String, String> partitionState() {
        return wm.getPm().getPartitionState(TP);
    }

    /**
     * THE ONE THAT FAILS AGAINST A TAIL-APPEND QUEUE. The failed record is out at a worker while ten newer records
     * arrive, so at the moment it comes back the queue already holds all of them. An offset-keyed re-entry puts it
     * in front of them; an append puts it behind all ten.
     * <p>
     * Deliberately arranged so the newer records arrive <em>during</em> the flight rather than after the failure -
     * fail-then-register would put the retry at the head of a FIFO too, and the test would pass for both designs.
     */
    @Test
    void aRetriedRecordIsOfferedBeforeTheRecordsThatArrivedWhileItWasOut() {
        setup(Duration.ZERO);
        register(0, 1);

        var taken = wm.getWorkIfAvailable(1);
        assertThat(taken).hasSize(1);
        var failing = taken.get(0);
        assertThat(failing.offset()).isEqualTo(0L);

        // newer work arrives while offset 0 is out at a worker - this is what a tail append would jump it behind
        register(1, 10);

        failing.onUserFunctionFailure(new FakeRuntimeException("deliberate, to force a retry"));
        wm.handleFutureResult(failing);

        var next = wm.getWorkIfAvailable(1);
        assertThat(next).hasSize(1);
        assertWithMessage("the retried record must be offered before the ten that arrived while it was out. "
                        + "Getting offset %s instead means retries are re-entering selection at the tail, which "
                        + "widens the in-flight offset spread the commit payload is encoded from - see this "
                        + "class's header, and theEncodedOffsetSpreadStaysBoundedWhenRecordsFailAndRetry for what "
                        + "it costs.",
                next.get(0).offset())
                .that(next.get(0).offset()).isEqualTo(0L);
    }

    /**
     * The same property as a measured quantity rather than a single comparison: run a full workload with a
     * failure rate and watch the encoded offset range the commit payload is built from.
     * <p>
     * <b>The bound is asserted, and the observed maximum is reported in the failure message</b>, because the
     * number is the artefact: <b>49 with offset-keyed re-entry, 2,000 with a tail-append one</b>, on this exact
     * workload. Forty-nine is one less than the concurrency, which is the floor - the frontier can only be held
     * back by work genuinely outstanding. Two thousand is the entire batch, because a record that failed early is
     * not retried until everything newer has been through.
     * <p>
     * The 2,000 figure was measured, not reasoned about: a throwaway control arm appended returning records to an
     * arrival-ordered queue instead of re-inserting them by offset, and this test and
     * {@link #aRetriedRecordIsOfferedBeforeTheRecordsThatArrivedWhileItWasOut()} both went red while
     * {@link #withNoFailuresAnUnorderedShardHandsRecordsOutInOffsetOrder()} stayed green - which is exactly the
     * split the class header predicts, and the reason a throughput benchmark would never have caught this.
     */
    @Test
    void theEncodedOffsetSpreadStaysBoundedWhenRecordsFailAndRetry() {
        setup(Duration.ZERO);

        final int total = 2_000;
        final int concurrency = 50;
        final int failEveryNth = 7;

        register(0, total);

        var state = partitionState();
        List<WorkContainer<String, String>> inFlight = new ArrayList<>();
        long worstSpread = 0;
        int succeeded = 0;
        int guard = 0;

        while (succeeded < total && guard++ < total * 10) {
            int room = concurrency - inFlight.size();
            if (room > 0) {
                inFlight.addAll(wm.getWorkIfAvailable(room));
            }
            if (inFlight.isEmpty()) {
                break;
            }
            var wc = inFlight.remove(0);
            // fail a record the FIRST time it is delivered only, so the run terminates
            if (wc.offset() % failEveryNth == 0 && !wc.hasPreviouslyFailed()) {
                wc.onUserFunctionFailure(new FakeRuntimeException("deliberate, to force a retry"));
            } else {
                wc.onUserFunctionSuccess();
                succeeded++;
            }
            wm.handleFutureResult(wc);

            long spread = state.getOffsetHighestSucceeded() - state.getOffsetHighestSequentialSucceeded();
            worstSpread = Math.max(worstSpread, spread);
        }

        assertWithMessage("the run must actually finish, or the spread figure describes a partial workload")
                .that(succeeded).isEqualTo(total);

        // MEASURED, not assumed. The observed maximum on this workload is 49 - one less than the concurrency,
        // because the frontier can only be held back by work that is genuinely outstanding. The same workload
        // run against a tail-append re-entry (a throwaway control arm that appended returning records to an
        // arrival-ordered queue instead of re-inserting them by offset) reached 2,000: the whole batch, because
        // a record that failed early was not retried until everything newer had been through. The bound is set
        // to a small multiple of the concurrency so it fails long before it approaches that.
        long bound = 4L * concurrency;
        assertWithMessage("the widest gap between the highest succeeded offset and the committable frontier was "
                        + "%s, over a %s-record run at concurrency %s with every %sth record failing once. That "
                        + "gap IS the offset-encoding payload's range, and it must stay bounded by the work in "
                        + "flight rather than growing with the batch. A figure approaching %s means retried "
                        + "records are being re-offered behind newer ones.",
                worstSpread, total, concurrency, failEveryNth, total)
                .that(worstSpread).isAtMost(bound);
    }

    /**
     * The happy path, stated separately so a regression cannot hide behind the retry case: with no failures at
     * all, an unordered shard hands records out in offset order.
     * <p>
     * This is not a guarantee PC makes to users - {@code UNORDERED} promises no ordering - but it is the property
     * that keeps the commit payload small, and it is free from a sorted container. It would survive a FIFO too;
     * it is here so that a container change which broke it (a stack, a work-stealing deque) fails immediately
     * rather than showing up as a commit-metadata ceiling in production.
     */
    @Test
    void withNoFailuresAnUnorderedShardHandsRecordsOutInOffsetOrder() {
        setup(Duration.ZERO);
        register(0, 200);

        List<Long> order = new ArrayList<>();
        while (order.size() < 200) {
            var taken = wm.getWorkIfAvailable(10);
            if (taken.isEmpty()) {
                break;
            }
            for (var wc : taken) {
                order.add(wc.offset());
                wc.onUserFunctionSuccess();
                wm.handleFutureResult(wc);
            }
        }

        List<Long> expected = new ArrayList<>(200);
        for (long i = 0; i < 200; i++) {
            expected.add(i);
        }
        assertWithMessage("an unordered shard walks its records in offset order when nothing fails")
                .that(order).isEqualTo(expected);
    }
}
