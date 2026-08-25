package bz.stub.parallelconsumer.state;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.internal.EpochAndRecordsMap;
import bz.stub.parallelconsumer.internal.PCModuleTestEnv;
import bz.stub.parallelconsumer.FakeRuntimeException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import pl.tlinkowski.unij.api.UniLists;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.truth.Truth.assertThat;

/**
 * Guards the resuming dispatch scan in {@link ProcessingShard#getWorkIfAvailable}.
 * <p>
 * The scan no longer restarts at the head of a shard on every pass, because records stay in the shard
 * until they SUCCEED - so restarting meant walking past every in-flight container to reach selectable
 * work, which costs more the more concurrency there is. These tests pin the two properties that change
 * has to preserve, and they are written against {@link WorkManager} rather than the shard directly so
 * they keep holding if the scan moves.
 * <p>
 * They would both fail against a scan that advanced WITHOUT wrapping, which is the tempting simpler
 * version of this optimisation.
 *
 * @author Antony Stubbs
 * @see ProcessingShard#getWorkIfAvailable
 */
class UnorderedShardScanResumeTest {

    static final String TOPIC = "resume-scan-topic";
    static final TopicPartition TP = new TopicPartition(TOPIC, 0);

    WorkManager<String, String> wm;
    PCModuleTestEnv module;

    void setup() {
        var options = ParallelConsumerOptions.<String, String>builder()
                .ordering(ParallelConsumerOptions.ProcessingOrder.UNORDERED)
                .build();
        module = new PCModuleTestEnv(options);
        wm = module.workManager();
        wm.onPartitionsAssigned(UniLists.of(TP));
    }

    void register(int fromOffset, int count) {
        List<ConsumerRecord<String, String>> recs = new ArrayList<>();
        for (int i = fromOffset; i < fromOffset + count; i++) {
            // one distinct key per record, so KEY ordering would shard them apart - UNORDERED does not,
            // which is exactly the case the resuming scan exists for
            recs.add(new ConsumerRecord<>(TOPIC, 0, i, "key-" + i, "value-" + i));
        }
        Map<TopicPartition, List<ConsumerRecord<String, String>>> m = new HashMap<>();
        m.put(TP, recs);
        wm.registerWork(new EpochAndRecordsMap<>(new ConsumerRecords<>(m), wm.getPm()));
    }

    /**
     * Every record is handed out exactly once across repeated small takes, while earlier takes are still
     * in flight. A scan that resumed but never wrapped would still pass this one; the next test is the
     * one that catches it.
     */
    @Test
    void everyRecordIsDispatchedExactlyOnceWhilePreviousTakesAreStillInFlight() {
        setup();
        int total = 50;
        register(0, total);

        Set<Long> seen = new HashSet<>();
        // deliberately never completing anything, so every taken record stays in the shard ahead of the
        // remaining work - the exact condition the old scan re-walked every pass
        for (int pass = 0; pass < total; pass++) {
            var taken = wm.getWorkIfAvailable(5);
            for (var wc : taken) {
                assertThat(seen.add(wc.offset())).isTrue();
            }
            if (seen.size() == total) break;
        }

        assertThat(seen).hasSize(total);
    }

    /**
     * The starvation case, and the reason the scan wraps instead of only advancing.
     * <p>
     * A failed record becomes selectable again BEHIND the resume point. If new work keeps arriving at
     * higher offsets, the resuming scan never runs out of tail to walk - so it never resets to the head,
     * and a scan that only advanced would leave that record unreachable indefinitely while the partition
     * sat blocked behind it.
     * <p>
     * This is deliberately harder than "fail one record and look for it": an earlier version of this test
     * did that, and it passed with the wrap deleted, because an empty tail resets the resume point and
     * hides the bug. Keeping the shard continuously fed is what makes the wrap load-bearing.
     */
    @Test
    void aFailedRecordIsNotStarvedByANeverEmptyingTail() {
        setup();
        register(0, 20);

        var first = wm.getWorkIfAvailable(1);
        assertThat(first).hasSize(1);
        var failed = first.get(0);
        long failedOffset = failed.offset();

        // drive the resume point past it
        wm.getWorkIfAvailable(15);

        failed.onUserFunctionFailure(new FakeRuntimeException("forced, to make it selectable again"));
        wm.onFailureResult(failed);

        // A failed record is not selectable until its retry delay elapses, so without moving the clock
        // this would pass for the wrong reason - proving it absent when it is merely not yet due.
        module.getMutableClock().add(ParallelConsumerOptions.DEFAULT_STATIC_RETRY_DELAY.plusSeconds(1));

        // Keep feeding higher offsets so the tail is never exhausted and the resume point never resets.
        Set<Long> offeredAgain = new HashSet<>();
        int nextOffset = 20;
        for (int pass = 0; pass < 20 && !offeredAgain.contains(failedOffset); pass++) {
            register(nextOffset, 10);
            nextOffset += 10;
            for (var wc : wm.getWorkIfAvailable(5)) {
                offeredAgain.add(wc.offset());
            }
        }

        assertThat(offeredAgain).contains(failedOffset);
    }
}
