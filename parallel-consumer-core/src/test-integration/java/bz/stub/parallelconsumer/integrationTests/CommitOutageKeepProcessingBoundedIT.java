package bz.stub.parallelconsumer.integrationTests;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.CommitFailureContext;
import bz.stub.parallelconsumer.CommitFailureHandler;
import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.ParallelEoSStreamProcessor;
import bz.stub.parallelconsumer.state.PartitionStateManager;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicBoolean;

import static bz.stub.parallelconsumer.internal.utils.ThreadUtils.sleepOrFail;
import static bz.stub.parallelconsumer.state.PartitionStateManager.USED_PAYLOAD_THRESHOLD_MULTIPLIER_DEFAULT;
import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static org.apache.commons.lang3.RandomUtils.nextInt;
import static pl.tlinkowski.unij.api.UniSets.of;

/**
 * KTD6 against a real broker (astubbs#317): with the commit-failure seam in
 * {@code KEEP_PROCESSING} mode and a CONTINUE handler, an extended broker-commit outage does NOT
 * accumulate unbounded processed-but-uncommitted state - the existing offset-map payload
 * back-pressure bounds it - and once the outage heals, the accumulated offsets commit within the
 * broker's offset-metadata limit.
 * <p>
 * The bounding mechanism under test (traced, not assumed): every commit attempt - including ones
 * whose {@code commitSync} then fails - first collects commit data, which encodes the incomplete
 * offsets ({@code PartitionState#tryToEncodeOffsets}); when the encoded payload crosses
 * {@code USED_PAYLOAD_THRESHOLD_MULTIPLIER} of the broker's metadata limit,
 * {@code updateBlockFromEncodingResult} sets {@code allowedMoreRecords=false} and
 * {@code PartitionState#couldBeTakenAsWork} stops releasing records above the highest-succeeded
 * offset (records below it stay eligible, because completing them SHRINKS the payload). So the
 * bound engages during the outage precisely because failing commits still encode.
 * <p>
 * The workload is designed so the offset map genuinely grows (the plan's trap: run-length encoding
 * compresses cleanly-completed runs to near nothing, so a "nothing grew" pass would be vacuous):
 * KEY ordering with a stuck subset of keys - their records fail and retry for the whole outage -
 * leaves incomplete offsets sprinkled through the completed range, the sparse-completion shape of
 * {@code CommitResponseTimeoutSymptomTest}. The threshold multiplier is lowered through the same
 * static test seam {@code OffsetEncodingBackPressureTest} uses, so the bound is reachable with an
 * integration-test-sized workload instead of tens of thousands of records.
 * <p>
 * The commit outage is injected in the consumer subclass (the {@link CustomConsumersTest} pattern):
 * {@code commitSync} throws the retriable {@link TimeoutException} while the outage flag is up, so
 * every commit budget exhausts and reaches the handler, while polling - and so group membership -
 * runs against the real broker throughout. The heal assertions therefore accept either end of a
 * real outage (the plan's review residual): a clean recommit, or a rebalance having intervened -
 * asserting the invariant that holds in both lanes: no record is lost (every offset processes
 * successfully at least once) and the dirty offsets eventually all commit.
 *
 * @author Antony Stubbs
 * @see CommitFailureHandler
 * @see bz.stub.parallelconsumer.state.PartitionState
 */
@Slf4j
@Timeout(420) // must exceed the sum of the scenario's awaits, which model a genuinely extended outage
class CommitOutageKeepProcessingBoundedIT extends BrokerIntegrationTest<String, String> {

    private static final int TOTAL_RECORDS = 5_000;

    private static final int UNIQUE_KEYS = 50;

    /** Keys whose records fail and retry for the whole outage - the sparse-completion generator. */
    private static final int STUCK_KEYS = 5;

    /**
     * Lowered payload threshold, so back-pressure engages within an IT-sized workload. 2% of the 4KB broker
     * metadata limit is roughly 80 encoded characters - a few hundred sparse offsets.
     */
    private static final double LOWERED_PAYLOAD_THRESHOLD = 0.02;

    /** Pacing inside the failing {@code commitSync}, so a budget makes a handful of attempts, not thousands. */
    private static final Duration FAILING_COMMIT_PACING = Duration.ofMillis(50);

    private ParallelEoSStreamProcessor<String, String> pc;

    @AfterEach
    void restoreThresholdAndClose() {
        PartitionStateManager.setUSED_PAYLOAD_THRESHOLD_MULTIPLIER(USED_PAYLOAD_THRESHOLD_MULTIPLIER_DEFAULT);
        if (pc != null && !pc.isClosedOrFailed()) {
            pc.closeDontDrainFirst();
        }
    }

    @Test
    void keepProcessingThroughCommitOutageIsBoundedByPayloadBackPressureAndHealsWithinMetadataLimit()
            throws Exception {
        PartitionStateManager.setUSED_PAYLOAD_THRESHOLD_MULTIPLIER(LOWERED_PAYLOAD_THRESHOLD);

        var commitOutage = new AtomicBoolean(true);
        var stuckKeysReleased = new AtomicBoolean(false);
        var exhaustions = new ConcurrentLinkedQueue<CommitFailureContext>();
        var succeededOffsets = new ConcurrentSkipListSet<Long>();

        setupTopic(getClass().getSimpleName());
        String groupId = "commit-outage-" + nextInt();
        Properties consumerProps = getKcu().setupConsumerProps(groupId);
        var consumer = new CommitOutageConsumer(consumerProps, commitOutage);

        CommitFailureHandler continueHandler = context -> {
            log.info("Commit budget exhausted (consecutive: {}) - continuing",
                    context.getConsecutiveExhaustedBudgets());
            exhaustions.add(context);
            return CommitFailureHandler.CommitFailureDecision.CONTINUE;
        };

        var options = ParallelConsumerOptions.<String, String>builder()
                .consumer(consumer)
                // the only mode with the commit retry budget today
                .commitMode(ParallelConsumerOptions.CommitMode.PERIODIC_CONSUMER_SYNC)
                .commitInterval(Duration.ofMillis(100))
                .offsetCommitTimeout(Duration.ofMillis(500)) // small budget: exhaustions come quickly
                .commitFailureHandler(continueHandler)
                // KEEP_PROCESSING is the default; stated because it is the subject of the test
                .commitFailureContinueMode(ParallelConsumerOptions.CommitFailureContinueMode.KEEP_PROCESSING)
                .defaultMessageRetryDelay(Duration.ofMillis(200)) // stuck keys churn fast enough to heal quickly
                .build();

        List<String> producedKeys = getKcu().produceMessages(getTopic(), TOTAL_RECORDS, UNIQUE_KEYS);
        Set<String> stuckKeys = pickStuckKeys(producedKeys);
        log.info("Produced {} records over {} keys; stuck keys: {}", TOTAL_RECORDS, UNIQUE_KEYS, stuckKeys);

        pc = new ParallelEoSStreamProcessor<>(options);
        pc.subscribe(of(getTopic()));
        pc.poll(recordContexts -> recordContexts.forEach(recordContext -> {
            if (stuckKeys.contains(recordContext.key()) && !stuckKeysReleased.get()) {
                // the sparse-completion generator: this key's lane fails and retries for the whole outage,
                // leaving an incomplete offset inside the completed range on every cycle
                throw new IllegalStateException("stuck key (test-controlled): " + recordContext.key());
            }
            // pace the healthy lanes, so back-pressure (evaluated once per commit attempt) can engage while
            // plenty of the workload is still unprocessed - an instant workload would finish before the bound
            sleepOrFail(Duration.ofMillis(15), "Interrupted while pacing a processing lane");
            succeededOffsets.add(recordContext.offset());
        }));

        // several failing commit cycles - each one re-encoded the (growing) sparse offset map
        Awaitility.await().atMost(Duration.ofSeconds(60)).untilAsserted(() ->
                assertThat(exhaustions.size()).isAtLeast(3));
        assertWithMessage("processing must have been running while commits failed - KEEP_PROCESSING")
                .that(succeededOffsets.size()).isAtLeast(1);

        // KTD6(a), the plateau: once the encoded map crosses the threshold the partition is blocked for the REST
        // of the outage - the committed base cannot advance while commits fail, so the encoded range (and with it
        // the payload) can never shrink back under the threshold. Successes converge rather than snap frozen:
        // records below the highest-succeeded offset stay legitimately takeable (completing them shrinks the map),
        // so the lanes' stragglers finish first - measured on this hardware as a ~130-record tail over one probe
        // window. The plateau is therefore asserted as an OBSERVED FREEZE: some window of two further failing
        // commit cycles in which not a single new success lands.
        int plateau = -1;
        boolean froze = false;
        int windowsProbed = 0;
        while (windowsProbed < 12 && !froze) {
            int exhaustionsAtWindowStart = exhaustions.size();
            int successesAtWindowStart = succeededOffsets.size();
            Awaitility.await().atMost(Duration.ofSeconds(60)).untilAsserted(() ->
                    assertThat(exhaustions.size()).isAtLeast(exhaustionsAtWindowStart + 2));
            plateau = succeededOffsets.size();
            froze = plateau == successesAtWindowStart;
            windowsProbed++;
            log.info("Probe window {}: successes {} -> {} (froze: {})", windowsProbed, successesAtWindowStart,
                    plateau, froze);
        }

        int nonStuckTotal = TOTAL_RECORDS - countRecordsWithKeys(producedKeys, stuckKeys);
        assertWithMessage("KTD6(a): payload back-pressure must throttle intake to a standstill - successes must " +
                "stop growing across failing commit cycles once the encoded map crossed the threshold")
                .that(froze).isTrue();
        assertWithMessage("KTD6(a): the plateau must be back-pressure, not workload exhaustion - plenty of " +
                "processable records must remain untaken")
                .that(plateau).isLessThan(nonStuckTotal);

        // heal: commits reach the broker again, and the stuck keys' records now succeed on retry
        log.info("Healing at plateau of {} succeeded offsets ({} exhausted budgets)", plateau, exhaustions.size());
        stuckKeysReleased.set(true);
        commitOutage.set(false);

        // KTD6(b) plus the either-lane invariant: whether the recovery was a clean recommit or a rebalance
        // intervened, no record may be lost - every offset eventually processes successfully - and the dirty
        // offsets must all reach the broker, which also proves the healing commit's metadata fit its limit
        Awaitility.await().atMost(Duration.ofSeconds(120)).untilAsserted(() ->
                assertThat(succeededOffsets.size()).isEqualTo(TOTAL_RECORDS));
        Awaitility.await().atMost(Duration.ofSeconds(60)).untilAsserted(() -> {
            OffsetAndMetadata committed = committedOffset(groupId);
            assertThat(committed).isNotNull();
            assertThat(committed.offset()).isEqualTo(TOTAL_RECORDS);
        });

        assertThat(pc.isClosedOrFailed()).isFalse();
        assertThat(pc.getFailureCause()).isNull();
    }

    /** The first {@link #STUCK_KEYS} distinct keys, in production order. */
    private static Set<String> pickStuckKeys(List<String> producedKeys) {
        var distinct = new LinkedHashSet<>(producedKeys);
        var stuck = new LinkedHashSet<String>();
        for (String key : distinct) {
            if (stuck.size() >= STUCK_KEYS) {
                break;
            }
            stuck.add(key);
        }
        assertThat(stuck).hasSize(STUCK_KEYS); // sanity: the produced key space must be big enough
        return stuck;
    }

    private static int countRecordsWithKeys(List<String> producedKeys, Set<String> keys) {
        return (int) producedKeys.stream().filter(keys::contains).count();
    }

    private OffsetAndMetadata committedOffset(String groupId) throws Exception {
        Map<TopicPartition, OffsetAndMetadata> committed = getKcu().getAdmin()
                .listConsumerGroupOffsets(groupId)
                .partitionsToOffsetAndMetadata()
                .get();
        return committed.get(new TopicPartition(getTopic(), 0));
    }

    /**
     * A real {@link KafkaConsumer} whose {@code commitSync} fails with the retriable
     * {@link TimeoutException} while the outage flag is up - so commit budgets exhaust and the seam engages -
     * while polling, and with it group membership, stays healthy against the real broker. Subclassing a real
     * consumer is the supported extension point ({@link CustomConsumersTest}, confluentinc#195).
     */
    private static class CommitOutageConsumer extends KafkaConsumer<String, String> {

        private final AtomicBoolean outage;

        CommitOutageConsumer(Properties properties, AtomicBoolean outage) {
            super(properties);
            this.outage = outage;
        }

        @Override
        public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
            if (outage.get()) {
                sleepOrFail(FAILING_COMMIT_PACING, "Interrupted while pacing a failing commit");
                throw new TimeoutException("simulated broker commit outage (test-controlled)");
            }
            super.commitSync(offsets);
        }
    }
}
