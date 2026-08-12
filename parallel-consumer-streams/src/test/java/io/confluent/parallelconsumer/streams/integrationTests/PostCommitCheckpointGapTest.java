package io.confluent.parallelconsumer.streams.integrationTests;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import io.confluent.parallelconsumer.integrationTests.utils.KafkaClientUtils;
import io.confluent.parallelconsumer.streams.PcDispatchCounters;
import io.confluent.parallelconsumer.streams.PcDispatchSwitch;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * A PC-dispatched stateful task <b>does</b> refresh its checkpoint under load - written to prove the
 * opposite, and the run refuted it (astubbs#255).
 *
 * <h2>The hypothesis this class was built to confirm, and why it was wrong</h2>
 * {@code TaskExecutor.commitTasksAndMaybeUpdateCommittableOffsets} walks its tasks twice. Loop 1 gates
 * {@code prepareCommit()} on {@code commitNeeded()} and commits PC's frontier. The commit's success path
 * reaches {@code StreamTask.updateCommittedOffsets} -> {@code pcDispatcher.onCommitSuccess}, which sets
 * {@code successesCommitted = successesCollected}. Loop 2 then <b>re-asks</b> {@code commitNeeded()} before
 * {@code task.postCommit(false)} - and that acknowledgement has just made it false.
 * <p>
 * The inference drawn from that - and stated as fact in a sibling class before this test ran - was that
 * {@code postCommit} therefore <em>never</em> runs on the PC path, so {@code maybeCheckpoint} never runs,
 * so {@code stateMgr.updateChangelogOffsets(...)} never advances {@code StateStoreMetadata.offset}, so the
 * delta {@code StateManagerUtil.checkpointNeeded} computes is structurally zero at any volume.
 * <p>
 * <b>Measured, that is false.</b> With 12,000 records the PC arm checkpointed at changelog offset 11,862
 * against stock's 11,999. The step the reasoning missed: {@code hasUncommittedWork()} can go true
 * <em>again</em> between the commit and loop 2, because more records finish in that window. Under
 * continuous load it almost always does, so loop 2 sees a task that legitimately needs another commit and
 * {@code postCommit} runs normally.
 *
 * <h2>What the defect actually is, once it is scoped honestly</h2>
 * The skip is real but narrow: it happens only when <b>no new work completes</b> between the commit and
 * loop 2 - that is, when the task goes idle at exactly that moment. The un-checkpointed tail is then
 * bounded by whatever completed in the final commit round, not by the whole run. A clean {@code close()}
 * writes an enforced checkpoint regardless, so this costs a little extra changelog replay after a crash
 * that lands in an idle window, and nothing else.
 * <p>
 * That is a much smaller thing than "never checkpoints", and it is recorded here rather than fixed:
 * changing the commit path for every PC caller to close a bounded idle-tail gap is not a trade this
 * evidence supports.
 *
 * <h2>Reading the arms</h2>
 * The checkpoint is read <b>before</b> {@code close()}, because closing writes an enforced checkpoint and
 * would mask any mid-run difference entirely. The stock arm is the control that proves the fixture can
 * observe a checkpoint at all - without it, a PC reading would be uninterpretable.
 *
 * @author Antony Stubbs
 * @see io.confluent.parallelconsumer.streams.PcTaskDispatcher#onCommitSuccess(java.util.Map)
 */
@Slf4j
// PcDispatchSwitch and PcDispatchCounters are process-wide.
@Isolated
class PostCommitCheckpointGapTest extends BrokerStreamsIntegrationTest {

    private static final int POOL_SIZE = 4;

    /**
     * Comfortably past {@code OFFSET_DELTA_THRESHOLD_FOR_CHECKPOINT} (10,000). One changelog record per
     * record, because the store has caching disabled, so this is also the changelog delta.
     */
    private static final int INPUT_RECORDS = 12_000;

    private static final String STORE = "checkpoint-gap-store";

    /** Short, so several commits land while the records drain. */
    private static final Duration COMMIT_INTERVAL = Duration.ofMillis(500);

    /** Several commit intervals after the drain, so a checkpoint has every chance to be written. */
    private static final Duration SETTLE = Duration.ofSeconds(5);

    private static final AtomicInteger recordsProcessed = new AtomicInteger();

    @TempDir
    Path stateDir;

    @BeforeEach
    void resetCounters() {
        recordsProcessed.set(0);
        PcDispatchCounters.reset();
    }

    @AfterEach
    void resetSwitch() {
        PcDispatchSwitch.resetToDefault();
    }

    /**
     * The control. Stock refreshes the checkpoint mid-run once the changelog delta clears the threshold,
     * which is what makes the PC arm's absence meaningful rather than a fixture that looks in the wrong
     * directory.
     */
    @Test
    void stockRefreshesTheCheckpointWhileRunning() {
        PcDispatchSwitch.disable();

        List<String> checkpoint = runArm("stock-checkpoint", false);

        assertThat(checkpoint)
                .as("STOCK CONTROL: after %s records - well past the 10,000 changelog delta "
                        + "checkpointNeeded requires - a running stock task must have written a "
                        + "checkpoint. If this is empty the fixture is not reading the right path and the "
                        + "PC arm below proves nothing.", INPUT_RECORDS)
                .isNotEmpty();
    }

    /**
     * The refutation. Same topology, same volume, seam on: the checkpoint advances, and it advances to
     * within a commit round of stock.
     * <p>
     * Asserted as a floor rather than an exact offset because the PC arm legitimately lags - it
     * checkpoints whatever the last commit round covered, and the run that produced this test measured
     * 11,862 against stock's 11,999. Pinning the exact number would make this a timing test.
     */
    @Test
    void pcPathAlsoRefreshesTheCheckpointUnderLoad() {
        PcDispatchSwitch.enable(POOL_SIZE);

        List<String> checkpoint = runArm("pc-checkpoint", true);

        assertThat(checkpoint)
                .as("REFUTES the sibling class's inference: postCommit(false) DOES run on the PC path "
                        + "under load, because hasUncommittedWork() goes true again between the commit and "
                        + "TaskExecutor's second loop - more records finish in that window. So "
                        + "maybeCheckpoint runs, updateChangelogOffsets advances, and a checkpoint is "
                        + "written. If this ever goes empty, the skip has stopped being confined to idle "
                        + "windows and the sibling class's original reading becomes correct after all.")
                .isNotEmpty();

        assertThat(changelogOffset(checkpoint))
                .as("and it advances to within roughly one commit round of the %s records processed - the "
                        + "gap is the tail of work committed after the last checkpoint, not the whole run",
                        INPUT_RECORDS)
                .isGreaterThan(INPUT_RECORDS - 2_000L);
    }

    /**
     * The changelog offset out of a checkpoint file. Format is a version line, an entry count, then one
     * {@code <topic> <partition> <offset>} line per changelog partition; this topology has exactly one.
     */
    private static long changelogOffset(final List<String> checkpointLines) {
        assertThat(checkpointLines)
                .as("a checkpoint file carries a version line, a count line, and one entry per changelog "
                        + "partition - this topology has exactly one store")
                .hasSize(3);
        String[] fields = checkpointLines.get(2).split(" ");
        assertThat(fields).as("checkpoint entry is '<topic> <partition> <offset>'").hasSize(3);
        return Long.parseLong(fields[2]);
    }

    /**
     * @return the checkpoint file's lines, read <b>before</b> close (which would write an enforced one),
     *         or an empty list when no checkpoint exists
     */
    private List<String> runArm(final String name, final boolean expectPcPath) {
        String inputTopic = setupTopic(name + "-in");
        ensureTopic(inputTopic, 1);
        String appId = name + "-" + System.nanoTime();

        produceInput(inputTopic);

        KafkaStreams streams = startTopology(appId, inputTopic);
        List<String> checkpoint;
        try {
            await().atMost(Duration.ofMinutes(5))
                    .until(() -> recordsProcessed.get() >= INPUT_RECORDS);
            log.info("=== [{}] all {} records processed", name, INPUT_RECORDS);

            long dispatched = PcDispatchCounters.getRecordsDispatchedToPool();
            log.info("=== [{}] dispatched to pool: {}", name, dispatched);
            if (expectPcPath) {
                assertThat(dispatched)
                        .as("[%s] must have gone through the PC dispatch seam", name)
                        .isEqualTo(INPUT_RECORDS);
            } else {
                assertThat(dispatched)
                        .as("[%s] is a stock arm and must not have dispatched through the seam", name)
                        .isZero();
            }

            sleepThrough(SETTLE, "letting a checkpoint be written in " + name);

            assertThat(streams.state())
                    .as("[%s] the client must still be RUNNING - a dead client writes no checkpoint "
                            + "either, and would satisfy the PC arm for the wrong reason", name)
                    .isEqualTo(KafkaStreams.State.RUNNING);

            checkpoint = readCheckpoint(appId);
            log.info("=== [{}] checkpoint lines: {}", name, checkpoint);
        } catch (Throwable failure) {
            streams.close(Duration.ofSeconds(60));
            throw failure;
        }

        assertThat(streams.close(Duration.ofSeconds(60)))
                .as("[%s] the client must shut down within the timeout", name)
                .isTrue();
        return checkpoint;
    }

    /**
     * Reads {@code <state.dir>/<application.id>/0_0/.checkpoint}. The task is 0_0 because the input topic
     * has one partition and the topology is a single sub-topology.
     */
    private List<String> readCheckpoint(final String appId) {
        Path checkpointFile = stateDir.resolve(appId).resolve("0_0").resolve(".checkpoint");
        if (!Files.exists(checkpointFile)) {
            log.info("No checkpoint file at {}", checkpointFile);
            return Collections.emptyList();
        }
        try {
            return Files.readAllLines(checkpointFile);
        } catch (IOException e) {
            throw new IllegalStateException("Could not read checkpoint at " + checkpointFile, e);
        }
    }

    private void produceInput(final String inputTopic) {
        try (KafkaProducer<String, String> producer =
                     getKcu().createNewProducer(KafkaClientUtils.ProducerMode.NOT_TRANSACTIONAL)) {
            for (int i = 0; i < INPUT_RECORDS; i++) {
                // Distinct keys, so PC can spread the work across its pool rather than serialising on one
                // key shard - the arm is about commit bookkeeping, not about head-of-line blocking.
                producer.send(new ProducerRecord<>(inputTopic, "key-" + i, "value-" + i));
            }
            producer.flush();
        }
        log.info("Produced {} records into {}", INPUT_RECORDS, inputTopic);
    }

    private KafkaStreams startTopology(final String appId, final String inputTopic) {
        StoreBuilder<KeyValueStore<String, String>> store = Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(STORE), Serdes.String(), Serdes.String())
                .withLoggingEnabled(Collections.emptyMap())
                // Caching off: one changelog record per put, so the changelog delta equals the record
                // count and the threshold arithmetic in the javadoc is readable. It is also the state the
                // PC path currently requires (U12).
                .withCachingDisabled();

        StreamsBuilder builder = new StreamsBuilder();
        builder.addStateStore(store);
        builder.<String, String>stream(inputTopic)
                .process((ProcessorSupplier<String, String, Void, Void>) () ->
                        new Processor<String, String, Void, Void>() {

                            private KeyValueStore<String, String> kvStore;

                            @Override
                            public void init(final ProcessorContext<Void, Void> context) {
                                kvStore = context.getStateStore(STORE);
                            }

                            @Override
                            public void process(final Record<String, String> record) {
                                // One changelog write per record - the thing whose offsets the checkpoint
                                // is supposed to track.
                                kvStore.put(record.key(), record.value());
                                recordsProcessed.incrementAndGet();
                            }
                        }, STORE);

        Properties props = baseStreamsProps(appId);
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, COMMIT_INTERVAL.toMillis());
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.toAbsolutePath().toString());

        return startAndAwaitRunning(builder, props, LOG_AND_SHUT_DOWN_CLIENT);
    }
}
