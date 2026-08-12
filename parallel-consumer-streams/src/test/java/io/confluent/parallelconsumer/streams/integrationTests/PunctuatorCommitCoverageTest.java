package io.confluent.parallelconsumer.streams.integrationTests;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import io.confluent.parallelconsumer.integrationTests.utils.KafkaClientUtils;
import io.confluent.parallelconsumer.streams.PcDispatchCounters;
import io.confluent.parallelconsumer.streams.PcDispatchSwitch;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;

import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * The Kafka Streams commit sensor is blind on the PC path: commits happen and {@code commit-total} never
 * moves (astubbs#255).
 *
 * <h2>What an earlier version of this class claimed, and why it was wrong</h2>
 * It set out to show that punctuation does not make a task committable on the PC path, using
 * {@code commit-total} as the observable, and reported that the PC arms committed offsets "by a route that
 * does not go through {@code StreamThread.maybeCommit()}" which it could not identify. <b>Both halves were
 * wrong, and code review caught it.</b> The route IS {@code maybeCommit()}. The sensor is what cannot see
 * it, and the reason makes {@code commit-total} structurally incapable of rising on the PC path - so the
 * original arms asserted a value that could never have been anything else. A test whose assertion cannot
 * fail proves nothing, which is the defect class this module has recorded against itself repeatedly.
 *
 * <h2>The actual mechanism, which is the thing worth pinning</h2>
 * {@code TaskExecutor.commitTasksAndMaybeUpdateCommittableOffsets} walks its task list <b>twice</b>:
 * <ol>
 *   <li>Loop 1 gates {@code prepareCommit()} on {@code task.commitNeeded()}. On the PC path that answers
 *       {@code pcDispatcher.hasUncommittedWork()}, which is true while completed work is uncollected - so
 *       the frontier is collected and handed to {@code commitOffsetsOrTransaction}.</li>
 *   <li>The commit succeeds, and its success path calls {@code updateTaskCommitMetadata} ->
 *       {@code StreamTask.updateCommittedOffsets} -> {@code pcDispatcher.onCommitSuccess}, which sets
 *       {@code successesCommitted = successesCollected}.</li>
 *   <li>Loop 2 <b>re-asks</b> {@code task.commitNeeded()} before {@code ++committed} and
 *       {@code postCommit(false)}. The acknowledgement in step 2 has just made it <b>false</b>.</li>
 * </ol>
 * So {@code committed} stays 0, {@code maybeCommit()} returns 0, and {@code StreamThread}'s
 * {@code if (committed > 0)} guard means {@code commitSensor.record()} never fires. The offsets are on the
 * broker; the sensor never counted them.
 * <p>
 * <b>The same second loop also skips {@code postCommit(false)}, and therefore
 * {@code maybeCheckpoint()}, on every successful PC-path commit.</b> That is a production consequence
 * rather than a test artifact, it is not what this class was written to find, and no implementation unit
 * currently owns it. Recorded here because this is where the evidence lives.
 *
 * <h2>What each arm is entitled to claim</h2>
 * The stock arms are unchanged and still sound: punctuation alone drives commits on stock, and the same
 * topology without a punctuator does not. That comparison is what makes the PC-side blindness legible
 * rather than a bare zero.
 * <p>
 * The PC arms no longer assert a lone zero. They assert the <b>conjunction</b> - the input partition IS
 * committed, and {@code commit-total} is still zero - which is falsifiable in both directions: if loop 2
 * ever counted, the total rises; if the commit stopped happening, the offset assertion fails.
 *
 * <h2>Observables rejected on the way here, all of which would have passed while proving nothing</h2>
 * <ul>
 *   <li><b>The checkpoint file.</b> {@code StateManagerUtil.checkpointNeeded} wants a changelog delta over
 *       {@code OFFSET_DELTA_THRESHOLD_FOR_CHECKPOINT} (10,000) unless the checkpoint is enforced, so
 *       neither arm moves it after a handful of punctuations.</li>
 *   <li><b>Punctuator output on a topic.</b> Without EOS the producer sends it on its own schedule whether
 *       or not {@code flush()} is ever called, so nothing disappears.</li>
 *   <li><b>{@code commit-total} alone on the PC path.</b> The subject of this rewrite: pinned at zero by
 *       construction, so a zero carries no information about punctuation.</li>
 * </ul>
 *
 * @author Antony Stubbs
 * @see io.confluent.parallelconsumer.streams.PcTaskDispatcher#hasUncommittedWork()
 * @see io.confluent.parallelconsumer.streams.PcTaskDispatcher#onCommitSuccess(Map)
 */
@Slf4j
// PcDispatchSwitch and PcDispatchCounters are process-wide; a concurrent test flipping the switch would
// change which dispatch path these arms measure, and would add to the counters they assert on.
@Isolated
class PunctuatorCommitCoverageTest extends BrokerStreamsIntegrationTest {

    private static final int POOL_SIZE = 4;

    private static final int INPUT_RECORDS = 5;

    /** Short, so several commit opportunities fall inside the punctuate-only window below. */
    private static final Duration COMMIT_INTERVAL = Duration.ofMillis(500);

    private static final Duration PUNCTUATE_INTERVAL = Duration.ofMillis(200);

    /** Enough punctuations that a commit interval cannot simply have failed to elapse. */
    private static final int PUNCTUATIONS_AWAITED = 6;

    /**
     * How long the punctuate-only window runs for. Long enough for several COMMIT_INTERVALs, so a flat
     * commit-total is a decision not to commit rather than a window that was too short to contain one.
     */
    private static final Duration IDLE_WINDOW = Duration.ofSeconds(6);

    /**
     * Time allowed after the last record is processed for the drain's own record-driven commits to
     * finish, before the measured window opens.
     * <p>
     * <b>Margin, stated rather than felt:</b> eight COMMIT_INTERVALs. The drain needs one commit
     * opportunity to clear its outstanding work, so this is 8x the minimum - the same "many times what is
     * needed" argument {@link #PUNCTUATIONS_AWAITED} carries. A settle that is too short does not produce
     * a false green here: it would leave a drain-tail commit inside the window and push the
     * <em>no-punctuator</em> arms toward a false RED, which is the safe direction to fail in.
     * <p>
     * <b>This was a wait-until-commit-total-stops-moving loop, and that loop could never terminate on the
     * stock arm</b> - with a punctuator registered, stock commits every interval indefinitely, so "flat"
     * is a state only the PC path ever reaches. A fixed settle measures every arm the same way instead of
     * encoding one arm's behaviour into the fixture.
     */
    private static final Duration SETTLE = Duration.ofSeconds(4);

    /**
     * Static because the processor is instantiated by Streams on its own threads; reset per test so one
     * arm's counts cannot leak into the next.
     */
    private static final AtomicInteger recordsProcessed = new AtomicInteger();

    private static final AtomicInteger punctuationsFired = new AtomicInteger();

    @BeforeEach
    void resetCounters() {
        recordsProcessed.set(0);
        punctuationsFired.set(0);
        PcDispatchCounters.reset();
    }

    @AfterEach
    void resetSwitch() {
        PcDispatchSwitch.resetToDefault();
    }

    /**
     * Stock, with a punctuator: punctuation alone drives commits, because stock reads the
     * {@code commitNeeded} that {@code maybePunctuateSystemTime} sets.
     */
    @Test
    void stockPathCommitsForPunctuationAlone() {
        PcDispatchSwitch.disable();

        ArmResult result = runArm("stock-punctuator", true, false);

        assertThat(result.commitDeltaDuringWindow)
                .as("STOCK BASELINE: %s punctuations over an idle task must drive repeated commits, "
                        + "because maybePunctuateSystemTime sets commitNeeded and stock's commitNeeded() "
                        + "reads it. Asserted as several commits rather than merely one, so a single "
                        + "straggling drain-tail commit cannot satisfy it.",
                        result.punctuationsDuringWindow)
                .isGreaterThanOrEqualTo(3);
    }

    /**
     * Stock, without a punctuator: the discriminator that keeps the arm above honest.
     * <p>
     * Stock's {@code commitNeeded()} override also sweeps the consumer position, which can set
     * {@code commitNeeded} for reasons unrelated to punctuation. Without this arm, the baseline would be
     * satisfied by that sweep and the comparison would credit punctuation with something it did not cause.
     */
    @Test
    void stockPathWithoutAPunctuatorDoesNotCommitWhenIdle() {
        PcDispatchSwitch.disable();

        ArmResult result = runArm("stock-no-punctuator", false, false);

        assertThat(result.commitDeltaDuringWindow)
                .as("DISCRIMINATOR: with no punctuator registered, an idle stock task must not commit. A "
                        + "non-zero value here means the baseline arm's commits come from the "
                        + "consumer-position sweep rather than from punctuation, and the comparison is "
                        + "confounded.")
                .isZero();
    }

    /**
     * The PC path commits, and the sensor does not see it. Both halves asserted together, because either
     * one alone is satisfiable for the wrong reason.
     */
    @Test
    void pcPathCommitsWhileTheCommitSensorStaysAtZero() {
        PcDispatchSwitch.enable(POOL_SIZE);

        ArmResult result = runArm("pc-punctuator", true, true);

        assertThat(result.committedOffsetAtEnd)
                .as("the PC path really did commit: loop 1 of "
                        + "TaskExecutor.commitTasksAndMaybeUpdateCommittableOffsets gated prepareCommit() on "
                        + "commitNeeded(), collected PC's frontier, and the commit reached the broker. "
                        + "Without this half, the zero below would be satisfiable by a task that simply "
                        + "never committed anything.")
                .isNotNull()
                .extracting(OffsetAndMetadata::offset)
                .isEqualTo((long) INPUT_RECORDS);

        assertThat(result.commitTotalAtEnd)
                .as("and the sensor never counted it: onCommitSuccess set successesCommitted = "
                        + "successesCollected during the commit, so loop 2's re-check of commitNeeded() "
                        + "answered false, ++committed was skipped, maybeCommit() returned 0, and "
                        + "StreamThread's `if (committed > 0)` guard kept commitSensor.record() from ever "
                        + "firing. If loop 2 ever starts counting, this rises and the test goes red - "
                        + "which is the point of asserting the total rather than the window delta.")
                .isZero();
    }

    /**
     * The same conjunction with no punctuator registered, which is what shows the blindness is a property
     * of the commit path and not of punctuation.
     * <p>
     * The earlier version of this class asserted a lone zero on the punctuator arm and treated it as
     * evidence about punctuation. This arm is why that was never available: the zero is identical here.
     */
    @Test
    void pcPathSensorIsBlindWithoutAPunctuatorToo() {
        PcDispatchSwitch.enable(POOL_SIZE);

        ArmResult result = runArm("pc-no-punctuator", false, true);

        assertThat(result.committedOffsetAtEnd)
                .as("the PC path commits with no punctuator registered at all")
                .isNotNull()
                .extracting(OffsetAndMetadata::offset)
                .isEqualTo((long) INPUT_RECORDS);

        assertThat(result.commitTotalAtEnd)
                .as("and the sensor is equally blind here, which is what makes the punctuator arm's zero "
                        + "uninformative about punctuation and this class's subject the sensor rather than "
                        + "the punctuator")
                .isZero();
    }

    /**
     * Drains the input, settles for a fixed period so the drain's own commits are done, then measures a
     * fixed window in which punctuation is the only activity. Every arm gets the same settle and the same
     * window, so the arms are comparable.
     * <p>
     * The assertions common to every arm live here - dispatch-path membership, liveness, the punctuation
     * premise, and clean shutdown - because each of them, if left unchecked, makes some arm's headline
     * assertion satisfiable for a reason that arm is not about.
     *
     * @param withPunctuator whether the processor registers a WALL_CLOCK_TIME punctuator
     * @param expectPcPath   whether this arm must have gone through the PC dispatch seam
     */
    private ArmResult runArm(final String name, final boolean withPunctuator, final boolean expectPcPath) {
        String inputTopic = setupTopic(name + "-in");
        ensureTopic(inputTopic, 1);
        String appId = name + "-" + System.nanoTime();
        TopicPartition inputPartition = new TopicPartition(inputTopic, 0);

        produceInput(inputTopic);

        KafkaStreams streams = startTopology(appId, inputTopic, withPunctuator);
        ArmResult result;
        try {
            // Phase 1: drain. Until every record has been processed there is record work outstanding, and
            // hasUncommittedWork() would be true for that reason rather than for punctuation.
            await().atMost(Duration.ofSeconds(60))
                    .until(() -> recordsProcessed.get() >= INPUT_RECORDS);
            log.info("=== [{}] all {} records processed", name, INPUT_RECORDS);

            // Which dispatch path actually ran. Asserted, not logged: without this, an arm that silently
            // fell back to the other path still satisfies a zero-valued headline assertion, and the whole
            // stock-versus-PC comparison becomes a comparison of two identical runs.
            long dispatched = PcDispatchCounters.getRecordsDispatchedToPool();
            long offered = PcDispatchCounters.getRecordsOfferedToWorkManager();
            long accepted = PcDispatchCounters.getRecordsAcceptedByWorkManager();
            log.info("=== [{}] dispatch counters: offered={} accepted={} dispatched={} completed={} failed={}",
                    name, offered, accepted, dispatched,
                    PcDispatchCounters.getRecordsCompletedSuccessfully(),
                    PcDispatchCounters.getRecordsFailed());
            if (expectPcPath) {
                assertThat(dispatched)
                        .as("[%s] must have gone through the PC dispatch seam", name)
                        .isEqualTo(INPUT_RECORDS);
                assertThat(accepted)
                        .as("[%s] every offered record was accepted - a shortfall is the silent "
                                + "epoch-drop PcDispatchCounters documents, and would mean the arm "
                                + "measured fewer records than it produced", name)
                        .isEqualTo(offered);
            } else {
                assertThat(dispatched)
                        .as("[%s] is a stock arm and must not have dispatched through the seam", name)
                        .isZero();
            }

            // Phase 2: let the drain's own record-driven commits finish, so the measured window is not
            // reading their tail. Fixed, not a converge-loop - see SETTLE.
            sleepThrough(SETTLE, "settling after the drain in " + name);

            double before = commitTotal(streams);
            int punctuationsBefore = punctuationsFired.get();

            // Phase 3: the punctuate-only window. No new input; nothing in flight; nothing outstanding.
            // A FIXED window, identical for every arm - an earlier "await N punctuations, then sleep"
            // shape gave the punctuator arms a longer window than the no-punctuator arms, and the
            // no-punctuator arms are the ones asserting zero.
            sleepThrough(IDLE_WINDOW, "measuring the idle window in " + name);
            int punctuationsDuring = punctuationsFired.get() - punctuationsBefore;

            // Liveness. A shut-down or dead client reports zero commits just as convincingly as a live one
            // that chose not to commit, and every arm here has a zero somewhere in its assertions.
            assertThat(streams.state())
                    .as("[%s] the client must still be RUNNING across the measured window - a dead or "
                            + "shut-down client produces the same zero the assertions are reading", name)
                    .isEqualTo(KafkaStreams.State.RUNNING);

            // The punctuation premise, both ways. At one punctuation per PUNCTUATE_INTERVAL the window
            // holds many times PUNCTUATIONS_AWAITED, so a shortfall means punctuation is not running.
            if (withPunctuator) {
                assertThat(punctuationsDuring)
                        .as("[%s] the punctuator must actually fire during the measured window, or a "
                                + "commit count says nothing about punctuation", name)
                        .isGreaterThanOrEqualTo(PUNCTUATIONS_AWAITED);
            } else {
                assertThat(punctuationsDuring)
                        .as("[%s] registered no punctuator, so nothing may have punctuated - a non-zero "
                                + "count means a previous arm's topology leaked into this one", name)
                        .isZero();
            }

            double after = commitTotal(streams);
            OffsetAndMetadata committed = committedOffsetOrNull(appId, inputPartition);
            log.info("=== [{}] idle window: commit-total {} -> {} over {} punctuations, committed={}",
                    name, before, after, punctuationsDuring, committed);

            result = new ArmResult(after - before, after, committed, punctuationsDuring);
        } catch (Throwable failure) {
            // Close on the way out, but let the original failure be the one that surfaces - a close
            // assertion here would mask whichever assertion above actually fired.
            streams.close(Duration.ofSeconds(30));
            throw failure;
        }

        // Asserted on the success path only. A close that times out leaves StreamThreads alive that keep
        // incrementing the static counters and the process-wide dispatch counters the NEXT arm asserts on,
        // so a silent timeout here surfaces as an inexplicable failure one arm later.
        assertThat(streams.close(Duration.ofSeconds(30)))
                .as("[%s] the client must shut down within the timeout, or its threads leak into the "
                        + "next arm's counters", name)
                .isTrue();
        return result;
    }

    /**
     * The thread-level {@code commit-total}, summed across threads.
     * <p>
     * {@code StreamThread} records this sensor only when {@code maybeCommit()} returns a non-zero count,
     * and it is a {@code CumulativeCount} - so it counts <b>commit rounds in which at least one task's
     * {@code commitNeeded()} answered true</b>, one per round, not the number of tasks committed. An
     * earlier version of this javadoc said it counted tasks.
     * <p>
     * Asserts the metric was actually found. Summing over an empty match set returns 0.0, which is
     * indistinguishable from "present and zero" - and every arm in this class has a zero-valued assertion
     * that a missing metric would satisfy for free.
     */
    private double commitTotal(final KafkaStreams streams) {
        double total = 0d;
        int matched = 0;
        for (Map.Entry<MetricName, ? extends Metric> entry : streams.metrics().entrySet()) {
            MetricName metricName = entry.getKey();
            if ("commit-total".equals(metricName.name())
                    && "stream-thread-metrics".equals(metricName.group())) {
                Object value = entry.getValue().metricValue();
                if (value instanceof Number) {
                    total += ((Number) value).doubleValue();
                    matched++;
                }
            }
        }
        assertThat(matched)
                .as("the commit-total sensor must exist, or a zero reading is an absent metric rather "
                        + "than an absent commit. One StreamThread is configured, so exactly one matches.")
                .isEqualTo(1);
        return total;
    }

    private void produceInput(final String inputTopic) {
        try (KafkaProducer<String, String> producer =
                     getKcu().createNewProducer(KafkaClientUtils.ProducerMode.NOT_TRANSACTIONAL)) {
            for (int i = 0; i < INPUT_RECORDS; i++) {
                producer.send(new ProducerRecord<>(inputTopic, "key-" + i, "value-" + i));
            }
            producer.flush();
        }
        log.info("Produced {} records into {}", INPUT_RECORDS, inputTopic);
    }

    private KafkaStreams startTopology(final String appId,
                                       final String inputTopic,
                                       final boolean withPunctuator) {
        StreamsBuilder builder = new StreamsBuilder();
        builder.<String, String>stream(inputTopic)
                .process((ProcessorSupplier<String, String, Void, Void>) () ->
                        new Processor<String, String, Void, Void>() {
                            @Override
                            public void init(final ProcessorContext<Void, Void> context) {
                                if (withPunctuator) {
                                    // WALL_CLOCK_TIME specifically: maybePunctuateSystemTime is untouched
                                    // by the patch and carries no warning, so this is the silent case.
                                    context.schedule(PUNCTUATE_INTERVAL,
                                            PunctuationType.WALL_CLOCK_TIME,
                                            timestamp -> punctuationsFired.incrementAndGet());
                                }
                            }

                            @Override
                            public void process(final Record<String, String> record) {
                                recordsProcessed.incrementAndGet();
                            }
                        });

        Properties props = baseStreamsProps(appId);
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, COMMIT_INTERVAL.toMillis());

        // Take the client down on an uncaught exception rather than carrying on with one thread fewer -
        // otherwise a dead StreamThread reads as a task that simply stopped committing.
        return startAndAwaitRunning(builder, props, LOG_AND_SHUT_DOWN_CLIENT);
    }

    /** What one arm measured. Plain fields: the Java 8 release target rules out a record. */
    private static final class ArmResult {

        private final double commitDeltaDuringWindow;

        private final double commitTotalAtEnd;

        private final OffsetAndMetadata committedOffsetAtEnd;

        private final int punctuationsDuringWindow;

        private ArmResult(final double commitDeltaDuringWindow,
                          final double commitTotalAtEnd,
                          final OffsetAndMetadata committedOffsetAtEnd,
                          final int punctuationsDuringWindow) {
            this.commitDeltaDuringWindow = commitDeltaDuringWindow;
            this.commitTotalAtEnd = commitTotalAtEnd;
            this.committedOffsetAtEnd = committedOffsetAtEnd;
            this.punctuationsDuringWindow = punctuationsDuringWindow;
        }
    }
}
