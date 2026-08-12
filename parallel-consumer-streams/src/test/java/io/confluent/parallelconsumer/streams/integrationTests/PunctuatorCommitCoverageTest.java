package io.confluent.parallelconsumer.streams.integrationTests;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import io.confluent.parallelconsumer.integrationTests.utils.KafkaClientUtils;
import io.confluent.parallelconsumer.streams.PcDispatchCounters;
import io.confluent.parallelconsumer.streams.PcDispatchSwitch;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.KafkaConsumer;
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

import pl.tlinkowski.unij.api.UniSets;

import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Punctuation does not make a task committable on the PC path, where on the stock path it does
 * (astubbs#255).
 * <p>
 * <b>The mechanism.</b> Stock sets {@code commitNeeded = true} in both punctuate paths -
 * {@code maybePunctuateStreamTime} and {@code maybePunctuateSystemTime}. The patch leaves both writes in
 * place and never reads them: {@code pcAwareCommitNeeded()} answers
 * {@code pcDispatcher.hasUncommittedWork()}, which is
 * {@code hasCommitDataOutstanding() || inFlight.get() > 0} - purely record-driven. So on the PC path the
 * field both punctuate paths set is discarded, and {@code TaskExecutor} - which gates
 * {@code prepareCommit()} and {@code postCommit()} on {@code task.commitNeeded()} - does neither.
 * <p>
 * <b>Why WALL_CLOCK_TIME is the case under test.</b> STREAM_TIME punctuators at least warn
 * ({@code pcStreamTimePunctuatorWarned}, U13). {@code maybePunctuateSystemTime} is byte-for-byte stock in
 * the patched file: a wall-clock punctuator gets no warning at all, and this divergence is silent.
 * <p>
 * <b>The window this bites in is a punctuate-only interval</b> - no records outstanding and none in
 * flight. Whenever there is record work, {@code hasUncommittedWork()} is true for that reason and the
 * punctuator's effects ride along on a commit that was happening anyway. So the arms below drain the
 * input first and then assert over an interval in which punctuation is the <em>only</em> thing happening.
 * <p>
 * <b>The observable is {@code commit-total}</b>, the thread-level sensor, because
 * {@code StreamThread} records it only when {@code maybeCommit()} returns a non-zero count, and that
 * count is exactly the number of tasks whose {@code commitNeeded()} answered true. It is the closest
 * thing to a direct read of the gate under test without asserting on the implementation.
 * <p>
 * <b>Two observables were rejected before this one, and both would have passed while proving nothing.</b>
 * The checkpoint file does not move: {@code StateManagerUtil.checkpointNeeded} requires a changelog delta
 * over {@code OFFSET_DELTA_THRESHOLD_FOR_CHECKPOINT} (10,000) unless the checkpoint is enforced, so
 * neither arm would write one after a handful of punctuations. Punctuator output on the output topic does
 * not disappear either: without EOS the producer sends it on its own schedule whether or not
 * {@code flush()} is ever called. Recorded here because a later reader will reach for both.
 *
 * <h2>What the diagnostics turned up, and why it outranks the punctuator question</h2>
 * The arms log dispatch counters and the group's committed offset as well as the metric, because
 * {@code commit-total} alone cannot tell "the PC path declined to commit" apart from "the seam was never
 * dispatching". Measured:
 * <ul>
 *   <li><b>The seam was dispatching</b> on the PC arms - offered=5 accepted=5 dispatched=5 completed=5,
 *       and zero on the stock arms. So the PC arms are genuinely the PC path.</li>
 *   <li><b>At the drain, every arm reads {@code commit-total=0} and no committed offset at all.</b> The
 *       commits come later, which is why the settle exists.</li>
 *   <li><b>The PC arms end with the input partition committed at offset 5 while {@code commit-total}
 *       never leaves zero.</b> Offsets reach the broker on the PC path by a route that does not go
 *       through {@code StreamThread.maybeCommit()}, whose sensor only records when
 *       {@code taskManager.commit(...)} returns a non-zero count.</li>
 * </ul>
 * <b>That route is not yet identified, and this class does not claim one.</b> The dispatcher runs no PC
 * control loop - it holds a {@code WorkManager} and a stub {@code Consumer} - so PC is not committing on
 * its own behalf. The remaining candidates are the {@code TaskManager} paths that commit without the
 * thread sensor: {@code handleCorruption}, {@code handleRevocation}, and the close/suspend path.
 * Narrowing further needs debug logging this module's two test-classpath logging bindings currently
 * defeat.
 * <p>
 * <b>The consequence for the punctuator work.</b> If {@code commitNeeded()} is not what drives offset
 * commits on the PC path, then the handover's framing - punctuator effects "never become commit-covered"
 * - is at best imprecise, and the one-line {@code || commitNeeded} candidate cannot be evidenced by
 * measuring commit cadence until the actual route is known.
 *
 * @author Antony Stubbs
 * @see io.confluent.parallelconsumer.streams.PcTaskDispatcher#hasUncommittedWork()
 */
@Slf4j
// PcDispatchSwitch is process-wide; a concurrent test flipping it would change which dispatch path these
// arms measure.
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
     * finish, before the measured window opens. Several COMMIT_INTERVALs.
     * <p>
     * <b>This was a wait-until-commit-total-stops-moving loop, and that loop could never terminate on the
     * stock arm</b> - which is the divergence itself, arriving early: with a punctuator registered, stock
     * commits every interval indefinitely, so "flat" is a state only the PC path ever reaches. A fixed
     * settle measures both arms the same way instead of encoding one arm's behaviour into the fixture.
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
     * The defect. A wall-clock punctuator fires repeatedly over an idle task and the thread never commits,
     * because the only thing that set {@code commitNeeded} was punctuation and nothing on the PC path
     * reads it.
     */
    @Test
    void pcPathDoesNotCommitForPunctuationAlone() {
        PcDispatchSwitch.enable(POOL_SIZE);

        double committedDuringIdle = runArm("pc-punctuator", true);

        assertThat(committedDuringIdle)
                .as("PC path: %s punctuations fired over an otherwise idle task and the thread committed "
                        + "%s times. Both punctuate paths set commitNeeded, but pcAwareCommitNeeded() "
                        + "answers hasUncommittedWork() - records only - so TaskExecutor's "
                        + "commitNeeded() gate is false and neither prepareCommit() (which is where "
                        + "flush() lives) nor postCommit() (which is where maybeCheckpoint() lives) runs. "
                        + "The stock arm below is the same topology with the seam off.",
                        punctuationsFired.get(), committedDuringIdle)
                .isZero();
    }

    /**
     * The control that makes the arm above mean something: the same topology, the same idle window, the
     * seam <b>off</b>. Stock reads {@code commitNeeded}, so punctuation alone drives commits here.
     */
    @Test
    void stockPathCommitsForPunctuationAlone() {
        PcDispatchSwitch.disable();

        double committedDuringIdle = runArm("stock-punctuator", true);

        assertThat(committedDuringIdle)
                .as("STOCK CONTROL: %s punctuations over an idle task must drive commits, because "
                        + "maybePunctuateSystemTime sets commitNeeded and stock's commitNeeded() reads "
                        + "it. If this is zero the experiment is void - the idle window would not be "
                        + "showing a PC-specific divergence at all.",
                        punctuationsFired.get())
                .isGreaterThan(0);
    }

    /**
     * The second discriminator, and the one that decides whether the PC arm above means anything.
     * <p>
     * The PC arm's {@code commit-total} is zero <b>for the whole run</b>, not merely across the idle
     * window - so the obvious rival explanation is that {@code StreamThread}'s commit path simply never
     * fires on the PC path, punctuator or no punctuator, and the PC arm would read zero whatever this
     * class did. If that is so, this arm reads zero too and the PC arm attributes to punctuation
     * something punctuation had no part in.
     * <p>
     * <b>Read this arm's result before quoting the PC arm's.</b> A zero here does not falsify the
     * mechanism - which is established by reading the patched source, and by the stock arms below - but
     * it does mean {@code commit-total} cannot be the thing that demonstrates it, and the PC arm is then
     * a regression pin rather than a proof.
     */
    @Test
    void pcPathWithoutAPunctuatorAlsoDoesNotCommitWhenIdle() {
        PcDispatchSwitch.enable(POOL_SIZE);

        double committedDuringIdle = runArm("pc-no-punctuator", false);

        log.info("=== PC WITHOUT A PUNCTUATOR committed {} times during the idle window", committedDuringIdle);
        assertThat(committedDuringIdle)
                .as("recorded, not a claim: if this is zero then the PC path's commit-total is zero "
                        + "whether or not a punctuator is registered, and pcPathDoesNotCommitForPunctuationAlone "
                        + "is pinning the PC path's commit behaviour generally rather than isolating "
                        + "punctuation. See this method's javadoc.")
                .isZero();
    }

    /**
     * The first discriminator: it keeps the stock control honest.
     * <p>
     * Stock's {@code commitNeeded()} override also sweeps the consumer position, which can set
     * {@code commitNeeded} for reasons that have nothing to do with punctuation - control records, most
     * obviously. Without this arm, the stock control above would be satisfied by that sweep and the
     * comparison would attribute to punctuation something punctuation did not cause. Same seam-off
     * topology, same idle window, <b>no punctuator</b>: commits must be absent here.
     */
    @Test
    void stockPathWithoutAPunctuatorDoesNotCommitWhenIdle() {
        PcDispatchSwitch.disable();

        double committedDuringIdle = runArm("stock-no-punctuator", false);

        assertThat(committedDuringIdle)
                .as("DISCRIMINATOR: with no punctuator registered, an idle stock task must not commit. A "
                        + "non-zero value here means the stock control arm's commits come from the "
                        + "consumer-position sweep rather than from punctuation, and the whole "
                        + "comparison is confounded.")
                .isZero();
    }

    /**
     * Drains the input, settles for a fixed period so the drain's own commits are done, then measures
     * {@code commit-total} across a fixed window in which punctuation is the only activity. Every arm gets
     * the same settle and the same window, so the arms are comparable.
     *
     * @param withPunctuator whether the processor registers a WALL_CLOCK_TIME punctuator
     * @return how much {@code commit-total} rose during the punctuate-only window
     */
    private double runArm(final String name, final boolean withPunctuator) {
        String inputTopic = setupTopic(name + "-in");
        ensureTopic(inputTopic, 1);
        String appId = name + "-" + System.nanoTime();
        TopicPartition inputPartition = new TopicPartition(inputTopic, 0);

        produceInput(inputTopic);

        KafkaStreams streams = startTopology(appId, inputTopic, withPunctuator);
        try {
            // Phase 1: drain. Until every record has been processed there is record work outstanding, and
            // hasUncommittedWork() would be true for that reason rather than for punctuation.
            await().atMost(Duration.ofSeconds(60))
                    .until(() -> recordsProcessed.get() >= INPUT_RECORDS);
            log.info("=== [{}] all {} records processed", name, INPUT_RECORDS);
            // Sampled before the settle, because when the commit lands decides which route committed it:
            // a commit already present here, with commit-total still at zero, cannot have come from
            // StreamThread.maybeCommit and points at a TaskManager path (revocation/corruption/close).
            log.info("=== [{}] at drain: commit-total={} committed={}",
                    name, commitTotal(streams), committedOffsetOrNull(appId, inputPartition));

            // Phase 2: let the drain's own record-driven commits finish, so the measured window is not
            // reading their tail. Fixed, not a converge-loop - see SETTLE.
            sleepThrough(SETTLE);

            double before = commitTotal(streams);
            int punctuationsBefore = punctuationsFired.get();

            // Phase 3: the punctuate-only window. No new input; nothing in flight; nothing outstanding.
            //
            // A FIXED window, identical for every arm, rather than "await N punctuations, then sleep".
            // That earlier shape gave the punctuator arms a longer window than the no-punctuator arms -
            // and the no-punctuator arms are the ones asserting ZERO, so the asymmetry handed them a
            // shorter observation period in which to find the commits whose absence is their whole claim.
            // A discriminator biased toward the result it asserts is not a discriminator.
            sleepThrough(IDLE_WINDOW);
            int punctuationsDuring = punctuationsFired.get() - punctuationsBefore;

            // The count the await used to guarantee is now checked rather than waited for: at one
            // punctuation per PUNCTUATE_INTERVAL, IDLE_WINDOW holds many times PUNCTUATIONS_AWAITED, so a
            // shortfall means punctuation is not running and the arm's premise is void.
            if (withPunctuator) {
                assertThat(punctuationsDuring)
                        .as("premise of the %s arm: the punctuator must actually fire during the measured "
                                + "window, or a zero commit count says nothing about punctuation", name)
                        .isGreaterThanOrEqualTo(PUNCTUATIONS_AWAITED);
            }

            double after = commitTotal(streams);
            log.info("=== [{}] idle window: commit-total {} -> {} over {} punctuations",
                    name, before, after, punctuationsFired.get() - punctuationsBefore);

            // Diagnostics, not assertions. commit-total alone cannot distinguish "the PC path decided not
            // to commit" from "the seam was never dispatching in the first place", and the two have
            // opposite consequences for what this class demonstrates. The counters answer the first
            // question and the group's committed offset answers the second.
            log.info("=== [{}] dispatch counters: offered={} accepted={} dispatched={} completed={} failed={}",
                    name,
                    PcDispatchCounters.getRecordsOfferedToWorkManager(),
                    PcDispatchCounters.getRecordsAcceptedByWorkManager(),
                    PcDispatchCounters.getRecordsDispatchedToPool(),
                    PcDispatchCounters.getRecordsCompletedSuccessfully(),
                    PcDispatchCounters.getRecordsFailed());
            log.info("=== [{}] committed offset for {}: {}",
                    name, inputPartition, committedOffsetOrNull(appId, inputPartition));

            return after - before;
        } finally {
            streams.close(Duration.ofSeconds(30));
        }
    }

    /**
     * The thread-level {@code commit-total} summed across threads. {@code StreamThread} records this
     * sensor only when {@code maybeCommit()} returned a non-zero count, and that count is the number of
     * tasks whose {@code commitNeeded()} answered true - which is the gate under test.
     */
    private static double commitTotal(final KafkaStreams streams) {
        double total = 0d;
        for (Map.Entry<MetricName, ? extends Metric> entry : streams.metrics().entrySet()) {
            MetricName metricName = entry.getKey();
            if ("commit-total".equals(metricName.name())
                    && "stream-thread-metrics".equals(metricName.group())) {
                Object value = entry.getValue().metricValue();
                if (value instanceof Number) {
                    total += ((Number) value).doubleValue();
                }
            }
        }
        return total;
    }

    /**
     * The application's own committed offset for its input partition, or {@code null} if the group has
     * never committed. The reader carries the app's group id but never subscribes, so it performs an
     * OffsetFetch without joining and cannot rebalance the topology under test - the same trick
     * {@code CommitFrontierCrashRestartTest} uses.
     */
    private OffsetAndMetadata committedOffsetOrNull(final String appId, final TopicPartition inputPartition) {
        try (KafkaConsumer<String, String> groupReader = getKcu().createNewConsumer(appId)) {
            return groupReader.committed(UniSets.of(inputPartition)).get(inputPartition);
        }
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

        return startAndAwaitRunning(builder, props);
    }

    private static void sleepThrough(final Duration duration) {
        try {
            Thread.sleep(duration.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted during the idle window", e);
        }
    }
}
