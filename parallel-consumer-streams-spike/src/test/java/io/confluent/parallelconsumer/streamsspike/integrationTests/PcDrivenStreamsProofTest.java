package io.confluent.parallelconsumer.streamsspike.integrationTests;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import io.confluent.parallelconsumer.streamsspike.PcDispatchCounters;
import io.confluent.parallelconsumer.streamsspike.PcDispatchSwitch;
import io.confluent.parallelconsumer.streamsspike.integrationTests.BaselineFixture.Row;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.kstream.ValueTransformerWithKeySupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * U6, the stateless proof: the same topology and the same records that stock Kafka Streams was fed in
 * {@code StockBaselineFixtureTest}, run instead through Parallel Consumer's {@code WorkManager} and a worker
 * pool - and asserted against what stock produced.
 * <p>
 * <b>The baseline is external on purpose</b> - see {@link BaselineFixture}.
 * {@link #thisJvmRunsThePatchedClassesWhileTheBaselineDidNot()} asserts the two halves of that claim from
 * this side; the fixture test asserts them from the other.
 * <p>
 * <b>The probe is only a partial answer to R9.</b> A stateless topology instantiates none of the store
 * wrappers that read the per-task record context ambiently, so the only ambient reader here is the probe this
 * spike wrote. {@link PcDrivenStatefulProofTest} is what makes Kafka's own ambient readers run.
 * <p>
 * {@code @Isolated} is load-bearing: {@link PcDispatchSwitch} is process-wide, so an arm running concurrently
 * with the flag-off arm here or with {@link ShadowedStreamsControlTest} would silently destroy the control.
 *
 * @author Antony Stubbs
 * @see ShadowedStreamsControlTest
 * @see PcDrivenStreamsDispatchTest
 * @see PcDrivenStatefulProofTest
 */
@Slf4j
@Isolated
class PcDrivenStreamsProofTest extends PcDrivenProofSupport {

    private static final String FIXTURE_RESOURCE = "/stock-baseline-fixture.tsv";

    private static final String SUFFIX = "-processed";

    /** Mirrors {@code StockBaselineFixtureTest.TOPOLOGY}; asserted against the fixture, not assumed. */
    private static final String TOPOLOGY = "stream -> mapValues((key, value) -> value + \"" + SUFFIX + "\") -> to";

    private static BaselineFixture fixture;

    private AmbientContextProbe probe;

    @BeforeAll
    static void loadFixture() throws IOException {
        fixture = BaselineFixture.load(FIXTURE_RESOURCE);
        log.info("Loaded stock baseline fixture: {} inputs, {} outputs, topology '{}'",
                fixture.getInputs().size(), fixture.getOutputs().size(), fixture.getTopology());
    }

    @BeforeEach
    void resetState() {
        PcDispatchCounters.reset();
        // No store name: a stateless topology has nowhere to write, which is precisely the R9 gap.
        probe = new AmbientContextProbe(fixture.getProbeHeader(), PROCESSING_COST.toMillis(), null);
    }

    /** Hand the JVM back at the artifact's default (on), so the next test states its own requirement. */
    @AfterEach
    void restoreDefaultDispatch() {
        PcDispatchSwitch.resetToDefault();
    }

    @Test
    void thisJvmRunsThePatchedClassesWhileTheBaselineDidNot() throws Exception {
        assertThisJvmRunsThePatchedClasses();

        assertThat(fixture.getTopology())
                .as("the fixture must have been generated from the same topology this test runs, or the two "
                        + "arms are not comparable however equal their output looks")
                .isEqualTo(TOPOLOGY);
        assertThat(fixture.getInputs()).as("the fixture must carry the inputs this test replays").isNotEmpty();
        assertThat(fixture.getOutputs())
                .as("stock produced one output per input; anything else means the baseline itself is broken")
                .hasSameSizeAs(fixture.getInputs());
    }

    /**
     * The proof, repeated. One green run of a concurrency experiment is a coin toss with the schedule; the
     * claim being made is that the seam holds, and that is a claim about repetition.
     */
    @RepeatedTest(3)
    void pcDrivenOutputMatchesTheStockBaseline() {
        PcDispatchSwitch.enable(POOL_SIZE);

        List<Row> outputs = runTopology("pc-proof");
        logEvidence("PC ON", probe);

        assertThat(PcDispatchCounters.getRecordsDispatchedToPool())
                .as("records must have reached the chain through PcTaskDispatcher's worker pool. Output "
                        + "equality alone would be satisfied by the stock path, so this counter is what makes "
                        + "the rest of this test mean anything.")
                .isGreaterThanOrEqualTo(fixture.getInputs().size());
        assertThat(PcDispatchCounters.getRecordsAcceptedByWorkManager())
                .as("and none may be dropped on the way in for want of a partition-assignment epoch - that "
                        + "drop is logged at warn and is otherwise silent")
                .isEqualTo(PcDispatchCounters.getRecordsOfferedToWorkManager());

        assertThat(probe.getPeakConcurrency())
                .as("with a pool of %s and a %s processor, at least 3 records must be inside the chain at "
                        + "once - a proof taken under serial dispatch would prove nothing about this seam",
                        POOL_SIZE, PROCESSING_COST)
                .isGreaterThanOrEqualTo(3);

        assertAmbientContextWasNeverCrossed(probe, fixture.getInputs().size());
        assertMatchesBaseline(outputs, fixture);
    }

    /**
     * The control for this test rather than for the spike: same harness, same patched classes, PC dispatch
     * off. It separates "the patch changed the output" from "parallel dispatch changed the output" - the two
     * diagnoses a failure of the arm above would otherwise be ambiguous between.
     */
    @Test
    void withDispatchOffTheSameHarnessStillMatchesTheStockBaseline() {
        // Explicit, and load-bearing: the seam defaults ON, so without this line this arm is not a control.
        PcDispatchSwitch.disable();

        List<Row> outputs = runTopology("pc-proof-off");
        logEvidence("PC OFF", probe);

        assertThat(PcDispatchCounters.getRecordsDispatchedToPool())
                .as("with the switch off no record may reach the worker pool")
                .isZero();
        assertThat(probe.getPeakConcurrency())
                .as("and the chain must be walked one record at a time, on the StreamThread")
                .isEqualTo(1);

        assertAmbientContextWasNeverCrossed(probe, fixture.getInputs().size());
        assertMatchesBaseline(outputs, fixture);
    }

    // ---------------------------------------------------------------------------------------------------

    private List<Row> runTopology(final String namePrefix) {
        String inputTopic = setupTopic(namePrefix + "-in");
        String outputTopic = setupTopic(namePrefix + "-out");
        ensureTopic(inputTopic, 1);
        ensureTopic(outputTopic, 1);

        probe.setExpectedTopic(inputTopic);

        // The probe node is value-, key- and order-transparent: it returns the value it was given and emits
        // nothing of its own, so its presence cannot change what reaches the sink. It is a reader, added
        // where the fixture topology has nothing, precisely so that the emitted records stay comparable.
        ValueTransformerWithKeySupplier<String, String, String> probeSupplier = this::newProbeTransformer;
        KafkaStreams streams = startTopology(namePrefix + "-" + System.nanoTime(), builder -> {
            KStream<String, String> stream = builder.stream(inputTopic);
            stream.transformValues(probeSupplier)
                    .mapValues((key, value) -> value + SUFFIX)
                    .to(outputTopic);
        });

        try {
            replayFixtureInputs(inputTopic, fixture, probe);
            return drain(outputTopic, fixture.getOutputs().size(), new Properties(), Function.identity());
        } finally {
            streams.close(Duration.ofSeconds(60));
        }
    }

    private ValueTransformerWithKey<String, String, String> newProbeTransformer() {
        return probe.newTransformer();
    }
}
