package bz.stub.parallelconsumer.state;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.PCRetriableException;
import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.internal.PCModuleTestEnv;
import bz.stub.parallelconsumer.metrics.PCMetricsDef;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import pl.tlinkowski.unij.api.UniLists;

import java.time.Duration;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

/**
 * The two parked gauges belong to the partition's own metric lifecycle, beside the six offset gauges
 * {@link PartitionState#initMetrics} already registers: they appear when the partition is assigned and go when it
 * is revoked, rather than being kept in step by anything watching the control loop.
 * <p>
 * <b>Why that placement is the contract and not an implementation detail.</b> Park is an engine feature -
 * {@link PCRetriableException#park(String)} - so every user of this library can produce a parked record, and a
 * gauge registered by a facade would leave the classic API with no way to see one. Registering at the source also
 * removes the only reason a facade needed a loop-end pass at all: the assignment changes on a rebalance, which is
 * exactly the event {@code onPartitionsAssigned} / {@code onPartitionsRemoved} already deliver.
 *
 * @see PartitionState#initMetrics()
 * @see ShardManager#getParkedWorkContainers(boolean)
 */
@Slf4j
class PartitionParkedGaugesTest {

    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();

    private final String topic = "topic";
    private final TopicPartition tp = new TopicPartition(topic, 0);

    private PCModuleTestEnv module;
    private WorkManager<String, String> wm;

    /**
     * Built here rather than through {@code BrokerlessWorkManagerTestBase} because these tests need a real
     * {@link SimpleMeterRegistry} in the options, and that base deliberately offers no seam for options - its
     * whole point is one fixture the subclasses agree on.
     */
    @BeforeEach
    void buildAModuleReportingToARealRegistry() {
        module = new PCModuleTestEnv(ParallelConsumerOptions.<String, String>builder()
                .meterRegistry(registry)
                .build());
        wm = module.workManager();
    }

    @Test
    void aGaugePairAppearsOnAssignmentAndIsGoneAfterRevocation() {
        assertWithMessage("FIXTURE: nothing is assigned yet, so neither gauge may exist")
                .that(parkedCountGauge()).isNull();

        wm.onPartitionsAssigned(UniLists.of(tp));

        assertWithMessage("the count gauge is registered by the assignment, tagged with its own partition")
                .that(parkedCountGauge()).isNotNull();
        assertWithMessage("and so is the oldest-age gauge - R19 asks for both and they are not the same figure")
                .that(oldestAgeGauge()).isNotNull();

        wm.onPartitionsRevoked(UniLists.of(tp));

        assertWithMessage("a gauge left behind after revocation reports zero for a partition somebody else now owns")
                .that(parkedCountGauge()).isNull();
        assertWithMessage("both halves of the pair go, not just the one the revocation path happened to name")
                .that(oldestAgeGauge()).isNull();
    }

    @Test
    void theCountFollowsAParkAndThenTheRebalance() {
        WorkContainer<String, String> parked = aRecordParkedInPlace();

        assertWithMessage("FIXTURE: the park must have reached the retry queue, or this asserts nothing")
                .that(wm.getSm().getRetryQueue().contains(parked)).isTrue();
        assertThat(parkedCountGauge().value()).isEqualTo(1d);

        wm.onPartitionsRevoked(UniLists.of(tp));
        assertWithMessage("the gauge goes with the partition, so there is nothing left to report a stale count")
                .that(parkedCountGauge()).isNull();

        wm.onPartitionsAssigned(UniLists.of(tp));
        assertWithMessage("the partition came back under a new epoch, so the container left in the retry queue is "
                + "stale and is not this generation's to report")
                .that(parkedCountGauge().value()).isEqualTo(0d);
    }

    @Test
    void theOldestAgeIsZeroWithNothingParkedAndIsTheParkedRecordsAgeOnceOneIs() {
        wm.onPartitionsAssigned(UniLists.of(tp));

        assertWithMessage("zero rather than NaN with nothing parked: a gauge that vanishes when the good news "
                + "arrives reads as a broken exporter")
                .that(oldestAgeGauge().value()).isEqualTo(0d);

        var ignoredParked = aRecordParkedInPlace(); // the container itself is not read here - the gauge is
        module.getMutableClock().add(Duration.ofSeconds(30));

        assertThat(oldestAgeGauge().value()).isEqualTo(30d);
    }

    /**
     * Assigns the partition, registers one record, takes it, and fails it with a hand-back that parks it - the
     * state both gauges exist to report.
     */
    private WorkContainer<String, String> aRecordParkedInPlace() {
        WorkContainer<String, String> wc = ModelUtils.registerOneRecordAndTakeIt(wm, tp);
        wc.onUserFunctionFailure(new PCRetriableException("deliberate - the test needs a parked record")
                .park("waiting for an operator to resume it"));
        wm.handleFutureResult(wc);

        assertWithMessage("FIXTURE: the hand-back must actually have parked it")
                .that(wc.isParked()).isTrue();
        return wc;
    }

    private Gauge parkedCountGauge() {
        return gaugeFor(PCMetricsDef.PARTITION_PARKED_RECORDS);
    }

    private Gauge oldestAgeGauge() {
        return gaugeFor(PCMetricsDef.PARTITION_PARKED_OLDEST_AGE);
    }

    private Gauge gaugeFor(PCMetricsDef def) {
        return registry.find(def.getName())
                .tags("topic", tp.topic(), "partition", String.valueOf(tp.partition()))
                .gauge();
    }
}
