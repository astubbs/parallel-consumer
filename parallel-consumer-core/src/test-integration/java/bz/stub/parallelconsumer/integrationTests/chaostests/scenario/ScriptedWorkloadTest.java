package bz.stub.parallelconsumer.integrationTests.chaostests.scenario;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The workload half of the framework: the levers that produce the conditions membership churn alone
 * cannot - a throughput ramp, induced failures, one key failing forever, and a slow user function.
 * <p>
 * Broker-free by construction: {@link WorkloadPublisher} is written against a {@link
 * WorkloadPublisher.RecordSink}, so the pacing and the rate changes - the parts worth testing - are
 * testable with a counter. The broker-backed sink is one lambda over the existing {@code KafkaClientUtils}
 * producer and is wired by whoever runs the scenario.
 */
class ScriptedWorkloadTest {

    // --- ScriptedFunction ---

    @Test
    void thePinnedKeyFailsEveryDeliveryAndTheRestPassThrough() {
        List<String> delivered = new ArrayList<>();
        ScriptedFunction function = new ScriptedFunction(delivered::add);
        function.setFailingKey("key-7");

        for (int i = 0; i < 3; i++) {
            assertThatThrownBy(() -> function.accept("key-7"))
                    .isInstanceOf(ScriptedFunction.InducedFailure.class)
                    .hasMessageContaining("pinned to fail");
        }
        function.accept("key-8");

        assertThat(delivered).containsExactly("key-8");
        assertThat(function.getPinnedKeyFailures().get()).isEqualTo(3);
        assertThat(function.getProcessed().get()).isEqualTo(1);
    }

    @Test
    void clearingThePinnedKeyLetsItThrough() {
        List<String> delivered = new ArrayList<>();
        ScriptedFunction function = new ScriptedFunction(delivered::add);
        function.setFailingKey("key-7");
        assertThatThrownBy(() -> function.accept("key-7")).isInstanceOf(ScriptedFunction.InducedFailure.class);

        function.setFailingKey(null);
        function.accept("key-7");

        assertThat(delivered).containsExactly("key-7");
    }

    /**
     * Failure selection is a pure function of the key, deliberately: a shared RNG consumed by many worker
     * threads would not replay, and key-derived failure is what actually strands a shard behind retry
     * backoff instead of skipping around at random.
     */
    @Test
    void theFailingProportionIsChosenByKeyAndIsStableAcrossDeliveries() {
        ScriptedFunction function = new ScriptedFunction(key -> {
        });
        function.setFailureProportion(0.5);

        List<Boolean> firstPass = outcomes(function, 200);
        List<Boolean> secondPass = outcomes(function, 200);

        assertWithMessage("the same key must fail (or not) identically on every delivery")
                .that(secondPass).isEqualTo(firstPass);
        long failed = firstPass.stream().filter(f -> f).count();
        assertWithMessage("about half of 200 keys should fall in a 0.5 proportion, got %s", failed)
                .that(failed > 60 && failed < 140).isTrue();
    }

    /**
     * The requested proportion must be the DELIVERED proportion. Consecutive workload keys hash to
     * consecutive integers, so without avalanche mixing in {@link ScriptedFunction#keyBucket} a requested
     * 0.5 delivered 0.05 - the lever would silently do almost nothing.
     */
    @Test
    void theDeliveredFailureRateTracksTheRequestedProportion() {
        ScriptedFunction function = new ScriptedFunction(key -> {
        });
        for (double requested : new double[]{0.1, 0.25, 0.5, 0.75}) {
            function.setFailureProportion(requested);
            long failed = outcomes(function, 2_000).stream().filter(f -> f).count();
            double delivered = failed / 2_000.0;
            assertWithMessage("requested %s, delivered %s over 2000 consecutive keys", requested, delivered)
                    .that(Math.abs(delivered - requested) < 0.05).isTrue();
        }
    }

    @Test
    void aZeroProportionFailsNothingAndAFullProportionFailsEverything() {
        ScriptedFunction function = new ScriptedFunction(key -> {
        });

        function.setFailureProportion(0);
        assertThat(outcomes(function, 50).stream().filter(f -> f).count()).isEqualTo(0);

        function.setFailureProportion(1);
        assertThat(outcomes(function, 50).stream().filter(f -> f).count()).isEqualTo(50);
    }

    @Test
    void anOutOfRangeProportionIsRejectedRatherThanClamped() {
        ScriptedFunction function = new ScriptedFunction(key -> {
        });
        assertThatThrownBy(() -> function.setFailureProportion(1.5))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("[0,1]");
    }

    @Test
    void theFunctionDelayIsApplied() {
        ScriptedFunction function = new ScriptedFunction(key -> {
        });
        function.setDelay(Duration.ofMillis(60));

        long start = System.nanoTime();
        function.accept("key-1");
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;

        assertThat(elapsedMs).isAtLeast(50L);
        assertThat(function.getDelay()).isEqualTo(Duration.ofMillis(60));
    }

    // --- WorkloadPublisher ---

    @Test
    void thePublisherPublishesAtRoughlyTheRequestedRateAndRespondsToAChange() throws Exception {
        AtomicLong sent = new AtomicLong();
        WorkloadPublisher publisher = new WorkloadPublisher((key, value) -> sent.incrementAndGet(),
                10, "key-", 200);
        publisher.start();
        try {
            Thread.sleep(400);
            long atLowRate = sent.get();
            assertWithMessage("200/s for ~400ms should send tens of records, sent %s", atLowRate)
                    .that(atLowRate > 10 && atLowRate < 400).isTrue();

            publisher.setRatePerSecond(0);
            Thread.sleep(200);
            long afterPause = sent.get();
            Thread.sleep(200);
            assertWithMessage("a zero rate pauses the producer without stopping it")
                    .that(sent.get()).isEqualTo(afterPause);

            publisher.setRatePerSecond(2_000);
            Thread.sleep(300);
            assertThat(sent.get()).isGreaterThan(afterPause);
        } finally {
            publisher.stop();
        }
        long atStop = sent.get();
        Thread.sleep(150);
        assertWithMessage("stop() must quiesce the producer").that(sent.get()).isEqualTo(atStop);
    }

    @Test
    void thePublisherCyclesOverItsKeySpaceSoWorkSpreadsAcrossShards() throws Exception {
        List<String> keys = new CopyOnWriteArrayList<>();
        WorkloadPublisher publisher = new WorkloadPublisher((key, value) -> keys.add(key), 4, "k-", 1_000);
        publisher.start();
        try {
            Thread.sleep(300);
        } finally {
            publisher.stop();
        }
        assertThat(keys.size()).isAtLeast(8);
        assertThat(new java.util.HashSet<>(keys)).containsExactly("k-0", "k-1", "k-2", "k-3");
    }

    @Test
    void aNegativeRateIsRejected() {
        assertThatThrownBy(() -> new WorkloadPublisher((k, v) -> {
        }, 4, "k-", -1)).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("negative");
    }

    // --- ScriptedWorkload and the actions over it ---

    @Test
    void workloadActionsDriveTheControlSurface() {
        ScriptedFunction function = new ScriptedFunction(key -> {
        });
        WorkloadPublisher publisher = new WorkloadPublisher((k, v) -> {
        }, 8, "key-", 100);
        ScriptedWorkload workload = new ScriptedWorkload(publisher, function);
        ScenarioContext context = context(workload);

        WorkloadActions.publishAt(750).apply(context, 0);
        assertThat(workload.getPublishRatePerSecond()).isEqualTo(750);

        WorkloadActions.scalePublishRate(2.0, 0, 1_000).apply(context, 0);
        assertWithMessage("the clamp must cap the ramp").that(workload.getPublishRatePerSecond()).isEqualTo(1_000);

        WorkloadActions.functionDelay(Duration.ofMillis(25)).apply(context, 0);
        assertThat(function.getDelay()).isEqualTo(Duration.ofMillis(25));

        WorkloadActions.failKeyRepeatedly("key-3").apply(context, 0);
        WorkloadActions.failProportion(0.25).apply(context, 0);
        assertThat(function.getFailingKey()).isEqualTo("key-3");
        assertThat(function.getFailureProportion()).isEqualTo(0.25);

        WorkloadActions.clearInducedFailures().apply(context, 0);
        assertThat(function.getFailingKey()).isNull();
        assertThat(function.getFailureProportion()).isEqualTo(0.0);
    }

    /** A half-wired workload must name the missing part, not silently do nothing. */
    @Test
    void aMissingWorkloadPartFailsByName() {
        ScriptedWorkload publisherOnly = new ScriptedWorkload(new WorkloadPublisher((k, v) -> {
        }, 2, "k-", 10), null);
        assertThatThrownBy(() -> publisherOnly.setFunctionDelay(Duration.ofMillis(5)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("scripted function");

        ScriptedWorkload functionOnly = new ScriptedWorkload(null, new ScriptedFunction(k -> {
        }));
        assertThatThrownBy(() -> functionOnly.setPublishRatePerSecond(5))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("publisher");
    }

    /** Parameterised actions carry their parameter in their name, so a timeline says what happened. */
    @Test
    void actionNamesCarryTheirParameters() {
        assertThat(WorkloadActions.publishAt(500).name()).isEqualTo("PUBLISH_AT(500/s)");
        assertThat(WorkloadActions.failKeyRepeatedly("key-9").name()).isEqualTo("FAIL_KEY(key-9)");
        assertThat(ScenarioActions.byName("FAIL_KEY(key-9)")).isPresent();
    }

    // --- helpers ---

    private static List<Boolean> outcomes(ScriptedFunction function, int keys) {
        List<Boolean> failed = new ArrayList<>();
        for (int i = 0; i < keys; i++) {
            try {
                function.accept("key-" + i);
                failed.add(false);
            } catch (ScriptedFunction.InducedFailure e) {
                failed.add(true);
            }
        }
        return failed;
    }

    private static ScenarioContext context(WorkloadControl workload) {
        return new ScenarioContext() {
            @Override
            public WorkloadControl workload() {
                return workload;
            }

            @Override
            public void record(String what, int instanceId) {
            }
        };
    }
}
