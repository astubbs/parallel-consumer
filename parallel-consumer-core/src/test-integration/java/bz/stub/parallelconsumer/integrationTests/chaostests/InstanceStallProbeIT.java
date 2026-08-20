package bz.stub.parallelconsumer.integrationTests.chaostests;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

/**
 * Non-vacuity regression for {@link ProgressProbe}'s instance-progress detector
 * ({@code INSTANCE_STALL/NO_WORK_COMPLETED}, {@link ProgressProbe#INSTANCE_STALL_BOUND}) - in BOTH
 * directions: a detector that cannot fire is decoration, and one that fires on healthy instances
 * would be disabled within a week. Drives {@link ProgressProbe#sampleInstanceProgress} directly with
 * constructed views and explicit instants - pure replay, no broker, no sampler thread - and is
 * deliberately NOT tagged {@code chaos}, so it gates every default integration build (the
 * {@link ProgressProbeLedgerIT} / {@link KeyOrderLedgerIT} pattern).
 */
class InstanceStallProbeIT {

    private static final Duration BOUND = ProgressProbe.INSTANCE_STALL_BOUND;
    private static final Instant T0 = Instant.parse("2026-01-01T00:00:00Z");

    /** Mutable scripted view - the test flips its fields between samples. */
    private static final class FakeInstance implements ProgressProbe.InstanceProgressView {
        final int id;
        boolean live = true;
        long queued;
        long outForProcessing;
        long workResultsReturned;
        Object incarnation = new Object();

        FakeInstance(int id) {
            this.id = id;
        }

        @Override
        public int instanceId() {
            return id;
        }

        @Override
        public boolean isLive() {
            return live;
        }

        @Override
        public long queuedInShards() {
            return queued;
        }

        @Override
        public long outForProcessing() {
            return outForProcessing;
        }

        @Override
        public long workResultsReturned() {
            return workResultsReturned;
        }

        @Override
        public Object incarnationMarker() {
            return incarnation;
        }
    }

    /** A probe with only the instance-progress detector armed - the ctor's null kcu is legal because
     * the sampler thread is never started. */
    private static ProgressProbe probeWatching(FakeInstance... instances) {
        List<ProgressProbe.InstanceProgressView> views = new ArrayList<>(Arrays.asList(instances));
        return new ProgressProbe(null, "test-group", "test-topic", () -> 0L, 0)
                .withInstanceProgress(() -> views);
    }

    private static Instant pastBound(Instant from) {
        return from.plus(BOUND).plusSeconds(1);
    }

    @Test
    void firesWhenWorkIsHeldAndNothingCompletesPastTheBound() {
        FakeInstance instance = new FakeInstance(7);
        instance.queued = 132;
        instance.outForProcessing = 10;
        instance.workResultsReturned = 5_000;
        ProgressProbe probe = probeWatching(instance);

        probe.sampleInstanceProgress(T0);
        probe.sampleInstanceProgress(pastBound(T0));

        List<String> violations = probe.getViolations();
        assertWithMessage("held work + frozen completion count past the bound is the detector's whole prey")
                .that(violations).hasSize(1);
        assertThat(violations.get(0)).contains("INSTANCE_STALL/NO_WORK_COMPLETED");
        assertThat(violations.get(0)).contains("instance 7");
        assertThat(violations.get(0)).contains("queued=132");
    }

    @Test
    void silentWhileCompletionsAdvance() {
        FakeInstance instance = new FakeInstance(1);
        instance.queued = 500;
        instance.outForProcessing = 10;
        ProgressProbe probe = probeWatching(instance);

        // far more than one bound's worth of wall clock, but a result returns between samples every
        // time - the slow-but-progressing case CLASS2_STALL false-positives on, and the exact case
        // this detector must stay silent for
        Instant now = T0;
        for (int i = 0; i < 5; i++) {
            probe.sampleInstanceProgress(now);
            instance.workResultsReturned++;
            now = now.plus(Duration.ofSeconds(100));
        }
        probe.sampleInstanceProgress(now);

        assertWithMessage("an instance returning results is progressing, however slowly")
                .that(probe.getViolations()).isEmpty();
    }

    @Test
    void silentWhenNoWorkIsHeld() {
        FakeInstance instance = new FakeInstance(2);
        // nothing queued, nothing out: an idle instance's completion count legitimately never moves
        ProgressProbe probe = probeWatching(instance);

        probe.sampleInstanceProgress(T0);
        probe.sampleInstanceProgress(pastBound(T0));
        probe.sampleInstanceProgress(pastBound(pastBound(T0)));

        assertWithMessage("an idle instance is not a stalled instance")
                .that(probe.getViolations()).isEmpty();
    }

    @Test
    void silentWhenTheInstanceIsStopped() {
        FakeInstance instance = new FakeInstance(3);
        instance.queued = 40; // a stopping PC can still hold state - it must not be reported
        instance.live = false;
        ProgressProbe probe = probeWatching(instance);

        probe.sampleInstanceProgress(T0);
        probe.sampleInstanceProgress(pastBound(T0));

        assertWithMessage("the harness stops and restarts members constantly; a stopped instance is not a stall")
                .that(probe.getViolations()).isEmpty();
    }

    @Test
    void restartGrantsAFreshFullWindow() {
        FakeInstance instance = new FakeInstance(4);
        instance.queued = 40;
        ProgressProbe probe = probeWatching(instance);

        probe.sampleInstanceProgress(T0);
        Instant restartAt = T0.plus(Duration.ofSeconds(100));
        instance.incarnation = new Object(); // conductor restarted it: new PC, same instance id
        probe.sampleInstanceProgress(restartAt);

        // past the bound from T0, but only 51s into the new incarnation's window
        probe.sampleInstanceProgress(pastBound(T0));
        assertWithMessage("a fresh incarnation must not inherit the old PC's silence")
                .that(probe.getViolations()).isEmpty();

        // ...and the new incarnation is still covered: its own full window elapsing fires
        probe.sampleInstanceProgress(pastBound(restartAt));
        assertThat(probe.getViolations()).hasSize(1);
    }

    @Test
    void reArmsAfterFiringInsteadOfFiringEverySample() {
        FakeInstance instance = new FakeInstance(5);
        instance.outForProcessing = 3;
        ProgressProbe probe = probeWatching(instance);

        probe.sampleInstanceProgress(T0);
        Instant firstFire = pastBound(T0);
        probe.sampleInstanceProgress(firstFire);
        probe.sampleInstanceProgress(firstFire.plusSeconds(1)); // 1s later - must NOT double-report
        assertWithMessage("one violation per stalled window, not one per 1s sample")
                .that(probe.getViolations()).hasSize(1);

        // a further full window with still nothing returned is a further violation
        probe.sampleInstanceProgress(pastBound(firstFire));
        assertThat(probe.getViolations()).hasSize(2);
    }

    @Test
    void oneStalledInstanceIsNotHiddenByHealthySiblings() {
        // the granularity claim itself: the fleet-wide NO_PROGRESS watermark cannot see one wedged
        // member behind advancing siblings - this detector exists to
        FakeInstance healthy = new FakeInstance(10);
        healthy.queued = 100;
        FakeInstance wedged = new FakeInstance(11);
        wedged.queued = 100;
        ProgressProbe probe = probeWatching(healthy, wedged);

        Instant now = T0;
        for (int i = 0; i < 4; i++) {
            probe.sampleInstanceProgress(now);
            healthy.workResultsReturned++; // only the healthy sibling advances
            now = now.plus(Duration.ofSeconds(60));
        }

        List<String> violations = probe.getViolations();
        assertThat(violations).hasSize(1);
        assertThat(violations.get(0)).contains("instance 11");
    }
}
