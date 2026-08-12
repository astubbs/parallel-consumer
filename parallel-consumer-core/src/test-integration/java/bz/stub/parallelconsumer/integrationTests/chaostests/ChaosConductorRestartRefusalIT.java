package bz.stub.parallelconsumer.integrationTests.chaostests;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions.CommitMode;
import bz.stub.parallelconsumer.ParallelConsumerOptions.ProcessingOrder;
import bz.stub.parallelconsumer.integrationTests.utils.ManagedPCInstance;
import bz.stub.parallelconsumer.integrationTests.utils.RecordingExecutor;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.google.common.truth.Truth.assertThat;

/**
 * The conductor half of the double-start fix. {@link ManagedPCInstance} refusing a second start is
 * only useful if {@link ChaosConductor#doRestart} honours the refusal - otherwise the conductor
 * records a RESTART that never happened and flips the instance to RUNNING, leaving a live PC that
 * teardown believes is stopped and will therefore never close. That is the orphaned group member
 * ({@code ZOMBIE_MEMBER/REBALANCE_BLOCKED}) half of the original failure.
 * <p>
 * Every other guard on this path is covered by {@code ManagedPCInstanceLifecycleIT}; this covers the
 * one line that connects them, so a "simplification" back to a bare {@code target.start(pcExecutor)}
 * fails here instead of silently in a chaos run months later.
 * <p>
 * Both tests drive the conductor's own actions in the order the failing run drew them -
 * STOP_NO_DRAIN, RESTART, STOP_NO_DRAIN, RESTART inside one close-wait - rather than claiming the
 * guard by hand, so the sequence under test is the one that actually occurs.
 * <p>
 * No broker, and deliberately NOT tagged {@code chaos}, so it gates each default integration build.
 */
class ChaosConductorRestartRefusalIT {

    /** Any roll works: pickInState indexes modulo the candidate list, and there is one candidate. */
    private static final int ANY_ROLL = 0;

    private static ManagedPCInstance newInstance() {
        ManagedPCInstance.Config config = ManagedPCInstance.Config.builder()
                .commitMode(CommitMode.PERIODIC_CONSUMER_SYNC)
                .order(ProcessingOrder.UNORDERED)
                .inputTopic("restart-refusal-topic")
                .build();
        return new ManagedPCInstance(config, null, key -> {
        });
    }

    /** Records what the probe would have been told, so a phantom RESTART is visible. */
    private static class RecordingObserver implements ChaosConductor.ChaosObserver {

        private final List<String> actions = new ArrayList<>();

        @Override
        public void onAction(int instanceId, ChaosConductor.ChaosAction action) {
            actions.add(action + "->" + instanceId);
        }
    }

    private static class Fixture {
        final ManagedPCInstance instance = newInstance();
        final RecordingExecutor executor = new RecordingExecutor();
        final RecordingObserver observer = new RecordingObserver();
        final ChaosConductor conductor = ChaosConductor.builder()
                .seed(1L)
                .maxFleetSize(4)
                .pcExecutor(executor)
                .initialFleet(Collections.singletonList(instance))
                .observer(observer)
                .build();

        ChaosConductor.InstanceState state() {
            return conductor.stateOf(instance.getInstanceId());
        }

        /**
         * Drive the draw sequence up to the second RESTART: stop, restart (whose run() stays queued
         * on the recording executor, holding the single-flight guard), then stop again. The instance
         * has no PC, so both stops are no-op closes - the guard interaction is what matters.
         */
        void upToTheSecondRestart() {
            conductor.doStopNoDrain(ANY_ROLL);
            conductor.doRestart(ANY_ROLL); // accepted: guard was free. run() is now queued.
            assertThat(state()).isEqualTo(ChaosConductor.InstanceState.RUNNING);
            conductor.doStopNoDrain(ANY_ROLL); // redrawn while that run() is still queued
            assertThat(state()).isEqualTo(ChaosConductor.InstanceState.STOPPED);
        }
    }

    private static long restartsIn(List<String> timeline) {
        return timeline.stream().filter(entry -> entry.contains("RESTART")).count();
    }

    /**
     * The second RESTART must leave the instance exactly as it was - STOPPED, undisturbed,
     * unannounced - rather than reporting a disturbance that never happened.
     */
    @Test
    void aRefusedRestartChangesNothing() {
        Fixture f = new Fixture();
        f.upToTheSecondRestart();

        int disturbancesBefore = f.conductor.getDisturbanceCount();
        int actionsBefore = f.observer.actions.size();
        int queuedBefore = f.executor.getTasks().size();
        long restartsBefore = restartsIn(f.conductor.getTimeline());

        f.conductor.doRestart(ANY_ROLL); // refused - the first restart's run() still holds the guard

        assertThat(f.state()).isEqualTo(ChaosConductor.InstanceState.STOPPED);
        assertThat(f.conductor.getDisturbanceCount()).isEqualTo(disturbancesBefore);
        assertThat(f.observer.actions).hasSize(actionsBefore); // no RESTART for the probe to explain
        assertThat(f.executor.getTasks()).hasSize(queuedBefore); // nothing double-submitted
        assertThat(restartsIn(f.conductor.getTimeline())).isEqualTo(restartsBefore);
    }

    /**
     * The other half of the contract: refusing must not strand the instance. Once the queued run()
     * aborts and releases the guard, a later RESTART is accepted - otherwise "leaves it STOPPED"
     * would be satisfied by an instance that never restarts again.
     */
    @Test
    void aRefusedInstanceRestartsOnceTheGuardIsReleased() {
        Fixture f = new Fixture();
        f.upToTheSecondRestart();
        f.conductor.doRestart(ANY_ROLL); // refused
        assertThat(f.state()).isEqualTo(ChaosConductor.InstanceState.STOPPED);

        // the queued run() finally executes: a stop was drawn while it waited, so it aborts without
        // building a PC (reaching the broker path would NPE on the null KafkaClientUtils, which
        // RecordingExecutor surfaces rather than swallowing) and releases the guard
        f.executor.runAll();

        f.conductor.doRestart(ANY_ROLL);

        assertThat(f.state()).isEqualTo(ChaosConductor.InstanceState.RUNNING);
        assertThat(f.observer.actions).contains("RESTART->" + f.instance.getInstanceId());
        assertThat(f.executor.getTasks()).hasSize(1); // the accepted restart, freshly queued
    }
}
