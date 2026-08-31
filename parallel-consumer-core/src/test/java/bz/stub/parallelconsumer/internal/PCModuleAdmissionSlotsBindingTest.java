package bz.stub.parallelconsumer.internal;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.ParallelConsumerOptions.AdaptiveConcurrencyMode;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

/**
 * {@link PCModule#isAdmissionSlotsCurrentlyBinding()} (U4/KTD6's slots-constrained marker): the pure read that
 * lets the navigator's attribution name {@code RESOURCE_AND_SLOTS_BLOCKED} when the engine-wide admission
 * target is ALSO occupying every slot, not just the tagged resource. Mirrors
 * {@link PCModule#admissionTargetSlots()}'s own state-derivation exactly, driven the same way
 * {@code AdaptiveConcurrencyCapabilityTest} drives {@code isAdaptiveConcurrencyActive()} - construct a real
 * processor, no control loop needed since this is a pure read over {@code adaptiveEnforcementActive()},
 * {@code getState()} and {@link UserFunctionTaskAccounting#getActive()}.
 * <p>
 * The full "resource AND slots block the SAME record in the SAME shard walk" case is proven at the seam level
 * only - this test proves the module-level signal; {@code NavigatorDecisionTest} proves the reason derivation
 * once both signals are in hand; wiring a live ProcessingShard scan under enforced adaptive concurrency with a
 * resource ALSO blocked needs a heavier control-loop harness than this unit adds - documented in the U4 report
 * per the plan's own escape hatch ("document precisely where the slots marker lands").
 */
class PCModuleAdmissionSlotsBindingTest {

    private TestParallelEoSStreamProcessor<String, String> pc;

    @AfterEach
    void tearDown() {
        if (pc != null) {
            pc.setState(State.CLOSED);
            pc.close();
            pc.workerThreadPool.get().shutdownNow();
        }
    }

    private PCModule<String, String> buildRunning(AdaptiveConcurrencyMode mode, int maxConcurrency) {
        var options = ParallelConsumerOptions.<String, String>builder()
                .consumer(new MockConsumer<>(OffsetResetStrategy.LATEST))
                .adaptiveConcurrencyMode(mode)
                .maxConcurrency(maxConcurrency)
                .useVirtualThreads(false)
                .build();
        var module = new PCModule<String, String>(options);
        pc = new TestParallelEoSStreamProcessor<>(options, module);
        pc.setState(State.RUNNING);
        return module;
    }

    @Test
    void notBindingWithNoActiveTasks() {
        var module = buildRunning(AdaptiveConcurrencyMode.ENFORCE, 4);

        assertWithMessage("zero active tasks against a positive target must never read as binding")
                .that(module.isAdmissionSlotsCurrentlyBinding()).isFalse();
    }

    @Test
    void bindingWhenActiveTasksReachTheTarget() {
        var module = buildRunning(AdaptiveConcurrencyMode.ENFORCE, 2);
        int target = module.admissionTargetSlots();

        for (int i = 0; i < target; i++) {
            pc.userFunctionTaskAccounting().onSubmitting();
            pc.userFunctionTaskAccounting().onTaskStarted();
        }

        assertWithMessage("every admission-target slot occupied by an active task must read as binding")
                .that(module.isAdmissionSlotsCurrentlyBinding()).isTrue();
    }

    @Test
    void notBindingWhenAdaptiveConcurrencyIsDisabled() {
        var module = buildRunning(AdaptiveConcurrencyMode.DISABLED, 1);
        // one task active against a target of 1 would bind under ENFORCE - but DISABLED must never read as binding
        pc.userFunctionTaskAccounting().onSubmitting();
        pc.userFunctionTaskAccounting().onTaskStarted();

        assertThat(module.isAdmissionSlotsCurrentlyBinding()).isFalse();
    }

    @Test
    void noAttachedProcessorReadsFalse() {
        var options = ParallelConsumerOptions.<String, String>builder()
                .consumer(new MockConsumer<>(OffsetResetStrategy.LATEST))
                .build();
        var bareModule = new PCModule<String, String>(options);

        assertWithMessage("a bare-module test env with no processor attached must never read as binding")
                .that(bareModule.isAdmissionSlotsCurrentlyBinding()).isFalse();
    }
}
