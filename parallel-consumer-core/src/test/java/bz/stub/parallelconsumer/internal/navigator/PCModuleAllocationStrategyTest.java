package bz.stub.parallelconsumer.internal.navigator;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.internal.PCModule;
import bz.stub.parallelconsumer.internal.PCModuleTestEnv;
import bz.stub.parallelconsumer.internal.utils.LongPollingMockConsumer;
import bz.stub.parallelconsumer.navigator.NavigatorView;
import bz.stub.parallelconsumer.navigator.PartitionShareResourceAllocator;
import bz.stub.parallelconsumer.navigator.ResourceAllocator;
import bz.stub.parallelconsumer.navigator.ResourceContract;
import bz.stub.parallelconsumer.navigator.StubResourceAllocator;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import pl.tlinkowski.unij.api.UniLists;

import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;

import static bz.stub.parallelconsumer.ParallelConsumerOptions.AllocationStrategy.CUSTOM;
import static bz.stub.parallelconsumer.ParallelConsumerOptions.AllocationStrategy.IN_PROCESS;
import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static org.apache.kafka.clients.consumer.OffsetResetStrategy.EARLIEST;

/**
 * The module's strategy resolution (the partition-share plan's U3, F4, KTD4): under the default strategy the
 * engine BUILDS a {@link PartitionShareResourceAllocator} through the module's protected seam and registers the
 * options-supplied contracts against it, so tags plus contracts work with no allocator code in the application;
 * under {@code IN_PROCESS} and {@code CUSTOM} the supplied instance is adopted exactly as before. Untagged
 * instances keep R3's zero-cost path whatever the strategy: no allocator built, the view inert, no navigator
 * meters registered.
 * <p>
 * Every module here is a bare {@link PCModule} or {@link PCModuleTestEnv} - no processor, no control loop -
 * because the seam under test is construction-time wiring.
 */
class PCModuleAllocationStrategyTest {

    private static final String API_X = "api-x";
    private static final String API_Y = "api-y";
    private static final ResourceContract POLICY_X = new ResourceContract(API_X, 2.0, 2, Duration.ofSeconds(1));
    private static final ResourceContract POLICY_Y = new ResourceContract(API_Y, 4.0, 1, Duration.ofSeconds(2));

    // ------------------------------------------------------------------
    // F4: tags + contracts, no strategy - the engine builds partition-share and registers the contracts
    // ------------------------------------------------------------------

    @Test
    void tagsAndContractsUnderTheDefaultStrategyBuildAPartitionShareAllocatorWithTheContractsRegistered() {
        var options = optionsBuilder()
                .resourceTags(UniLists.of(API_X, API_Y))
                .resourceContracts(UniLists.of(POLICY_X, POLICY_Y))
                .build();
        options.validate();
        var module = new PCModule<>(options);

        ResourceAllocator allocator = module.resourceAllocator().orElseThrow(
                () -> new AssertionError("the default strategy must build an allocator for a tagged instance"));
        assertThat(allocator).isInstanceOf(PartitionShareResourceAllocator.class);
        assertWithMessage("the options-supplied contracts are registered on the built allocator (R6)")
                .that(allocator.lookup(API_X)).hasValue(POLICY_X);
        assertThat(allocator.lookup(API_Y)).hasValue(POLICY_Y);

        var participant = module.navigatorParticipant();
        assertWithMessage("tags alone make the instance a navigator participant (KD6: nothing chosen silently, " +
                "nothing configured by default)").that(participant.isActive()).isTrue();
        assertThat(participant.resourceTags()).containsExactly(API_X, API_Y);
        assertThat(module.navigatorView().isActive()).isTrue();
        // the same instance every read - memoised, not rebuilt
        assertThat(module.resourceAllocator()).hasValue(allocator);
    }

    @Test
    void theInProcessStrategyAdoptsTheSuppliedInstance() {
        var supplied = new StubResourceAllocator();
        supplied.register(POLICY_X);
        var options = optionsBuilder()
                .resourceTags(UniLists.of(API_X))
                .allocationStrategy(IN_PROCESS)
                .resourceAllocator(supplied)
                .build();
        options.validate();
        var module = new PCModule<>(options);

        assertThat(module.resourceAllocator()).hasValue(supplied);
        assertThat(module.navigatorParticipant().isActive()).isTrue();
    }

    @Test
    void theCustomStrategyAdoptsTheSuppliedInstance() {
        ResourceAllocator supplied = Mockito.mock(ResourceAllocator.class);
        Mockito.when(supplied.lookup(API_X)).thenReturn(java.util.Optional.of(POLICY_X));
        var options = optionsBuilder()
                .resourceTags(UniLists.of(API_X))
                .allocationStrategy(CUSTOM)
                .resourceAllocator(supplied)
                .build();
        options.validate();
        var module = new PCModule<>(options);

        assertThat(module.resourceAllocator()).hasValue(supplied);
        assertThat(module.navigatorParticipant().isActive()).isTrue();
    }

    // ------------------------------------------------------------------
    // R3: the untouched path - no tags, no allocator, inert view, no meters
    // ------------------------------------------------------------------

    @Test
    void noTagsUnderTheDefaultStrategyBuildsNothingAndStaysInert() {
        var registry = new SimpleMeterRegistry();
        var options = optionsBuilder().meterRegistry(registry).build();
        options.validate();
        var module = new PCModule<>(options);

        assertWithMessage("an untagged instance builds no allocator (R3)")
                .that(module.resourceAllocator().isPresent()).isFalse();
        assertThat(module.navigatorParticipant().isActive()).isFalse();
        assertThat(module.navigatorView()).isSameInstanceAs(NavigatorView.inert());
        assertWithMessage("no navigator meter is registered for an untagged instance")
                .that(navigatorMeterNames(registry)).isEmpty();
        registry.close();
    }

    private static List<String> navigatorMeterNames(SimpleMeterRegistry registry) {
        return registry.getMeters().stream()
                .map(Meter::getId).map(Meter.Id::getName)
                .filter(name -> name.contains("navigator"))
                .collect(Collectors.toList());
    }

    private static ParallelConsumerOptions.ParallelConsumerOptionsBuilder<String, String> optionsBuilder() {
        return ParallelConsumerOptions.<String, String>builder()
                .consumer(new LongPollingMockConsumer<>(EARLIEST));
    }
}
