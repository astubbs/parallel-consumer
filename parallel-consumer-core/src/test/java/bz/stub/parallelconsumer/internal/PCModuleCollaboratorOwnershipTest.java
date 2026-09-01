package bz.stub.parallelconsumer.internal;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.state.ShardManager;
import bz.stub.parallelconsumer.state.WorkManager;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * A {@link PCModule} holds ONE PC instance's collaborator graph. Two of its getters memoise a collaborator while
 * taking that collaborator's owner as a parameter - {@link PCModule#shardManager(WorkManager)} and
 * {@link PCModule#brokerPoller(AbstractParallelEoSStreamProcessor)} - so on the cache-hit path the argument is
 * available to be ignored, and ignoring it hands a second owner a collaborator still wired to the first. Neither
 * failure surfaces where it is caused: the shard manager case leaves a {@code PartitionStateManager} and a shard
 * manager operating on different owners, and the poller case leaves a second processor polling for records that
 * are delivered to the first. Both are guarded, both getters are final so an override cannot take the guard away
 * with it, and this is where a new guard of that shape gets its test.
 */
class PCModuleCollaboratorOwnershipTest {

    private PCModuleTestEnv module() {
        return new PCModuleTestEnv(ParallelConsumerOptions.<String, String>builder().build());
    }

    @Test
    void theOwningWorkManagerGetsTheSameShardManagerBackOnEveryAsk() {
        var module = module();
        var wm = module.workManager();

        assertThat(module.shardManager(wm)).isSameInstanceAs(module.shardManager(wm));
    }

    @Test
    void aSecondWorkManagerOnOneModuleIsRejectedRatherThanGivenTheFirstsShardManager() {
        var module = module();
        var ignoredFirstOwner = module.workManager(); // memoises the shard manager against this one

        var thrown = assertThrows(IllegalStateException.class,
                () -> new WorkManager<>(module, module.dynamicExtraLoadFactor()));

        assertWithMessage("the failure must name what to do instead, since it fires deep inside a constructor")
                .that(thrown).hasMessageThat().contains("construct a second PCModule");
    }

    /**
     * The reason {@link PCModule#shardManager(WorkManager)} is final and substitution goes through
     * {@link PCModule#createShardManager(WorkManager)}: an override of the getter takes the guard with it, which
     * is exactly what {@code RegistrationRaceStaleResidentIT}'s pausable shard manager used to do.
     */
    @Test
    void substitutingTheShardManagerThroughTheFactorySeamKeepsTheGuard() {
        var substituted = new AtomicInteger();
        var module = new PCModuleTestEnv(ParallelConsumerOptions.<String, String>builder().build()) {
            @Override
            protected ShardManager<String, String> createShardManager(WorkManager<String, String> owner) {
                substituted.incrementAndGet();
                return new ShardManager<>(this, owner);
            }
        };
        var ignoredFirstOwner = module.workManager();

        var thrown = assertThrows(IllegalStateException.class,
                () -> new WorkManager<>(module, module.dynamicExtraLoadFactor()));

        assertWithMessage("the substituted factory must be what built the memoised instance")
                .that(substituted.get()).isEqualTo(1);
        assertThat(thrown).hasMessageThat().contains("construct a second PCModule");
    }

    @Test
    void theOwningProcessorGetsTheSameBrokerPollerBackOnEveryAsk() {
        var module = module();
        var pc = processorOn(module);

        assertThat(module.brokerPoller(pc)).isSameInstanceAs(module.brokerPoller(pc));
    }

    @Test
    void aSecondProcessorOnOneModuleIsRejectedRatherThanGivenTheFirstsBrokerPoller() {
        var module = module();
        var ignoredFirstOwner = module.brokerPoller(processorOn(module)); // memoises the poller against that one

        var thrown = assertThrows(IllegalStateException.class, () -> module.brokerPoller(processorOn(module)));

        assertWithMessage("the failure must name what to do instead - a second poller is a wiring mistake, "
                + "not a transient condition")
                .that(thrown).hasMessageThat().contains("construct a second PCModule");
    }

    /**
     * A mock rather than a real processor: the guard compares owner identity, and building a second real
     * {@link AbstractParallelEoSStreamProcessor} against one module is precisely what it forbids - so the real
     * thing cannot be used to reach the case under test. {@code getModule()} is stubbed because
     * {@link BrokerPollSystem}'s constructor resolves this module's metrics through it.
     */
    private AbstractParallelEoSStreamProcessor<String, String> processorOn(PCModuleTestEnv module) {
        @SuppressWarnings("unchecked")
        AbstractParallelEoSStreamProcessor<String, String> pc = Mockito.mock(AbstractParallelEoSStreamProcessor.class);
        Mockito.when(pc.getModule()).thenReturn(module);
        return pc;
    }
}
