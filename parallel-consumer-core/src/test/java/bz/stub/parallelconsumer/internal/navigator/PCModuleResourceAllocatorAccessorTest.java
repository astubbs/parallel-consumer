package bz.stub.parallelconsumer.internal.navigator;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.internal.PCModule;
import bz.stub.parallelconsumer.internal.utils.LongPollingMockConsumer;
import org.junit.jupiter.api.Test;

import static org.apache.kafka.clients.consumer.OffsetResetStrategy.EARLIEST;
import static com.google.common.truth.Truth.assertThat;

/**
 * {@link PCModule#resourceAllocator()}'s memoised-accessor shape (U1): empty when no allocator is configured
 * (the R3 untouched path), present and identical across repeated reads when one is.
 */
class PCModuleResourceAllocatorAccessorTest {

    @Test
    void noAllocatorConfiguredReadsEmpty() {
        var module = new PCModule<>(optionsBuilder().build());

        assertThat(module.resourceAllocator().isPresent()).isFalse();
    }

    @Test
    void aConfiguredAllocatorIsReturnedAndMemoised() {
        var allocator = new StubResourceAllocator();
        var module = new PCModule<>(optionsBuilder().resourceAllocator(allocator).build());

        assertThat(module.resourceAllocator()).hasValue(allocator);
        // second read is the memoised value, not a fresh wrap
        assertThat(module.resourceAllocator()).hasValue(allocator);
    }

    private static ParallelConsumerOptions.ParallelConsumerOptionsBuilder<String, String> optionsBuilder() {
        return ParallelConsumerOptions.<String, String>builder()
                .consumer(new LongPollingMockConsumer<>(EARLIEST));
    }
}
