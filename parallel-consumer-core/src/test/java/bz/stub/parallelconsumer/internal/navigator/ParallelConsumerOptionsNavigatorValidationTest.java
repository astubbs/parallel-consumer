package bz.stub.parallelconsumer.internal.navigator;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.internal.utils.LongPollingMockConsumer;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;

import static org.apache.kafka.clients.consumer.OffsetResetStrategy.EARLIEST;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@link ParallelConsumerOptions#validate()}'s navigator checks (U1's declaration-side fail-fast: R4, R19), plus
 * the untouched path (R3) proving nothing here changes {@code validate()}'s behaviour for an instance that never
 * mentions the navigator.
 */
class ParallelConsumerOptionsNavigatorValidationTest {

    private static final ResourceContract API_X = new ResourceContract("api-x", 2.0, 2, Duration.ofSeconds(1));

    /**
     * Covers R3. No tags, no allocator: {@code validate()} behaves exactly as it did before the navigator
     * surface existed - the untouched path the whole feature must not disturb.
     */
    @Test
    void noTagsAndNoAllocatorValidatesExactlyAsToday() {
        assertThatCode(() -> optionsWithConsumerOnly().validate()).doesNotThrowAnyException();
    }

    /**
     * Covers AE3, R4. Tagging a resource that was never registered with the supplied allocator must fail at
     * {@code validate()}, naming the unknown resource.
     */
    @Test
    void taggingAnUnregisteredResourceFailsNamingIt() {
        StubResourceAllocator allocator = new StubResourceAllocator();
        // deliberately does NOT register "api-y"

        var options = optionsBuilder()
                .resourceAllocator(allocator)
                .resourceTags(Collections.singletonList("api-y"))
                .build();

        assertThatThrownBy(options::validate)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("api-y");
    }

    /**
     * Covers AE3, R19. A tag against a resource that IS registered must validate cleanly.
     */
    @Test
    void taggingARegisteredResourceValidatesCleanly() {
        StubResourceAllocator allocator = new StubResourceAllocator();
        allocator.register(API_X);

        var options = optionsBuilder()
                .resourceAllocator(allocator)
                .resourceTags(Collections.singletonList("api-x"))
                .build();

        assertThatCode(options::validate).doesNotThrowAnyException();
    }

    /**
     * Covers AE3, R19. Tags present but no allocator supplied must fail at construction/validation - never a
     * silent no-op and never a runtime failure deep in the engine.
     */
    @Test
    void tagsWithNoAllocatorSuppliedFailsAtValidation() {
        var options = optionsBuilder()
                .resourceTags(Collections.singletonList("api-x"))
                .build();

        assertThatThrownBy(options::validate)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("resourceTags")
                .hasMessageContaining("resourceAllocator");
    }

    /**
     * An empty (but non-null) tag list, with no allocator, must not be treated as "tags without an allocator" -
     * it is the same untouched path as {@link #noTagsAndNoAllocatorValidatesExactlyAsToday()}.
     */
    @Test
    void emptyTagsWithNoAllocatorValidatesCleanly() {
        var options = optionsBuilder()
                .resourceTags(Collections.emptyList())
                .build();

        assertThatCode(options::validate).doesNotThrowAnyException();
    }

    /**
     * A duplicated tag would otherwise pass the unknown-resource check and then spend two credits per poll
     * against the same resource ({@link NavigatorParticipant#spendOneCreditPerTag} debits once per list entry,
     * not per distinct resource), silently halving this instance's effective rate. It must be rejected at
     * {@code validate()}, naming the duplicated tag.
     */
    @Test
    void duplicateTagFailsNamingIt() {
        StubResourceAllocator allocator = new StubResourceAllocator();
        allocator.register(API_X);

        var options = optionsBuilder()
                .resourceAllocator(allocator)
                .resourceTags(Arrays.asList("api-x", "api-x"))
                .build();

        assertThatThrownBy(options::validate)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("api-x");
    }

    /**
     * A null entry in {@link ParallelConsumerOptions#getResourceTags()} must fail with a named validation error,
     * not a bare {@link NullPointerException} inside the allocator's map.
     */
    @Test
    void nullTagEntryFailsWithNamedErrorNotNpe() {
        StubResourceAllocator allocator = new StubResourceAllocator();
        allocator.register(API_X);

        var options = optionsBuilder()
                .resourceAllocator(allocator)
                .resourceTags(Arrays.asList("api-x", null))
                .build();

        assertThatThrownBy(options::validate)
                .isInstanceOf(IllegalArgumentException.class)
                .isNotInstanceOf(NullPointerException.class)
                .hasMessageContaining("resourceTags");
    }

    /**
     * A blank entry is just as unresolvable as a null one, and must fail the same way.
     */
    @Test
    void blankTagEntryFails() {
        StubResourceAllocator allocator = new StubResourceAllocator();
        allocator.register(API_X);

        var options = optionsBuilder()
                .resourceAllocator(allocator)
                .resourceTags(Arrays.asList("api-x", "  "))
                .build();

        assertThatThrownBy(options::validate)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("resourceTags");
    }

    private static ParallelConsumerOptions.ParallelConsumerOptionsBuilder<String, String> optionsBuilder() {
        return ParallelConsumerOptions.<String, String>builder()
                .consumer(new LongPollingMockConsumer<>(EARLIEST));
    }

    private static ParallelConsumerOptions<String, String> optionsWithConsumerOnly() {
        return optionsBuilder().build();
    }
}
