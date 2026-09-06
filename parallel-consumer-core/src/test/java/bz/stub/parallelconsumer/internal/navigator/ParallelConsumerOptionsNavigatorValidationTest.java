package bz.stub.parallelconsumer.internal.navigator;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.ParallelConsumerOptions.AllocationStrategy;
import bz.stub.parallelconsumer.internal.utils.LongPollingMockConsumer;
import bz.stub.parallelconsumer.navigator.ResourceAllocator;
import bz.stub.parallelconsumer.navigator.ResourceContract;
import bz.stub.parallelconsumer.navigator.StubResourceAllocator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static bz.stub.parallelconsumer.ParallelConsumerOptions.AllocationStrategy.CUSTOM;
import static bz.stub.parallelconsumer.ParallelConsumerOptions.AllocationStrategy.IN_PROCESS;
import static bz.stub.parallelconsumer.ParallelConsumerOptions.AllocationStrategy.PARTITION_SHARE;
import static org.apache.kafka.clients.consumer.OffsetResetStrategy.EARLIEST;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@link ParallelConsumerOptions#validate()}'s navigator checks (U1's declaration-side fail-fast: R4, R19), plus
 * the untouched path (R3) proving nothing here changes {@code validate()}'s behaviour for an instance that never
 * mentions the navigator.
 * <p>
 * The partition-share rung added the {@link AllocationStrategy} menu and the {@code resourceContracts} field
 * (KD6, KD10, R6): {@link #theStrategyMatrixValidatesAsThePlanTableStates} enumerates every combination of
 * strategy x allocator-present x contracts-present x tags-present (AE6), and the tests after it cover the cells
 * the matrix's one fixed allocator cannot - the contract collision, the convenience registration, and the
 * unusable policy under partition-share.
 */
class ParallelConsumerOptionsNavigatorValidationTest {

    private static final ResourceContract API_X = new ResourceContract("api-x", 2.0, 2, Duration.ofSeconds(1));

    /** Same name as {@link #API_X}, different policy - the R19 collision. */
    private static final ResourceContract API_X_DIFFERENT_POLICY =
            new ResourceContract("api-x", 5.0, 2, Duration.ofSeconds(1));

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
                .allocationStrategy(IN_PROCESS)
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
                .allocationStrategy(IN_PROCESS)
                .resourceAllocator(allocator)
                .resourceTags(Collections.singletonList("api-x"))
                .build();

        assertThatCode(options::validate).doesNotThrowAnyException();
    }

    /**
     * Covers AE3, R6, R19. Tags present, nothing else: the strategy defaults to partition-share, so the tag is
     * resolved against {@code resourceContracts} - empty here - and fails naming the tag and the field that
     * would have declared it. Never a silent no-op and never a runtime failure deep in the engine. (Before the
     * strategy menu this cell failed naming the missing allocator; under KD6 the allocator is the engine's to
     * build, so the contracts are what the user left out.)
     */
    @Test
    void tagsWithNoContractsUnderTheDefaultStrategyFailsNamingTheTagAndTheContractsField() {
        var options = optionsBuilder()
                .resourceTags(Collections.singletonList("api-x"))
                .build();

        assertThatThrownBy(options::validate)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("resourceTags")
                .hasMessageContaining("api-x")
                .hasMessageContaining("resourceContracts");
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
                .allocationStrategy(IN_PROCESS)
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
                .allocationStrategy(IN_PROCESS)
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
                .allocationStrategy(IN_PROCESS)
                .resourceAllocator(allocator)
                .resourceTags(Arrays.asList("api-x", "  "))
                .build();

        assertThatThrownBy(options::validate)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("resourceTags");
    }

    // ------------------------------------------------------------------
    // The strategy matrix (AE6, R6, KD6, KD10) - strategy x allocator x contracts x tags
    // ------------------------------------------------------------------

    /**
     * One row of the validation-matrix table in the partition-share plan, transcribed: the four inputs and
     * either "validates" or the substrings the failure message must name (R6's "naming both fields").
     */
    private static final class Cell {
        final AllocationStrategy strategyOrNull;
        final boolean allocatorPresent;
        final boolean contractsPresent;
        final boolean tagsPresent;
        /** Empty means the cell validates; otherwise every entry must appear in the failure message. */
        final List<String> failureMustName;

        Cell(AllocationStrategy strategyOrNull, boolean allocatorPresent, boolean contractsPresent,
             boolean tagsPresent, List<String> failureMustName) {
            this.strategyOrNull = strategyOrNull;
            this.allocatorPresent = allocatorPresent;
            this.contractsPresent = contractsPresent;
            this.tagsPresent = tagsPresent;
            this.failureMustName = failureMustName;
        }

        @Override
        public String toString() {
            return "strategy=" + (strategyOrNull == null ? "absent" : strategyOrNull)
                    + " allocator=" + allocatorPresent + " contracts=" + contractsPresent + " tags=" + tagsPresent
                    + (failureMustName.isEmpty() ? " -> validates" : " -> fails naming " + failureMustName);
        }
    }

    /**
     * Every combination, with the expected outcome taken row by row from the plan's validation-matrix table -
     * NOT re-derived from the code under test. {@code null} strategy is the builder default left alone.
     */
    static Stream<Arguments> strategyMatrix() {
        List<Cell> cells = new ArrayList<>();
        AllocationStrategy[] strategies = {null, PARTITION_SHARE, IN_PROCESS, CUSTOM};
        for (AllocationStrategy strategy : strategies) {
            for (boolean allocator : new boolean[]{false, true}) {
                for (boolean contracts : new boolean[]{false, true}) {
                    for (boolean tags : new boolean[]{false, true}) {
                        cells.add(new Cell(strategy, allocator, contracts, tags,
                                outcomeFromThePlanTable(strategy, allocator, contracts, tags)));
                    }
                }
            }
        }
        return cells.stream().map(Arguments::of);
    }

    private static List<String> outcomeFromThePlanTable(AllocationStrategy strategyOrNull, boolean allocator,
                                                       boolean contracts, boolean tags) {
        boolean partitionShare = strategyOrNull == null || strategyOrNull == PARTITION_SHARE;
        if (partitionShare) {
            if (allocator) {
                // rows 2 and 3: absent-or-explicit partition-share with an instance fails naming BOTH fields -
                // and the reason, which only the KD10 message carries (the unknown-tag remedy also names both
                // fields, so without the phrase the tags-no-contracts cells would pass through the wrong check)
                return Arrays.asList("allocationStrategy", "resourceAllocator", "silently unused");
            }
            if (tags && !contracts) {
                // row 1's R7 clause: a tag nothing declares fails naming the tag, as it always has
                return Arrays.asList("resourceTags", "api-x", "resourceContracts");
            }
            return Collections.emptyList(); // row 1: partition-share; contracts, if any, accepted
        }
        if (!allocator) {
            // row 4: the strategy needs an instance it was not given
            return Arrays.asList("allocationStrategy", strategyOrNull.name(), "resourceAllocator");
        }
        return Collections.emptyList(); // row 5: that instance; the identical contract reconciles cleanly
    }

    /**
     * Covers AE6, R6 (KD6, KD10). The whole matrix; the allocator, when present, is a fresh
     * {@link StubResourceAllocator} already knowing {@link #API_X}, so the in-process rows exercise the
     * identical-contract reconciliation and the tag resolves. The two KD10 cells (partition-share, absent or
     * explicit, with an instance) must name both fields.
     */
    @ParameterizedTest
    @MethodSource("strategyMatrix")
    void theStrategyMatrixValidatesAsThePlanTableStates(Cell cell) {
        var builder = optionsBuilder();
        if (cell.strategyOrNull != null) {
            builder.allocationStrategy(cell.strategyOrNull);
        }
        if (cell.allocatorPresent) {
            StubResourceAllocator allocator = new StubResourceAllocator();
            allocator.register(API_X);
            builder.resourceAllocator(allocator);
        }
        if (cell.contractsPresent) {
            builder.resourceContracts(Collections.singletonList(API_X));
        }
        if (cell.tagsPresent) {
            builder.resourceTags(Collections.singletonList("api-x"));
        }
        var options = builder.build();

        if (cell.failureMustName.isEmpty()) {
            assertThatCode(options::validate).as(cell.toString()).doesNotThrowAnyException();
            assertThat(options.getAllocationStrategy()).as("an absent choice resolves to partition-share (R6)")
                    .isEqualTo(cell.strategyOrNull == null ? PARTITION_SHARE : cell.strategyOrNull);
        } else {
            var thrown = assertThatThrownBy(options::validate).as(cell.toString())
                    .isInstanceOf(IllegalArgumentException.class);
            for (String name : cell.failureMustName) {
                thrown.hasMessageContaining(name);
            }
        }
    }

    /**
     * Covers AE6, R6. In-process with an instance and an options-supplied contract that differs from the one
     * already registered on it: fails naming the collision - the resource, and both fields that disagree.
     */
    @Test
    void inProcessWithADifferingOptionsContractFailsNamingTheCollision() {
        StubResourceAllocator allocator = new StubResourceAllocator();
        allocator.register(API_X);

        var options = optionsBuilder()
                .allocationStrategy(IN_PROCESS)
                .resourceAllocator(allocator)
                .resourceContracts(Collections.singletonList(API_X_DIFFERENT_POLICY))
                .build();

        assertThatThrownBy(options::validate)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("api-x")
                .hasMessageContaining("resourceContracts")
                .hasMessageContaining("resourceAllocator")
                .hasMessageContaining(API_X.toString())
                .hasMessageContaining(API_X_DIFFERENT_POLICY.toString());
    }

    /**
     * Covers R6. In-process with an instance that does not know an options-supplied contract at all: the
     * contract is registered on the instance at validation - the one-line migration path's convenience - so a
     * tag against it resolves and the instance can grant against it afterwards.
     */
    @Test
    void inProcessRegistersAnOptionsContractTheInstanceDoesNotKnow() {
        StubResourceAllocator allocator = new StubResourceAllocator();
        assertThat(allocator.lookup("api-x")).isEmpty();

        var options = optionsBuilder()
                .allocationStrategy(IN_PROCESS)
                .resourceAllocator(allocator)
                .resourceContracts(Collections.singletonList(API_X))
                .resourceTags(Collections.singletonList("api-x"))
                .build();

        assertThatCode(options::validate).doesNotThrowAnyException();
        assertThat(allocator.lookup("api-x")).hasValue(API_X);
    }

    /**
     * Covers R6, R7 (KD10's "custom is any implementation"). A custom allocator is reconciled through the seam
     * alone - {@link ResourceAllocator#lookup} and {@link ResourceAllocator#register} - never by type: a known
     * identical contract is left alone, an unknown one is registered, and the validation passes.
     */
    @Test
    void customStrategyReconcilesContractsThroughTheSeamOnly() {
        ResourceAllocator custom = Mockito.mock(ResourceAllocator.class);
        Mockito.when(custom.lookup("api-x")).thenReturn(Optional.of(API_X));
        Mockito.when(custom.lookup("api-y")).thenReturn(Optional.empty());
        ResourceContract apiY = new ResourceContract("api-y", 1.0, 1, Duration.ofSeconds(1));

        var options = optionsBuilder()
                .allocationStrategy(CUSTOM)
                .resourceAllocator(custom)
                .resourceContracts(Arrays.asList(API_X, apiY))
                .resourceTags(Collections.singletonList("api-x")) // the mock's register is a no-op, so api-y would not resolve
                .build();

        assertThatCode(options::validate).doesNotThrowAnyException();
        Mockito.verify(custom, Mockito.never()).register(API_X);
        Mockito.verify(custom).register(apiY);
    }

    /**
     * Covers R7 under partition-share. No allocator exists at {@code validate()} time on this strategy, so the
     * unusable-policy rules (R19) are applied to the options-supplied contracts against a fresh registry: a
     * zero quantum fails here, with the registry's own message, not on the engine's first quantum read.
     */
    @Test
    void partitionShareContractWithAnUnusablePolicyFailsAtValidation() {
        ResourceContract zeroQuantum = new ResourceContract("api-x", 2.0, 2, Duration.ZERO);

        var options = optionsBuilder()
                .resourceContracts(Collections.singletonList(zeroQuantum))
                .resourceTags(Collections.singletonList("api-x"))
                .build();

        assertThatThrownBy(options::validate)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("api-x")
                .hasMessageContaining("non-positive quantum")
                .hasMessageContaining("R19");
    }

    /**
     * Covers R7 under partition-share. The same name declared twice in {@code resourceContracts} with different
     * policies is the R19 collision, caught at validation naming the resource and both policies.
     */
    @Test
    void partitionShareContractsDeclaringOneNameTwiceWithDifferentPoliciesFails() {
        var options = optionsBuilder()
                .resourceContracts(Arrays.asList(API_X, API_X_DIFFERENT_POLICY))
                .build();

        assertThatThrownBy(options::validate)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("api-x")
                .hasMessageContaining("DIFFERENT policy");
    }

    /**
     * Covers R7. A tag naming a resource no supplied contract declares, under partition-share with OTHER
     * contracts present, fails naming the tag - a typo must not silently mint an unconstrained resource (KD5).
     */
    @Test
    void partitionShareTagThatNoContractNamesFailsNamingTheTag() {
        var options = optionsBuilder()
                .resourceContracts(Collections.singletonList(API_X))
                .resourceTags(Collections.singletonList("api-y"))
                .build();

        assertThatThrownBy(options::validate)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("api-y")
                .hasMessageContaining("resourceContracts");
    }

    /**
     * A null entry in {@code resourceContracts} must fail with a named validation error, not a bare
     * {@link NullPointerException} inside the registry.
     */
    @Test
    void nullContractEntryFailsWithNamedErrorNotNpe() {
        var options = optionsBuilder()
                .resourceContracts(Arrays.asList(API_X, null))
                .build();

        assertThatThrownBy(options::validate)
                .isInstanceOf(IllegalArgumentException.class)
                .isNotInstanceOf(NullPointerException.class)
                .hasMessageContaining("resourceContracts");
    }

    /**
     * A contract with a null or blank name can never be tagged and would NPE inside the registry's map;
     * it fails here naming the field.
     */
    @Test
    void blankContractNameFailsWithNamedError() {
        var options = optionsBuilder()
                .resourceContracts(Collections.singletonList(new ResourceContract(" ", 2.0, 2, Duration.ofSeconds(1))))
                .build();

        assertThatThrownBy(options::validate)
                .isInstanceOf(IllegalArgumentException.class)
                .isNotInstanceOf(NullPointerException.class)
                .hasMessageContaining("resourceContracts");
    }

    private static ParallelConsumerOptions.ParallelConsumerOptionsBuilder<String, String> optionsBuilder() {
        return ParallelConsumerOptions.<String, String>builder()
                .consumer(new LongPollingMockConsumer<>(EARLIEST));
    }

    private static ParallelConsumerOptions<String, String> optionsWithConsumerOnly() {
        return optionsBuilder().build();
    }
}
