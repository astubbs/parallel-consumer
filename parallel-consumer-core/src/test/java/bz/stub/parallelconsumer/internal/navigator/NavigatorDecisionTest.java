package bz.stub.parallelconsumer.internal.navigator;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.navigator.ResourceDeferral;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import static com.google.common.truth.Truth.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static pl.tlinkowski.unij.api.UniLists.of;

/**
 * {@link NavigatorDecision#of}'s reason derivation (U4, R9): the ONLY constructor, so a decision's reason can
 * never disagree with its own blocking-resource shape. Covers the three {@link NavigatorDecisionReason} arms and
 * the "at least one blocking resource" invariant.
 */
class NavigatorDecisionTest {

    private static final ResourceDeferral API_A = new ResourceDeferral("api-a", Optional.of(Instant.ofEpochSecond(2)));
    private static final ResourceDeferral API_B = new ResourceDeferral("api-b", Optional.of(Instant.ofEpochSecond(4)));

    @Test
    void oneBlockingResourceAndNoSlotsBindingIsSingleResourceBlocked() {
        var decision = NavigatorDecision.of(of(API_A), false);

        assertThat(decision.getReason()).isEqualTo(NavigatorDecisionReason.SINGLE_RESOURCE_BLOCKED);
        assertThat(decision.getBlockingResources()).containsExactly(API_A);
        assertThat(decision.isAdmissionSlotsAlsoBinding()).isFalse();
    }

    @Test
    void twoBlockingResourcesAndNoSlotsBindingIsMultiResourceBlocked() {
        var decision = NavigatorDecision.of(of(API_A, API_B), false);

        assertThat(decision.getReason()).isEqualTo(NavigatorDecisionReason.MULTI_RESOURCE_BLOCKED);
        assertThat(decision.getBlockingResources()).containsExactly(API_A, API_B);
    }

    /**
     * The slots term wins the reason whenever it is true, REGARDLESS of how many resources are blocking - R9's
     * all-binding-predicates clause means an operator reading the ONE reason value must see every binding
     * predicate reflected, so a single-resource-plus-slots deferral must not read as merely SINGLE_RESOURCE_BLOCKED.
     */
    @Test
    void slotsAlsoBindingWinsTheReasonEvenWithOnlyOneBlockingResource() {
        var decision = NavigatorDecision.of(of(API_A), true);

        assertThat(decision.getReason()).isEqualTo(NavigatorDecisionReason.RESOURCE_AND_SLOTS_BLOCKED);
    }

    @Test
    void slotsAlsoBindingWinsTheReasonWithMultipleBlockingResourcesToo() {
        var decision = NavigatorDecision.of(of(API_A, API_B), true);

        assertThat(decision.getReason()).isEqualTo(NavigatorDecisionReason.RESOURCE_AND_SLOTS_BLOCKED);
    }

    /** There is no "deferred for no reason" case - a decision must name at least one blocking resource. */
    @Test
    void emptyBlockingResourcesIsRefused() {
        assertThatThrownBy(() -> NavigatorDecision.of(of(), false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("at least one blocking resource");
    }

    @Test
    void handAssignedValuesAreDistinctAndNotOrdinals() {
        assertThat(NavigatorDecisionReason.SINGLE_RESOURCE_BLOCKED.getValue())
                .isNotEqualTo(NavigatorDecisionReason.SINGLE_RESOURCE_BLOCKED.ordinal());
        Set<Integer> values = new HashSet<>();
        for (NavigatorDecisionReason reason : NavigatorDecisionReason.values()) {
            assertThat(values.add(reason.getValue())).isTrue();
        }
    }
}
