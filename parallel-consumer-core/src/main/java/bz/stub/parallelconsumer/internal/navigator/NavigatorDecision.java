package bz.stub.parallelconsumer.internal.navigator;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.Value;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * The outcome of one resource-deferral attribution (U4, R9): every currently-blocking tagged resource plus
 * whether the engine-wide admission target is also binding this pass - the navigator's sibling of
 * {@code bz.stub.parallelconsumer.internal.admission.AdmissionDecision}, mirroring its shape (a value, and the
 * reason that produced it).
 * <p>
 * {@link #of} is the only constructor a caller needs: it derives {@link #getReason()} from the predicate shape
 * so a caller can never construct a decision whose reason disagrees with its own blocking-resource list.
 */
@Value
public class NavigatorDecision {

    /**
     * Every tagged resource currently holding no spendable credit for this member - NEVER a chosen subset
     * (R9's all-binding-predicates clause). Always non-empty: {@link #of} refuses to construct a decision with
     * nothing blocking.
     */
    List<ResourceDeferral> blockingResources;

    /**
     * Whether the engine-wide admission target (the slots seam KD1 conjuncts with) is ALSO binding this pass -
     * a fact read where it is cheaply observable (see {@code PCModule#isAdmissionSlotsCurrentlyBinding()}),
     * not re-derived here.
     */
    boolean admissionSlotsAlsoBinding;

    /**
     * Which arm of the vocabulary this decision is - derived, never chosen independently of
     * {@link #blockingResources} and {@link #admissionSlotsAlsoBinding} (see {@link #of}).
     */
    NavigatorDecisionReason reason;

    /**
     * Builds a decision from the predicate shape, deriving {@link #reason} so it can never disagree with
     * {@code blockingResources}/{@code admissionSlotsAlsoBinding} (R9's all-binding-predicates clause: the slots
     * term wins the reason whenever it is also true, since an operator reading ONE reason value must see every
     * binding predicate reflected, not just the resource one).
     *
     * @throws IllegalArgumentException {@code blockingResources} is empty - a decision must name at least one
     *                                  binding resource; there is no "deferred for no reason" case
     */
    public static NavigatorDecision of(List<ResourceDeferral> blockingResources, boolean admissionSlotsAlsoBinding) {
        if (blockingResources == null || blockingResources.isEmpty()) {
            throw new IllegalArgumentException("A NavigatorDecision must name at least one blocking resource - "
                    + "there is no deferral with nothing binding it");
        }
        NavigatorDecisionReason reason = admissionSlotsAlsoBinding
                ? NavigatorDecisionReason.RESOURCE_AND_SLOTS_BLOCKED
                : blockingResources.size() > 1
                ? NavigatorDecisionReason.MULTI_RESOURCE_BLOCKED
                : NavigatorDecisionReason.SINGLE_RESOURCE_BLOCKED;
        return new NavigatorDecision(
                Collections.unmodifiableList(new ArrayList<>(blockingResources)), admissionSlotsAlsoBinding, reason);
    }
}
