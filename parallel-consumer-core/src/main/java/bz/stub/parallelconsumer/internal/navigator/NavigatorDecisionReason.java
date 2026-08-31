package bz.stub.parallelconsumer.internal.navigator;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.Getter;

import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * Why a record is resource-deferred (U4, R9): which combination of binding predicates the navigator's
 * attribution names. A navigator-scoped sibling of
 * {@link bz.stub.parallelconsumer.internal.admission.AdmissionDecisionReason} - the SAME hand-assigned-value,
 * never-reuse discipline (KTD6) - but deliberately its own type: that enum's javadoc enumerates only the arms
 * of the adaptive concurrency control law, and the navigator's deferral predicates are a different axis
 * entirely (a credit gate, not a target-concurrency law).
 * <p>
 * Each carries a hand-assigned {@link #getValue() metrics value} so {@code PCMetricsDef#NAVIGATOR_DEFERRAL_REASON}
 * can publish "which reason bound the most recent deferral" as a number, and
 * {@code PCMetricsDef#NAVIGATOR_DEFERRAL_EPISODES} can tag one counter per reason - the same device
 * {@code AdmissionDecisionReason} uses. The values are deliberately NOT ordinals: retiring or adding a reason
 * must never silently renumber one a dashboard already keys on.
 * <p>
 * <b>Retired values, never to be reused:</b> none yet - this is the first cut of the vocabulary. When a reason
 * is ever retired, its value moves here with the same discipline {@code AdmissionDecisionReason} documents.
 */
public enum NavigatorDecisionReason {

    /**
     * Exactly one tagged resource holds no spendable credit for this member right now (R7's single-resource
     * case).
     */
    SINGLE_RESOURCE_BLOCKED(1),

    /**
     * Two or more tagged resources hold no spendable credit simultaneously - R9's all-binding-predicates
     * clause: the attribution names every one of them, not a chosen one.
     */
    MULTI_RESOURCE_BLOCKED(2),

    /**
     * At least one tagged resource is blocking AND the engine-wide admission target (the slots seam KD1
     * conjuncts with, evaluated upstream of the claim - see {@code PCModule#admissionTargetSlots()}) is also
     * currently binding this pass. The two predicates gate at different seams (KD1: the slots target bounds
     * what enters selection, the resource predicate gates the claim), so this reason exists purely for
     * attribution - naming both rather than leaving an operator to infer the second from a separately-logged
     * admission constraint report.
     */
    RESOURCE_AND_SLOTS_BLOCKED(3);

    /**
     * What the reason gauge publishes when nothing is currently deferred - no reason is binding. Zero is
     * reserved for it, matching {@code AdmissionDecisionReason#NO_DECISION_YET_VALUE}.
     */
    public static final int NO_DEFERRAL_VALUE = 0;

    /**
     * The metrics value published for this reason - hand-assigned, never the ordinal (see the class javadoc).
     */
    @Getter
    private final int value;

    NavigatorDecisionReason(int value) {
        this.value = value;
    }

    /**
     * The value-to-reason mapping, rendered for a meter description so the gauge's numbers are readable
     * without this source file - {@code AdmissionDecisionReason#getReasonToValueListing()}'s counterpart for
     * {@code PCMetricsDef#NAVIGATOR_DEFERRAL_REASON}.
     */
    public static String getReasonToValueListing() {
        return NO_DEFERRAL_VALUE + ":NONE, " + Arrays.stream(values())
                .map(reason -> reason.value + ":" + reason)
                .collect(Collectors.joining(", "));
    }
}
