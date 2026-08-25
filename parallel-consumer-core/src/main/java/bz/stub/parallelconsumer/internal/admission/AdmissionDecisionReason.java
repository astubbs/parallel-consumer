package bz.stub.parallelconsumer.internal.admission;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.Getter;

import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * Why {@link AdmissionControlLaw} chose the target concurrency it did for a window.
 * <p>
 * The reasons are listed in the precedence order the band machine evaluates them - the first gate whose condition
 * holds wins the window (see {@link AdmissionControlLaw}'s class javadoc for the full precedence).
 * <p>
 * Each carries a hand-assigned {@link #getValue() metrics value} so the
 * {@code PCMetricsDef#ADMISSION_CONSTRAINT} gauge can publish "which constraint is binding" as a number - the same
 * device {@link bz.stub.parallelconsumer.internal.State} uses for {@code pc.status}. The values are deliberately NOT
 * ordinals: adding or removing a reason must never silently renumber the ones a dashboard is already keyed on.
 * <p>
 * <b>Retired values, never to be reused:</b> {@code 1} ({@code APP_LIMITED}) and {@code 4} ({@code PROBING})
 * belonged to arms of the deleted Gradient2 port (the 2026-08-24-003 design's KTD8 deletions). A dashboard keyed
 * on them must read those values as historical; a future reason - including U6's escape probe - takes a fresh
 * number, so the old semantics can never be silently re-assigned.
 */
public enum AdmissionDecisionReason {

    /**
     * At least one admission was dropped for overload this window - multiplicative AIMD backoff was applied
     * (an absolute brake; fires whatever the elasticity verdict says).
     */
    BACKOFF(2),

    /**
     * The non-success fraction of outcomes exceeded the threshold - growth is frozen for the window (an absolute
     * brake). A fast-failing overloaded downstream produces high useful-looking completion rates and low latency;
     * without this brake the law could read overload as headroom.
     */
    FAILURE_LIMITED(3),

    /**
     * The law moved the target on elasticity evidence: a RISE-band accelerator step, a retraction of growth the
     * following verdict showed had not paid, or a FALL-band multiplicative contraction. The movement log carries
     * the direction and the verdict that drove it.
     */
    ADAPTING(5),

    /**
     * The law wanted to grow but the ceiling clamp bound.
     */
    AT_CAP(6),

    /**
     * The law wanted to contract but the one-slot floor clamp bound.
     */
    AT_FLOOR(7),

    /**
     * A real change to this instance's partition assignment discarded the sample window and the law's history, and
     * froze the target at its pre-rebalance value (carried over as the best available prior) for a cooldown - the
     * old assignment's measurements say nothing about the new workload, and adapting on settle-time noise would
     * move the target on evidence about a system that no longer exists. Set by
     * {@link AdmissionController}, not the law - the law never sees a rebalance.
     */
    COOLDOWN(8),

    /**
     * The window carried too few samples to adjudicate - held, with ALL law state untouched: a signal-free window
     * must neither move the target nor teach the elasticity estimator (the design's adjudication gate).
     */
    INSUFFICIENT_SIGNAL(9),

    /**
     * No elasticity verdict is in force (cold start, post-rebalance reconstruction) and the limit is binding, so
     * the warmup band granted one additive accelerator step on binding alone (the design's R3/KTD2).
     */
    WARMUP(10),

    /**
     * No elasticity verdict is in force and the warmup band's per-episode allowance is spent - held. The named
     * steady state for a plant that cannot adjudicate enough windows per horizon is preserve (plus, from U6, the
     * escape cadence), never unbounded blind growth (KTD2's cap).
     */
    WARMUP_EXHAUSTED(11),

    /**
     * A live elasticity verdict is in force and the law is not moving this window: either the verdict is the HOLD
     * band (growth has stopped paying - the knee), or the law is between accelerator steps waiting for
     * post-movement evidence to accumulate (the settle cadence). Either way the target is deliberately parked on
     * elasticity evidence.
     */
    PLATEAU(12),

    /**
     * The limit did not bind this window because the application ran out of work - preserved bit-identical, never
     * decayed (the design's R5: absence of data yields no decision). One of the three separated starvation causes
     * (the R13 reporting requirement).
     */
    NO_WORK(13),

    /**
     * The limit did not bind this window because buffered work existed that the shards could not yield - ordering,
     * not the limit or absence of work, held the slots empty. Preserved bit-identical (R5). The single most
     * valuable diagnosis available to an operator of this library, per the design.
     */
    ORDERING_STARVED(14),

    /**
     * The limit did not bind this window because the poller had paused itself for throttling - self-inflicted
     * emptiness, which must never be read as evidence of anything. Preserved bit-identical (R5).
     */
    SELF_THROTTLED(15),

    /**
     * At least one assigned partition was refusing records under offset-encoding back-pressure at the boundary
     * (the design's R8 absolute brake): held, never grown - a partition refusing records makes growth meaningless.
     */
    OFFSET_BACK_PRESSURE(16),

    /**
     * The U6 floor-escape probe is running (the design's R6): consecutive floor windows forced a re-measurement
     * from the floor with a cleared elasticity history, on a path no gated signal can suppress. Hand-assigned a
     * FRESH value - never {@code PROBING}'s retired {@code 4}, whose probe-DOWN semantics dashboards may already
     * key on (see the class javadoc's retired-values note).
     */
    ESCAPE_PROBE(17),

    /**
     * The U6 descent probe is running (the design's R14 sweep-from-above): sustained plateau evidence at a target
     * above the floor triggered a one-accelerator-step-down re-measurement - kept when throughput holds (the
     * lower target paid), restored when it fell. Its own value rather than sharing {@link #ESCAPE_PROBE}: the two
     * probes answer different operator questions (stranded-at-floor vs parked-above-the-knee), and a shared gauge
     * value would make them indistinguishable on a dashboard.
     */
    DESCENT_PROBE(18),

    /**
     * The stagnation probe is running: {@code WARMUP_EXHAUSTED} persisted with growth pending adjudication and
     * no verdict ever computing - the operating point is spread-less, so the verdict that owes the episode its
     * confirm-or-retract is structurally unreachable (the 2026-08-25 comparison-IT freeze) - and the controller
     * is re-measuring one accelerator step UP (the direction the blind growth went), evaluated by throughput
     * like the descent probe. Every reachable state must have an evidence-driven exit; this is that state's.
     * Its own gauge value: a stranded warmup is a different operator question from either sibling probe.
     */
    STAGNATION_PROBE(19),

    /**
     * The recovery re-ask probe is running (law-U13): the target has been parked under a LIVE verdict for a
     * full re-ask cadence - or the parked level's own throughput drifted above the park-era reference, where
     * that is observable - and the controller is re-measuring one accelerator step UP, evaluated by throughput
     * like its sibling probes. The park it exits is otherwise absorbing when downstream capacity RECOVERS:
     * below the knee a level's throughput carries no capacity term, so recovery is invisible at every level
     * the controller visits and only asking upward can reveal it (the 2026-08-25 capacity-recovery falsifier's
     * bit-identical-windows measurement; the comparison IT's phase-3 strand). Its own gauge value: "holding a
     * level that stopped being the knee" is a different operator question from a stranded warmup
     * ({@link #STAGNATION_PROBE}) or a parked-above-the-knee walk-down ({@link #DESCENT_PROBE}).
     */
    RECOVERY_PROBE(20);

    /**
     * What the constraint gauge publishes before the first window has closed - no gate has decided anything yet, so
     * no reason is binding. Zero is reserved for it, which is why the reasons themselves start at one.
     */
    public static final int NO_DECISION_YET_VALUE = 0;

    /**
     * The metrics value published for this reason - hand-assigned, never the ordinal (see the class javadoc).
     */
    @Getter
    private final int value;

    AdmissionDecisionReason(int value) {
        this.value = value;
    }

    /**
     * The value-to-reason mapping, rendered for a meter description so the gauge's numbers are readable without
     * this source file - the {@code State}-to-value listing's counterpart for
     * {@code PCMetricsDef#ADMISSION_CONSTRAINT}.
     */
    public static String getReasonToValueListing() {
        return NO_DECISION_YET_VALUE + ":NONE, " + Arrays.stream(values())
                .map(reason -> reason.value + ":" + reason)
                .collect(Collectors.joining(", "));
    }
}
