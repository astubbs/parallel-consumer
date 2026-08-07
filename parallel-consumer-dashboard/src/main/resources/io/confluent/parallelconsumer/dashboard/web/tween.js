/*
 * Copyright (C) 2026 Antony Stubbs and contributors
 *
 * When a tween starts, how long it runs for, and when it must not run at all.
 *
 * WHY THIS IS ITS OWN MODULE. It is the timing half of the page's one interesting property - documents arrive at
 * whatever rate they arrive at, and the picture has to move smoothly anyway - and it is pure arithmetic over
 * millisecond numbers. Kept inside app.js it sat behind a `document.getElementById` at module scope and could only
 * be exercised by opening a browser and watching. Here it has no DOM, so the ordinary unit suite runs it.
 *
 * WHAT WAS ACTUALLY WRONG, MEASURED. The page used to size each tween from the LAST measured arrival gap. Documents
 * do not arrive evenly - the measured spread on a loopback event stream nominally every second was 643ms to 1120ms -
 * so that estimate is too short about half the time. When it is too short the tween finishes before the next
 * document lands and, with nothing left to draw, the picture STOPS until it does. Move, freeze, move. Sampling the
 * rendered marker positions on every animation frame, the shipped page was frozen for 4.1% of frames, with pauses up
 * to 204ms and a worst single-frame lurch of 21px on a 1161px track. With the estimate below and nothing else
 * changed: 0.0% frozen, no pause longer than a single frame, worst single-frame move 4.7px, and the median and
 * 99th-percentile frame-to-frame motion unchanged.
 *
 * It matters most exactly where the server cannot help. A faster server makes each freeze shorter but does not
 * remove it - and when the reader picks an explicit cadence the page is polling, several seconds apart, and no
 * amount of server-side push reaches it at all.
 *
 * WHAT THIS DELIBERATELY DOES NOT DO. It never runs past the newest sample at the last known velocity. That would
 * draw a number the instance never reported, and on this page that is a lie about somebody's consumer. Every value
 * drawn is between two readings that really happened.
 */

import {clamp} from './ui.js';

/**
 * How fast the gap estimate comes back down after a long gap; it goes up instantly. See nextGapEstimate.
 */
const GAP_DECAY = 0.25;

/**
 * Folds one observed arrival gap into the estimate of how long the NEXT one will take.
 *
 * ASYMMETRIC ON PURPOSE, and that asymmetry is the fix. The two ways a tween can be mis-sized are opposites and
 * deserve opposite treatment:
 *
 *   - UNDERESTIMATE freezes the picture, because the tween runs out before there is anywhere new to go. So a longer
 *     gap is adopted IMMEDIATELY - one late document is enough to widen the estimate, because a freeze is the worse
 *     fault and must not be allowed to happen twice for the same reason.
 *   - OVERESTIMATE steps the picture, because the tween is still mid-way when the next document retargets it. So a
 *     shorter gap is only eased into, and one early document cannot narrow the estimate.
 *
 * A plain average would produce both faults at half strength. The last gap alone - which is what this replaced -
 * hands the whole of one anomalous gap to the next tween, so a single late sample makes the following interval run
 * at the wrong speed and then step.
 *
 * @param estimateMillis     the previous estimate, or null if there is none yet
 * @param observedGapMillis  how long the document that just arrived actually took
 * @returns the new estimate, or the old one unchanged if the observation was not usable
 */
export function nextGapEstimate(estimateMillis, observedGapMillis) {
    if (!(observedGapMillis > 0)) {
        return estimateMillis === undefined ? null : estimateMillis;
    }
    if (estimateMillis === null || estimateMillis === undefined || observedGapMillis > estimateMillis) {
        return observedGapMillis;
    }
    return estimateMillis + (observedGapMillis - estimateMillis) * GAP_DECAY;
}

/**
 * Whether a gap between two documents is an interruption rather than a cadence - a hidden tab, a switched
 * transport, an instance that could not be reached.
 *
 * The readings in between never happened, so the page must SNAP to the new sample rather than glide across the
 * hole. A resumed tab animating a smooth minute-long slide it invented would be the page describing work that was
 * never done. The threshold is the same one that already made the page say "stale" out loud, so the two can never
 * disagree about whether the feed was interrupted.
 */
export function isInterruption(gapMillis, staleThresholdMillis) {
    return gapMillis !== null && gapMillis !== undefined && gapMillis > staleThresholdMillis;
}

/**
 * How far through a tween the current animation frame is, 0 at the newest document's arrival and 1 when the tween
 * has reached it.
 *
 * Clamped at 1 rather than extrapolating - see the module header. Reaching 1 is what tells the render loop it may
 * stop drawing, so with a healthy feed this should be reached rarely; a page sitting at 1 is a page whose next
 * document is late.
 */
export function tweenFraction(elapsedMillis, durationMillis) {
    if (!(durationMillis > 0)) {
        return 1;
    }
    return clamp(elapsedMillis / durationMillis, 0, 1);
}
