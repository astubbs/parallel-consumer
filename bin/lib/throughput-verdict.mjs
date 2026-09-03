// Copyright (C) 2026 Antony Stubbs and contributors
//
// The verdict, DERIVED BY CONSERVATION rather than by correcting a delta with another delta.
//
// WHAT WAS WRONG WITH THE DELTA FORM
//
// It took a RATE (records per second), derived a machine index from control class TIMES, multiplied
// one by the other, and compared. Two objections, and the second is fatal:
//
//   * It mixes dimensions. `expected = referenceRate x (referenceControlSeconds / observedControlSeconds)`
//     assumes throughput scales inversely with somebody else's wall clock.
//   * The correction was MEASURED NOT TO HOLD. On one unchanged commit the control classes moved 5%
//     while the subject moved 30%, so a machine index built from the controls does not describe the
//     subject. A correction that does not correct is worse than none, because it looks like rigour.
//
// THE CONSERVED QUANTITY
//
// Every test in this lane processes a FIXED number of records - the volumes are constants in the test
// classes, not a function of the machine. So within a single run, the ratio of one test's time to
// another's is invariant under machine speed: a runner twice as slow doubles both terms and leaves the
// ratio alone. Nothing has to be corrected, because nothing was ever uncorrected.
//
//   share = subjectSeconds / controlSeconds        (both from the SAME run)
//
// A regression raises the subject's share. The machine cancels identically rather than approximately,
// which is the difference between conservation and a correction factor.
//
// WHY PER-METHOD TIMES AND NOT CLASS TIMES - this is part of the same argument, not a refinement.
// A class time is `work + setup`: container startup, topic creation, @BeforeAll. Setup does NOT scale
// with the work done, so it is exactly the non-conserved term, and leaving it in breaks the invariant
// the whole method rests on. <testcase> carries per-method times and the lanes now upload the failsafe
// XML, so both sides have them.
//
// MEASURED 2026-09-01, AND THE RESULT DOES NOT SUPPORT NORMALISING AT ALL ON ONE MACHINE.
// Eight full-lane runs on a single unchanged commit, robust (outlier-insensitive) spread:
//
//     LoadTest        (0-5ms sleep/record)   0.9%   <- inert; not a machine-speed signal, a constant
//     LargeVolume     (3ms sleep/record)     0.9%   <- inert
//     VeryLarge       (CPU-bound)            6.1%
//     subject                               13.4%
//     share / all three controls            16.6%
//     share / CPU-bound control only        17.2%
//     subject raw, no normalisation         13.4%   <- the quietest signal available
//
// Two conclusions, and the second was not expected. The sleeping controls really are inert, exactly as
// review argued - 0.9% cannot describe a machine. But narrowing to the CPU-bound control makes things
// WORSE, not better: every normalisation ADDS about four points of noise, because dividing by a
// control compounds its variance rather than cancelling anything. On one idle box there is no
// machine-to-machine variance to cancel, so the division can only cost.
//
// WHAT THIS DOES AND DOES NOT SETTLE. It does not disprove the cross-runner case normalising exists
// for - every run here is the same box, so this data cannot test it. It does prove normalising is not
// free, and that the bounds below were calibrated against a quantity noisier than the raw measurement
// they were meant to improve on.
//
// SO THE VERDICT IS ADVISORY, NOT BLOCKING (see maven.yml). Shipping a required check on bounds the
// measurement does not support would be the "flapping gate that gets switched off within a week" this
// whole design is written to avoid - and switching it off would take the collection with it.

/** Operator ruling, 2026-09-01: a 50% loss fails, a 30% loss flags. */
export const FAIL_BELOW = 0.50
export const WARN_BELOW = 0.70

// THE RUN-TO-RUN SPREAD OF THE QUANTITY THE VERDICT IS BUILT FROM, taken from the measurement table
// above: the share moved 16.6-17.2% across eight runs of one unchanged commit. Rounded UP to the
// higher figure, because a floor that understates its own noise is the thing this constant exists to
// stop. It is not a threshold - nothing branches on it - it is what the headline sentence has to
// disclose so a small difference is not read as a finding.
export const NOISE_FLOOR = 0.17

// A TRUE median: on an even-sized set, average the two middle values. The first version took the
// lower-middle, which systematically understates the reference share and so makes every verdict
// ratio stricter than the calibrated bound claims - with the default ten reference runs, always.
export const median = xs => {
  const s = [...xs].sort((a, b) => a - b)
  const mid = s.length >> 1
  return s.length % 2 ? s[mid] : (s[mid - 1] + s[mid]) / 2
}

/**
 * @param {{subject:number, control:number}} observed per-method seconds from this run
 * @param {{subject:number, control:number}[]} reference the same, from recent master runs
 */
export function verdictFor(observed, reference) {
  if (reference.length === 0) return { kind: 'no-reference' }
  if (!(observed.subject > 0)) return { kind: 'no-subject' }
  if (!(observed.control > 0)) return { kind: 'no-control' }

  const observedShare = observed.subject / observed.control
  const referenceShare = median(reference.map(r => r.subject / r.control))

  // Expressed so that 1.0 is "as expected" and BELOW 1.0 is worse, matching how a throughput ratio
  // reads. A share that has grown means the subject now takes more of the lane than it did.
  const ratio = observedShare > 0 ? referenceShare / observedShare : 0

  const icon = ratio < FAIL_BELOW ? '🔴' : ratio < WARN_BELOW ? '🟡' : '🟢'
  const word = ratio < FAIL_BELOW ? 'FAIL' : ratio < WARN_BELOW ? 'FLAG' : 'OK'
  return { kind: 'verdict', observedShare, referenceShare, ratio, icon, word,
           failed: ratio < FAIL_BELOW }
}

/** Which of the three bands a ratio falls in. Same expression as `icon`/`word`, named once. */
export const bandOf = ratio => (ratio < FAIL_BELOW ? 'fail' : ratio < WARN_BELOW ? 'flag' : 'ok')

/**
 * The ratio as the report should PRINT it.
 *
 * `ratio.toFixed(3)` rounds, and at a bound the rounding crosses it: 0.4996 prints as `0.500` beside
 * a 🔴 whose bound the very same comment prints as "< 0.50", and 0.6996 prints as `0.700` beside a
 * 🟡. A reader who does the honest thing - check the number against the stated range - then gets the
 * opposite answer from the icon, in a report whose whole redesign was about not being misread.
 * Narrow window, but the fix is to widen the precision until the printed value lands in the same
 * band as the raw one, so the two can never disagree.
 */
export const displayRatio = ratio => {
  for (const dp of [3, 4, 5, 6, 9]) {
    const shown = ratio.toFixed(dp)
    if (bandOf(Number(shown)) === bandOf(ratio)) return shown
  }
  // `String(n)` round-trips a double exactly, so this last resort cannot disagree with its own band.
  return String(ratio)
}

/**
 * The report's headline sentence.
 *
 * SAY THE SIZE OF THE RULER IN THE SAME BREATH AS THE MEASUREMENT. The first version stated "This
 * branch is about 8% faster than master" flatly, two paragraphs above the same comment's own
 * statement that this test moves 13-17% run to run - so the report contradicted itself and the
 * headline won, because the headline is what gets read. A difference smaller than the instrument's
 * spread is a reading, not a result, and the sentence now says which of the two it is.
 *
 * The verb matters as much as the caveat: "measured about 8% faster" is a fact about this run,
 * "is about 8% faster" is a claim about the branch, and only the first one is true.
 */
export const headlineFor = ratio => {
  const pct = n => `${(n * 100).toFixed(0)}%`
  const diff = Math.abs(1 - ratio)
  if (diff < 0.005) return '**This branch measured the same speed as master**, on the one test this measures.'
  const sentence = `**This branch measured about ${pct(diff)} ${ratio >= 1 ? 'faster' : 'slower'} than master**, on the one test this measures.`
  return diff < NOISE_FLOOR
    ? `${sentence} That is INSIDE this test's own run-to-run spread of about ${pct(NOISE_FLOOR)}, so read it as a reading and not as a result - re-running the same commit moves it by about as much.`
    : `${sentence} That is larger than this test's own run-to-run spread of about ${pct(NOISE_FLOOR)}, so it is worth looking at.`
}
