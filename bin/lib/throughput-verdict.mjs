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
// THE CONTROL MUST BE CPU-BOUND, WHICH IS NOT A DETAIL - IT IS THE PREMISE. Conservation holds only
// while every term scales with the machine. A test dominated by fixed sleeps does not: LoadTest sleeps
// 0-5ms per record and LargeVolumeInMemoryTests sleeps 3ms per record, so on a slower CPU their
// wall-clock barely moves while the subject's grows. Using them as controls would mean the denominator
// stays put while the numerator rises, and the check reports a regression that is only a slow runner -
// the exact failure the normalisation exists to prevent. Found in review; both are now excluded.
//
// It also reframes an earlier measurement rather than merely fixing code. Eight runs on one unchanged
// commit put the subject at a 33% spread with the controls inside 8%, and that was read as "the
// machine is stable, the test is noisy". With two of three controls sleep-dominated, "the controls
// could not move" explains the same numbers, and the data cannot separate the two. The noise floor
// needs re-measuring against a CPU-bound control before any bound here is called calibrated.
//
// WHAT THIS STILL CANNOT DO. It removes machine-to-machine variance. It does not remove the subject's
// OWN run-to-run variance - that is a property of the test, not of the comparison, and no arithmetic
// here can touch it. It is why the reference is a MEDIAN over several runs and why the bounds are
// coarse.

/** Operator ruling, 2026-09-01: a 50% loss fails, a 30% loss flags. */
export const FAIL_BELOW = 0.50
export const WARN_BELOW = 0.70

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
