#!/usr/bin/env node
// Copyright (C) 2026 Antony Stubbs and contributors
//
// Self-test for bin/lib/throughput-verdict.mjs. Every number below is measured, not invented, and the
// file is the evidence the bounds came from - a bound justified only in a commit message is one nobody
// can re-derive, and one nobody can re-derive is one nobody defends when it fires inconveniently.
//
// THE NOISE CASES MATTER MOST. measured on one unchanged commit, the raw subject moves ~13% while the
// sleeping controls move ~1% and the CPU-bound one ~6% - and normalising by any of them RAISES the
// spread to ~17%. See the measurement table in lib/throughput-verdict.mjs. Any bound that fires inside that spread produces a
// red on a quiet day, and the first person to hit one switches the gate off - taking the collection
// with it. So this asserts what the gate must NOT do at least as hard as what it must.

import { verdictFor, FAIL_BELOW, WARN_BELOW, median } from './lib/throughput-verdict.mjs'

let failures = 0
const check = (desc, actual, expected) => {
  if (actual === expected) console.log(`ok    ${desc}`)
  else { console.error(`FAIL  ${desc} - expected ${expected}, got ${actual}`); failures++ }
}

// Reference: real runs from the noise-floor characterisation on origin/master, per-method subject
// seconds against the summed control classes.
const REF = [
  { subject: 25.915, control: 79.451 },
  { subject: 27.936, control: 80.684 },
  { subject: 29.898, control: 77.597 },
]

check('median picks the middle of an odd set', median([3, 1, 2]), 2)
check('median does not mutate its input', (() => { const a = [3, 1, 2]; median(a); return a[0] })(), 3)

// --- the machine must cancel, which is the whole claim -------------------------------------------
{
  // Same tree on a runner twice as slow: every time doubles. Conservation says the verdict is
  // identical. If this ever fails, the comparison has stopped being machine-independent and the
  // thresholds mean nothing.
  const normal = verdictFor({ subject: 27.936, control: 80.684 }, REF)
  const slow = verdictFor({ subject: 55.872, control: 161.368 }, REF)
  check('a machine twice as slow gives the SAME ratio', slow.ratio.toFixed(6), normal.ratio.toFixed(6))
  check('and the same verdict', slow.icon, normal.icon)
}

// --- noise must not fire -------------------------------------------------------------------------
check('noise: low end of the measured spread is green',
  verdictFor({ subject: 25.915, control: 79.451 }, REF).icon, '🟢')
check('noise: high end of the measured spread is green',
  verdictFor({ subject: 33.783, control: 80.206 }, REF).icon, '🟢')

// --- the A/B I once called a confirmed regression ------------------------------------------------
{
  // Both arms are green under conservation, which is the third independent line of evidence that the
  // "20% regression" was the instrument rather than the code. Pinned so the claim cannot quietly
  // come back.
  check('the A/B before-arm is green', verdictFor({ subject: 23.347, control: 80.95 }, REF).icon, '🟢')
  check('the A/B after-arm is green', verdictFor({ subject: 28.603, control: 80.89 }, REF).icon, '🟢')
}

// --- real regressions must be caught --------------------------------------------------------------
{
  // A subject taking 1.8x as long - the scale of the astubbs/parallel-consumer#29 shortfall - flags but
  // does not fail, because the operator set the fail line at a 50% loss and this is about 44%. Pinned
  // as an assertion so the consequence is visible to anyone changing the bound.
  const v = verdictFor({ subject: 50.0, control: 80.0 }, REF)
  check('a 1.8x subject is FLAGGED', v.icon, '🟡')
  check('a 1.8x subject does not FAIL at a 50% bound', v.failed, false)
  // Three times as long is unambiguous.
  check('a 3x subject FAILS', verdictFor({ subject: 84.0, control: 80.0 }, REF).failed, true)
}

// --- the not-a-pass paths -------------------------------------------------------------------------
check('no reference yet is its own state', verdictFor({ subject: 27, control: 80 }, []).kind, 'no-reference')
check('a missing subject is not a pass', verdictFor({ subject: 0, control: 80 }, REF).kind, 'no-subject')
check('a missing control is not a pass', verdictFor({ subject: 27, control: 0 }, REF).kind, 'no-control')

check('fail bound is a 50% loss', FAIL_BELOW, 0.50)
check('flag bound is a 30% loss', WARN_BELOW, 0.70)

if (failures === 0) { console.log('\nAll throughput-verdict self-tests passed'); process.exit(0) }
console.error(`\n${failures} throughput-verdict self-test(s) failed`)
process.exit(1)
