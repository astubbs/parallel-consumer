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

import { readFileSync, writeFileSync, mkdirSync, rmSync, existsSync, globSync } from 'node:fs'
import { join } from 'node:path'
import { fileURLToPath } from 'node:url'
import { spawnSync } from 'node:child_process'
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


// THE REPORT MUST CARRY A REAL STATUS, CHECKED BY RUNNING IT RATHER THAN BY GREPPING FOR ONE.
//
// The first version of this test asserted each status literal appeared somewhere in the source. It
// passed while all six had been inserted INSIDE the prose of their own messages - the report read
// "control classes (`...`, 'no-control') produced a time" and the payload came out `{}`. Grepping
// for a string proves the string exists, not that it is in the right place.
//
// The reporter chdirs to the repo root (so it always finds the lane's own target/), which means this
// cannot be isolated with a scratch cwd - it has to run in the repo and put its fixture where the
// reporter will look. Anything already there is saved and put back, because a developer with a real
// report on disk should not lose it to a self-test. `no-control` is the cheapest path to reach: a
// summary carrying a rate, with no control classes present.
//
// FOUR THINGS BELOW EXIST BECAUSE "IT RUNS THE REPORTER" IS NOT THE SAME AS "IT CHECKED THE RUN",
// and the first version of this block made all four mistakes at once:
//
//   * THE FIXTURE IS ONLY HALF THE INPUT. The reporter also globs `**/target/failsafe-reports/`, so a
//     developer holding real failsafe XML gives it a control class, which takes it OFF the no-control
//     path and out to `gh run list` - a networked, credentialed, non-deterministic run wearing this
//     test's name. The precondition is asserted and fails LOUDLY, because a skip is not a pass.
//   * A STALE REPORT SCORES AS A FRESH ONE. `target/throughput-report.md` is removed before the spawn,
//     so the assertions can only read a file this run produced. Without that, a reporter that wrote
//     nothing at all passes against last week's output.
//   * THE SPAWN RESULT WAS DISCARDED. A reporter that writes the right report and then returns the
//     WRONG exit code passed every assertion, while the workflow branches on that code
//     (`steps.throughput.outputs.code`). Exit 2 is CANNOT-MEASURE, which is what no-control means.
//   * `finally` DOES NOT RUN ON Ctrl-C. Node terminates on SIGINT with no listener installed, so an
//     interrupted test left the developer's real files replaced by the fixture. The handlers restore
//     and then re-raise with the default disposition.
{
  const root = fileURLToPath(new URL('..', import.meta.url))
  const summary = join(root, 'target/performance-throughput.txt')
  const report = join(root, 'target/throughput-report.md')
  const saved = [summary, report].map(f => [f, existsSync(f) ? readFileSync(f, 'utf8') : null])
  const restore = () => {
    for (const [f, body] of saved) {
      if (body === null) rmSync(f, { force: true })
      else writeFileSync(f, body)
    }
  }
  const onSignal = sig => { restore(); process.removeListener(sig, onSignal); process.kill(process.pid, sig) }
  for (const sig of ['SIGINT', 'SIGTERM']) process.on(sig, onSignal)
  try {
    const stray = globSync('**/target/failsafe-reports/TEST-*.xml', { cwd: root })
    if (stray.length) {
      console.error(`FAIL  leftover failsafe reports (${stray.length}) would take the reporter off the`)
      console.error('      no-control path and out to the network - run `./mvnw clean` and retry.')
      failures++
    } else {
      mkdirSync(join(root, 'target'), { recursive: true })
      rmSync(report, { force: true })
      writeFileSync(summary,
        'PC-THROUGHPUT test=MultiInstanceHighVolumeTest processed=3 expected=3 elapsedMs=1 recordsPerSecond=75000 outcome=PASSED\n')
      const run = spawnSync(process.execPath, [join(root, 'bin/check-throughput-regression.mjs')], { encoding: 'utf8' })
      check('the reporter ran', run.error === undefined, true)
      // 2 is CANNOT, the documented code for "the check could not measure". Pinned here because the
      // workflow reads it and this is the only place the four-value contract is asserted at all.
      check('the reporter exited CANNOT for no-control', run.status, 2)
      const text = existsSync(report) ? readFileSync(report, 'utf8') : ''
      const m = /<!-- pc-throughput-data: (.*?) -->/.exec(text)
      const data = m ? JSON.parse(m[1]) : {}
      check('the report carries a machine-readable payload', Boolean(m), true)
      check('the payload names a real status', data.status, 'no-control')
      // ABSENCE, NOT PRESENCE. The first version asserted the message's opening text was still there -
      // which stayed true whether or not the status had been injected into it, so the assertion
      // written to catch that exact bug could not fail. Split the payload off and assert the status
      // literal appears NOWHERE in the prose a human reads.
      check('the status did not land inside the message', text.split('<!-- pc-throughput-data:')[0].includes('no-control'), false)
    }
  } finally {
    for (const sig of ['SIGINT', 'SIGTERM']) process.removeListener(sig, onSignal)
    restore()
  }
}

if (failures === 0) { console.log('\nAll throughput-verdict self-tests passed'); process.exit(0) }
console.error(`\n${failures} throughput-verdict self-test(s) failed`)
process.exit(1)

