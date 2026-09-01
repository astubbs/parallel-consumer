#!/usr/bin/env node
//
// Copyright (C) 2026 Antony Stubbs and contributors
//

// PORTED FROM bin/check-throughput-regression.sh, which cb8d18d65 deleted - read it with
// `git show cb8d18d65^:bin/check-throughput-regression.sh`, the repair docs/citations.md prescribes
// for a path that is gone. The BUSINESS behaviour is identical - same inputs,
// same arithmetic, same thresholds, same exit codes - and bin/test-check-throughput-regression.mjs
// pins that with the same six cases and the same four ratios the shell version produced. Mixing a
// rewrite into a port is how a language change gets blamed for a behaviour change, so the known
// improvements (per-method comparison, a measured noise floor) are tracked separately and are not here.
//
// It is NOT a transliteration, though. Two things the shell version did because shell made them easy
// are done properly now, and one of them was a latent bug:
//
//   * Reports are found with fs.globSync, not by shelling out to `find` through `sh -c`. A subprocess
//     to list files is a shell habit, not a requirement.
//   * Class times are read into a Map keyed by the class NAME parsed out of the XML, instead of
//     matching a baseline name against a file PATH with a substring test. The substring version would
//     have matched the wrong report for any class whose name is a prefix of another's - it happens not
//     to bite today, which is exactly the kind of thing that starts biting when somebody adds a class.

import { readFileSync, existsSync, globSync } from 'node:fs'
import { dirname, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'

// ANCHORED TO THE REPO ROOT, like the shell version's `cd "$ROOT"`. The first port dropped that and
// kept the relative paths, so run from anywhere but the root it found no baseline and no summary and
// exited 3 - "nothing in scope" - which reads as a clean tree rather than as a gate that could not
// look. A gate whose answer depends on the caller's working directory is worse than no gate.
process.chdir(resolve(dirname(fileURLToPath(import.meta.url)), '..'))

const BASELINE = 'docs/perf-baseline.tsv'
const SUMMARY = 'target/performance-throughput.txt'
const FAIL_BELOW = 0.70
const WARN_BELOW = 0.85
const STALE_ABOVE = 1.15

/** exit codes, per bin/check-all.sh's contract */
const VIOLATION = 1, CANNOT = 2, NOTHING_IN_SCOPE = 3

const die = (code, ...lines) => { console.error(lines.join('\n')); process.exit(code) }

if (!existsSync(BASELINE)) die(CANNOT, `check-throughput-regression: ${BASELINE} is missing - cannot compare.`)
if (!existsSync(SUMMARY) || !readFileSync(SUMMARY, 'utf8').trim()) {
  console.log(`check-throughput-regression: no ${SUMMARY} - the performance lane has not run here.`)
  process.exit(NOTHING_IN_SCOPE)
}

/** The baseline as meaning rather than as columns: one subject rate, and the control classes. */
const baseline = readFileSync(BASELINE, 'utf8').split('\n')
  .filter(l => l && !l.startsWith('#'))
  .map(l => l.split('\t'))
  .reduce((acc, [kind, name, value]) => {
    if (kind === 'rate') acc.subject = { name, rate: Number(value) }
    if (kind === 'class-seconds') acc.controls.set(name, Number(value))
    return acc
  }, { subject: null, controls: new Map() })

if (!baseline.subject) {
  die(CANNOT, `check-throughput-regression: ${BASELINE} has no 'rate' row - nothing to compare against.`)
}

// The subject name comes from a data file and is interpolated into a pattern, so it is escaped.
// Java class names cannot contain regex metacharacters today; the escape costs nothing and means the
// gate cannot be broken by editing a .tsv.
const escapeRe = s => s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
const observedRate = Number(
  [...readFileSync(SUMMARY, 'utf8').matchAll(
    new RegExp(`test=${escapeRe(baseline.subject.name)}\\s.*?recordsPerSecond=(-?\\d+)`, 'g'))].pop()?.[1] ?? 0)

if (!(observedRate > 0)) {
  // A missing or -1 rate is a real finding, not a quiet pass: either the test did not run or
  // ThroughputReport stopped being reached, and both look identical to a clean lane otherwise.
  die(CANNOT,
    `check-throughput-regression: no usable recordsPerSecond for ${baseline.subject.name} in ${SUMMARY}.`,
    '  Either it did not run, or ThroughputReport is no longer reached. Not treating this as a pass.')
}

/** Every failsafe report, keyed by the class name the XML itself declares. */
const observedSeconds = new Map(
  globSync('**/target/failsafe-reports/TEST-*.xml')
    .map(f => readFileSync(f, 'utf8').match(/<testsuite[^>]*\bname="(?:.*\.)?(\w+)"[^>]*\btime="([\d.]+)"/))
    .filter(Boolean)
    .map(([, name, time]) => [name, Number(time)]))

const ran = [...baseline.controls.keys()].filter(n => observedSeconds.has(n))
const skipped = [...baseline.controls.keys()].filter(n => !observedSeconds.has(n))

if (ran.length === 0) {
  // Without a neighbour there is no machine-speed proxy, and a raw comparison against an instrument
  // with this much spread is worse than none.
  die(CANNOT,
    'check-throughput-regression: no baseline neighbour class ran, so machine speed cannot be',
    '  cancelled. Refusing to compare raw numbers across machines - see this script header.')
}

const sum = (acc, n) => acc + n
const observedControl = ran.map(n => observedSeconds.get(n)).reduce(sum, 0)
const baselineControl = ran.map(n => baseline.controls.get(n)).reduce(sum, 0)

const machineIndex = baselineControl / observedControl
const expected = Math.round(baseline.subject.rate * machineIndex)
const ratio = expected > 0 ? observedRate / expected : 0

console.log(`check-throughput-regression: ${baseline.subject.name}`)
console.log(`  observed        ${observedRate} records/second`)
console.log(`  baseline        ${baseline.subject.rate} records/second`)
console.log(`  machine index   ${machineIndex.toFixed(4)}  (from ${ran.length} neighbour class(es): ${observedControl.toFixed(2)}s observed vs ${baselineControl.toFixed(2)}s baseline)`)
console.log(`  expected here   ${expected} records/second`)
console.log(`  RATIO           ${ratio.toFixed(3)}  (1.0 = exactly what the machine speed predicts)`)
if (skipped.length) console.log(`  skipped (did not run): ${skipped.join(' ')}`)

if (ratio < FAIL_BELOW) {
  console.log(`\nFAILED: ratio ${ratio.toFixed(3)} is below ${FAIL_BELOW}.`)
  console.log('Every regressed run ever measured here scored between 0.407 and 0.605; every healthy one')
  console.log('scored 0.778 or above. This is in the first band. Do not re-baseline to clear it without')
  console.log('establishing which of the two it is - node bin/perf-backfill.mjs shows you the history.')
  process.exit(VIOLATION)
}

if (ratio < WARN_BELOW) {
  console.log(`\nWARNING: ratio ${ratio.toFixed(3)} is below ${WARN_BELOW}. Slower than the neighbours explain, but`)
  console.log('inside the band where nobody has measured the normalised spread yet, so this does not fail')
  console.log('the lane. The slowest healthy run measured here scored 0.778. Re-run before dismissing it.')
} else if (ratio > STALE_ABOVE) {
  console.log(`\nBASELINE MAY BE STALE: ratio ${ratio.toFixed(3)} - this tree beat the baseline by more than 15%`)
  console.log('after normalising. One run does not establish that. If master keeps landing here, the')
  console.log('baseline is a floor the code has left behind:  node bin/perf-backfill.mjs --suggest-baseline')
} else {
  console.log('\nOK: within what machine speed accounts for.')
}
