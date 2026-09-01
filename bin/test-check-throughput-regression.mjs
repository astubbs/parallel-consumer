#!/usr/bin/env node
// Copyright (C) 2026 Antony Stubbs and contributors
//
// Self-test for bin/check-throughput-regression.mjs, using REAL numbers from real CI runs.
//
// The cases are not invented. Each is an observation recovered by bin/perf-backfill.mjs from a run
// inside GitHub's log-retention window, so this file is simultaneously the check's test and the
// evidence its thresholds came from - a threshold justified only in a commit message is one nobody
// can re-derive.
//
// IT IS ALSO THE PORT'S PROOF. The shell version passed exactly these six cases with these exit
// codes; the Node version must too. A port that changes behaviour while claiming to change only
// language is the failure this pins down.
//
// Per-class seconds for the regressed cases are apportioned from the recorded neighbour TOTAL in the
// baseline's proportions. The check sums matched classes, so the total is what it reads and the split
// is presentational - said here rather than left for somebody to find the numbers are not verbatim.

import { execFileSync } from 'node:child_process'
import { mkdtempSync, mkdirSync, writeFileSync, copyFileSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

const CHECK = join(process.cwd(), 'bin', 'check-throughput-regression.mjs')
const BASE = join(process.cwd(), 'docs', 'perf-baseline.tsv')
let failures = 0

function runCase(desc, expected, rate, very, large, load) {
  const dir = mkdtempSync(join(tmpdir(), 'thr-'))
  try {
    mkdirSync(join(dir, 'bin'), { recursive: true })
    mkdirSync(join(dir, 'docs'), { recursive: true })
    mkdirSync(join(dir, 'target'), { recursive: true })
    const reports = join(dir, 'parallel-consumer-core', 'target', 'failsafe-reports')
    mkdirSync(reports, { recursive: true })
    copyFileSync(CHECK, join(dir, 'bin', 'check-throughput-regression.mjs'))
    copyFileSync(BASE, join(dir, 'docs', 'perf-baseline.tsv'))

    // An EMPTY rate means the summary EXISTS but carries no usable figure - a broken lane. That is a
    // different case from no summary at all, which is a clean tree, and the two must not collapse.
    writeFileSync(join(dir, 'target', 'performance-throughput.txt'),
      rate === null
        ? '# machine cpu=synthetic cores=2 memkb=1\n'
        : `PC-THROUGHPUT test=MultiInstanceHighVolumeTest processed=3000000 expected=3000000 elapsedMs=1 recordsPerSecond=${rate} outcome=X\n`)

    for (const [name, secs] of [['VeryLargeMessageVolumeTest', very],
                                ['LargeVolumeInMemoryTests', large],
                                ['LoadTest', load]]) {
      if (secs === null) continue
      writeFileSync(join(reports, `TEST-x.${name}.xml`),
        `<?xml version="1.0"?>\n<testsuite name="x.${name}" time="${secs}" tests="1"/>\n`)
    }

    let rc = 0, out = ''
    try { out = execFileSync('node', ['bin/check-throughput-regression.mjs'], { cwd: dir, encoding: 'utf8' }) }
    catch (e) { rc = e.status ?? 1; out = `${e.stdout ?? ''}${e.stderr ?? ''}` }

    const ratio = out.match(/RATIO\s+([\d.]+)/)?.[1] ?? ''
    if (rc === expected) console.log(`ok    ${desc.padEnd(52)} exit ${rc}  ${ratio && 'ratio=' + ratio}`)
    else { console.error(`FAIL  ${desc.padEnd(52)} expected ${expected}, got ${rc}\n${out}`); failures++ }
  } finally { rmSync(dir, { recursive: true, force: true }) }
}

// MUST FAIL - run 33478449495 on astubbs/parallel-consumer#29: 43,552 rec/s, 134.65s of neighbours.
runCase('regressed: astubbs#29 at 43,552 (the real one)', 1, 43552, 53.47, 40.18, 40.99)
// MUST FAIL - the worst observed. If the coarse end stops failing, the check is broken.
runCase('regressed: worst observed, 29,372', 1, 29372, 55.81, 41.95, 42.79)
// MUST PASS - the same branch after the one-line fix, run 33487673494: 76,950 against 131.88s.
runCase('healthy: astubbs#29 after the fix, 76,950', 0, 76950, 52.37, 39.36, 40.15)
// MUST PASS - slowest healthy run seen, 57,215. Warns, must not fail: a warning is "look at this".
runCase('healthy but slow: 57,215 (warns, must not fail)', 0, 57215, 54.78, 41.17, 41.99)
// MUST NOT PASS - summary present, no usable rate. Exit 2, never 0.
runCase('summary exists but carries no rate', 2, null, 51.69, 38.85, 39.63)
// MUST NOT PASS - no neighbour, so machine speed cannot be cancelled.
runCase('no neighbour class ran', 2, 77960, null, null, null)

if (failures === 0) { console.log('\nAll check-throughput-regression self-tests passed'); process.exit(0) }
console.error(`\n${failures} check-throughput-regression self-test(s) failed`)
process.exit(1)
