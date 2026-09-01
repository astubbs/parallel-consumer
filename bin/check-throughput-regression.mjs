#!/usr/bin/env node
// Copyright (C) 2026 Antony Stubbs and contributors
//
// Compares this run's throughput against RECENT MASTER RUNS, read from their artifacts, and writes a
// report the workflow posts to the PR.
//
// THERE IS NO COMMITTED BASELINE, DELIBERATELY. There was: docs/perf-baseline.tsv, one run's numbers
// updated by hand. Three things were wrong with it and all three are structural rather than fixable.
// It rots - a number nobody re-measures is trusted for as long as nobody looks. It cannot answer "how
// much does this vary", which is the only question that makes a threshold meaningful. And comparing
// against a figure from months ago is worse than re-measuring, because the machine, the runner image
// and the test have all moved since. If you want to know how a release compares, run the test on that
// release; do not trust a number somebody wrote down.
//
// So the reference is the last N `perf baseline (master)` runs, whose artifacts carry both the rate
// and the per-class times. No table to maintain, and the data is as fresh as the last push to master.
//
// THE MEDIAN, NOT THE LAST RUN, AND THIS IS THE WHOLE POINT. Measured 2026-09-01 on an idle machine:
// measured on one unchanged commit, the raw subject moves ~13% while the
// sleeping controls move ~1% and the CPU-bound one ~6% - and normalising by any of them RAISES the
// spread to ~17%. See the measurement table in lib/throughput-verdict.mjs. Against an instrument that noisy, comparing
// against any SINGLE previous run is a coin flip, and a threshold drawn around one point is drawn
// around noise. A median over several runs is the cheapest thing that is not.
//
// It also explains the thresholds. A 20% regression cannot be separated from that spread by any
// bound, so pretending otherwise would produce a gate that fires on quiet Tuesdays and gets switched
// off within a week. Operator ruling, 2026-09-01: fail at a 50% loss, flag at 30%. The fail line sits
// OUTSIDE the measured noise; the flag line sits at its edge and says so rather than claiming to be a
// verdict.
//
// MACHINE SPEED IS STILL CANCELLED USING THE NEIGHBOURS, and its weakness is now measured rather than
// assumed: measured on one unchanged commit, the raw subject moves ~13% while the
// sleeping controls move ~1% and the CPU-bound one ~6% - and normalising by any of them RAISES the
// spread to ~17%. See the measurement table in lib/throughput-verdict.mjs. The normalisation removes machine-to-machine drift, which
// is real; it does not remove this test's own variance, which is larger. That is why the bounds are
// coarse and why the report says what the allowable range is rather than only whether it passed.
//
// EXIT CODES follow bin/check-all.sh: 0 pass, 1 violation, 2 cannot run, 3 nothing in scope.

import { execFileSync } from 'node:child_process'
import { readFileSync, writeFileSync, existsSync, globSync, mkdtempSync, rmSync, readdirSync, mkdirSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join, dirname, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'
import { verdictFor, FAIL_BELOW, WARN_BELOW } from './lib/throughput-verdict.mjs'

// Anchored like the shell original's `cd "$ROOT"`. Without it the relative paths below resolve
// against the caller's directory, and the gate reports "nothing in scope" - a clean tree - when what
// actually happened is that it could not look.
process.chdir(resolve(dirname(fileURLToPath(import.meta.url)), '..'))

const REPO = process.env.PC_REPO ?? 'astubbs/parallel-consumer'
const SUBJECT = 'MultiInstanceHighVolumeTest'
// CPU-BOUND CONTROLS ONLY - see the premise note in lib/throughput-verdict.mjs. LoadTest and
// LargeVolumeInMemoryTests are excluded because they sleep per record (0-5ms and 3ms), so their
// wall-clock does not scale with the machine and they cannot serve as a machine-speed reference.
// One honest control beats three where two are inert.
const CONTROLS = ['VeryLargeMessageVolumeTest']
const REFERENCE_RUNS = Number(process.env.PC_REFERENCE_RUNS ?? 10)
const REPORT = 'target/throughput-report.md'

const VIOLATION = 1, CANNOT = 2, NOTHING_IN_SCOPE = 3
const sh = (c, a, o = {}) => execFileSync(c, a, { encoding: 'utf8', maxBuffer: 256 * 1024 * 1024, ...o })

/**
 * Per-METHOD seconds from a failsafe XML set, summed per class.
 *
 * Not the <testsuite> aggregate, and that is the conservation argument rather than a nicety: a class
 * time is `work + setup`, and container startup and @BeforeAll do not scale with the work done, so
 * they are the non-conserved term. Leaving them in breaks the invariant the comparison rests on.
 */
const methodSecondsFrom = files => {
  const byClass = new Map()
  const caseNames = new Set()
  for (const f of files) {
    for (const m of readFileSync(f, 'utf8')
      .matchAll(/<testcase[^>]*\bname="([^"]*)"[^>]*\bclassname="(?:.*\.)?(\w+)"[^>]*\btime="([\d.]+)"/g)) {
      byClass.set(m[2], (byClass.get(m[2]) ?? 0) + Number(m[3]))
      caseNames.add(`${m[2]}#${m[1]}`)
    }
  }
  // THE CASE SET, not just the totals. Summing every <testcase> means a run that GAINED a case has a
  // larger denominator for a reason that is not performance - a parameterised control picking up one
  // more @EnumSource value makes the ratio look healthier and can mask a real subject regression.
  // Comparing the identities refuses that silently-wrong comparison instead of averaging it in.
  byClass.set('__cases__', [...caseNames].sort().join('|'))
  return byClass
}

const rateFrom = text =>
  Number([...text.matchAll(new RegExp(`test=${SUBJECT}\\s.*?recordsPerSecond=(-?\\d+)`, 'g'))].pop()?.[1] ?? 0)

// EVERY EXIT WRITES A REPORT. Defined here, above the first early return, because that is the whole
// bug it exists to prevent: the first attempt at "always write a report" only covered the empty-runs
// path, while the exit that actually fires in practice - the bootstrapping 404 below - returned
// earlier and wrote nothing. Two exits, one reporting, and the silent one was the common case. If you
// add an exit, it writes a report or it is the same bug again.
// EVERY report carries a machine-readable payload, and that is not decoration. The PR comment is
// updated in place, so without it each push destroys the only record of what the previous push
// measured, and the reader gets a number with nothing to compare it to. The workflow reads this back
// off its own last comment to render a delta and to notice when the STATUS changed.
//
// The status is set into `reportStatus` on the line before each call rather than passed as an
// argument. That is deliberate: the report bodies are template literals containing escaped
// backticks, and an earlier attempt to add a second argument landed the status INSIDE the prose of
// all six messages, because the closing `) it matched was an escaped one in the text. A separate
// assignment cannot go wrong that way.
let reportStatus = 'unset'
const writeReport = (body, data = {}) => {
  mkdirSync('target', { recursive: true })
  const payload = JSON.stringify({ status: reportStatus, ...data })
  writeFileSync(REPORT, `${body}\n\n<!-- pc-throughput-data: ${payload} -->\n`)
  console.log(body.replace(/[|*#]/g, '').replace(/\n{2,}/g, '\n'))
}

// ---- this run -----------------------------------------------------------------------------------
const summaryFile = 'target/performance-throughput.txt'
if (!existsSync(summaryFile) || !readFileSync(summaryFile, 'utf8').trim()) {
  console.log(`check-throughput-regression: no ${summaryFile} - the performance lane has not run here.`)
  process.exit(NOTHING_IN_SCOPE)
}
const observedRate = rateFrom(readFileSync(summaryFile, 'utf8'))
const observedClasses = methodSecondsFrom(globSync('**/target/failsafe-reports/TEST-*.xml'))
const observedControl = CONTROLS.filter(c => observedClasses.has(c))
  .reduce((a, c) => a + observedClasses.get(c), 0)
const observedSubject = observedClasses.get(SUBJECT) ?? 0

if (!(observedRate > 0)) {
  // A missing rate is a finding, not a quiet pass: the test did not run, or the emitter is no longer
  // reached, and both look identical to a clean lane otherwise.
  console.error(`check-throughput-regression: no usable rate for ${SUBJECT} in ${summaryFile}.`)
  reportStatus = 'no-rate'
  writeReport(`### 🟠 Throughput — no rate to report

The performance lane ran but produced no usable \`recordsPerSecond\` for \`${SUBJECT}\`. Either the test did not run, or \`ThroughputReport\` is no longer reached.

**Not a pass** — both of those look identical to a clean lane otherwise, which is exactly why this is reported rather than skipped.`)
  process.exit(CANNOT)
}
if (!(observedControl > 0)) {
  console.error('check-throughput-regression: no control class ran, so machine speed cannot be cancelled.')
  reportStatus = 'no-control'
  writeReport(`### 🟠 Throughput — no control ran

| | |
|---|---|
| **This run** | ${observedRate} rec/s |

None of the control classes (\`${CONTROLS.join(', ')}\`) produced a time, so machine speed cannot be cancelled and no comparison is possible. The raw number above is recorded but comparable to nothing.

**Not a pass.**`)
  process.exit(CANNOT)
}

// ---- the reference: recent master runs, from their artifacts -------------------------------------
let runs = []
try {
  runs = sh('gh', ['run', 'list', '-R', REPO, '--workflow', 'perf-baseline.yml',
    '--branch', 'master', '--limit', String(REFERENCE_RUNS),
    '--json', 'databaseId,headSha', '--jq', '.[] | [.databaseId, .headSha] | @tsv'])
    .split('\n').filter(Boolean).map(l => l.split('\t'))
} catch (e) {
  // BOOTSTRAPPING IS NOT A FAILURE, and the first version could not tell the two apart. `gh run list
  // --workflow` resolves the workflow FILE against the DEFAULT BRANCH, so while perf-baseline.yml
  // exists only on this branch the call 404s - and a blanket catch reported that as "gh unavailable
  // or unauthenticated", exited 2 (cannot run), and failed the lane. The PR that introduces the
  // workflow therefore always failed its own check, blaming the wrong thing.
  const err = `${e?.stderr ?? ''}${e?.stdout ?? ''}${e?.message ?? ''}`
  if (/404|could not find any workflows|not found/i.test(err)) {
    reportStatus = 'bootstrapping'
    writeReport(`### ⚪ Throughput — no reference yet (bootstrapping)

| | |
|---|---|
| **This run** | ${observedRate} rec/s |
| Subject time | ${observedSubject.toFixed(1)}s |
| Control time | ${observedControl.toFixed(1)}s |

\`perf-baseline.yml\` is not on the default branch yet, so there are no master runs to compare against. **This is the bootstrapping state, not a fault** — it resolves once this workflow lands on master and runs once.

The numbers above are this run's, recorded here rather than left in a job log.`)
    process.exit(NOTHING_IN_SCOPE)
  }
  const firstLine = err.trim().split('\n')[0]
  console.error('check-throughput-regression: could not list master baseline runs.')
  console.error(`  ${firstLine}`)
  reportStatus = 'check-failed'
  writeReport(`### 🟠 Throughput — the check could not run

| | |
|---|---|
| **This run** | ${observedRate} rec/s |
| Subject time | ${observedSubject.toFixed(1)}s |

Could not list master baseline runs, so no comparison was made:

\`\`\`
${firstLine}
\`\`\`

**This is not a pass.** A check that could not look must say so rather than stay silent.`)
  process.exit(CANNOT)
}
// A REPORT IS WRITTEN EVEN WITH NO REFERENCE. The first version wrote one only when it could compute a
// verdict, so on a repository where the master lane has never run - which is every repository until
// this lands - the check exited quietly and the PR comment never appeared at all. The PR that adds
// throughput reporting displayed no throughput report. A report saying "here are this run's numbers,
// there is nothing to compare them against yet" is the useful thing to post, not silence.
if (runs.length === 0) {
  reportStatus = 'no-reference'
  writeReport(`### ⚪ Throughput — no reference yet

| | |
|---|---|
| **This run** | ${observedRate} rec/s |
| Subject time | ${observedSubject.toFixed(1)}s |
| Control time | ${observedControl.toFixed(1)}s |

Nothing to compare against: the \`perf baseline (master)\` workflow has not run on master yet, so there are no reference artifacts. **This is bootstrapping, not a fault** — the reference builds itself once that workflow runs, which cannot happen until this PR lands.

Recorded here so the numbers are visible now rather than only in a job log.`)
  process.exit(NOTHING_IN_SCOPE)
}

const work = mkdtempSync(join(tmpdir(), 'thr-'))
const reference = []
const incomplete = []
const mismatched = []
const observedCases = observedClasses.get('__cases__')
try {
  for (const [id, sha] of runs) {
    const dir = join(work, id); mkdirSync(dir, { recursive: true })
    try { sh('gh', ['run', 'download', id, '-R', REPO, '-D', dir]) } catch { continue }
    const files = readdirSync(dir, { recursive: true }).map(f => join(dir, String(f)))
    const summary = files.find(f => f.endsWith('performance-throughput.txt'))
    const xml = files.filter(f => /TEST-.*\.xml$/.test(f))
    if (!summary || xml.length === 0) continue
    const rate = rateFrom(readFileSync(summary, 'utf8'))
    const classes = methodSecondsFrom(xml)
    const control = CONTROLS.filter(c => classes.has(c)).reduce((a, c) => a + classes.get(c), 0)
    const subject = classes.get(SUBJECT) ?? 0
    // COMPLETE RUNS ONLY. `if: always()` uploads artifacts from a FAILED lane too, which may hold a
    // subset of the reports. A partial control set gives an incomparable denominator and can move the
    // median enough to invent a pass or a regression, so every expected control must be present.
    const missing = CONTROLS.filter(c => !classes.has(c))
    const caseSet = classes.get('__cases__')
    if (observedCases && caseSet && caseSet !== observedCases) {
      mismatched.push(sha.slice(0, 9))
      continue
    }
    if (subject > 0 && control > 0 && missing.length === 0) {
      reference.push({ id, sha: sha.slice(0, 9), rate, subject, control })
    } else if (missing.length) {
      incomplete.push(`${sha.slice(0, 9)} (missing ${missing.join(', ')})`)
    }
  }
} finally { rmSync(work, { recursive: true, force: true }) }

if (reference.length === 0) {
  // "No comparable reference" has been read as "no baseline exists". Baselines usually DO exist here
  // - they just ran a different set of tests, most often because the PR itself enabled or disabled
  // some. Say how many were found and what this run's case set is, so the reader can tell a matrix
  // change they made from one they did not.
  const why = mismatched.length
    ? `${mismatched.length} baseline run(s) were found and none ran the same set of test cases as this run (${mismatched.join(', ')}), so none is comparable. This is usually the PR's own doing: enabling or disabling a test in this lane changes the matrix.`
    : 'master baseline runs exist but carry no usable artifact yet'
  reportStatus = 'incomparable'
  writeReport(`### ⚪ Throughput — no comparable reference

| | |
|---|---|
| **This run** | ${observedRate} rec/s |
| Subject time | ${observedSubject.toFixed(1)}s |

${why}.

Refusing to compare against a run whose workload differs, rather than averaging the difference in and calling the result a verdict.`)
  process.exit(NOTHING_IN_SCOPE)
}

const v = verdictFor({ subject: observedSubject, control: observedControl }, reference)
const { observedShare, referenceShare, ratio, icon, word } = v
const pct = n => `${(n * 100).toFixed(0)}%`
const verdict = { icon, word, exit: v.failed ? VIOLATION : 0 }

const shares = reference.map(r => (r.subject / r.control))
const spread = `${Math.min(...shares).toFixed(3)} – ${Math.max(...shares).toFixed(3)}`

const report = `### ${icon} Throughput — ${word}

| | |
|---|---|
| **Subject share, this run** | ${observedShare.toFixed(3)} |
| **Reference share (median)** | ${referenceShare.toFixed(3)} |
| **Ratio** | **${ratio.toFixed(3)}** |
| Subject time | ${observedSubject.toFixed(1)}s |
| Control time | ${observedControl.toFixed(1)}s |
| Reported rate | ${observedRate} rec/s |

**Allowable range** 🟢 ≥ ${WARN_BELOW.toFixed(2)} · 🟡 ${FAIL_BELOW.toFixed(2)}–${WARN_BELOW.toFixed(2)} (about a ${pct(1 - WARN_BELOW)} loss) · 🔴 < ${FAIL_BELOW.toFixed(2)} (about a ${pct(1 - FAIL_BELOW)} loss)

<details><summary>How this is derived, and what it cannot tell you</summary>

**By conservation, not by correction.** Every test in this lane processes a fixed number of records, so within one run the ratio of one test's time to another's is invariant under machine speed — a runner twice as slow doubles both terms and leaves the ratio alone. There is no machine-index correction to be wrong, because nothing needed correcting. \`share = subjectSeconds / controlSeconds\`, both from this same run.

**Per-method times, not class times.** A class time is \`work + setup\`, and container startup and \`@BeforeAll\` do not scale with work — they are the non-conserved term, and leaving them in breaks the invariant.

**Reference is the median of ${reference.length} recent \`perf baseline (master)\` run(s)**, read from their artifacts. There is no committed baseline to go stale, and a share is dimensionless, so an old entry stays comparable to a new one without re-baselining. Shares observed: ${spread}.

**What this still cannot do.** It removes machine-to-machine variance. It does **not** remove this test's own run-to-run variance, measured at about 30% on a single unchanged commit while its controls stayed within 5%. That is a property of the test, not of the comparison, and no arithmetic here can touch it — which is why the reference is a median and the bounds are deliberately coarse. 🟡 means look at this; only 🔴 is outside the measured spread.

Runs used: ${reference.map(r => r.sha).join(', ')}
</details>`

reportStatus = word
writeReport(report, { ratio: Number(ratio.toFixed(3)), rate: observedRate,
                      share: Number(observedShare.toFixed(3)), refs: reference.length })
console.log(`\nreport written to ${REPORT}`)
process.exit(verdict.exit)
