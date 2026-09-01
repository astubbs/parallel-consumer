#!/usr/bin/env node
// Copyright (C) 2026 Antony Stubbs and contributors
//
// SPIKE: the Node counterpart of bin/perf-backfill.mjs, written to answer whether these data-shaping
// scripts belong in shell at all. Read the comparison in
// docs/inflight/ci-node-query-client.md before extending either one - two implementations of the same
// thing is the worst possible resting state, so this is a decision aid with a deadline, not a fork.
//
// IT IS NOT A TRANSLATION. Two things are different on purpose, both of which the shell version got
// wrong and neither of which is about the language:
//
//   1. IT KNOWS WHAT IT CANNOT SEE. Failsafe writes <testcase name= classname= time=> per METHOD, and
//      method time is the better instrument: it excludes container startup and @BeforeAll, so it does
//      not move when infrastructure does. Measured on one A/B pair, the class view put the subject at
//      +19% against neighbours flat to 1%; the method view put it at +22.5% against eight control
//      methods flat to 2.7%.
//
//      BUT THE PER-METHOD FIGURES ARE NOT IN A CI LOG. They exist only in target/failsafe-reports
//      XML, which a job log does not contain and which nothing uploads. So mining logs is
//      class-granular by construction, and this reads classes and says so rather than implying a
//      precision it cannot deliver. Uploading the failsafe XML from the performance lane - the chaos
//      lane already uploads exactly that - is what would make per-method history possible going
//      forward, and it costs one workflow step.
//   2. IT KEEPS EVERY METHOD, not just the subject and a neighbour sum. The sum was a lossy
//      projection chosen because summing in awk was easy; keeping the rows costs nothing here and
//      lets a later question be asked without re-mining the logs.
//
// WHY NODE AND NOT PYTHON, which is a fair question and is settled by the repo rather than by taste:
// .github/scripts/ already holds eight JS gate implementations, each with a .test.js sibling
// (file-ref-gate, issue-ref-gate, changelog-ref-gate, roadmap-stage-gate), against exactly one Python
// file in the whole tree. Node is the established second language here AND already carries the
// testing convention these scripts lack; Python would be the outlier. That is consistency, not a new
// dependency.
//
// WHAT SHELLING OUT STILL HAPPENS, so the comparison is honest: `gh` for the API (it owns auth) and
// `unzip` for the archive (Node has no bundled zip reader, and adding a dependency to bin/ is a bigger
// decision than this spike gets to make). Everything else - parsing, grouping, medians, TSV - is
// in-process, where the shell version spawned awk nine times, sed four and grep five.

import { execFileSync } from 'node:child_process'
import { mkdtempSync, rmSync, readdirSync, readFileSync, existsSync, mkdirSync, appendFileSync, writeFileSync } from 'node:fs'
import { tmpdir, homedir } from 'node:os'
import { join, dirname } from 'node:path'

const REPO = 'astubbs/parallel-consumer'
const SUBJECT = 'MultiInstanceHighVolumeTest'
const HISTORY = process.env.PC_PERF_HISTORY ?? join(homedir(), '.parallel-consumer', 'perf-history.tsv')

const sh = (cmd, args, opts = {}) =>
  execFileSync(cmd, args, { encoding: 'utf8', maxBuffer: 256 * 1024 * 1024, ...opts })

/** Every <testcase> in a failsafe job log's XML, as {clazz, method, seconds}. */
function methodsFrom(text) {
  const out = []
  // Failsafe prints "Tests run: ..., Time elapsed: N s -- in <fqcn>" per class, but the per-METHOD
  // figures only exist in the XML, which the job log does not contain. So the log route gives class
  // granularity and a local run gives method granularity - stated because it is the one place where
  // this script is weaker than reading a checkout's target/ directory, and it is not a fixable one.
  for (const m of text.matchAll(/Time elapsed: ([\d.]+) s[^-]*-- in [\w.]*\.(\w+)/g)) {
    out.push({ clazz: m[2], method: null, seconds: Number(m[1]) })
  }
  return out
}

function collect(maxRuns) {
  mkdirSync(dirname(HISTORY), { recursive: true })
  if (!existsSync(HISTORY)) {
    writeFileSync(HISTORY, '# run_id\tcreated\tbranch\tconclusion\tclazz\tseconds\trate\tsha\n')
  }
  const seen = new Set(
    readFileSync(HISTORY, 'utf8').split('\n').filter(l => l && !l.startsWith('#')).map(l => l.split('\t')[0]))

  const runs = sh('gh', ['run', 'list', '-R', REPO, '--workflow', 'maven.yml', '--limit', String(maxRuns),
    '--json', 'databaseId,createdAt,headBranch,conclusion,headSha',
    '--jq', '.[] | [.databaseId, .createdAt, .headBranch, (.conclusion // "running"), .headSha] | @tsv'])
    .split('\n').filter(Boolean).map(l => l.split('\t'))

  let added = 0, skipped = 0, nodata = 0
  for (const [id, created, branch, conclusion, sha] of runs) {
    if (seen.has(id)) { skipped++; continue }
    const work = mkdtempSync(join(tmpdir(), 'perf-'))
    try {
      const zip = join(work, 'logs.zip')
      try {
        writeFileSync(zip, sh('gh', ['api', `repos/${REPO}/actions/runs/${id}/logs`], { encoding: 'buffer' }))
      } catch { nodata++; continue }
      sh('unzip', ['-qq', '-o', zip, '-d', work])
      const log = readdirSync(work, { recursive: true })
        .find(f => /Performance Tests.*\.txt$/.test(String(f)))
      if (!log) { nodata++; continue }
      const rows = methodsFrom(readFileSync(join(work, String(log)), 'utf8'))
      if (rows.length === 0) { nodata++; continue }
      const rate = readFileSync(join(work, String(log)), 'utf8')
        .match(new RegExp(`PC-THROUGHPUT test=${SUBJECT} .*?recordsPerSecond=(-?\\d+)`))?.[1] ?? 'none'
      for (const r of rows) {
        appendFileSync(HISTORY, [id, created.slice(0, 19), branch, conclusion,
          r.clazz, r.seconds.toFixed(2), rate, sha.slice(0, 9)].join('\t') + '\n')
      }
      added++
    } finally { rmSync(work, { recursive: true, force: true }) }
  }
  console.log(`\n${added} added, ${skipped} already present, ${nodata} with no usable performance log`)
  console.log(`History: ${HISTORY}`)
}

function suggestBaseline() {
  // MASTER RUNS ONLY. The shell version's first real invocation proposed 43,552 rec/s - the REGRESSED
  // figure from an unmerged branch that dominated the sample - which would have LOWERED the baseline
  // and disarmed the check. A warning is not a guard, so this filters to trees that actually shipped.
  const rows = readFileSync(HISTORY, 'utf8').split('\n')
    .filter(l => l && !l.startsWith('#')).map(l => l.split('\t'))
    .filter(r => r[2] === 'master' && r[6] !== 'none')
  if (rows.length === 0) {
    console.error('No MASTER run carries a rate. A baseline may only come from a tree that shipped;')
    console.error('PR-branch runs are excluded so an unmerged regression cannot become the new normal.')
    process.exit(3)
  }
  // THE MEDIAN RUN, not the fastest - baselining on an outlier makes every ordinary run afterwards
  // read as a regression, which is how a performance gate earns its reputation and gets switched off.
  const byRun = new Map()
  for (const r of rows) if (!byRun.has(r[0])) byRun.set(r[0], Number(r[6]))
  const runs = [...byRun.entries()].sort((a, b) => a[1] - b[1])
  const [runId, rate] = runs[Math.floor((runs.length - 1) / 2)]

  const current = Number(readFileSync('docs/perf-baseline.tsv', 'utf8').split('\n')
    .find(l => l.startsWith('rate\t'))?.split('\t')[2] ?? 0)
  if (current && rate < current) {
    console.error(`REFUSING TO RECOMMEND: ${rate} is BELOW the current baseline of ${current}.`)
    console.error('  Raising a baseline is routine. Lowering one claims the product got slower and that')
    console.error('  this is acceptable, which is never a scripted decision. Edit it by hand and say why.')
    process.exit(1)
  }

  // ONE RUN'S NUMBERS, never a blend: a rate from one run normalised by class times from another is
  // not a comparison.
  console.log(`\n# Every row below is from run ${runId} - do not mix runs.`)
  console.log(`rate\t${SUBJECT}\t${rate}\trecords-per-second`)
  for (const r of rows.filter(r => r[0] === runId && r[4] !== SUBJECT)) {
    console.log(`class-seconds\t${r[4]}\t${r[5]}\tseconds`)
  }
  console.log('\nPaste into docs/perf-baseline.tsv ONLY if the product genuinely got faster.')
  console.log('A regression that has become the new normal produces exactly the same output.')
}

const args = process.argv.slice(2)
const suggest = args[0] === '--suggest-baseline'
collect(Number((suggest ? args[1] : args[0]) ?? 40))
if (suggest) suggestBaseline()
