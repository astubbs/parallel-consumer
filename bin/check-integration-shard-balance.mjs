#!/usr/bin/env node
//
// Copyright (C) 2026 Antony Stubbs and contributors
//
// Is the integration lane's shard partition still a good one?
//
// WHAT THIS IS FOR, and why it is not a balance ASSERTION. bin/ci-integration-test.sh carries ONE
// named class list plus a catch-all defined by subtraction, derived by longest-processing-time
// packing over measured per-class wall times. Those times drift: a test gets slower, a class is added, a scenario is
// split. The partition does not drift with them, and nothing goes red when it stops being good -
// the lane just quietly gets slower than it needs to be, which is the least visible kind of decay
// there is.
//
// So this does not check that the shards ARE balanced. It recomputes what the best partition would
// be from the times actually recorded by recent runs, and reports how much wall-clock the current
// one is leaving on the table. Every CI run feeds Codecov; every run therefore improves the signal
// that says when to re-derive the lists. That is the whole design: the thing that makes a static
// partition go stale is also the thing that measures its staleness.
//
// ADVISORY BY DEFAULT. A number derived from a shared runner's wall-clock is not stable enough to
// block a merge on - this repo has 119s of measured noise on a 620s lane - so a drifted partition
// prints and exits 0. `--fail-over <seconds>` makes it blocking for a caller that wants that, and
// is what a scheduled job would use rather than the PR gate.
//
// Exit codes follow the bin/ convention: 0 ran (whatever it found), 2 could not run, 3 nothing in
// scope. It needs the network (Codecov is public, no token) and is skipped offline rather than
// guessed at - a partition check that invents its input is worse than none.

import { execFileSync } from 'node:child_process'
import { readFileSync } from 'node:fs'
import { fileURLToPath } from 'node:url'
import { dirname, join } from 'node:path'
import { slowest } from './lib/codecov.mjs'

const HERE = dirname(fileURLToPath(import.meta.url))
const SCRIPT = join(HERE, 'ci-integration-test.sh')
const BUILD_OVERHEAD_SECONDS = 160 // serial build + job setup, re-paid by every shard
const FORKS = 4 // forkCount inside each shard; a measured ceiling, see the script's header

// A malformed --fail-over must not degrade to advisory. `Number(undefined)` is NaN, and every
// `drift > NaN` is false - so a scheduled job that asked to BLOCK would have exited 0 having
// gated on nothing, which is the silent-pass shape this directory's exit codes exist to prevent.
const failOverIdx = process.argv.indexOf('--fail-over')
let failOver = null
if (failOverIdx > -1) {
    if (process.argv.lastIndexOf('--fail-over') !== failOverIdx) {
        console.error('check-integration-shard-balance: --fail-over given more than once')
        process.exit(2)
    }
    const raw = process.argv[failOverIdx + 1]
    failOver = Number(raw)
    if (raw === undefined || raw.startsWith('--') || !Number.isFinite(failOver) || failOver < 0) {
        console.error(`check-integration-shard-balance: --fail-over needs one finite, non-negative threshold in seconds (got ${raw ?? 'nothing'})`)
        process.exit(2)
    }
}

// --- the partition as it is checked in -------------------------------------------------------
function heavyFromScript() {
    const m = readFileSync(SCRIPT, 'utf8').match(/readonly HEAVY_CLASSES="([^"]*)"/)
    return m ? m[1].split(',').filter(Boolean) : null
}

// --- packing ---------------------------------------------------------------------------------
// Longest-processing-time first. Used twice and for different things: to pack CLASSES into forks
// inside one shard, and to pack classes into SHARDS. Same algorithm, same reason - the long jobs
// have to go first or the tail cannot pack tight.
function lpt(items, bins) {
    const b = Array.from({ length: bins }, () => [])
    const sums = new Array(bins).fill(0)
    for (const it of [...items].sort((x, y) => y.seconds - x.seconds)) {
        const i = sums.indexOf(Math.min(...sums))
        b[i].push(it)
        sums[i] += it.seconds
    }
    return { bins: b, sums }
}

// A shard's wall is its classes packed across FORKS - NOT its total work. A shard holding one
// 356s class is 356s wide however little else is in it, which is the whole reason the 857 probe
// dominated this lane before it was split.
const shardWall = (classes) => Math.max(...lpt(classes, FORKS).sums) + BUILD_OVERHEAD_SECONDS
const criticalPath = (shards) => Math.max(...shards.map(shardWall))

// --- main ------------------------------------------------------------------------------------
const heavy = heavyFromScript()
if (!heavy) {
    console.error('check-integration-shard-balance: could not read HEAVY_CLASSES from bin/ci-integration-test.sh')
    process.exit(2)
}

// The integration classes THIS TREE actually has, by declaration rather than by filename - the
// same reason ci-integration-test.sh enumerates declarations: several package-private top-level
// classes may share one file, and the partition operates on compiled classes.
function classesInTree() {
    // Scoped by PACKAGE, mirroring ci-integration-test.sh's `-path '*/integrationTest*/*'` and the
    // pom's collection pattern - NOT by `src/test-integration/`, which is one module's layout. The
    // examples modules keep their integration tests in `src/test/java/.../integrationTests/`, so a
    // directory-based scope silently dropped CoreAppMetricsIntegrationTest and StreamsAppTest and
    // understated catch-all work - the same one-module blind spot the shard report check had.
    const files = execFileSync('git', ['ls-files'], { encoding: 'utf8' })
        .split('\n')
        .filter((f) => f.endsWith('.java') && /\/integrationTest[^/]*\//.test(f))
    const found = new Set()
    for (const f of files) {
        for (const m of readFileSync(f, 'utf8').matchAll(/^([a-z]+ )*class (\w+)/gm)) {
            if (!m[0].includes('abstract')) found.add(m[2])
        }
    }
    return found
}

// NOT branch-scoped, and that is measured rather than assumed. Scoping to the default branch is the
// obvious fix for "the newest observation for a class can come from any branch", but there is no
// default-branch corpus to scope TO: api.codecov.io reports count=0 for branch=master AND for
// branch=main, and 0 of 12000 sampled rows across 5 branches are on master. Test analytics here is
// uploaded by PR builds under their own head branch. `slowest(4000, { branch: 'master' })` therefore
// returns nothing and this checker exits 3 forever - it stops measuring, which is worse than the
// drift. The harm that scoping was meant to prevent is handled below instead, by intersecting
// against the classes this tree actually has: a class that is branch-only, renamed or deleted
// cannot then be modelled as catch-all work, whichever branch observed it.
const res = slowest(4000)
if (!res.ok) {
    // `reason`, not `error` - every failure path in lib/codecov.mjs sets that field, so reading
    // `error` printed "(unknown)" every time and hid HTTP and empty-corpus failures, which need
    // different remedies from being offline.
    console.log(`check-integration-shard-balance: could not reach Codecov (${res.reason ?? 'unknown'}) - skipped`)
    process.exit(2)
}

// Integration classes only, and by CLASS: the shards select classes, so per-test rows have to be
// summed back up to the unit the partition actually operates on.
const inTree = classesInTree()
const perClass = new Map()
for (const r of res.value.rows) {
    if (!(r.flags ?? []).includes('integration')) continue
    const cls = r.name.split('::')[0].split('.').pop()
    if (!inTree.has(cls)) continue
    perClass.set(cls, (perClass.get(cls) ?? 0) + r.seconds)
}
if (perClass.size === 0) {
    console.log('check-integration-shard-balance: no integration timings recorded yet - nothing in scope')
    process.exit(3)
}

const all = [...perClass].map(([name, seconds]) => ({ name, seconds }))
const named = new Set(heavy)
const current = [all.filter((c) => named.has(c.name)), all.filter((c) => !named.has(c.name))]
// The best achievable TWO-way split, found by trying every "largest N classes" heavy set. That is
// the shape the guide in ci-integration-test.sh tells a maintainer to pick, so the comparison is
// against a partition they could actually choose - not against an unconstrained optimum.
let optimal = current
let bestCp = Infinity
const bySize = [...all].sort((a, b) => b.seconds - a.seconds)
for (let k = 1; k < Math.min(bySize.length, 20); k++) {
    const hn = new Set(bySize.slice(0, k).map((c) => c.name))
    const cand = [all.filter((c) => hn.has(c.name)), all.filter((c) => !hn.has(c.name))]
    const cp = criticalPath(cand)
    if (cp < bestCp) { bestCp = cp; optimal = cand }
}

const now = criticalPath(current)
const best = criticalPath(optimal)
const drift = now - best

console.log(`check-integration-shard-balance: ${perClass.size} integration classes with recorded times`)
current.forEach((s, i) => {
    const label = i === 0 ? 'heavy' : 'catch-all'
    console.log(`  ${label.padEnd(10)} ${String(Math.round(shardWall(s))).padStart(4)}s wall  ${s.length} classes  ${Math.round(s.reduce((t, c) => t + c.seconds, 0))}s work`)
})
// The number the guide tells a maintainer to compare a candidate class against.
const catchAllBound = current[1].reduce((t, c) => t + c.seconds, 0) / FORKS
console.log(`  a class is worth moving to HEAVY_CLASSES only if its own wall exceeds ${Math.round(catchAllBound)}s`)
console.log(`  current critical path ${Math.round(now)}s | best two-way split ${Math.round(best)}s | drift ${Math.round(drift)}s`)

// Classes with recorded times that no shard claims are fine - the catch-all has them by
// construction. Classes NAMED but never recorded are not: that is a rename or a deletion, and the
// shard's own report check will fail on it at run time. Saying so here is cheaper than a red build.
const stale = [...named].filter((n) => !perClass.has(n))
if (stale.length) {
    console.log(`  NAMED BUT NEVER RECORDED (renamed or deleted?): ${stale.join(', ')}`)
}
if (res.value.truncated) {
    console.log('  NOTE: hit the Codecov page bound, so this is not the whole history - drift may be understated.')
}

if (failOver !== null && drift > failOver) {
    console.error(`check-integration-shard-balance: drift ${Math.round(drift)}s exceeds --fail-over ${failOver}s`)
    console.error('  Re-derive HEAVY_CLASSES in bin/ci-integration-test.sh from current timings.')
    process.exit(1)
}
process.exit(0)
