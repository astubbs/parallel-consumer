#!/usr/bin/env node
// Copyright (C) 2026 Antony Stubbs and contributors
//
// Self-test for bin/check-integration-shard-balance.mjs.
//
// This script shipped two silent-pass bugs before it had a test, both found by review rather than by
// anything red: a malformed `--fail-over` degraded to NaN so a job that asked to BLOCK exited 0 having
// gated on nothing, and the could-not-run diagnostic read a field `lib/codecov.mjs` never sets, so it
// printed "(unknown)" every time. bin/AGENTS.md says a checker's fixes get locked in by its self-test;
// this is that lock. The pure parts - argument parsing and the LPT packing the drift number rests on -
// are imported and pinned directly. The process contract - exit 2 on a malformed flag BEFORE any
// network is considered, exit 3 without the opt-in, main() still running through a symlink - is
// pinned by spawning the real script.
//
// It never reaches Codecov: SHARD_BALANCE_NETWORK is deleted from the spawned environment, so the
// furthest the script can get is the opt-in guard. The network path is covered by the Repo Hygiene
// job, the one caller that opts in.

import { spawnSync } from 'node:child_process'
import { mkdtempSync, rmSync, symlinkSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'
import { BUILD_OVERHEAD_SECONDS, FORKS, criticalPath, lpt, parseFailOver, shardWall } from './check-integration-shard-balance.mjs'

const HERE = dirname(fileURLToPath(import.meta.url))
const GATE = join(HERE, 'check-integration-shard-balance.mjs')

let failures = 0
const check = (desc, actual, expected) => {
    if (JSON.stringify(actual) === JSON.stringify(expected)) console.log(`ok    ${desc}`)
    else { console.error(`FAIL  ${desc}\n        expected: ${JSON.stringify(expected)}\n        actual:   ${JSON.stringify(actual)}`); failures++ }
}
const contains = (desc, haystack, needle) => {
    if (haystack.includes(needle)) console.log(`ok    ${desc}`)
    else { console.error(`FAIL  ${desc}\n        expected to contain: ${JSON.stringify(needle)}\n        actual:\n${haystack}`); failures++ }
}

// --- --fail-over parsing: every malformed shape is an error, never NaN --------------------------
const argv = (...rest) => ['node', GATE, ...rest]
check('absent flag means no threshold', parseFailOver(argv()), { failOver: null })
check('a valid threshold parses', parseFailOver(argv('--fail-over', '30')), { failOver: 30 })
check('zero is a valid threshold (fail on any drift)', parseFailOver(argv('--fail-over', '0')), { failOver: 0 })
check('a missing value is an error, not NaN', 'error' in parseFailOver(argv('--fail-over')), true)
check('a non-numeric value is an error', 'error' in parseFailOver(argv('--fail-over', 'abc')), true)
check('a negative value is an error', 'error' in parseFailOver(argv('--fail-over', '-5')), true)
check('a following flag is not a value', 'error' in parseFailOver(argv('--fail-over', '--other')), true)
check('the flag given twice is an error', 'error' in parseFailOver(argv('--fail-over', '30', '--fail-over', '40')), true)
contains('  ...and the error names what it got', parseFailOver(argv('--fail-over', 'abc')).error, 'abc')

// --- LPT packing: longest first onto the emptiest bin, against a hand-checkable optimum ------------
// Six classes over two bins. Sorted: 9,7,5,4,3,2 -> bin0 9,4,2 (15); bin1 7,5,3 (15). Optimal split.
const six = [2, 9, 4, 7, 3, 5].map((seconds, i) => ({ name: `C${i}`, seconds }))
const packed = lpt(six, 2)
check('lpt packs six classes over two bins to the optimal 15/15', packed.sums.slice().sort(), [15, 15])
check('  ...and places every class exactly once', packed.bins.flat().length, 6)
// One class larger than everything else together: it owns a bin alone, the rest share the other.
const skewed = [{ name: 'big', seconds: 100 }, ...[10, 10, 10].map((s, i) => ({ name: `s${i}`, seconds: s }))]
check('a dominant class gets a bin to itself', lpt(skewed, 2).sums.slice().sort((a, b) => b - a), [100, 30])

// --- shard wall and critical path: max(slowest fork) + fixed overhead ----------------------------
// FORKS forks, one class each: the wall is the slowest class plus the overhead every shard re-pays.
const oneEach = Array.from({ length: FORKS }, (_, i) => ({ name: `f${i}`, seconds: 10 * (i + 1) }))
check('a shard of one class per fork walls at its slowest class plus overhead', shardWall(oneEach), 10 * FORKS + BUILD_OVERHEAD_SECONDS)
check('the critical path is the slower shard', criticalPath([oneEach, [{ name: 'x', seconds: 1 }]]), 10 * FORKS + BUILD_OVERHEAD_SECONDS)

// --- the process contract --------------------------------------------------------------------------
const env = { ...process.env }
delete env.SHARD_BALANCE_NETWORK
const run = (args, gatePath = GATE) => spawnSync('node', [gatePath, ...args], { encoding: 'utf8', env })

let r = run(['--fail-over', 'abc'])
check('a malformed --fail-over exits 2 before the network is even considered', r.status, 2)
contains('  ...naming the flag', r.stderr, '--fail-over')

r = run([])
check('without SHARD_BALANCE_NETWORK the gate is nothing-in-scope (exit 3)', r.status, 3)
contains('  ...and says the read is opt-in', r.stdout, 'opt-in')

r = run(['--fail-over', '30'])
check('a valid --fail-over still cannot reach the network without the opt-in (exit 3)', r.status, 3)

// Invoked through a symlink: the realpath guard means main() still runs. Exit 3 is the correct
// answer here; the spelling comparison this guard replaced produced exit 0 with no output.
const linkDir = mkdtempSync(join(tmpdir(), 'balance-link-'))
const link = join(linkDir, 'drift.mjs')
symlinkSync(GATE, link)
r = run([], link)
check('a symlinked invocation still runs main() (exit 3 with output, not silent exit 0)', r.status, 3)
check('  ...and produced output', r.stdout.length > 0, true)
rmSync(linkDir, { recursive: true, force: true })

if (failures) { console.error(`\n${failures} self-test(s) FAILED`); process.exit(1) }
console.log('\nAll bin/check-integration-shard-balance.mjs self-tests passed')
