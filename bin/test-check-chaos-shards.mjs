#!/usr/bin/env node
// Copyright (C) 2026 Antony Stubbs and contributors
//
// Self-test for bin/check-chaos-shards.mjs.
//
// TWO LAYERS, because the gate has two things that can go wrong independently. `parseChaosShards`
// is pure (text in, entries out) and is exercised directly with small YAML fragments, including a
// fragment shaped like the REAL workflow - an unrelated list (job steps) at a different indentation
// sitting right next to the `include:` list - because the first draft of the parser tracked ONE
// indentation for the whole file and silently returned zero shards the moment a second list appeared
// before the chaos one; that shape is exactly what the real maven.yml has (a `runs-on:`/`steps:`
// block precedes `strategy: matrix: include:`), so a fixture that skips it would have passed the
// broken parser too.
//
// The FULL GATE (workflow file + chaostests directory + exit code + message) is exercised by spawning
// the real script against a throwaway fixture tree, using CHAOS_SHARDS_CHECK_ROOT the same way
// bin/test-check-quarantine-registry.sh points check-quarantine-registry.sh at a fixture with
// QUARANTINE_CHECK_ROOT - never against this repo's own files.
//
// EVERY CASE NAMES THE OFFENDER IN THE OUTPUT, not just the exit code. A gate that says "fail" without
// saying which scenario is exactly as useless as the missing-report guard the design section
// points at (docs/ci.md, "Chaos runs as four shards") - the reader still has to re-derive what
// broke by hand.

import { mkdtempSync, mkdirSync, writeFileSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { fileURLToPath } from 'node:url'
import { spawnSync } from 'node:child_process'
import { parseChaosShards, findProblems, CHAOSTESTS_RELATIVE, WORKFLOW_RELATIVE } from './check-chaos-shards.mjs'

let failures = 0
const check = (desc, actual, expected) => {
  if (actual === expected) console.log(`ok    ${desc}`)
  else { console.error(`FAIL  ${desc}\n        expected: ${JSON.stringify(expected)}\n        actual:   ${JSON.stringify(actual)}`); failures++ }
}
const contains = (desc, haystack, needle) => {
  if (haystack.includes(needle)) console.log(`ok    ${desc}`)
  else { console.error(`FAIL  ${desc}\n        expected to contain: ${JSON.stringify(needle)}\n        actual:\n${haystack}`); failures++ }
}

// ---- parseChaosShards (pure) ----------------------------------------------------------------------

{
  // Shaped like the real file: a steps list at one indentation, THEN the matrix include list at a
  // different one, THEN a plain key (`name:`) that ends the list and belongs to the job body. The
  // parser must not let the first list's indentation leak into how it reads the second.
  const yaml = `jobs:
  test:
    steps:
      - uses: actions/checkout@v6
      - name: Build
        run: ./mvnw test
    strategy:
      matrix:
        include:
          - suite: unit
            name: "Unit"
            scenarios: ""
          - suite: chaos
            name: "Chaos Pain Suite 1/2"
            shard: "1"
            scenarios: "AlphaIT,BetaIT"
          - suite: chaos
            name: "Chaos Pain Suite 2/2"
            shard: "2"
            scenarios: "GammaIT"
    name: "\${{ matrix.name }}"
    runs-on: ubuntu-latest
`
  const shards = parseChaosShards(yaml)
  check('parses exactly the two chaos entries (not the unit one)', shards.length, 2)
  check('first chaos entry keeps its name', shards[0]?.name, 'Chaos Pain Suite 1/2')
  check('first chaos entry keeps its scenarios', shards[0]?.scenarios, 'AlphaIT,BetaIT')
  check('second chaos entry keeps its shard', shards[1]?.shard, '2')
  check('an unrelated list before it does not leak into parsing', shards.some(s => s.name === 'Unit'), false)
}

{
  // A quoted scenarios value with a trailing comment, and an unquoted one - both must strip cleanly.
  const yaml = `    include:
      - suite: chaos
        name: "Shard A" # first
        scenarios: "OneIT,TwoIT" # comma list
      - suite: chaos
        name: 'Shard B'
        scenarios: ThreeIT
`
  const shards = parseChaosShards(yaml)
  check('strips a quoted value plus trailing comment', shards[0]?.scenarios, 'OneIT,TwoIT')
  check('strips single quotes too', shards[1]?.name, 'Shard B')
  check('reads an unquoted scalar', shards[1]?.scenarios, 'ThreeIT')
}

// ---- findProblems (pure) ---------------------------------------------------------------------------

{
  const shard = (name, scenarios, lineNo = 1) => ({ suite: 'chaos', name, shard: null, scenarios, lineNo })
  check('a clean partition has no problems',
    findProblems(['A', 'B'], [shard('S1', 'A'), shard('S2', 'B')]).length, 0)
  check('a scenario assigned nowhere is named',
    findProblems(['A', 'B'], [shard('S1', 'A')]).some(p => p.includes('B: assigned to NO shard')), true)
  check('a scenario assigned twice names both shards',
    findProblems(['A'], [shard('S1', 'A'), shard('S2', 'A')])
      .some(p => p.includes('A: assigned to 2 shards') && p.includes('S1') && p.includes('S2')), true)
  check('an unknown name is named',
    findProblems(['A'], [shard('S1', 'A,Ghost')]).some(p => p.startsWith('Ghost: in the matrix')), true)
  check('an empty scenarios entry is named',
    findProblems(['A'], [shard('S1', ''), shard('S2', 'A')]).some(p => p.includes('empty `scenarios:` on shard "S1"')), true)
}

// ---- the full gate, spawned against a fixture tree --------------------------------------------------

const ROOT = fileURLToPath(new URL('..', import.meta.url))
const SCRIPT = join(ROOT, 'bin/check-chaos-shards.mjs')

/** Writes one @Tag("chaos") fixture class - just enough Java for the regex the gate reads. */
function writeChaosClass(dir, className) {
  mkdirSync(dir, { recursive: true })
  writeFileSync(join(dir, `${className}.java`), `package fixture.chaostests;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("chaos")
class ${className} {
    @Test
    void runs() {}
}
`)
}

/** Writes a plain (non-chaos) class, to prove the tag - not just the file's presence - gates inclusion. */
function writeUntaggedClass(dir, className) {
  mkdirSync(dir, { recursive: true })
  writeFileSync(join(dir, `${className}.java`), `package fixture.chaostests;

class ${className} {
}
`)
}

/** Renders one `- suite: chaos` matrix entry block, matching the real workflow's shape. */
function chaosEntry({ name, shard, scenarios }) {
  return `          - suite: chaos
            name: "${name}"
            cmd: "bin/chaos-test.sh"
            shard: "${shard}"
            scenarios: "${scenarios}"
`
}

/** Writes a fixture workflow file with a chaos matrix built from `entries`, or a bare non-chaos one. */
function writeWorkflow(root, entries) {
  mkdirSync(join(root, '.github/workflows'), { recursive: true })
  const body = entries === null
    ? `jobs:
  test:
    strategy:
      matrix:
        include:
          - suite: unit
            name: "Unit"
            scenarios: ""
    runs-on: ubuntu-latest
`
    : `jobs:
  test:
    strategy:
      matrix:
        include:
${entries.map(chaosEntry).join('')}    name: "\${{ matrix.name }}"
    runs-on: ubuntu-latest
`
  writeFileSync(join(root, WORKFLOW_RELATIVE), body)
}

/** Runs the real gate against `root` and returns { status, stdout, stderr }. */
function runGate(root) {
  const r = spawnSync(process.execPath, [SCRIPT], {
    encoding: 'utf8',
    env: { ...process.env, CHAOS_SHARDS_CHECK_ROOT: root },
  })
  return { status: r.status, stdout: r.stdout ?? '', stderr: r.stderr ?? '', error: r.error }
}

function withFixture(fn) {
  const root = mkdtempSync(join(tmpdir(), 'chaos-shards-test-'))
  try {
    fn(root)
  } finally {
    rmSync(root, { recursive: true, force: true })
  }
}

// PASS: three tagged classes, partitioned exactly once across two shards.
withFixture(root => {
  const dir = join(root, CHAOSTESTS_RELATIVE)
  writeChaosClass(dir, 'AlphaIT')
  writeChaosClass(dir, 'BetaIT')
  writeChaosClass(dir, 'GammaIT')
  writeUntaggedClass(dir, 'AbstractHelper') // untagged neighbour - must not count as a scenario
  writeWorkflow(root, [
    { name: 'Shard 1/2', shard: '1', scenarios: 'AlphaIT,BetaIT' },
    { name: 'Shard 2/2', shard: '2', scenarios: 'GammaIT' },
  ])
  const r = runGate(root)
  check('pass: exit 0', r.status, 0)
  contains('pass: reports the count', r.stdout, '3 chaos scenario(s) partitioned exactly once across 2 shard(s)')
})

// MISSING: a tagged class assigned to no shard.
withFixture(root => {
  const dir = join(root, CHAOSTESTS_RELATIVE)
  writeChaosClass(dir, 'AlphaIT')
  writeChaosClass(dir, 'BetaIT')
  writeChaosClass(dir, 'GammaIT')
  writeWorkflow(root, [
    { name: 'Shard 1/1', shard: '1', scenarios: 'AlphaIT,BetaIT' },
  ])
  const r = runGate(root)
  check('missing: exit 1', r.status, 1)
  contains('missing: names the unassigned scenario', r.stderr, 'GammaIT: assigned to NO shard')
})

// DUPLICATE: a tagged class assigned to two shards - both shard names must appear.
withFixture(root => {
  const dir = join(root, CHAOSTESTS_RELATIVE)
  writeChaosClass(dir, 'AlphaIT')
  writeChaosClass(dir, 'BetaIT')
  writeWorkflow(root, [
    { name: 'Shard 1/2', shard: '1', scenarios: 'AlphaIT,BetaIT' },
    { name: 'Shard 2/2', shard: '2', scenarios: 'AlphaIT' },
  ])
  const r = runGate(root)
  check('duplicate: exit 1', r.status, 1)
  contains('duplicate: names the scenario and shard count', r.stderr, 'AlphaIT: assigned to 2 shards')
  contains('duplicate: names the first shard', r.stderr, 'Shard 1/2')
  contains('duplicate: names the second shard', r.stderr, 'Shard 2/2')
})

// UNKNOWN NAME: a matrix entry names something with no matching @Tag("chaos") class (a typo).
withFixture(root => {
  const dir = join(root, CHAOSTESTS_RELATIVE)
  writeChaosClass(dir, 'AlphaIT')
  writeChaosClass(dir, 'BetaIT')
  writeWorkflow(root, [
    { name: 'Shard 1/1', shard: '1', scenarios: 'AlphaIT,BetaIT,AlphaITT' }, // typo'd extra scenario
  ])
  const r = runGate(root)
  check('unknown: exit 1', r.status, 1)
  contains('unknown: names the typo', r.stderr, 'AlphaITT: in the matrix but matches no @Tag("chaos") class')
})

// EMPTY SHARD: a chaos entry with an empty `scenarios:`.
withFixture(root => {
  const dir = join(root, CHAOSTESTS_RELATIVE)
  writeChaosClass(dir, 'AlphaIT')
  writeWorkflow(root, [
    { name: 'Empty Shard', shard: '1', scenarios: '' },
    { name: 'Shard 2', shard: '2', scenarios: 'AlphaIT' },
  ])
  const r = runGate(root)
  check('empty: exit 1', r.status, 1)
  contains('empty: names the empty shard', r.stderr, 'empty `scenarios:` on shard "Empty Shard"')
})

// CANNOT RUN: no `suite: chaos` entries at all - the matrix shape changed, or the scan cannot see it.
withFixture(root => {
  const dir = join(root, CHAOSTESTS_RELATIVE)
  writeChaosClass(dir, 'AlphaIT')
  writeWorkflow(root, null)
  const r = runGate(root)
  check('no-chaos-entries: exit 2 (cannot run)', r.status, 2)
})

// CANNOT RUN: the chaostests directory does not exist.
withFixture(root => {
  writeWorkflow(root, [{ name: 'Shard 1/1', shard: '1', scenarios: 'AlphaIT' }])
  const r = runGate(root)
  check('missing chaostests dir: exit 2 (cannot run)', r.status, 2)
})

// CANNOT RUN: the workflow file does not exist.
withFixture(root => {
  const dir = join(root, CHAOSTESTS_RELATIVE)
  writeChaosClass(dir, 'AlphaIT')
  const r = runGate(root)
  check('missing workflow file: exit 2 (cannot run)', r.status, 2)
})

if (failures === 0) { console.log('\nAll check-chaos-shards self-tests passed'); process.exit(0) }
console.error(`\n${failures} check-chaos-shards self-test(s) failed`)
process.exit(1)
