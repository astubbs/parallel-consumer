#!/usr/bin/env node
// Copyright (C) 2026 Antony Stubbs and contributors
//
// The Chaos Pain Suite's four CI shards are a STATIC matrix - a `scenarios:` comma-list of simple
// class names per `suite: chaos` entry in .github/workflows/maven.yml - chosen deliberately over a
// derived split because a static list is honest and greppable (docs/ci.md, "Chaos runs as four
// shards"). Its cost is drift: nothing stops a shard's list falling out of sync with the
// scenario classes actually tagged `@Tag("chaos")` as scenarios are added, renamed or retyped. The
// design note names the fix directly - "a static matrix with a check that every chaos scenario
// appears in exactly one shard gets both, and is the same shape as the quarantine registry's own
// gate" (bin/check-quarantine-registry.sh). This is that check.
//
// EXPECTED SET: every *.java under the chaostests package whose source contains @Tag("chaos") -
// the simple class name, from the file name. @Quarantined is deliberately NOT evaluated here: a
// quarantined scenario still belongs to a shard on paper - bin/chaos-test.sh excludes it at RUN
// TIME, and its own missing-report guard (CHAOS_SCENARIOS requested a scenario, none produced a
// report - see bin/chaos-test.sh's header) is deliberately the thing that notices a quarantined
// class was assigned, one shard at a time. Duplicating that here would just be a second, weaker copy
// of a check that already exists and already runs.
//
// ACTUAL ASSIGNMENT: no YAML parser is available in this tree (no package.json, no node_modules, no
// `yaml`/`js-yaml` anywhere under it - checked before writing this) and bin/AGENTS.md forbids adding
// a dependency to sidestep that. So this parses the workflow with a DELIBERATELY NARROW line scan of
// the `include:` list rather than a real YAML parser: walk lines, track the current `- suite: ...`
// block up to the next `- <key>:` at the same indentation (or dedent out of the list), and read that
// block's `scenarios:` and `name:` keys. It does not attempt general YAML - flow sequences, anchors,
// multi-line scalars - none of which this list uses; if the workflow's shape changes enough to need
// those, this scan is the wrong tool and a real parser (or a schema check) should replace it, not
// extend it.
//
// EXIT CODES follow bin/check-all.sh: 0 pass, 1 violation, 2 cannot run. Exit 3 ("nothing in scope")
// is deliberately NOT used - there is always something in scope: either chaos-tagged classes exist
// (the normal case) or the workflow's chaos shards exist, and either produces a real verdict.

import { readFileSync, readdirSync } from 'node:fs'
import { dirname, resolve, join } from 'node:path'
import { fileURLToPath } from 'node:url'
import { pathToFileURL } from 'node:url'

const VIOLATION = 1, CANNOT = 2

const WORKFLOW_RELATIVE = '.github/workflows/maven.yml'
const CHAOSTESTS_RELATIVE =
  'parallel-consumer-core/src/test-integration/java/bz/stub/parallelconsumer/integrationTests/chaostests'

/**
 * Repo root the gate reads from. CHAOS_SHARDS_CHECK_ROOT overrides it - used by
 * bin/test-check-chaos-shards.mjs to point the whole gate at a throwaway fixture tree instead of the
 * real repo, the same override shape bin/check-quarantine-registry.sh uses (QUARANTINE_CHECK_ROOT).
 */
function repoRoot() {
  if (process.env.CHAOS_SHARDS_CHECK_ROOT) return resolve(process.env.CHAOS_SHARDS_CHECK_ROOT)
  return resolve(dirname(fileURLToPath(import.meta.url)), '..')
}

/**
 * Every *.java directly under `dir` carrying @Tag("chaos"), as its simple class name (the filename
 * minus .java - every class in this package is named after its file, matching the repo's own
 * convention, so this does not also parse the `class X` declaration).
 */
function expectedScenarios(dir) {
  let entries
  try {
    entries = readdirSync(dir, { withFileTypes: true })
  } catch (e) {
    throw new ReadError(`cannot read ${dir} - ${e.message}`)
  }
  const names = []
  for (const entry of entries) {
    if (!entry.isFile() || !entry.name.endsWith('.java')) continue
    const path = join(dir, entry.name)
    let text
    try {
      text = readFileSync(path, 'utf8')
    } catch (e) {
      throw new ReadError(`cannot read ${path} - ${e.message}`)
    }
    if (/@Tag\(\s*"chaos"\s*\)/.test(text)) names.push(entry.name.slice(0, -'.java'.length))
  }
  return names.sort()
}

class ReadError extends Error {}

/**
 * Parses `.github/workflows/maven.yml`'s `include:` list for every `- suite: chaos` entry, returning
 * one `{ suite, name, shard, scenarios, lineNo }` object per entry (1-indexed lineNo, for messages).
 *
 * NARROW BY DESIGN, not a YAML parser - see the file header for why. It tracks one thing: am I
 * currently inside a list entry, and what entry have I not yet flushed. The whole workflow file has
 * MANY unrelated YAML lists at many different indentations (job steps, `with:` blocks, and so on),
 * so the entry's OWN indentation - not one indentation remembered for the whole file - is what a
 * continuation line is compared against: a `- key: value` line at or above the open entry's
 * indentation starts a fresh entry and flushes whatever came before it (this also transitions
 * cleanly between two unrelated lists, since the first item of a new list always opens the current
 * entry's slot); a plain `key: value` line at or above that indentation ends the entry (it belongs to
 * whatever comes after `include:` in the job body, e.g. the `name: "${{ matrix.name }}"` step key,
 * or to the next sibling item). A `- key: value` line indented DEEPER than the open entry (a nested
 * list inside one field) is not a shape this matrix uses, so it is read but never flushes on its own.
 */
function parseChaosShards(text) {
  const lines = text.split('\n')
  const shards = []
  let current = null // { suite, name, shard, scenarios, lineNo, indent }

  const flush = () => {
    if (current && current.suite === 'chaos') shards.push(current)
    current = null
  }

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i]
    const itemMatch = line.match(/^(\s*)-\s+([A-Za-z_][\w-]*):\s*(.*)$/)
    if (itemMatch) {
      const [, indent, key, value] = itemMatch
      if (current === null || indent.length <= current.indent) {
        flush()
        current = { suite: null, name: null, shard: null, scenarios: null, lineNo: i + 1, indent: indent.length }
      }
      applyKey(current, key, value)
      continue
    }
    if (current === null) continue
    const kvMatch = line.match(/^(\s*)([A-Za-z_][\w-]*):\s*(.*)$/)
    if (kvMatch) {
      const [, indent, key, value] = kvMatch
      if (indent.length <= current.indent) { flush(); continue }
      applyKey(current, key, value)
      continue
    }
    // Blank/comment/other lines neither start nor end an entry.
  }
  flush()
  return shards
}

function applyKey(entry, key, rawValue) {
  const value = stripQuotesAndComment(rawValue)
  if (key === 'suite') entry.suite = value
  else if (key === 'name') entry.name = value
  else if (key === 'shard') entry.shard = value
  else if (key === 'scenarios') entry.scenarios = value
}

/** Strips a trailing `# comment` (outside quotes) and one layer of matching quotes. */
function stripQuotesAndComment(raw) {
  const quoted = raw.match(/^"([^"]*)"\s*(#.*)?$/) || raw.match(/^'([^']*)'\s*(#.*)?$/)
  if (quoted) return quoted[1]
  const hashIdx = raw.indexOf('#')
  return (hashIdx === -1 ? raw : raw.slice(0, hashIdx)).trim()
}

function labelFor(shard) {
  return shard.name || (shard.shard ? `shard ${shard.shard}` : `line ${shard.lineNo}`)
}

/**
 * Compares the expected chaos scenario set against the shard assignments parsed from the workflow
 * and returns a list of problem strings - empty means a clean partition. Pure (no I/O), so this is
 * what the self-test exercises directly, and what `main()` below reports.
 */
function findProblems(expected, shards) {
  const problems = []

  const emptyShards = shards.filter(s => !s.scenarios || s.scenarios.trim() === '')
  for (const s of emptyShards) {
    problems.push(`empty \`scenarios:\` on shard "${labelFor(s)}" (line ${s.lineNo})`)
  }

  const assignedTo = new Map() // scenario class name -> [shard labels]
  for (const s of shards) {
    if (!s.scenarios || s.scenarios.trim() === '') continue
    for (const raw of s.scenarios.split(',')) {
      const name = raw.trim()
      if (!name) continue
      if (!assignedTo.has(name)) assignedTo.set(name, [])
      assignedTo.get(name).push(labelFor(s))
    }
  }

  const expectedSet = new Set(expected)

  for (const name of expected) {
    if (!assignedTo.has(name)) problems.push(`${name}: assigned to NO shard`)
  }
  for (const [name, labels] of assignedTo.entries()) {
    if (labels.length > 1) problems.push(`${name}: assigned to ${labels.length} shards (${labels.join(', ')})`)
  }
  for (const name of assignedTo.keys()) {
    if (!expectedSet.has(name)) {
      problems.push(`${name}: in the matrix but matches no @Tag("chaos") class under ${CHAOSTESTS_RELATIVE}`)
    }
  }
  return problems
}

function main() {
  const root = repoRoot()
  const workflowPath = join(root, WORKFLOW_RELATIVE)
  const chaostestsPath = join(root, CHAOSTESTS_RELATIVE)

  let workflowText
  try {
    workflowText = readFileSync(workflowPath, 'utf8')
  } catch (e) {
    console.error(`check-chaos-shards: cannot read ${workflowPath} - ${e.message}`)
    process.exit(CANNOT)
  }

  let expected
  try {
    expected = expectedScenarios(chaostestsPath)
  } catch (e) {
    if (e instanceof ReadError) {
      console.error(`check-chaos-shards: ${e.message}`)
      process.exit(CANNOT)
    }
    throw e
  }

  const shards = parseChaosShards(workflowText)

  if (shards.length === 0) {
    console.error(`check-chaos-shards: no \`suite: chaos\` entries found in ${workflowPath} - the`)
    console.error('  matrix shape changed, or this scan\'s narrow parse no longer matches it. Cannot')
    console.error('  verify the chaos shard partition.')
    process.exit(CANNOT)
  }

  const problems = findProblems(expected, shards)

  if (problems.length > 0) {
    console.error('check-chaos-shards: the chaos matrix does not partition the chaos scenarios:\n')
    for (const p of problems) console.error(`  - ${p}`)
    process.exit(VIOLATION)
  }

  console.log(
    `check-chaos-shards: ${expected.length} chaos scenario(s) partitioned exactly once across ${shards.length} shard(s).`,
  )
  process.exit(0)
}

// Exported for the self-test - see bin/AGENTS.md's "Scripts that guard other scripts": the self-test
// exercises this logic directly (pure functions plus fixture I/O against CHAOS_SHARDS_CHECK_ROOT),
// not a re-implementation of the parsing.
export { expectedScenarios, parseChaosShards, findProblems, CHAOSTESTS_RELATIVE, WORKFLOW_RELATIVE }

// Run only when executed directly - not when imported by the self-test, which would otherwise call
// process.exit() as a side effect of `import`.
if (import.meta.url === pathToFileURL(process.argv[1] ?? '').href) main()
