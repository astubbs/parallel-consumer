#!/usr/bin/env node
// Copyright (C) 2026 Antony Stubbs and contributors
//
// One runner for every "grep with a reason attached" rule in this repo. The rules live in
// bin/lib/source-patterns.mjs; this file is the machinery they all share, and the point is that they
// share exactly one copy of it.
//
// WHAT IT REPLACES, and why that is the whole argument: bin/ grew a script per rule, and most of them
// are the same program - walk files, match a regex, complain - each re-implementing file walking,
// exclusions, an opt-out marker, an exit-code contract and a failure message. Five things to get
// subtly different from your neighbour, times the number of gates. Here they exist once.
//
// SCOPES. A rule is either about the whole tree or only about what a branch ADDED (`scope:
// 'added-files'`). The second matters for rules adopted after the fact: "no new shell scripts" must
// not fail on the hundred that already exist, and the merge base - not origin/master's tip - is what
// separates them, because resetting to the tip calls everything master gained since the branch was
// cut "new here".
//
// `requires` IS NOT A NICETY - it arrived from a real rule. Folding check-shell-sigpipe.sh in needed
// "only files that also set pipefail", because piping into `grep -q` is only a wrong ANSWER when
// pipefail promotes the reader's SIGPIPE to the pipeline's status. Without the precondition the rule
// flags correct code. A table that could not express that would have been a table that only fits the
// rules its author invented for it.
//
// OPT-OUTS ARE SENTENCES. A rule may name an `allowIf` marker, and the convention is that the marker
// carries a reason (`shell-justified: <why>`). A bare flag lets somebody silence a rule without
// saying anything a reviewer can disagree with.
//
// EXIT CODES follow bin/check-all.sh: 0 pass, 1 violation, 2 cannot run, 3 nothing in scope.

import { execFileSync } from 'node:child_process'
import { readFileSync } from 'node:fs'
import { RULES } from './lib/source-patterns.mjs'

/** exit codes, per bin/check-all.sh's contract */
const VIOLATION = 1, CANNOT = 2, NOTHING_IN_SCOPE = 3

const sh = (cmd, args) => execFileSync(cmd, args, { encoding: 'utf8' }).trim()

// SHALLOW CLONES DO NOT ERROR, THEY LIE. `git merge-base HEAD origin/master` on a shallow clone
// returns a commit that is not the merge base, with exit 0 - so an `added-files` rule silently
// compares against the wrong point and reports a wrong answer confidently. Reproduced on a real
// shallow checkout during review of this PR, not theorised.
//
// This repo already answers it that way elsewhere - check-copyright-headers.sh guards its fork point
// with `git cat-file -e <ref>^{commit}` before trusting it. This is the one new script here that
// skipped the convention, which is a poor look for the gate whose whole subject is silent wrong
// answers.
//
// FAIL CLOSED (exit 2, "cannot run"), never open. A gate that cannot establish its baseline must say
// so; reporting "no violations" because it could not look is the failure this file exists to prevent.
let mergeBase = null
let baseUnavailable = null
try {
  if (sh('git', ['rev-parse', '--is-shallow-repository']) === 'true') {
    baseUnavailable = 'the repository is a shallow clone, so merge-base cannot be trusted'
  } else {
    // Prove the ref is actually present before asking merge-base about it: a single-branch or
    // narrowly-scoped fetch leaves no local origin/master, and merge-base's answer then means nothing.
    sh('git', ['cat-file', '-e', 'origin/master^{commit}'])
    mergeBase = sh('git', ['merge-base', 'HEAD', 'origin/master'])
  }
} catch {
  baseUnavailable = 'origin/master is not present locally (single-branch or narrow fetch)'
}

if (baseUnavailable) {
  const needsBase = RULES.some(r => r.scope === 'added-files')
  if (needsBase) {
    console.error(`check-source-patterns: cannot determine what this branch ADDED - ${baseUnavailable}.`)
    console.error('  Fetch full history for origin/master (git fetch --unshallow, or fetch that ref) and re-run.')
    console.error('  Refusing to report "no violations" from a baseline that cannot be established.')
    process.exit(CANNOT)
  }
}

const tracked = sh('git', ['ls-files']).split('\n').filter(Boolean)
const added = mergeBase
  ? sh('git', ['diff', '--name-only', '--diff-filter=A', mergeBase, 'HEAD']).split('\n').filter(Boolean)
  : null

// One read per file, not one per rule. Rules overlap - `^bin/.*\.sh$` and `\.(sh|bash)$` both match
// every shell script in bin/ - so an uncached read is O(files x rules) for no gain.
const contents = new Map()
const read = f => {
  if (!contents.has(f)) {
    try { contents.set(f, readFileSync(f, 'utf8')) } catch { contents.set(f, null) }
  }
  return contents.get(f)
}

let violations = 0, inScope = 0, cannot = 0, baseUsed = null
for (const rule of RULES) {
  let candidates
  if (rule.scope === 'added-files') {
    if (added === null) {
      console.error(`${rule.id}: no merge base with origin/master - cannot tell new files from old.`)
      cannot++; continue
    }
    candidates = added
    // NAME THE BASE. `origin/master` is whatever was last fetched, and a stale ref moves the merge
    // base backwards - which makes files master added since look "new here" and flags them. Printing
    // it is the difference between a confusing false positive and an obvious one.
    baseUsed = mergeBase
  } else {
    candidates = tracked
  }
  const files = candidates.filter(f => rule.files.test(f))
  if (files.length === 0) continue
  inScope += files.length

  const hits = []
  for (const f of files) {
    const text = read(f)
    if (text === null) continue
    if (rule.requires && !rule.requires.test(text)) continue
    if (rule.allowIf && rule.allowIf.test(text)) continue
    // No `forbid` means the file's existence is the violation - see the Rule typedef.
    if (!rule.forbid || rule.forbid.test(text)) hits.push(f)
  }
  if (hits.length === 0) continue

  violations += hits.length
  console.error(`\n[${rule.id}] ${hits.length} violation(s)\n`)
  for (const f of hits) console.error(`  ${f}`)
  console.error(`\n  WHY: ${rule.why}`)
  console.error(`  FIX: ${rule.fix}`)
}

if (cannot > 0 && violations === 0) process.exit(CANNOT)
if (violations > 0) {
  console.error('\nRules live in bin/lib/source-patterns.mjs - adding one is a row, not a script.')
  process.exit(VIOLATION)
}
if (inScope === 0) {
  console.log('check-source-patterns: no files in scope for any rule')
  process.exit(NOTHING_IN_SCOPE)
}
const against = baseUsed ? `, new-file rules against ${baseUsed.slice(0, 9)}` : ''
console.log(`check-source-patterns: ${RULES.length} rule(s) clean over ${inScope} file(s) in scope${against}`)
