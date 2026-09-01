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

const sh = (cmd, args) => execFileSync(cmd, args, { encoding: 'utf8' }).trim()

let mergeBase = null
try { mergeBase = sh('git', ['merge-base', 'HEAD', 'origin/master']) } catch { /* reported per rule */ }

const tracked = sh('git', ['ls-files']).split('\n').filter(Boolean)
const added = mergeBase
  ? sh('git', ['diff', '--name-only', '--diff-filter=A', mergeBase, 'HEAD']).split('\n').filter(Boolean)
  : null

const read = f => { try { return readFileSync(f, 'utf8') } catch { return null } }

let violations = 0, inScope = 0, cannot = 0
for (const rule of RULES) {
  let candidates
  if (rule.scope === 'added-files') {
    if (added === null) {
      console.error(`${rule.id}: no merge base with origin/master - cannot tell new files from old.`)
      cannot++; continue
    }
    candidates = added
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
    if (rule.forbid.test(text)) hits.push(f)
  }
  if (hits.length === 0) continue

  violations += hits.length
  console.error(`\n[${rule.id}] ${hits.length} violation(s)\n`)
  for (const f of hits) console.error(`  ${f}`)
  console.error(`\n  WHY: ${rule.why}`)
  console.error(`  FIX: ${rule.fix}`)
}

if (cannot > 0 && violations === 0) process.exit(2)
if (violations > 0) {
  console.error('\nRules live in bin/lib/source-patterns.mjs - adding one is a row, not a script.')
  process.exit(1)
}
if (inScope === 0) {
  console.log('check-source-patterns: no files in scope for any rule')
  process.exit(3)
}
console.log(`check-source-patterns: ${RULES.length} rule(s) clean over ${inScope} file(s) in scope`)
