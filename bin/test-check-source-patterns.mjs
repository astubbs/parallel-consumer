#!/usr/bin/env node
// Copyright (C) 2026 Antony Stubbs and contributors
//
// Self-test for the rule table in bin/lib/source-patterns.mjs.
//
// EVERY CASE IS A CONTROL PAIR - one input that must match and one that must not. A pattern verified
// only against text that should match is a pattern nobody has shown to be selective, and this repo has
// already shipped one gate that matched nothing and reported success over the defect it was written
// for. The must-NOT-match half is the half that catches that.
//
// It tests the RULES, not the runner: the runner walks git, which a unit test should not.

import { RULES } from './lib/source-patterns.mjs'

let failures = 0
const check = (desc, actual, expected) => {
  if (actual === expected) { console.log(`ok    ${desc}`) }
  else { console.error(`FAIL  ${desc} - expected ${expected}, got ${actual}`); failures++ }
}

const rule = id => {
  const r = RULES.find(x => x.id === id)
  if (!r) { console.error(`FAIL  no rule with id ${id}`); failures++ }
  return r
}

// Every rule must carry a reason. A rule nobody can argue with outlives the thing it guarded.
for (const r of RULES) {
  check(`[${r.id}] states a why`, typeof r.why === 'string' && r.why.length > 40, true)
  check(`[${r.id}] states a fix`, typeof r.fix === 'string' && r.fix.length > 10, true)
}

// --- new-shell-script -------------------------------------------------------------------------
{
  const r = rule('new-shell-script')
  check('new-shell: matches a path in bin/', r.files.test('bin/thing.sh'), true)
  check('new-shell: ignores shell outside bin/', r.files.test('scripts/thing.sh'), false)
  check('new-shell: ignores .mjs in bin/', r.files.test('bin/thing.mjs'), false)
  check('new-shell: a justified script is allowed',
    r.allowIf.test('#!/usr/bin/env bash\n# shell-justified: wraps one docker command\n'), true)
  check('new-shell: an EMPTY justification does not count',
    r.allowIf.test('# shell-justified:\n'), false)
  check('new-shell: an unjustified script is not allowed',
    r.allowIf.test('#!/usr/bin/env bash\necho hi\n'), false)
}

// --- awk-endfile ------------------------------------------------------------------------------
{
  const r = rule('awk-endfile')
  check('endfile: catches the gawk-only block', r.forbid.test('  ENDFILE {\n    print\n  }\n'), true)
  check('endfile: does not catch plain END', r.forbid.test('  END { print }\n'), false)
  check('endfile: does not catch the word in prose',
    r.forbid.test('# mawk has no ENDFILE, which is why this exists\n'), false)
  check('endfile: applies to shell files', r.files.test('bin/x.sh'), true)
  check('endfile: does not apply to Node', r.files.test('bin/x.mjs'), false)
}

// --- awk-reserved-exp -------------------------------------------------------------------------
{
  const r = rule('awk-reserved-exp')
  check('reserved: catches exp= inside an awk program',
    r.forbid.test(`awk 'BEGIN { exp = 3 }'`), true)
  check('reserved: does not catch a shell variable named exp outside awk',
    r.forbid.test('exp=3\necho "$exp"\n'), false)
  check('reserved: does not catch a comparison',
    r.forbid.test(`awk 'BEGIN { if (exp == 3) print }'`), false)
}

if (failures === 0) { console.log('\nAll source-pattern self-tests passed'); process.exit(0) }
console.error(`\n${failures} source-pattern self-test(s) failed`)
process.exit(1)
