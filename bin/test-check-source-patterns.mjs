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

// --- sigpipe-into-grep-q ----------------------------------------------------------------------
// Migrated from bin/check-shell-sigpipe.sh, so these cases are ITS cases: the port must agree with
// the gate it replaced, and the first draft did not - it flagged thirteen files the shell gate passes.
{
  const r = rule('sigpipe-into-grep-q')
  const pipefail = 'set -euo pipefail\n'
  check('sigpipe: only applies where pipefail is set',
    r.requires.test(pipefail), true)
  check('sigpipe: a file without pipefail is out of scope',
    r.requires.test('#!/usr/bin/env bash\necho hi\n'), false)
  check('sigpipe: catches a pipe into grep -q',
    r.forbid.test(pipefail + 'printf x | grep -q foo\n'), true)
  check('sigpipe: catches the long flag',
    r.forbid.test(pipefail + 'printf x | grep --quiet foo\n'), true)
  check('sigpipe: catches it with other flags first',
    r.forbid.test(pipefail + 'printf x | grep -E -q foo\n'), true)
  // The comment guard. Without it the first draft flagged every file that merely DOCUMENTS the hazard,
  // including the gate being replaced - thirteen false positives against a gate that reports none.
  check('sigpipe: does NOT catch it inside a comment',
    r.forbid.test(pipefail + '# never write: printf x | grep -q foo\n'), false)
  check('sigpipe: does NOT catch a logical or',
    r.forbid.test(pipefail + 'foo || grep -q bar file\n'), false)
  check('sigpipe: does NOT catch a herestring, which is the fix',
    r.forbid.test(pipefail + 'grep -q foo <<<"$data"\n'), false)
  // Every flag spelling the deleted gate covered. These are not decoration: -qE, -qF and the
  // space-separated `grep -v -q` are the ones a hand-written regex gets wrong, and reasoning about
  // whether they match is how you convince yourself of the wrong answer. The old gate's own self-test
  // had an arm for each; losing them silently is how a migration becomes a regression.
  for (const flags of ['-q', '-qE', '-qF', '-Eq', '--quiet', '--silent', '-v -q', '-E -q']) {
    check(`sigpipe: catches grep ${flags}`,
      r.forbid.test(pipefail + `printf x | grep ${flags} foo\n`), true)
  }
  check('sigpipe: applies to shell', r.files.test('bin/x.sh'), true)
}

if (failures === 0) { console.log('\nAll source-pattern self-tests passed'); process.exit(0) }
console.error(`\n${failures} source-pattern self-test(s) failed`)
process.exit(1)
