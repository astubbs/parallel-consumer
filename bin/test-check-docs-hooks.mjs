#!/usr/bin/env node
/*
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

// Self-test for the document context query's hooks - today the read-time divergence header,
// `.claude/hooks/inject-docs-divergence.mjs`; the prompt-keyword hook joins it when it lands.
//
// The shape is bin/test-check-solutions-hook.mjs's: every positive has a silent twin, every
// invocation asserts the never-blocks contract (exit 0, no signal, nothing on stderr), and one
// mutant control proves the suite cannot pass with the hook's matching broken. What is new is the
// FIXTURE: a throwaway repository holding every state the header reports (bin/lib/fixture-repos.mjs
// owns it), plus a worktree of its branch-only note, so the cases that turn on WHICH tree the hook
// resolved against have two trees to tell apart.
//
// The hook is driven as the harness drives it - JSON on stdin, `cwd` as the payload carries it, a
// session nonce per run so the per-session dedupe cannot leak between runs - and never imported:
// the contract under test is the process's, not a function's.

import { spawnSync } from 'node:child_process';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { buildDocsFixture, windowRepo } from './lib/fixture-repos.mjs';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
// One literal path, not joined segments: bin/test-check-agent-hooks.sh proves every registered hook
// is self-tested by finding `.claude/hooks/<name>` in test CODE, so the path has to be visible as one.
const HOOK = path.join(root, '.claude/hooks/inject-docs-divergence.mjs');
const DELIVERY = 'read-time header';

const RUN = `t${process.pid}-${Math.floor(Date.now() % 1e6)}`;
// The failure record goes to the tool's cache directory; this run gets its own, so a record left by
// a real session cannot satisfy the case that asserts one was written.
const CACHE = fs.mkdtempSync(path.join(os.tmpdir(), 'docs-hooks-cache-'));

let fails = 0;
let n = 0;

/**
 * Run the hook once. `payload` is the event as an object (serialised here) or a raw string for the
 * inputs the hook must survive rather than read. Returns stdout; the never-blocks contract is
 * asserted on every call, whatever the case then asserts about the output.
 */
function runHook(caseName, payload, { env = {}, hook = HOOK } = {}) {
  const input = typeof payload === 'string' ? payload : JSON.stringify({
    session_id: `${RUN}-${caseName}`,
    hook_event_name: 'PostToolUse',
    ...payload,
  });
  const r = spawnSync('node', [hook], {
    input,
    encoding: 'utf8',
    timeout: 20_000,
    killSignal: 'SIGKILL',
    env: { ...process.env, CLAUDE_PROJECT_DIR: root, PC_INFLIGHT_CACHE_DIR: CACHE, ...env },
  });
  if (r.status !== 0 || r.signal || r.stderr) {
    const stderr = (r.stderr || '').trim().slice(0, 300);
    console.log(`  FAIL ${caseName} - broke the never-blocks contract: status=${r.status} signal=${r.signal} stderr='${stderr}'`);
    fails += 1;
  }
  return r.stdout || '';
}

const read = (caseName, filePath, cwd, opts) => runHook(caseName, { cwd, tool_name: 'Read', tool_input: { file_path: filePath } }, opts);
const bash = (caseName, command, cwd, opts) => runHook(caseName, { cwd, tool_name: 'Bash', tool_input: { command } }, opts);

/** The additionalContext out of an envelope, or '' - and a FAIL if stdout is non-empty but not an envelope. */
function contextOf(label, out) {
  if (!out.trim()) return '';
  try {
    const env = JSON.parse(out);
    const ctx = env.hookSpecificOutput && env.hookSpecificOutput.additionalContext;
    if (typeof ctx === 'string' && env.hookSpecificOutput.hookEventName === 'PostToolUse') return ctx;
    console.log(`  FAIL ${label} - stdout is not a PostToolUse envelope: ${out.slice(0, 200)}`);
  } catch {
    console.log(`  FAIL ${label} - stdout is not JSON: ${out.slice(0, 200)}`);
  }
  fails += 1;
  return '';
}

function expectFires(label, out, ...needles) {
  n += 1;
  const ctx = contextOf(label, out);
  const missing = needles.filter((s) => !ctx.includes(s));
  if (ctx && missing.length === 0) console.log(`  ok   ${label}`);
  else {
    console.log(`  FAIL ${label} - expected context naming ${missing.map((s) => `'${s}'`).join(', ')}, got: ${ctx || '<silence>'}`);
    fails += 1;
  }
}

function expectSilent(label, out) {
  n += 1;
  if (!out.trim()) console.log(`  ok   ${label} (silent)`);
  else {
    console.log(`  FAIL ${label} - expected silence, got: ${out.slice(0, 300)}`);
    fails += 1;
  }
}

function check(label, ok, detail = '') {
  n += 1;
  if (ok) console.log(`  ok   ${label}`);
  else {
    console.log(`  FAIL ${label}${detail ? ` - ${detail}` : ''}`);
    fails += 1;
  }
}

const failures = () => {
  try {
    return JSON.parse(fs.readFileSync(path.join(CACHE, 'delivery-failures.json'), 'utf8')).value || {};
  } catch {
    return {};
  }
};

// --- The fixture: the corpus repository, a worktree on its branch-only note, and two bystanders. ---
const fx = buildDocsFixture();
const only = `${fx.dir}-only-here`;
fx.git('worktree', 'add', '-q', only, 'only-here');
// A checkout with no corpus at all, and a directory that is not a checkout.
const bystander = windowRepo();
fs.writeFileSync(path.join(bystander.dir, 'README.md'), 'nothing here\n');
bystander.commit('a repository with no docs');
const nowhere = fs.mkdtempSync(path.join(os.tmpdir(), 'docs-hooks-nowhere-'));
fs.writeFileSync(path.join(fx.dir, 'notes.txt'), 'not a corpus file\n');

const NOTE = 'docs/inflight/note.md';
const NOTE_ABS = path.join(fx.dir, NOTE);
const HEADER_CMD = `node bin/inflight.mjs docs header ${NOTE}`;

try {
  console.log('the Read tool:');
  expectFires(
    'a corpus note with divergent branch versions names the path, the count and the header command',
    read('r1', NOTE_ABS, fx.dir),
    `docs context: divergence header for ${NOTE}`, '2 divergent versions on 2 live refs', 'baseline\'s version (master)', `more: ${HEADER_CMD}`,
  );
  expectFires(
    'a version held only by a tag is reported preserved, by ref kind',
    read('r2', NOTE_ABS, fx.dir),
    '1 preserved (tag only)', 'refs searched (4 live, 1 archival)',
  );
  expectSilent('a file outside the corpus areas', read('r3', path.join(fx.dir, 'notes.txt'), fx.dir));
  expectSilent('a corpus-looking path that does not exist', read('r4', path.join(fx.dir, 'docs/inflight/missing.md'), fx.dir));
  expectSilent('a file in a checkout with no corpus', read('r5', path.join(bystander.dir, 'README.md'), bystander.dir));
  expectSilent('a tool that is neither Read nor Bash', runHook('r6', { cwd: fx.dir, tool_name: 'Write', tool_input: { file_path: NOTE_ABS, content: 'x' } }));

  console.log('the tree is the one the event names, never the session\'s:');
  expectFires(
    'a Read whose payload cwd is another checkout resolves against the tree holding file_path',
    read('t1', NOTE_ABS, bystander.dir, { env: { CLAUDE_PROJECT_DIR: bystander.dir } }),
    `divergence header for ${NOTE}`, '2 divergent versions',
  );
  expectFires(
    'a relative file_path resolves against the payload cwd, not the session root',
    read('t2', NOTE, fx.dir, { env: { CLAUDE_PROJECT_DIR: bystander.dir } }),
    `divergence header for ${NOTE}`,
  );
  expectSilent(
    'the same relative path against a cwd that does not hold it, even when the session root does',
    read('t3', NOTE, nowhere, { env: { CLAUDE_PROJECT_DIR: fx.dir } }),
  );

  console.log('Bash, best-effort by path token:');
  expectFires('cat <corpus path> fires like a Read', bash('b1', `cat ${NOTE}`, fx.dir), `divergence header for ${NOTE}`, '2 divergent versions');
  expectFires('a quoted literal path is still a literal', bash('b2', `sed -n '1,5p' "${NOTE}"`, fx.dir), `divergence header for ${NOTE}`);
  expectSilent('cat "$f" - a variable is not resolved', bash('b3', 'cat "$f"', fx.dir, { env: { f: NOTE_ABS } }));
  expectSilent('a glob is not resolved', bash('b4', 'cat docs/inflight/*.md', fx.dir));
  expectSilent('a Bash command naming no path', bash('b5', 'git status --short', fx.dir));
  expectFires(
    'cd <worktree> && cat <note> resolves against the worktree, from a cwd that does not hold the note',
    bash('b6', `cd ${only} && cat ${NOTE}`, nowhere),
    `divergence header for ${NOTE}`, 'copy is', 'more: ',
  );
  expectSilent('cd "$W" && cat <note> - a variable cd is refused rather than guessed', bash('b7', `cd "$W" && cat ${NOTE}`, nowhere, { env: { W: only } }));

  console.log('no git call before a corpus file is found:');
  // A `git` shim first on PATH that logs every invocation, then runs the real one. The positive
  // control proves the shim intercepts at all; without it, an empty log would also mean the shim
  // was never on the path.
  const shimDir = fs.mkdtempSync(path.join(os.tmpdir(), 'docs-hooks-git-shim-'));
  const realGit = spawnSync('sh', ['-c', 'command -v git'], { encoding: 'utf8' }).stdout.trim();
  const shimLog = path.join(shimDir, 'calls.log');
  fs.writeFileSync(path.join(shimDir, 'git'), `#!/bin/sh\necho "$@" >> "${shimLog}"\nexec "${realGit}" "$@"\n`, { mode: 0o755 });
  const shimEnv = { env: { PATH: `${shimDir}${path.delimiter}${process.env.PATH}` } };
  expectFires('control: the shim sees the corpus Read\'s git calls', read('g1', NOTE_ABS, fx.dir, shimEnv), `divergence header for ${NOTE}`);
  check('control: the shim logged at least one call', fs.existsSync(shimLog) && fs.readFileSync(shimLog, 'utf8').trim().length > 0, 'the shim was not on the path');
  fs.rmSync(shimLog, { force: true });
  expectSilent('a Read outside the corpus', read('g2', path.join(fx.dir, 'notes.txt'), fx.dir, shimEnv));
  check('...and it made no git call', !fs.existsSync(shimLog), `git was called: ${fs.existsSync(shimLog) ? fs.readFileSync(shimLog, 'utf8') : ''}`);
  fs.rmSync(shimDir, { recursive: true, force: true });

  console.log('once per session per divergence state:');
  const first = read('dd', NOTE_ABS, fx.dir);
  expectFires('the first read of a path in a session fires', first, '2 divergent versions');
  expectSilent('a second read of the same path in the same session', read('dd', NOTE_ABS, fx.dir));
  expectSilent('the same path through Bash in that session is the same state', bash('dd', `cat ${NOTE}`, fx.dir));
  fx.git('checkout', '-q', '-b', 'adds-more', 'master');
  fx.write(NOTE, `${fx.NOTE}a third divergent version\n`);
  fx.commit('a third version');
  fx.git('checkout', '-q', 'master');
  expectFires('after another branch adds a version, the same session is told again', read('dd', NOTE_ABS, fx.dir), '3 divergent versions on 3 live refs');
  expectSilent('...and once only', read('dd', NOTE_ABS, fx.dir));

  console.log('copy states:');
  expectFires(
    'a branch-only note, read in the worktree that holds it, reports branch-only',
    read('c1', path.join(only, 'docs/inflight/branch-only.md'), only),
    'on NO baseline ref (branch-only)', '1 live ref carry it', 'created on this branch',
  );
  // A second note whose only other version is on a tag: zero divergent, one preserved.
  fx.git('checkout', '-q', '-b', 'to-tag-2', 'master');
  fx.write('docs/inflight/tagged.md', '# Tagged\n\nbase\n');
  fx.commit('a note to park');
  fx.git('checkout', '-q', 'master');
  fx.git('merge', '-q', '--ff-only', 'to-tag-2');
  fx.git('checkout', '-q', 'to-tag-2');
  fx.write('docs/inflight/tagged.md', '# Tagged\n\nbase\nparked edit\n');
  fx.commit('parked edit');
  fx.git('tag', 'preserved/parked-2');
  fx.git('checkout', '-q', 'master');
  fx.git('branch', '-q', '-D', 'to-tag-2');
  expectFires(
    'a note carried only elsewhere on a tag reports preserved and zero divergent',
    read('c2', path.join(fx.dir, 'docs/inflight/tagged.md'), fx.dir),
    '0 divergent versions on 0 live refs', '1 preserved (tag only)', 'baseline\'s version (master)',
  );
  // The `only-here` worktree's note.md IS the baseline's blob, so from that branch the copy state is
  // still "the baseline's version" - the state is about the content, not the branch it sits on.
  expectFires(
    'the baseline\'s note read from a worktree on another branch is still the baseline\'s version',
    read('c3', path.join(only, NOTE), only),
    'baseline\'s version (master)',
  );
  // The own-divergent state needs a branch that edited it:
  const own = `${fx.dir}-adds-heading`;
  fx.git('worktree', 'add', '-q', own, 'adds-heading');
  expectFires(
    'a note read on the branch that rewrote it reports that branch\'s OWN divergent version',
    read('c4', path.join(own, NOTE), own),
    'adds-heading\'s OWN divergent version (+', 'since its merge-base)',
  );

  console.log('uncommitted edits:');
  const committed = fs.readFileSync(NOTE_ABS, 'utf8');
  fs.writeFileSync(NOTE_ABS, `${committed}an edit nobody committed\n`);
  try {
    expectFires(
      'a dirty working-tree copy reports the edits and the committed version\'s state',
      read('u1', NOTE_ABS, fx.dir),
      'UNCOMMITTED edits', 'describes the committed version', 'baseline\'s version (master)',
    );
  } finally {
    fs.writeFileSync(NOTE_ABS, committed);
  }
  expectFires('a clean copy does not claim edits', read('u2', NOTE_ABS, fx.dir), 'baseline\'s version');
  n += 1;
  if (!contextOf('u2', read('u2b', NOTE_ABS, fx.dir)).includes('UNCOMMITTED')) console.log('  ok   ...and the clean copy is not called uncommitted');
  else { console.log('  FAIL a clean copy was reported as uncommitted'); fails += 1; }
  fs.writeFileSync(path.join(fx.dir, 'docs/inflight/brand-new.md'), '# Brand new\n');
  expectFires(
    'a note no ref carries yet says so, rather than inventing a committed version',
    read('u3', path.join(fx.dir, 'docs/inflight/brand-new.md'), fx.dir),
    'at that path on none of', 'UNCOMMITTED edits',
  );

  console.log('failing open, with a record:');
  check('no failure is recorded while the hook is healthy', !(DELIVERY in failures()), JSON.stringify(failures()));
  const broken = read('f1', NOTE_ABS, fx.dir, { env: { GIT_DIR: path.join(nowhere, 'not-a-repo') } });
  expectSilent('a forced git failure prints nothing', broken);
  const rec = failures()[DELIVERY];
  check('...and records the delivery, a reason and a time', rec && typeof rec.reason === 'string' && rec.reason.length > 0 && !Number.isNaN(Date.parse(rec.time)), JSON.stringify(failures()));
  expectFires('a following success still answers', read('f2', NOTE_ABS, fx.dir), '3 divergent versions');
  check('...and clears the record', !(DELIVERY in failures()), JSON.stringify(failures()));

  console.log('inputs the hook must survive rather than read - each MUST be silent AND exit 0:');
  expectSilent('unparseable stdin', runHook('raw1', 'this is not json'));
  expectSilent('empty stdin', runHook('raw2', ''));
  expectSilent('a JSON scalar', runHook('raw3', '42'));
  expectSilent('a payload with no tool_input', runHook('raw4', { cwd: fx.dir, tool_name: 'Read' }));
  expectSilent('a Read whose file_path is not a string', runHook('raw5', { cwd: fx.dir, tool_name: 'Read', tool_input: { file_path: 7 } }));
  expectSilent('a Bash whose command is not a string', runHook('raw6', { cwd: fx.dir, tool_name: 'Bash', tool_input: { command: ['cat', NOTE] } }));

  console.log('the header comment states its budget and its measured cost:');
  const source = fs.readFileSync(HOOK, 'utf8');
  check('the hook names a budget in ms and a measured figure', /BUDGET: \d+ ms/.test(source) && /MEASURED \d{4}-\d{2}-\d{2}/.test(source));

  console.log('negative control - the suite cannot pass with the matching broken:');
  // A copy of the hook with its corpus check emptied, imports rewritten to absolute URLs so the copy
  // runs from a temp dir. The mutation is asserted to have applied: a control that silently failed
  // to mutate would pass by proving nothing.
  const mutantDir = fs.mkdtempSync(path.join(os.tmpdir(), 'docs-hooks-mutant-'));
  const hooksLib = pathToFileURL(path.join(root, '.claude/hooks/lib/')).href;
  const binLib = pathToFileURL(path.join(root, 'bin/lib/')).href;
  const rewired = source.replaceAll("'./lib/", `'${hooksLib}`).replaceAll("'../../bin/lib/", `'${binLib}`);
  const mutant = rewired.replace('DOC_AREAS.some(', '[].some(');
  check('the mutation applied', mutant !== rewired);
  const mutantPath = path.join(mutantDir, 'inject-docs-divergence.mjs');
  fs.writeFileSync(mutantPath, mutant);
  const intactPath = path.join(mutantDir, 'intact.mjs');
  fs.writeFileSync(intactPath, rewired);
  expectFires('control: the rewired-but-unmutated copy still fires', read('m0', NOTE_ABS, fx.dir, { hook: intactPath }), '3 divergent versions');
  expectSilent('the mutant is silent on the positive case', read('m1', NOTE_ABS, fx.dir, { hook: mutantPath }));
  fs.rmSync(mutantDir, { recursive: true, force: true });
} finally {
  for (const w of [only, `${fx.dir}-adds-heading`]) {
    try { fx.git('worktree', 'remove', '--force', w); } catch { /* already gone, or never added */ }
  }
  for (const d of [fx.dir, bystander.dir, nowhere, CACHE]) fs.rmSync(d, { recursive: true, force: true });
}

console.log('');
if (fails === 0) {
  console.log(`test-check-docs-hooks: ${n} case(s), all passed.`);
  process.exit(0);
}
console.log(`test-check-docs-hooks: ${fails} of ${n} case(s) FAILED.`);
process.exit(1);
