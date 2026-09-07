#!/usr/bin/env node
/*
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

// Self-test for the document context query's hooks: the read-time divergence header,
// `.claude/hooks/inject-docs-divergence.mjs`, and the prompt-keyword injection,
// `.claude/hooks/inject-docs-for-prompt.mjs`.
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
import { buildDocsFixture, buildTermsFixture, windowRepo } from './lib/fixture-repos.mjs';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
// One literal path, not joined segments: bin/test-check-agent-hooks.sh proves every registered hook
// is self-tested by finding `.claude/hooks/<name>` in test CODE, so the path has to be visible as one.
const HOOK = path.join(root, '.claude/hooks/inject-docs-divergence.mjs');
const PROMPT_HOOK = path.join(root, '.claude/hooks/inject-docs-for-prompt.mjs');
const DELIVERY = 'read-time header';
const PROMPT_DELIVERY = 'docs-for-prompt';

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
/** The prompt hook, driven as the harness drives it: `prompt` in the payload, the UserPromptSubmit event. */
const prompt = (caseName, text, cwd, opts = {}) => runHook(caseName, { cwd, hook_event_name: 'UserPromptSubmit', prompt: text }, { hook: PROMPT_HOOK, ...opts });

/** The additionalContext out of an envelope, or '' - and a FAIL if stdout is non-empty but not an envelope for `event`. */
function contextOf(label, out, event = 'PostToolUse') {
  if (!out.trim()) return '';
  try {
    const env = JSON.parse(out);
    const ctx = env.hookSpecificOutput && env.hookSpecificOutput.additionalContext;
    if (typeof ctx === 'string' && env.hookSpecificOutput.hookEventName === event) return ctx;
    console.log(`  FAIL ${label} - stdout is not a ${event} envelope: ${out.slice(0, 200)}`);
  } catch {
    console.log(`  FAIL ${label} - stdout is not JSON: ${out.slice(0, 200)}`);
  }
  fails += 1;
  return '';
}

function expectContext(label, ctx, needles) {
  n += 1;
  const missing = needles.filter((s) => !ctx.includes(s));
  if (ctx && missing.length === 0) console.log(`  ok   ${label}`);
  else {
    console.log(`  FAIL ${label} - expected context naming ${missing.map((s) => `'${s}'`).join(', ')}, got: ${ctx || '<silence>'}`);
    fails += 1;
  }
}

const expectFires = (label, out, ...needles) => expectContext(label, contextOf(label, out), needles);
const expectPromptFires = (label, out, ...needles) => expectContext(label, contextOf(label, out, 'UserPromptSubmit'), needles);

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
// Two clones of the fixture, made where the shallow case needs them; declared here so the cleanup sees them.
let shallow = null;
let full = null;
let neverFetched = null;

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
  // A DIRECTORY CHANGE THE LEADING-cd RULE DID NOT CONSUME drops every RELATIVE token: the command
  // read the OTHER tree's copy, and every worktree of this repository carries the same note paths,
  // so a relative token resolved against the payload cwd describes the wrong tree's copy with the
  // hook's badge on it. The cwd here HOLDS the note, so silence is the rule and not an accident -
  // the first cut of the `$W` case ran from a cwd without the note and could only ever pass.
  expectSilent('cd "$W" && cat <note> - a variable cd is refused rather than resolved against the cwd', bash('b7', `cd "$W" && cat ${NOTE}`, fx.dir, { env: { W: only } }));
  expectSilent('(cd <worktree> && cat <note>) - a subshell cd is not the leading cd', bash('b8', `(cd ${only} && cat ${NOTE})`, fx.dir));
  expectSilent('git -C <worktree> diff -- <note> - a relative path in a git -C command', bash('b9', `git -C ${only} diff -- ${NOTE}`, fx.dir));
  expectFires(
    'control: an ABSOLUTE token in such a command still fires, for the tree it names',
    bash('b10', `(cd ${only} && cat ${path.join(only, NOTE)})`, fx.dir),
    `divergence header for ${NOTE}`, 'copy is',
  );

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

  console.log('many paths in one event, one ref listing:');
  // The refs and the baseline are the same for every path one event names, so the hook resolves
  // them once and threads them into each query, as matchDocs does. Before it did, every path
  // listed every ref again: a four-path Bash read measured 613-793 ms against the 500 ms budget,
  // with `for-each-ref` alone at 119 ms a call on this repository. The four paths are committed,
  // so each is a full query - an uncommitted file takes the same route, but this is the dear one.
  const FOURTH = 'docs/plans/2026-01-02-001-second.md';
  fx.write(FOURTH, '# A second plan\n\nmore steps\n');
  fx.commit('a fourth corpus file');
  const FOUR = [NOTE, 'docs/solutions/ci/sol.md', 'docs/plans/2026-01-01-001-plan.md', FOURTH];
  const fourCtx = contextOf('g3', bash('g3', `cat ${FOUR.join(' ')}`, fx.dir, shimEnv));
  check('control: a Bash command naming four corpus files answers for all four', FOUR.every((p) => fourCtx.includes(`divergence header for ${p}`)), fourCtx || '<silence>');
  const shimCalls = fs.existsSync(shimLog) ? fs.readFileSync(shimLog, 'utf8').split('\n').filter(Boolean) : [];
  check('control: the shim logged the four-path read\'s git calls', shimCalls.length > 0, 'the shim was not on the path');
  const refListings = shimCalls.filter((l) => l.startsWith('for-each-ref')).length;
  check('...and the refs were listed exactly once for the four paths', refListings === 1, `for-each-ref was called ${refListings} time(s)`);
  fs.rmSync(shimDir, { recursive: true, force: true });

  console.log('past the cap, the rest is named rather than dropped:');
  // Four is the cap. A fifth path is not checked - and an answer that looks complete while a
  // named path went unchecked is the truncated-but-plausible index the session hook refuses to be.
  const FIFTH = 'docs/plans/2026-01-03-001-fifth.md';
  fx.write(FIFTH, '# A fifth plan\n\neven more steps\n');
  fx.commit('a fifth corpus file');
  check('control: four paths name nothing as unchecked', !fourCtx.includes('NOT checked'), fourCtx);
  const fiveCtx = contextOf('g4', bash('g4', `cat ${[...FOUR, FIFTH].join(' ')}`, fx.dir));
  check('control: the first four still answer', FOUR.every((p) => fiveCtx.includes(`divergence header for ${p}`)), fiveCtx || '<silence>');
  const tail = fiveCtx.trimEnd().split('\n').at(-1) ?? '';
  check('a fifth path is named on the trailing line, as unchecked, with its own header command', tail.includes('1 more corpus path') && tail.includes('NOT checked') && tail.includes(`node bin/inflight.mjs docs header ${FIFTH}`), tail);
  check('...and it was not answered for', !fiveCtx.includes(`divergence header for ${FIFTH}`), fiveCtx);

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

  console.log('a partial ref set voids the count, and the line says so first:');
  // A shallow clone sees only the fetched depth of the baseline's history, so a version the
  // baseline ONCE held is classed divergent - a confident wrong count, and the hook is the one
  // channel that answers without being asked. FIXTURE clones only: a repository hook refuses
  // `git fetch --depth` against the real clone, and a shallow real clone is the incident itself.
  // `file://`, because git silently ignores `--depth` on a plain local path.
  shallow = fs.mkdtempSync(path.join(os.tmpdir(), 'docs-hooks-shallow-'));
  full = fs.mkdtempSync(path.join(os.tmpdir(), 'docs-hooks-full-'));
  const clone = (...args) => {
    const r = spawnSync('git', ['clone', '-q', ...args], { encoding: 'utf8' });
    if (r.status !== 0) throw new Error(`fixture clone failed: ${r.stderr}`);
  };
  clone('--depth', '1', '--no-single-branch', `file://${fx.dir}`, shallow);
  clone(`file://${fx.dir}`, full);
  check('control: the shallow clone is shallow', spawnSync('git', ['rev-parse', '--is-shallow-repository'], { cwd: shallow, encoding: 'utf8' }).stdout.trim() === 'true');
  const UNRELIABLE = 'UNRELIABLE (shallow - run: git fetch --unshallow): ';
  const shallowCtx = contextOf('sh1', read('sh1', path.join(shallow, NOTE), shallow));
  check('a read in a shallow clone is prefixed UNRELIABLE, with the warning id and its remedy', shallowCtx.includes(`\n${UNRELIABLE}${NOTE}: `), shallowCtx || '<silence>');
  const fullCtx = contextOf('sh2', read('sh2', path.join(full, NOTE), full));
  check('control: the full clone of the same repository answers', fullCtx.includes(`divergence header for ${NOTE}`), fullCtx || '<silence>');
  check('...with no UNRELIABLE prefix', !fullCtx.includes('UNRELIABLE'), fullCtx);
  // The other invalidating state, in ITS OWN repository: no FETCH_HEAD and no packed-refs. Not the
  // shared fixture, whose state here depends on the git version - 2.39 writes packed-refs when the
  // fixture deletes its `to-tag` branch, so on the Linux lane that fixture counts as fetched. The
  // precondition is asserted, so a git that packs refs on commit fails this loudly rather than
  // letting the case pass on a repository that is not in the state it claims.
  neverFetched = windowRepo();
  fs.mkdirSync(path.join(neverFetched.dir, 'docs/inflight'), { recursive: true });
  fs.writeFileSync(path.join(neverFetched.dir, NOTE), '# A note nobody fetched\n\nbody\n');
  neverFetched.commit('a repository that has never fetched');
  check('control: the never-fetched repository has no FETCH_HEAD and no packed-refs', !fs.existsSync(path.join(neverFetched.dir, '.git/FETCH_HEAD')) && !fs.existsSync(path.join(neverFetched.dir, '.git/packed-refs')));
  const neverCtx = contextOf('sh3', read('sh3', path.join(neverFetched.dir, NOTE), neverFetched.dir));
  check('a never-fetched clone is prefixed with that id and its remedy', neverCtx.includes(`\nUNRELIABLE (never-fetched - run: git fetch origin): ${NOTE}: `), neverCtx || '<silence>');

  console.log('failing open, with a record:');
  check('no failure is recorded while the hook is healthy', !(DELIVERY in failures()), JSON.stringify(failures()));
  const broken = read('f1', NOTE_ABS, fx.dir, { env: { GIT_DIR: path.join(nowhere, 'not-a-repo') } });
  expectSilent('a forced git failure prints nothing', broken);
  const rec = failures()[DELIVERY];
  check('...and records the delivery, a reason and a time', rec && typeof rec.reason === 'string' && rec.reason.length > 0 && !Number.isNaN(Date.parse(rec.time)), JSON.stringify(failures()));
  expectFires('a following success still answers', read('f2', NOTE_ABS, fx.dir), '3 divergent versions');
  check('...and clears the record', !(DELIVERY in failures()), JSON.stringify(failures()));

  console.log('a later path failing never marks an earlier one seen:');
  // Two corpus paths in one command, the SECOND made to fail: a git shim that refuses to hash that
  // one file, so its query throws after the first path's header was computed. A shim rather than
  // `chmod 000`, because root reads a mode-000 file and the Linux lane runs as root.
  const cascadeShim = fs.mkdtempSync(path.join(os.tmpdir(), 'docs-hooks-cascade-shim-'));
  const SECOND = 'docs/solutions/ci/sol.md';
  fs.writeFileSync(path.join(cascadeShim, 'git'), `#!/bin/sh\ncase "$*" in *hash-object*${SECOND}*) exit 1;; esac\nexec "${realGit}" "$@"\n`, { mode: 0o755 });
  const cascadeEnv = { env: { PATH: `${cascadeShim}${path.delimiter}${process.env.PATH}` } };
  expectSilent('control: a command naming a path whose hash fails prints nothing', bash('cas', `cat ${NOTE} ${SECOND}`, fx.dir, cascadeEnv));
  check('control: ...and records the second path as the failure', (failures()[DELIVERY]?.reason ?? '').includes(SECOND), JSON.stringify(failures()));
  expectFires(
    'the first path, computed but never shown, still fires on its next read in the same session',
    read('cas', NOTE_ABS, fx.dir),
    `divergence header for ${NOTE}`, '3 divergent versions',
  );
  // The shim stays for the cascade mutant below; the cleanup removes it.

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

  console.log('negative controls - the suite cannot pass with the hook broken:');
  // Copies of the hook, imports rewritten to absolute URLs so each copy runs from a temp dir, ONE
  // mutation each. Every mutation is asserted to have applied: a control that silently failed to
  // mutate would pass by proving nothing. The intact rewired copy is the control for the rewiring.
  const mutantDir = fs.mkdtempSync(path.join(os.tmpdir(), 'docs-hooks-mutant-'));
  const hooksLib = pathToFileURL(path.join(root, '.claude/hooks/lib/')).href;
  const binLib = pathToFileURL(path.join(root, 'bin/lib/')).href;
  const rewired = source.replaceAll("'./lib/", `'${hooksLib}`).replaceAll("'../../bin/lib/", `'${binLib}`);
  const mutantHook = (label, from, to) => {
    const mutated = rewired.replace(from, to);
    check(`the ${label} mutation applied`, mutated !== rewired);
    const file = path.join(mutantDir, `${label}.mjs`);
    fs.writeFileSync(file, mutated);
    return file;
  };
  const intactPath = path.join(mutantDir, 'intact.mjs');
  fs.writeFileSync(intactPath, rewired);
  expectFires('control: the rewired-but-unmutated copy still fires', read('m0', NOTE_ABS, fx.dir, { hook: intactPath }), '3 divergent versions');
  const matching = mutantHook('matching', 'DOC_AREAS.some(', '[].some(');
  expectSilent('the matching mutant is silent on the positive case', read('m1', NOTE_ABS, fx.dir, { hook: matching }));
  // The cascade: remembering inside the loop again, before the write.
  const cascade = mutantHook('cascade', '    if (store && store.has(key)) continue;\n', '    if (store && store.has(key)) continue;\n    if (store) store.remember([key]);\n');
  expectSilent('control: the cascade mutant is silent when the second path fails', bash('mcas', `cat ${NOTE} ${SECOND}`, fx.dir, { ...cascadeEnv, hook: cascade }));
  expectSilent('the cascade mutant never shows the first path again that session', read('mcas', NOTE_ABS, fx.dir, { hook: cascade }));
  // Composition: relative tokens kept through an unconsumed directory change.
  const composition = mutantHook('composition', 'return CHANGES_DIRECTORY.test(past) ? tokens.filter((t) => path.isAbsolute(t)) : tokens;', 'return tokens;');
  expectFires('the composition mutant describes the session tree for (cd <worktree> && cat <note>)', bash('m2', `(cd ${only} && cat ${NOTE})`, fx.dir, { hook: composition }), `divergence header for ${NOTE}`);
  // The partial ref set: warnings not handed to the renderer.
  const unwarned = mutantHook('unwarned', 'warnings: unreliable }', 'warnings: [] }');
  const unwarnedCtx = contextOf('m3', read('m3', path.join(shallow, NOTE), shallow, { hook: unwarned }));
  check('the unwarned mutant answers from the shallow clone with no UNRELIABLE prefix', unwarnedCtx.includes(`divergence header for ${NOTE}`) && !unwarnedCtx.includes('UNRELIABLE'), unwarnedCtx || '<silence>');
  // The cap: the dropped paths go unnamed.
  const uncounted = mutantHook('uncounted', '  if (dropped.length > 0) {', '  if (false) {');
  const uncountedCtx = contextOf('m4', bash('m4', `cat ${[...FOUR, FIFTH].join(' ')}`, fx.dir, { hook: uncounted }));
  check('the uncounted mutant answers for four paths and names no fifth', FOUR.every((p) => uncountedCtx.includes(`divergence header for ${p}`)) && !uncountedCtx.includes('NOT checked'), uncountedCtx || '<silence>');
  fs.rmSync(mutantDir, { recursive: true, force: true });
  fs.rmSync(cascadeShim, { recursive: true, force: true });

  // =============================================================================================
  // THE PROMPT-KEYWORD HOOK. Its own fixture: the corpus plus the documents each tier is specified
  // against (bin/lib/fixture-repos.mjs's buildTermsFixture), driven from a payload whose cwd is the
  // fixture while CLAUDE_PROJECT_DIR is this repository - so every positive here also proves the
  // tree came from the payload, never the session root.
  // =============================================================================================
  console.log('\nthe prompt hook - a prompt naming a mechanism:');
  const tfx = buildTermsFixture();
  const SOLUTION = 'docs/solutions/ci/retry-queue.md';
  expectPromptFires(
    'a class named only in a branch-only solution\'s related_components field injects that title, off baseline',
    prompt('p1', 'why does RetryQueueDrainer run twice per shard?', tfx.dir),
    'docs context: prompt terms RetryQueueDrainer', 'The retry queue drained twice', SOLUTION, '(off baseline)',
    'more: node bin/inflight.mjs prior-art --headings RetryQueueDrainer',
  );
  // `learned` is in the heading adds-heading's version of note.md added and in nothing on master:
  // the path is on the baseline, the version that matched is not, so the mark is divergence.
  expectPromptFires(
    'a word carried only by a divergent version of a baseline note marks it divergent elsewhere, and the frame names every term',
    prompt('p2', 'compare what the branch `learned` with WidgetSpinner', tfx.dir),
    'prompt terms learned, WidgetSpinner', 'The note  docs/inflight/note.md  (divergent elsewhere)', 'A rollout plan  docs/plans/2026-02-02-001-widget.md',
  );
  expectSilent('a prompt naming a mechanism no document carries', prompt('p3', 'what does NoSuchMechanismAnywhere do?', tfx.dir));
  expectSilent('a prompt sent from a directory that is not a checkout', prompt('p4', 'why does RetryQueueDrainer run twice?', nowhere));
  expectSilent('a prompt naming a class, in a checkout with no corpus', prompt('p5', 'why does RetryQueueDrainer run twice?', bystander.dir));

  console.log('the prompt hook - no git call before a term survives:');
  const pShim = fs.mkdtempSync(path.join(os.tmpdir(), 'docs-hooks-prompt-shim-'));
  const pLog = path.join(pShim, 'calls.log');
  fs.writeFileSync(path.join(pShim, 'git'), `#!/bin/sh\necho "$@" >> "${pLog}"\nexec "${realGit}" "$@"\n`, { mode: 0o755 });
  const pShimEnv = { env: { PATH: `${pShim}${path.delimiter}${process.env.PATH}` } };
  expectPromptFires('control: the shim sees the firing prompt\'s git calls', prompt('pg1', 'RetryQueueDrainer again', tfx.dir, pShimEnv), 'The retry queue drained twice');
  const pCalls = fs.existsSync(pLog) ? fs.readFileSync(pLog, 'utf8').split('\n').filter(Boolean) : [];
  check('control: the shim logged at least one call', pCalls.length > 0, 'the shim was not on the path');
  check('...exactly one of them a grep, and none an ls-tree', pCalls.filter((c) => c.startsWith('grep ')).length === 1 && !pCalls.some((c) => c.startsWith('ls-tree')), pCalls.join(' | '));
  fs.rmSync(pLog, { force: true });
  expectSilent('a prompt with no identifier in it', prompt('pg2', 'please fix the tests and push', tfx.dir, pShimEnv));
  check('...and it made no git call', !fs.existsSync(pLog), `git was called: ${fs.existsSync(pLog) ? fs.readFileSync(pLog, 'utf8') : ''}`);
  fs.rmSync(pShim, { recursive: true, force: true });

  console.log('the prompt hook - once per session per state:');
  expectPromptFires('the first prompt naming a class in a session fires', prompt('pd', 'RetryQueueDrainer', tfx.dir), 'The retry queue drained twice');
  expectSilent('the same prompt again in the same session', prompt('pd', 'RetryQueueDrainer', tfx.dir));
  expectSilent('a different prompt naming the same document in that session', prompt('pd', 'is RetryQueueDrainer idempotent?', tfx.dir));
  expectPromptFires('a new session is told again', prompt('pd2', 'RetryQueueDrainer', tfx.dir), 'The retry queue drained twice');

  console.log('the prompt hook - capped, with the rest counted:');
  const capped = contextOf('pc', prompt('pc', 'what is GadgetFlange?', tfx.dir), 'UserPromptSubmit');
  const gadgetLines = capped.split('\n').filter((l) => l.startsWith('- Gadget ')).length;
  check('a term matching more titles than the cap injects the cap', gadgetLines === 12, `${gadgetLines} title lines: ${capped}`);
  check('...and a +N more line naming the count left', capped.includes('+2 more'), capped);
  check('...and the frame closes with the prior-art command', capped.trimEnd().endsWith('more: node bin/inflight.mjs prior-art --headings GadgetFlange'), capped);

  console.log('the prompt hook - failing open, with a record:');
  check('no failure is recorded while the prompt hook is healthy', !(PROMPT_DELIVERY in failures()), JSON.stringify(failures()));
  expectSilent('a forced git failure prints nothing', prompt('pf1', 'RetryQueueDrainer', tfx.dir, { env: { GIT_DIR: path.join(nowhere, 'not-a-repo') } }));
  const prec = failures()[PROMPT_DELIVERY];
  check('...and records the delivery, a reason and a time', prec && typeof prec.reason === 'string' && prec.reason.length > 0 && !Number.isNaN(Date.parse(prec.time)), JSON.stringify(failures()));
  expectPromptFires('a following success still answers', prompt('pf2', 'RetryQueueDrainer', tfx.dir), 'The retry queue drained twice');
  check('...and clears the record', !(PROMPT_DELIVERY in failures()), JSON.stringify(failures()));

  console.log('the prompt hook - inputs it must survive rather than read, each silent AND exit 0:');
  expectSilent('unparseable stdin', runHook('praw1', 'this is not json', { hook: PROMPT_HOOK }));
  expectSilent('empty stdin', runHook('praw2', '', { hook: PROMPT_HOOK }));
  expectSilent('a JSON scalar', runHook('praw3', '42', { hook: PROMPT_HOOK }));
  expectSilent('a payload with no prompt', runHook('praw4', { cwd: tfx.dir, hook_event_name: 'UserPromptSubmit' }, { hook: PROMPT_HOOK }));
  expectSilent('a prompt that is not a string', runHook('praw5', { cwd: tfx.dir, hook_event_name: 'UserPromptSubmit', prompt: ['RetryQueueDrainer'] }, { hook: PROMPT_HOOK }));

  console.log('the prompt hook - the header comment states its budget and its measured cost:');
  const psource = fs.readFileSync(PROMPT_HOOK, 'utf8');
  check('the prompt hook names a budget in ms and a measured figure', /BUDGET: \d+ ms/.test(psource) && /MEASURED \d{4}-\d{2}-\d{2}/.test(psource));

  console.log('the prompt hook - negative control, the suite cannot pass with term extraction broken:');
  const pMutantDir = fs.mkdtempSync(path.join(os.tmpdir(), 'docs-hooks-prompt-mutant-'));
  const pRewired = psource.replaceAll("'./lib/", `'${hooksLib}`).replaceAll("'../../bin/lib/", `'${binLib}`);
  const pMutant = pRewired.replace('const terms = termsFromPrompt(ev.prompt);', 'const terms = [];');
  check('the mutation applied', pMutant !== pRewired);
  const pMutantPath = path.join(pMutantDir, 'inject-docs-for-prompt.mjs');
  fs.writeFileSync(pMutantPath, pMutant);
  const pIntactPath = path.join(pMutantDir, 'intact.mjs');
  fs.writeFileSync(pIntactPath, pRewired);
  expectPromptFires('control: the rewired-but-unmutated copy still fires', prompt('pm0', 'RetryQueueDrainer', tfx.dir, { hook: pIntactPath }), 'The retry queue drained twice');
  expectSilent('the mutant is silent on the positive case', prompt('pm1', 'RetryQueueDrainer', tfx.dir, { hook: pMutantPath }));
  fs.rmSync(pMutantDir, { recursive: true, force: true });
  fs.rmSync(tfx.dir, { recursive: true, force: true });
} finally {
  for (const w of [only, `${fx.dir}-adds-heading`]) {
    try { fx.git('worktree', 'remove', '--force', w); } catch { /* already gone, or never added */ }
  }
  for (const d of [fx.dir, bystander.dir, nowhere, CACHE, shallow, full, neverFetched?.dir].filter(Boolean)) fs.rmSync(d, { recursive: true, force: true });
}

console.log('');
if (fails === 0) {
  console.log(`test-check-docs-hooks: ${n} case(s), all passed.`);
  process.exit(0);
}
console.log(`test-check-docs-hooks: ${fails} of ${n} case(s) FAILED.`);
process.exit(1);
