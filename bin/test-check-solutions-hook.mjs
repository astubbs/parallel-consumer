#!/usr/bin/env node
/*
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

// Self-test for the solutions-surfacing hook.
//
// `docs/compound-engineering.md`: "an untested guard is a guard-shaped comment", and the failure
// class this repo hits most is green that asserted nothing. So the cases come in pairs - every
// positive has a negative control that must go SILENT, and the suite fails if a control fires.
//
// The load-bearing case is the REGRESSION one: text of the shape that produced the 2026-09-02
// incident must surface the write-up that would have prevented it. If a future change stops the
// hook matching, that case goes red rather than the hook silently helping nobody - which is the
// exact failure mode it was built against.
//
// The fixture is inline rather than read from the plan it came from, so the case runs on every
// branch instead of only where that plan happens to live.

import { spawnSync } from 'node:child_process';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { match, render, writeUps } from '../.claude/hooks/lib/solutions-for-named-components.mjs';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
// One literal path, not joined segments: bin/test-check-agent-hooks.sh proves every registered hook
// is self-tested by finding `.claude/hooks/<name>` in test CODE, so the path has to be visible as one.
const HOOK = path.join(root, '.claude/hooks/inject-solutions-for-named-components.mjs');

// The hook remembers, per session, what it has already surfaced. Fixed session ids would pass on a
// clean machine and fail on every run after - a red that means nothing - so each invocation of this
// suite gets its own nonce.
const RUN = `t${process.pid}-${Math.floor(Date.now() % 1e6)}`;

let fails = 0;
let n = 0;

// Drives the hook as the harness does: JSON on stdin, `cwd` set as the payload carries it. `raw`
// bypasses the JSON entirely, for the inputs the hook must survive rather than read.
//
// EVERY invocation asserts the never-blocks contract - exit 0, no signal, nothing on stderr -
// whatever the case then asserts about stdout. The first version mapped a non-zero exit to '',
// which is the pass condition of every negative control: a mutant that exited 2 on the no-match
// path, blocking every edit in production, passed 15 of 15.
function runHook(caseName, filePath, content, env = {}, toolInput = null, raw = null) {
  const payload = raw ?? JSON.stringify({
    session_id: `${RUN}-${caseName}`,
    hook_event_name: 'PreToolUse',
    cwd: root,
    tool_input: toolInput ?? { file_path: filePath, content },
  });
  const r = spawnSync('node', [HOOK], {
    input: payload,
    encoding: 'utf8',
    timeout: 10_000,
    killSignal: 'SIGKILL',
    env: { ...process.env, CLAUDE_PROJECT_DIR: root, ...env },
  });
  if (r.status !== 0 || r.signal || r.stderr) {
    const stderr = (r.stderr || '').trim().slice(0, 200);
    console.log(`  FAIL ${caseName} - broke the never-blocks contract: status=${r.status} signal=${r.signal} stderr='${stderr}'`);
    fails += 1;
  }
  return r.stdout || '';
}

function expectFires(label, out, needle) {
  n += 1;
  if (out.includes(needle)) console.log(`  ok   ${label}`);
  else {
    console.log(`  FAIL ${label} - expected output naming '${needle}', got: ${out || '<silence>'}`);
    fails += 1;
  }
}

function expectSilent(label, out) {
  n += 1;
  if (!out.trim()) console.log(`  ok   ${label} (silent)`);
  else {
    console.log(`  FAIL ${label} - expected silence, got: ${out}`);
    fails += 1;
  }
}

console.log('positives:');

// REGRESSION. A requirements document, no Java file touched: a path-matching design scores zero
// here, which is why the hook matches text.
const incident =
  'Recovery aborts the transaction, discards the producer, and rejoins the consumer group while ' +
  'ProducerManager holds the commit write lock. ThreadConfinedConsumer confines the consumer to ' +
  'the broker-poll thread.';
expectFires(
  'a rejoin-under-the-commit-lock plan surfaces the commit-seam write-up',
  runHook('regress', 'docs/plans/x.md', incident),
  'two-threads-one-consumer',
);

expectFires(
  'a bare component name in prose fires',
  runHook('p1', 'notes.md', 'We touch ProducerManager here.'),
  'docs/solutions/',
);

console.log('negative controls - each MUST be silent:');

expectSilent(
  'no component named',
  runHook('n1', 'notes.md', 'A note about retries and backoff with no component named at all.'),
);

// Word-boundary control. If matching degrades to a substring test, this fires and the suite reddens.
expectSilent(
  'substring of a component name does not match',
  runHook('n2', 'notes.md', 'MyProducerManagerFactory and ProducerManagerish are different things.'),
);

// Relative paths carry no leading slash. Requiring one let an edit of a write-up surface that
// write-up to its own author - a real bug this case caught before the hook shipped.
expectSilent(
  'editing a write-up does not surface it to its own author',
  runHook(
    'n3',
    'docs/solutions/architecture-patterns/two-threads-one-consumer-why-the-commit-seam-keeps-deadlocking.md',
    'ProducerManager and ConsumerManager',
  ),
);

expectSilent('empty content', runHook('n4', 'notes.md', ''));

// This file names components on purpose; editing it must not surface write-ups to itself.
expectSilent(
  'editing this self-test does not surface write-ups to itself',
  runHook('n5', 'bin/test-check-solutions-hook.mjs', 'ProducerManager ThreadConfinedConsumer'),
);

console.log('inputs the hook must survive rather than read - each MUST be silent AND exit 0:');
expectSilent('unparseable stdin', runHook('raw1', null, null, {}, null, 'this is not json'));
expectSilent('empty stdin', runHook('raw2', null, null, {}, null, ''));
expectSilent('a payload with no tool_input', runHook('raw3', null, null, {}, null, JSON.stringify({ session_id: `${RUN}-raw3` })));

console.log('dedupe:');
const first = runHook('dd', 'notes.md', 'ProducerManager');
const second = runHook('dd', 'notes.md', 'ProducerManager');
n += 1;
if (first.trim() && !second.trim()) console.log('  ok   second mention in the same session is silent');
else {
  console.log(`  FAIL dedupe - first='${first.slice(0, 60)}' second='${second.slice(0, 60)}'`);
  fails += 1;
}

console.log('hidden-count honesty:');
const many = runHook(
  'cap',
  'notes.md',
  'ProducerManager ThreadConfinedConsumer WorkContainer PartitionState ConsumerManager',
  { PC_SOLUTIONS_HOOK_CAP: '1' },
);
n += 1;
if (/further write-up/.test(many)) console.log('  ok   over-cap matches are counted, not dropped');
else {
  console.log(`  FAIL cap - no hidden-count line; output: ${many.slice(0, 200)}`);
  fails += 1;
}

console.log('input shapes:');
// MultiEdit carries its text in edits[].new_string, not content. The hook parses that array, and until
// this case nothing drove it - a wrong property name there would have shipped silently.
const multi = runHook('me', null, null, {}, {
  file_path: 'notes.md',
  edits: [{ old_string: 'x', new_string: 'nothing here' }, { old_string: 'y', new_string: 'ProducerManager' }],
});
expectFires('MultiEdit edits[].new_string is read', multi, 'docs/solutions/');

// Edit is the commonest write tool, and its text arrives as a top-level new_string. Until this case
// nothing sent one: dropping `ti.new_string` from the hook passed every case.
expectFires(
  'Edit new_string is read',
  runHook('ed', null, null, {}, { file_path: 'notes.md', old_string: 'x', new_string: 'ProducerManager' }),
  'docs/solutions/',
);
expectSilent(
  'Edit old_string - the text being replaced - is not matched',
  runHook('ed2', null, null, {}, { file_path: 'notes.md', old_string: 'ProducerManager', new_string: 'nothing here' }),
);

console.log('root derivation, and the cap against the dedupe (synthetic tree):');
// A checkout is a `.git` entry (directory, or the file a worktree carries) above the file. The tree
// carries one Java type per write-up so each name maps to exactly one write-up.
function syntheticRepo(components) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'solutions-hook-repo-'));
  fs.mkdirSync(path.join(dir, '.git'));
  const java = path.join(dir, 'parallel-consumer-core', 'src', 'main', 'java');
  const sol = path.join(dir, 'docs', 'solutions', 'x');
  fs.mkdirSync(java, { recursive: true });
  fs.mkdirSync(sol, { recursive: true });
  for (const [type, file] of Object.entries(components)) {
    fs.writeFileSync(path.join(java, `${type}.java`), '');
    fs.writeFileSync(path.join(sol, file), `---\nrelated_components:\n  - ${type}\n---\n# About ${type}\n`);
  }
  return dir;
}
const repoA = syntheticRepo({ Alpha: 'alpha.md', Beta: 'beta.md' });
const elsewhere = fs.mkdtempSync(path.join(os.tmpdir(), 'solutions-hook-elsewhere-'));
fs.mkdirSync(path.join(elsewhere, '.git'));
try {
  // $CLAUDE_PROJECT_DIR names the SESSION's root. A session started in one checkout that writes
  // into another must be matched against the tree the file is going into - before this case the
  // hook read the session's corpus and was silent on the incident text written into a worktree.
  expectFires(
    'the corpus is the tree the file is in, not $CLAUDE_PROJECT_DIR',
    runHook('root1', path.join(repoA, 'notes.md'), 'Alpha', { CLAUDE_PROJECT_DIR: elsewhere }),
    'docs/solutions/x/alpha.md',
  );
  expectSilent(
    'a file outside any corpus tree is silent even when the session root has one',
    runHook('root2', path.join(elsewhere, 'notes.md'), 'Alpha', { CLAUDE_PROJECT_DIR: repoA }),
  );

  // The store must remember only what was SHOWN. Recording every fresh hit marked the ones the cap
  // hid as seen, so they were silenced for the session without ever being printed.
  const capped = runHook('cd', path.join(repoA, 'notes.md'), 'Alpha Beta', { PC_SOLUTIONS_HOOK_CAP: '1' });
  expectFires('cap: the first write shows one write-up and counts the other', capped, '1 further write-up');
  n += 1;
  if (!capped.includes('beta.md')) console.log('  ok   cap: the hidden write-up is not listed');
  else { console.log(`  FAIL cap: hidden write-up was listed: ${capped}`); fails += 1; }
  expectFires(
    'cap: a hidden write-up still surfaces on a later write in the same session',
    runHook('cd', path.join(repoA, 'notes.md'), 'Beta', { PC_SOLUTIONS_HOOK_CAP: '1' }),
    'beta.md',
  );
  expectSilent(
    'dedupe: once both have been shown, naming both again is silent',
    runHook('cd', path.join(repoA, 'notes.md'), 'Alpha Beta', { PC_SOLUTIONS_HOOK_CAP: '1' }),
  );
} finally {
  fs.rmSync(repoA, { recursive: true, force: true });
  fs.rmSync(elsewhere, { recursive: true, force: true });
}

console.log('fixture-based (decoupled from the live corpus):');
// The cases above drive the real docs/solutions/ tree, so a retitled write-up or a moved class reddens
// them for reasons unrelated to hook logic. These pin the LOGIC against a fixed vocabulary and fixed
// write-ups, injected through the parameters match() exposes for exactly this purpose.
const types = new Set(['Alpha', 'Beta']);
const docs = [
  { relPath: 'docs/solutions/x/alpha.md', title: 'About Alpha', components: ['Alpha', 'concept'],
    appliesWhen: ['Touching Alpha at all', 'Second reason', 'Third reason', 'Fourth reason'] },
  { relPath: 'docs/solutions/x/beta.md', title: 'About Beta', components: ['Beta'], appliesWhen: [] },
];
const fx = (text) => match(text, '/nonexistent', { types, docs });

n += 1;
const one = fx('We change Alpha here.');
if (one.length === 1 && one[0].relPath.endsWith('alpha.md') && one[0].named.join() === 'Alpha') {
  console.log('  ok   fixture: exact component name matches exactly one write-up');
} else { console.log(`  FAIL fixture exact: ${JSON.stringify(one)}`); fails += 1; }

n += 1;
if (fx('AlphaBet and betaAlpha and Alpha_x').length === 0) {
  console.log('  ok   fixture: boundary control - prefix, suffix and underscore do not match');
} else { console.log('  FAIL fixture boundary control fired'); fails += 1; }

n += 1;
if (fx('concept').length === 0) {
  console.log('  ok   fixture: a concept-only related_components entry is inert (not a Java type)');
} else { console.log('  FAIL fixture: concept entry matched'); fails += 1; }

n += 1;
const rendered = render(one, 4);
const shownWhen = (rendered.match(/applies when:/g) || []).length;
if (rendered.includes('applies when: Touching Alpha at all') && shownWhen === 3) {
  console.log('  ok   fixture: applies_when is shown for a matched write-up, capped at three lines');
} else { console.log(`  FAIL fixture applies_when render (${shownWhen} lines):\n${rendered}`); fails += 1; }

n += 1;
const both = fx('Alpha and Beta');
const capped = render(both, 1);
if (both.length === 2 && /1 further write-up/.test(capped)) {
  console.log('  ok   fixture: render says how many it hid');
} else { console.log(`  FAIL fixture hidden count: ${capped}`); fails += 1; }

// Quoted YAML list items. The fixtures above inject `docs` directly, which bypasses writeUps()'s
// own front-matter regex entirely - so none of them would have caught a real corpus write-up like
// docs/solutions/architecture-patterns/a-deprecation-without-an-explanation-misattributes-itself-to-the-wrong-party.md,
// whose related_components are all quoted (`- "PcUnsupportedConstruct"`). A parser that strips the
// leading `- ` but not a surrounding quote pair leaves the quotes attached, so `vocabulary.has(c)`
// is always false and the entry can never fire. This drives writeUps() itself against a synthetic
// tree so the case is decoupled from the live corpus but still exercises the real parser.
n += 1;
const quotedRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'solutions-hook-quoted-'));
try {
  const quotedDir = path.join(quotedRoot, 'docs', 'solutions', 'x');
  fs.mkdirSync(quotedDir, { recursive: true });
  fs.writeFileSync(
    path.join(quotedDir, 'quoted.md'),
    [
      '---',
      'related_components:',
      '  - "Alpha"',
      "  - 'Beta'",
      'applies_when:',
      '  - "Touching Alpha in a specific way"',
      '---',
      '# Quoted front matter',
      'body',
      '',
    ].join('\n'),
  );
  const [wu] = writeUps(quotedRoot);
  const ok = wu
    && wu.components.join(',') === 'Alpha,Beta'
    && wu.appliesWhen.join(',') === 'Touching Alpha in a specific way';
  if (ok) console.log('  ok   fixture: quoted related_components/applies_when list items lose their quotes');
  else { console.log(`  FAIL fixture quoted list items: ${JSON.stringify(wu)}`); fails += 1; }
} finally {
  fs.rmSync(quotedRoot, { recursive: true, force: true });
}

console.log('');
if (fails === 0) {
  console.log(`test-check-solutions-hook: ${n} case(s), all passed.`);
  process.exit(0);
}
console.log(`test-check-solutions-hook: ${fails} of ${n} case(s) FAILED.`);
process.exit(1);
