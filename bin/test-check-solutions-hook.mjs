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

import { execFileSync } from 'node:child_process';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const HOOK = path.join(root, '.claude', 'hooks', 'inject-solutions-for-named-components.mjs');

// The hook remembers, per session, what it has already surfaced. Fixed session ids would pass on a
// clean machine and fail on every run after - a red that means nothing - so each invocation of this
// suite gets its own nonce.
const RUN = `t${process.pid}-${Math.floor(Date.now() % 1e6)}`;

let fails = 0;
let n = 0;

function runHook(caseName, filePath, content, env = {}) {
  const payload = JSON.stringify({
    session_id: `${RUN}-${caseName}`,
    hook_event_name: 'PreToolUse',
    tool_input: { file_path: filePath, content },
  });
  try {
    return execFileSync('node', [HOOK], {
      input: payload,
      encoding: 'utf8',
      env: { ...process.env, CLAUDE_PROJECT_DIR: root, ...env },
    });
  } catch {
    return '';
  }
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

console.log('');
if (fails === 0) {
  console.log(`test-check-solutions-hook: ${n} case(s), all passed.`);
  process.exit(0);
}
console.log(`test-check-solutions-hook: ${fails} of ${n} case(s) FAILED.`);
process.exit(1);
