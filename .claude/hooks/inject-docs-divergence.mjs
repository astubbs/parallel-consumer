#!/usr/bin/env node
/*
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

// PostToolUse hook on `Read|Bash`: when the agent has just read a document under one of the three
// corpus areas (docs/inflight/, docs/solutions/, docs/plans/), put the divergence header's summary
// line beside the read - how many versions of that document exist on other live refs carrying
// content the baseline has never held, whether this copy is the baseline's, its branch's own, or
// branch-only, and the command for the rest.
//
// WHY. An agent holding a note cannot see from its own branch that another branch rewrote the same
// note; the stale-copy incident the plan opens with was exactly that, and the header is the one
// piece that would have caught it. The write-time solutions hook beside this one answers "does a
// write-up exist about what you are writing"; this answers "is the document you just read the
// only version of itself". The query is `drift(path, {detail: 'summary'})` in bin/lib/notes.mjs,
// the same function `note drift` and `inflight docs header` render at the full tier, so the hook
// and the command cannot disagree about which versions are divergent (the plan's KTD2).
//
// BUDGET: 500 ms cold, the plan's R19 figure for this delivery. MEASURED 2026-09-03 on this
// repository (Apple Silicon, warm disk, about 610 refs), end to end from stdin to exit with a fresh
// session each run, median of five: a Read of docs/inflight/bug-857-family.md costs about 210 ms
// (200 ms before the working-tree hash moved to `git hash-object`, which is the one process the
// change added); the silent path (a Read outside the corpus) about 70 ms, against 50 ms for a
// bare `node -e 0` on the same host - because nothing git-touching is imported until a corpus file
// is found. Seven git processes make up the difference when it fires: the branch name, the blob at
// HEAD, the working-tree file's hash, and the four the summary tier costs (refs, blobs, history,
// one merge-base and diff). There is no warm state: each firing is a fresh process, and the query
// keeps no corpus cache (KTD5).
//
// ONCE PER SESSION PER STATE (KTD4). The seen key is the path, the committed blob at HEAD and the
// sorted set of divergent blobs; a repeat read is silent until that set changes, and a change -
// another branch adding a version - makes the header fire again, because that is news.
//
// THE COMPARISON SUBJECT IS THE COMMITTED BLOB AT HEAD (KTD15), in the tree the event names - a
// Read's file_path, a Bash command's path tokens resolved against its leading `cd`, then the
// payload's cwd, with the session root last (the 2026-08-31 wrong-directory solution). When the
// working-tree file differs from that blob, the header says so and describes the committed one.
//
// BASH IS BEST-EFFORT BY PATH TOKEN (KTD12). A whitespace-split token that resolves to an existing
// file under a corpus area counts; `cat "$f"`, a glob, or a path built by a pipeline is not
// resolved, and the header promises nothing for those. Variables are refused rather than guessed
// at for the reason pre-commit-gate.sh refuses `git -C "$W"`: the hook reads the command before
// the shell expands it.
//
// IT NEVER BLOCKS, AND IT NEVER PRINTS ON FAILURE (R20). Every failure path exits 0 with nothing
// on stdout; the failure is recorded instead, in the tool's cache as `delivery-failures.json`
// (KTD13), and bare `inflight docs` prints a one-line notice while the record exists - because a
// hook that has been broken for a week is otherwise indistinguishable from one with nothing to say.
// A later success of this delivery clears its entry.
//
// The `PostToolUse` event on the Read tool delivering `additionalContext` was verified live on
// Claude Code 2.1.258 before this was written; docs/agent-harness.md records the check.
//
// Self-tested by bin/test-check-docs-hooks.mjs, including the silent twins and a mutant control.

import fs from 'node:fs';
import path from 'node:path';
import { readStdin, baseDir, treeContaining, seenStore, runFailingOpen } from './lib/hook-common.mjs';
import { DOC_AREAS } from '../../bin/lib/repo.mjs';

/** The name this delivery records failures under; `inflight docs` prints it back. */
const DELIVERY = 'read-time header';

// A Bash command naming many corpus files (a `cat` over a directory listing, say) is capped so the
// budget holds: each path is one query, and four is already more than a reader takes in.
const MAX_PATHS = 4;

/** One matching pair of quotes stripped, and the punctuation a shell leaves stuck to a path. */
const unquote = (t) => t.replace(/^(["'])(.*)\1$/, '$2').replace(/[;|)&]+$/, '');

/** A token the shell would pass through unchanged - no expansion, no option. */
const literal = (t) => t.length > 0 && !/[$`*?[{]/.test(t) && !t.startsWith('-') && !t.startsWith('~');

/** The paths this event names, before any of them is resolved. */
function namedPaths(ev) {
  const ti = ev.tool_input || {};
  if (ev.tool_name === 'Read') {
    return typeof ti.file_path === 'string' && ti.file_path ? [ti.file_path] : [];
  }
  if (ev.tool_name === 'Bash' && typeof ti.command === 'string') {
    return ti.command.split(/\s+/).map(unquote).filter(literal);
  }
  return [];
}

/**
 * The token as a corpus file: its checkout, and its path relative to that checkout - or null. This
 * is the whole pre-git filter: an event naming nothing under a corpus area returns before any
 * git-touching module is even imported.
 */
function corpusFile(token, base) {
  const abs = path.resolve(base, token);
  let stat;
  try {
    stat = fs.statSync(abs);
  } catch {
    return null;
  }
  if (!stat.isFile()) return null;
  const tree = treeContaining(path.dirname(abs));
  if (!tree) return null;
  const rel = path.relative(tree, abs).split(path.sep).join('/');
  if (!DOC_AREAS.some((area) => rel.startsWith(`${area.dir}/`))) return null;
  return { tree, rel, abs };
}

async function main() {
  const raw = readStdin();
  if (!raw.trim()) return;
  let ev;
  try {
    ev = JSON.parse(raw);
  } catch {
    return;
  }
  if (!ev || typeof ev !== 'object') return;

  const base = baseDir(ev);
  const found = [];
  for (const token of namedPaths(ev)) {
    const f = corpusFile(token, base);
    if (f && !found.some((g) => g.abs === f.abs)) found.push(f);
  }
  if (found.length === 0) return;

  // Loaded only now: the silent path above must cost Node's start and nothing else.
  const [{ drift }, { exec, workingTreeBlob }, { formatDivergenceHeader, sourceFrame }, { clearDeliveryFailure }] = await Promise.all([
    import('../../bin/lib/notes.mjs'),
    import('../../bin/lib/git.mjs'),
    import('../../bin/lib/docs-views.mjs'),
    import('../../bin/lib/cache.mjs'),
  ]);

  const store = seenStore('docs-divergence', String(ev.session_id || ''));
  const blocks = [];
  for (const f of found.slice(0, MAX_PATHS)) {
    // Every bin/lib call reads git from the process's directory, so the process goes to the tree
    // the event named - never the session's.
    process.chdir(f.tree);
    // The branch name, so the header can say "adds-heading's OWN divergent version" rather than
    // "HEAD's"; a detached HEAD has no short name and is reported as HEAD.
    const symbolic = exec('git', ['symbolic-ref', '--short', '--quiet', 'HEAD']);
    const ref = symbolic.ok && symbolic.out.trim() ? symbolic.out.trim() : 'HEAD';
    const head = exec('git', ['rev-parse', '--verify', '--quiet', `HEAD:${f.rel}`]);
    // An untracked or freshly created file has no committed blob; the query then reports that no
    // ref carries the path, which is the true state of a note nobody has committed yet.
    const blob = head.ok ? head.out.trim() : null;
    // Hashed by git, on the FIRING path only: `hash-object --path` applies the clean filters and
    // line-ending normalisation git would at `git add`, where a hash over the raw bytes calls a
    // clean CRLF checkout edited. A hash git cannot produce is a failure to record, not an edit.
    let uncommitted = blob === null;
    if (!uncommitted) {
      const onDisk = workingTreeBlob(f.rel);
      if (onDisk === null) throw new Error(`${f.rel}: git hash-object failed on the working-tree file`);
      uncommitted = onDisk !== blob;
    }

    const d = drift(f.rel, { detail: 'summary', at: blob ? { ref, blob } : { ref } });
    if (d.ok === false) throw new Error(`${f.rel}: ${d.reason}`);

    const key = [f.rel, blob ?? 'uncommitted', ...(d.divergent ?? []).map((c) => c.blob).sort()].join(' ');
    if (store && store.has(key)) continue;
    if (store) store.remember([key]);

    blocks.push(sourceFrame(
      'header',
      f.rel,
      formatDivergenceHeader(d, { tier: 'summary', uncommitted }),
      `node bin/inflight.mjs docs header ${f.rel}`,
    ));
  }

  // Reached only when every query answered: the record this clears is the one the catch below writes.
  clearDeliveryFailure(DELIVERY);
  if (blocks.length === 0) return;

  process.stdout.write(JSON.stringify({
    hookSpecificOutput: {
      hookEventName: ev.hook_event_name || 'PostToolUse',
      additionalContext: blocks.join('\n\n'),
    },
  }));
}

await runFailingOpen(DELIVERY, main);
