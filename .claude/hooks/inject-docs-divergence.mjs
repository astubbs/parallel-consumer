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
// is found. Eight git processes make up the difference when it fires: the branch name, the
// freshness probe (one `rev-parse` answering three flags), the blob at HEAD, the working-tree
// file's hash, and the four the summary tier costs (refs, blobs, history, one merge-base and
// diff). There is no warm state: each firing is a fresh process, and the query keeps no corpus
// cache (KTD5). A Bash command naming FOUR corpus files - MAX_PATHS, the most one event answers
// for - costs about 460 ms (456-468 over five runs, same host, same method): the refs, the
// baseline, the branch name and the probe are resolved once per tree and threaded into every
// query, and what remains per path is its own blob, its hash, and the summary tier's four. Before
// that hoist each path listed every ref again, and the same read measured 613-793 ms.
// RE-MEASURED 2026-09-03 when the probe was added, interleaved against the version without it,
// nine runs each, on the same host at a load average of about 5: one path 237 ms against 229
// before, four paths 508 ms against 509 - the probe is within the noise, and the absolute figures
// on a loaded host are the host's (its own previous version measured 509 there, against the 456-468
// above on the quiet one). `freshnessWarnings`'s full call cost 25 ms more; `invalidatingOnly`
// exists because of that measurement.
//
// ONCE PER SESSION PER STATE (KTD4). The seen key is the path, the committed blob at HEAD and the
// sorted set of divergent blobs; a repeat read is silent until that set changes, and a change -
// another branch adding a version - makes the header fire again, because that is news. The keys
// are written once, just before the envelope, for the paths in it: a key written per path inside
// the loop marked a header seen that a later path's failure then kept from ever being shown.
//
// A PARTIAL REF SET IS SAID FIRST. On a shallow or never-fetched clone the baseline history the
// divergent set is computed against is truncated, so a version the baseline once held counts as
// content it has never held - a confident wrong count. The line then opens with `UNRELIABLE (<id>
// - run: <remedy>):`, from the same INVALIDATING_WARNINGS classification `docs show` prints in full;
// the dating warnings are not repeated here, because a hook whose silence must be earned cannot
// cry wolf on every read.
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
// the shell expands it. And a directory change the leading-`cd` rule did not consume - a `cd` or
// `pushd` later in the command, `git -C`, `--git-dir`, `GIT_DIR=` - keeps only the ABSOLUTE
// tokens, because a relative one resolved against the payload's cwd names the session tree's copy
// of a file the command read in another worktree (`namedPaths` carries the reasoning).
//
// PAST THE CAP, THE REST IS NAMED. Four paths are answered for; a fifth and beyond are listed on
// one trailing line as not checked, each with its own `docs header` command, because an answer
// that looks complete while a named path went unchecked is the truncated-but-plausible index the
// session hook refuses to print.
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
import { readStdin, baseDir, leadingCd, LEADING_CD, treeContaining, seenStore, runFailingOpen } from './lib/hook-common.mjs';
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

/**
 * A directory change that `leadingCd` did not consume: a `cd` or `pushd` as a command word anywhere
 * past the leading literal one (a subshell's, a second segment's, or the leading one when its target
 * was a variable and so refused), `git -C`, `--git-dir`, or a `GIT_DIR=` assignment. A relative
 * token in such a command resolves against a tree this hook cannot name.
 */
const CHANGES_DIRECTORY = /(^|[\s;&|(`{])(cd|pushd)(\s|$)|\bgit\s+(?:-\S+\s+)*-C(\s|$)|--git-dir(=|\s)|(^|[\s;&|(`{])GIT_DIR=/;

/**
 * The paths this event names, before any of them is resolved.
 *
 * ONLY ABSOLUTE TOKENS SURVIVE A DIRECTORY CHANGE THE LEADING-cd RULE DID NOT CONSUME. Every
 * worktree of this repository carries the same note paths, so `(cd <wt> && cat docs/inflight/x.md)`
 * or `git -C <wt> diff -- docs/inflight/x.md` resolved against the payload cwd describes the SESSION
 * tree's copy of a file the command read in another tree - the stale-copy incident, delivered with
 * the hook's badge on it. Silence over a guess, the rule `leadingCd` applies to `cd "$W"`.
 */
function namedPaths(ev) {
  const ti = ev.tool_input || {};
  if (ev.tool_name === 'Read') {
    return typeof ti.file_path === 'string' && ti.file_path ? [ti.file_path] : [];
  }
  if (ev.tool_name === 'Bash' && typeof ti.command === 'string') {
    const tokens = ti.command.split(/\s+/).map(unquote).filter(literal);
    const past = leadingCd(ti.command) === null ? ti.command : ti.command.replace(LEADING_CD, '');
    return CHANGES_DIRECTORY.test(past) ? tokens.filter((t) => path.isAbsolute(t)) : tokens;
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
  // The paths past the cap are NAMED at the end, never silently dropped: an answer that looks
  // complete while a path the command named went unchecked is the truncated-but-plausible index
  // the session hook refuses to print. Each carries its own `docs header` command instead.
  const checked = found.slice(0, MAX_PATHS);
  const dropped = found.slice(MAX_PATHS);

  // Loaded only now: the silent path above must cost Node's start and nothing else.
  const [{ drift }, { INVALIDATING_WARNINGS, baseline, exec, freshnessWarnings, refTips, workingTreeBlob }, { formatDivergenceHeader, sourceFrame }, { clearDeliveryFailure }] = await Promise.all([
    import('../../bin/lib/notes.mjs'),
    import('../../bin/lib/git.mjs'),
    import('../../bin/lib/docs-views.mjs'),
    import('../../bin/lib/cache.mjs'),
  ]);

  // THE REFS, THE BASELINE AND THE BRANCH NAME ARE RESOLVED ONCE PER TREE, NOT ONCE PER PATH. They
  // are the same answer for every path in one event, and `drift` re-asks git for the first two
  // when it is not handed them - a `for-each-ref` at 119 ms and a `rev-parse` per path, which is
  // how a four-path Bash read measured 613-793 ms against the 500 ms budget while a single path
  // sat at 210 ms. Keyed by tree because the paths a Bash command names may resolve to different
  // checkouts, and a ref listing belongs to the repository it was taken in. matchDocs in
  // bin/lib/terms.mjs threads the same pair through its hit loop for the same reason. Resolved
  // lazily, inside the loop, so the process is already in the right tree when git is asked.
  const resolvedPerTree = new Map();
  const treeFacts = (tree) => {
    if (!resolvedPerTree.has(tree)) {
      const tips = refTips();
      if (!tips.ok) throw new Error(`${tree}: cannot list refs - is this a git repository?`);
      // The branch name, so the header can say "adds-heading's OWN divergent version" rather than
      // "HEAD's"; a detached HEAD has no short name and is reported as HEAD.
      const symbolic = exec('git', ['symbolic-ref', '--short', '--quiet', 'HEAD']);
      const ref = symbolic.ok && symbolic.out.trim() ? symbolic.out.trim() : 'HEAD';
      const base = baseline();
      // ONLY THE WARNINGS THAT VOID THE ANSWER, and only on the firing path. A shallow or
      // never-fetched clone truncates the baseline history the divergent set is computed against
      // (bin/lib/notes.mjs's baselineHistoryBlobs), so a version the baseline ONCE held is counted
      // as content it has never held - a confident wrong count, from the one channel that answers
      // unasked. The parallel-agent re-shallowing docs/agent-harness.md describes makes this a
      // recurring state of the real clone, not a hypothetical. Rendered as the line's prefix. One
      // git process; the filter is the contract, the option is what makes it one process.
      const unreliable = freshnessWarnings(base, tips.tips.length, { invalidatingOnly: true }).filter((w) => INVALIDATING_WARNINGS.has(w.id));
      resolvedPerTree.set(tree, { tips: tips.tips, base, ref, unreliable });
    }
    return resolvedPerTree.get(tree);
  };

  const store = seenStore('docs-divergence', String(ev.session_id || ''));
  // Collected, not yet remembered: a later path can still throw, and a key written before the
  // write would mark a header seen that nobody saw - silenced for the session, unprinted, which is
  // the defect hook-common.mjs's seenStore comment names. Remembered once, just before the write.
  const pending = [];
  for (const f of checked) {
    // Every bin/lib call reads git from the process's directory, so the process goes to the tree
    // the event named - never the session's.
    process.chdir(f.tree);
    const { tips, base, ref, unreliable } = treeFacts(f.tree);
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

    const d = drift(f.rel, { detail: 'summary', at: blob ? { ref, blob } : { ref }, tips, base });
    if (d.ok === false) throw new Error(`${f.rel}: ${d.reason}`);

    const key = [f.rel, blob ?? 'uncommitted', ...(d.divergent ?? []).map((c) => c.blob).sort()].join(' ');
    if (store && store.has(key)) continue;

    pending.push({
      key,
      block: sourceFrame(
        'header',
        f.rel,
        formatDivergenceHeader(d, { tier: 'summary', uncommitted, warnings: unreliable }),
        `node bin/inflight.mjs docs header ${f.rel}`,
      ),
    });
  }

  // Reached only when every query answered: the record this clears is the one the catch below writes.
  clearDeliveryFailure(DELIVERY);
  const lines = pending.map((p) => p.block);
  // Every time, not once per session: nothing was learned about these paths, so there is no state
  // to have seen. Printed even when every checked path was already shown, because the unchecked
  // ones were not.
  if (dropped.length > 0) {
    const one = dropped.length === 1;
    lines.push(`+${dropped.length} more corpus path${one ? '' : 's'} this command named ${one ? 'was' : 'were'} NOT checked `
      + `(the header answers for at most ${MAX_PATHS} per command) - ask for each: ${dropped.map((d) => `node bin/inflight.mjs docs header ${d.rel}`).join(' ; ')}`);
  }
  if (lines.length === 0) return;
  if (store && pending.length > 0) store.remember(pending.map((p) => p.key));

  process.stdout.write(JSON.stringify({
    hookSpecificOutput: {
      hookEventName: ev.hook_event_name || 'PostToolUse',
      additionalContext: lines.join('\n\n'),
    },
  }));
}

await runFailingOpen(DELIVERY, main);
