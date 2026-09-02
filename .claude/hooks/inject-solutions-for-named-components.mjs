#!/usr/bin/env node
/*
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

// PreToolUse hook: surface a `docs/solutions/` write-up at the moment its subject is being written
// about, rather than once at session start among all the others.
//
// The matching, the reasoning behind it, and the worked incident that produced it live in
// `.claude/hooks/lib/solutions-for-named-components.mjs`. This file is the wiring: read the event,
// suppress repeats, print the block.
//
// IT NEVER BLOCKS. A missed match must cost nothing, so every failure path here exits 0 silently -
// a hook that can fail a tool call in order to be helpful is worse than no hook.
//
// The first draft was bash driving python via two heredocs, and a second stdin redirection silently
// clobbered the first: the hook produced no output, which is indistinguishable from "nothing
// matched". That is the class `bin/AGENTS.md` cites for the Node-first ruling, met on the first try.

import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import crypto from 'node:crypto';
import { match, render, isSelfReferential } from './lib/solutions-for-named-components.mjs';

// A cap that does not parse, or is not positive, falls back to the default: `Number('abc')` is NaN,
// and `slice(0, NaN)` is an empty list under a header that promises entries.
const requestedCap = Number.parseInt(process.env.PC_SOLUTIONS_HOOK_CAP ?? '', 10);
const CAP = Number.isInteger(requestedCap) && requestedCap > 0 ? requestedCap : 4;

function readStdin() {
  try {
    return fs.readFileSync(0, 'utf8');
  } catch {
    return '';
  }
}

/** The nearest enclosing checkout: a `.git` entry marks it - a directory, or the file a worktree carries. */
function treeContaining(dir) {
  let d = path.resolve(dir);
  for (;;) {
    if (fs.existsSync(path.join(d, '.git'))) return d;
    const parent = path.dirname(d);
    if (parent === d) return null;
    d = parent;
  }
}

/**
 * The tree the write is going INTO, derived from the write itself - the way `pre-commit-gate.sh`
 * derives the commit's tree from its command rather than from the session. `$CLAUDE_PROJECT_DIR`
 * names the SESSION's root, and a session started in the main checkout that writes into a worktree
 * would otherwise be matched against master's corpus and silently miss what the branch added.
 * Reproduced before this ordering existed: the incident text, written into a worktree whose
 * session root was the main checkout, surfaced nothing. The variable is the last resort, not the
 * first answer.
 */
function repoRoot(ev) {
  const ti = ev.tool_input || {};
  const cwd = typeof ev.cwd === 'string' && ev.cwd ? ev.cwd : process.cwd();
  const candidates = [];
  if (typeof ti.file_path === 'string' && ti.file_path) {
    candidates.push(path.dirname(path.resolve(cwd, ti.file_path)));
  }
  candidates.push(cwd);
  for (const c of candidates) {
    try {
      const found = treeContaining(c);
      if (found) return found;
    } catch {
      /* an unreadable candidate is not the answer; keep looking */
    }
  }
  return process.env.CLAUDE_PROJECT_DIR || null;
}

/**
 * Fire once per write-up per session: a plan naming a component twenty times must not reprint the
 * same warning twenty times. Keyed on the session, so a later session is told again.
 */
function seenStore(sessionId) {
  if (!sessionId) return null;
  try {
    const dir = path.join(os.tmpdir(), `pc-solutions-hook-${typeof process.getuid === 'function' ? process.getuid() : 'x'}`);
    fs.mkdirSync(dir, { recursive: true, mode: 0o700 });
    return path.join(dir, crypto.createHash('sha256').update(sessionId).digest('hex').slice(0, 16));
  } catch {
    return null;
  }
}

function main() {
  const raw = readStdin();
  if (!raw.trim()) return;

  let ev;
  try {
    ev = JSON.parse(raw);
  } catch {
    return;
  }

  const root = repoRoot(ev);
  if (!root || !fs.existsSync(path.join(root, 'docs', 'solutions'))) return;

  const ti = ev.tool_input || {};
  if (isSelfReferential(ti.file_path)) return;

  // Everything the agent is about to commit to the tree, plus where it is going. Edit carries the
  // replacement text, Write the whole body, MultiEdit a list of edits.
  const parts = [ti.file_path, ti.content, ti.new_string];
  for (const e of Array.isArray(ti.edits) ? ti.edits : []) {
    if (e && typeof e === 'object') parts.push(e.new_string);
  }
  const text = parts.filter((p) => typeof p === 'string' && p).join('\n');

  const hits = match(text, root);
  if (hits.length === 0) return;

  const store = seenStore(String(ev.session_id || ''));
  let seen = new Set();
  if (store && fs.existsSync(store)) {
    try {
      seen = new Set(fs.readFileSync(store, 'utf8').split('\n').filter(Boolean));
    } catch {
      /* a cache that cannot be read is a cache miss, never an error */
    }
  }

  const fresh = hits.filter((h) => !seen.has(h.relPath));
  if (fresh.length === 0) return;

  // Remember only what is about to be SHOWN. Recording the whole fresh set marked the write-ups the
  // cap hid as seen, so they were silenced for the rest of the session without ever being printed.
  // Hits are in a fixed order (category, then filename), so the next write that names a hidden
  // one's component surfaces it instead of repeating the first CAP.
  if (store) {
    try {
      fs.appendFileSync(store, fresh.slice(0, CAP).map((h) => h.relPath).join('\n') + '\n');
    } catch {
      /* failing to remember costs a repeat, not correctness */
    }
  }

  process.stdout.write(
    JSON.stringify({
      hookSpecificOutput: {
        hookEventName: ev.hook_event_name || 'PreToolUse',
        additionalContext: render(fresh, CAP),
      },
    }),
  );
}

try {
  main();
} catch {
  /* never block a tool call to be helpful */
}
process.exit(0);
