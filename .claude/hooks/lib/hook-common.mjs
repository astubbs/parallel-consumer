/*
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

// What every Node hook in this directory needs and none should carry its own copy of: reading the
// event, deriving the tree the event is ABOUT, and remembering per session what has already been
// said.
//
// Extracted from inject-solutions-for-named-components.mjs when a second hook needed all three
// (the plan's KTD4). The tree derivation is the part that earns a shared home, because it encodes
// an ordering that was got wrong three times in one day: a hook process's own directory, and
// `$CLAUDE_PROJECT_DIR`, describe the SESSION, never the command the hook is guarding. The
// incident and the order that fixed it are in
// docs/solutions/workflow-issues/a-hook-processes-own-directory-describes-the-session-not-the-command-2026-08-31.md;
// what is here is that order, once.
//
// NOTHING HERE PRINTS OR EXITS. A hook's stdout is the JSON envelope only, and whether to print one
// is the hook's decision; this file answers questions.

import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import crypto from 'node:crypto';

/** The whole of stdin, or '' when there is none - a hook must survive being run by hand. */
export function readStdin() {
  try {
    return fs.readFileSync(0, 'utf8');
  } catch {
    return '';
  }
}

/** The nearest enclosing checkout: a `.git` entry marks it - a directory, or the file a worktree carries. */
export function treeContaining(dir) {
  let d = path.resolve(dir);
  for (;;) {
    if (fs.existsSync(path.join(d, '.git'))) return d;
    const parent = path.dirname(d);
    if (parent === d) return null;
    d = parent;
  }
}

/**
 * A `cd <path>` that a Bash command OPENS with - the second-strongest statement of where it runs.
 *
 * Literal paths only. The hook reads the command before the shell expands it, so `cd "$W" &&`
 * arrives as the text `$W`, and resolving that against anything produces a path that does not
 * exist. Rather than fall through to a wrong tree, an unexpandable `cd` yields null and the caller
 * moves to the next source. `~` is left alone for the same reason: the hook's HOME is the session's.
 */
export function leadingCd(command) {
  if (typeof command !== 'string') return null;
  const m = command.match(/^\s*cd\s+("([^"]+)"|'([^']+)'|(\S+))\s*(?:&&|;)/);
  if (!m) return null;
  const target = m[2] ?? m[3] ?? m[4];
  if (/[$`~*?[{]/.test(target)) return null;
  return target;
}

/**
 * THE DIRECTORY RELATIVE PATHS IN THIS EVENT RESOLVE AGAINST, strongest source first: a leading
 * literal `cd` in the command, then the payload's own `cwd` - where a subagent's real directory
 * arrives - then the session root, then this process's directory as the last resort. The two
 * session-level answers are last because they describe the session, not the command.
 */
export function baseDir(ev) {
  const ti = (ev && ev.tool_input) || {};
  const cwd = ev && typeof ev.cwd === 'string' && ev.cwd ? ev.cwd : process.env.CLAUDE_PROJECT_DIR || process.cwd();
  const cd = leadingCd(ti.command);
  return cd ? path.resolve(cwd, cd) : cwd;
}

/**
 * The tree the event is ABOUT, derived from what it names before where it was sent from. A session
 * started in the main checkout that writes into a worktree would otherwise be matched against
 * master's corpus and silently miss what the branch added - reproduced before this ordering
 * existed. `$CLAUDE_PROJECT_DIR` names the SESSION's root and is the last resort, not the first
 * answer.
 *
 * `named` is the list of paths the event names (a Read's `file_path`, a Bash command's path
 * tokens); it defaults to the payload's `file_path` so the write hooks need not spell it.
 */
export function repoRoot(ev, named = null) {
  const ti = (ev && ev.tool_input) || {};
  const base = baseDir(ev);
  const paths = named ?? (typeof ti.file_path === 'string' && ti.file_path ? [ti.file_path] : []);
  const candidates = [...paths.map((p) => path.dirname(path.resolve(base, p))), base];
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
 * Fire once per thing per session: a plan naming a component twenty times must not reprint the
 * same warning twenty times. Keyed on the session, so a later session is told again; namespaced by
 * hook, so two hooks remembering different kinds of key never read each other's.
 *
 * Returns `{has, remember}` over a plain line-per-key file, or null when there is no session id or
 * no writable temp dir - and a null store means "say it", because failing to remember costs a
 * repeat, not correctness.
 */
export function seenStore(hookName, sessionId) {
  if (!sessionId) return null;
  let file;
  try {
    const dir = path.join(os.tmpdir(), `pc-hook-seen-${typeof process.getuid === 'function' ? process.getuid() : 'x'}`);
    fs.mkdirSync(dir, { recursive: true, mode: 0o700 });
    file = path.join(dir, `${hookName}-${crypto.createHash('sha256').update(String(sessionId)).digest('hex').slice(0, 16)}`);
  } catch {
    return null;
  }
  let seen = new Set();
  if (fs.existsSync(file)) {
    try {
      seen = new Set(fs.readFileSync(file, 'utf8').split('\n').filter(Boolean));
    } catch {
      /* a cache that cannot be read is a cache miss, never an error */
    }
  }
  return {
    has: (key) => seen.has(key),
    // Remember only what is about to be SHOWN - the solutions hook once recorded every fresh hit,
    // which marked the ones its cap hid as seen and silenced them for the session unprinted.
    remember: (keys) => {
      try {
        fs.appendFileSync(file, keys.map((k) => `${k}\n`).join(''));
      } catch {
        /* failing to remember costs a repeat, not correctness */
      }
    },
  };
}
