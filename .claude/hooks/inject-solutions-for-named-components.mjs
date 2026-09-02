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
import { execFileSync } from 'node:child_process';
import { match, render, isSelfReferential } from './lib/solutions-for-named-components.mjs';

const CAP = Number(process.env.PC_SOLUTIONS_HOOK_CAP || 4);

function readStdin() {
  try {
    return fs.readFileSync(0, 'utf8');
  } catch {
    return '';
  }
}

function repoRoot() {
  if (process.env.CLAUDE_PROJECT_DIR) return process.env.CLAUDE_PROJECT_DIR;
  try {
    return execFileSync('git', ['rev-parse', '--show-toplevel'], { encoding: 'utf8' }).trim();
  } catch {
    return null;
  }
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

  const root = repoRoot();
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

  if (store) {
    try {
      fs.appendFileSync(store, fresh.map((h) => h.relPath).join('\n') + '\n');
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
