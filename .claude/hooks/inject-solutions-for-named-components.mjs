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
import path from 'node:path';
import { readStdin, repoRoot, seenStore } from './lib/hook-common.mjs';
import { match, render, isSelfReferential } from './lib/solutions-for-named-components.mjs';

// A cap that does not parse, or is not positive, falls back to the default: `Number('abc')` is NaN,
// and `slice(0, NaN)` is an empty list under a header that promises entries.
const requestedCap = Number.parseInt(process.env.PC_SOLUTIONS_HOOK_CAP ?? '', 10);
const CAP = Number.isInteger(requestedCap) && requestedCap > 0 ? requestedCap : 4;

// `readStdin`, `repoRoot` (the tree the write is going INTO, derived from the write before the
// session) and the per-session `seenStore` were born here and moved to ./lib/hook-common.mjs when
// a second hook needed them; that file owns the reasoning behind each.

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

  const store = seenStore('solutions', String(ev.session_id || ''));
  const fresh = hits.filter((h) => !(store && store.has(h.relPath)));
  if (fresh.length === 0) return;

  // Remember only what is about to be SHOWN. Recording the whole fresh set marked the write-ups the
  // cap hid as seen, so they were silenced for the rest of the session without ever being printed.
  // Hits are in a fixed order (category, then filename), so the next write that names a hidden
  // one's component surfaces it instead of repeating the first CAP.
  if (store) store.remember(fresh.slice(0, CAP).map((h) => h.relPath));

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
