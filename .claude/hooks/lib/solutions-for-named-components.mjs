/*
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

// Match a body of text against the components `docs/solutions/` write-ups declare themselves to be
// about, so the relevant write-up can be surfaced while the text is being written.
//
// WHY THIS EXISTS
//
// `docs/solutions/` front matter carries `related_components` - the classes a write-up is about -
// and `applies_when` - the situations it should be read in. Until this module, NOTHING READ EITHER
// FIELD: `grep -rln applies_when bin/ .claude/` returned nothing while a majority of write-ups
// carried one. The retrieval metadata was written, reviewed, and inert.
//
// What existed instead was `inject-recorded-knowledge.sh`, which lists every solution title at
// session start. That is right for discovery and wrong for RELEVANCE - a flat list of every title
// competes with itself, and `docs/compound-engineering.md` states the constraint outright: "the
// index is only useful while it is short enough to be read".
//
// WORKED INCIDENT, 2026-09-02 - the fourth of its kind, which is the point
//
// A review of astubbs#225 proposed that recovery from producer fencing rejoin the consumer group
// from the control thread while holding the produce/commit write lock. That deadlocks:
// `onPartitionsRevoked` spins on `isTransactionCommittingInProgress()`, which IS that write lock,
// and the consumer is confined to the broker-poll thread besides.
//
// `docs/solutions/architecture-patterns/two-threads-one-consumer-why-the-commit-seam-keeps-deadlocking.md`
// says so, and its `applies_when` names the situation twice over. Its own preamble records that
// THREE earlier investigations each re-derived part of it before acting. The review round that
// caught the deadlock was the fourth re-derivation, and it cost a round to do what reading one
// paragraph would have done.
//
// WHY IT MATCHES TEXT, NOT PATHS - and the measurement that decided it
//
// The obvious design fires when an agent edits a file a write-up names. Measured against the
// incident above, that design fires ZERO times: the episode was spent writing a requirements
// document and no Java file was touched at any point. Matching the TEXT fires five, including both
// write-ups that would have prevented the defect. A plan that discusses `ProducerManager` is as
// much "about" it as a diff that edits it, and design time is when the warning is still cheap.
//
// WHAT IT DELIBERATELY DOES NOT DO
//
// It does not MATCH on `applies_when`, whose lines are free prose with no reliable match against
// arbitrary text; a fuzzy match there reproduces the noise that made session-start injection
// ineffective. `related_components` is already structured, so matching uses the half that is
// machine-checkable. `applies_when` IS shown for a write-up that has already matched - at that
// point it costs no false positives, and it is the field carrying the retrieval intent. Coverage
// is therefore bounded by how many write-ups name real classes.

import fs from 'node:fs';
import path from 'node:path';

/** Basenames of every Java type in the repo - the vocabulary a `related_components` entry can name. */
export function javaTypeNames(root) {
  const names = new Set();
  const walk = (dir, depth) => {
    if (depth > 12) return;
    let entries;
    try {
      entries = fs.readdirSync(dir, { withFileTypes: true });
    } catch {
      return;
    }
    for (const e of entries) {
      if (e.name === 'target' || e.name === '.git' || e.name === 'node_modules') continue;
      const p = path.join(dir, e.name);
      if (e.isDirectory()) walk(p, depth + 1);
      else if (e.name.endsWith('.java')) names.add(e.name.slice(0, -5));
    }
  };
  let modules;
  try {
    modules = fs.readdirSync(root, { withFileTypes: true });
  } catch {
    return names;
  }
  for (const m of modules) {
    if (m.isDirectory() && m.name.startsWith('parallel-consumer-')) walk(path.join(root, m.name, 'src'), 0);
  }
  return names;
}

/** A front-matter list block as its unquoted, non-blank items - or [] when the field is absent. */
function parseYamlList(regex, body) {
  const m = regex.exec(body);
  return m ? m[1].split('\n').map(unquoteListItem).filter(Boolean) : [];
}

const RELATED = /^related_components:\n((?:[ \t]+-[ \t].*\n)+)/m;
const APPLIES = /^applies_when:\n((?:[ \t]+-[ \t].*\n)+)/m;

/**
 * One YAML list line (`  - Foo` or `  - "Foo"`) to its bare value.
 *
 * Front matter across this corpus mixes quoted and unquoted list items - the parser is a regex,
 * not a YAML library, so it must strip a balanced surrounding quote pair itself. Left unstripped,
 * a quoted `related_components` entry keeps its literal quote characters and can never match the
 * unquoted vocabulary `javaTypeNames()` builds, and a quoted `applies_when` line prints stray
 * quote marks in the rendered output.
 */
function unquoteListItem(line) {
  const bare = line.trim().replace(/^-\s*/, '').trim();
  const quoted = bare.match(/^(["'])(.*)\1$/);
  return quoted ? quoted[2] : bare;
}

/** Every write-up under docs/solutions/, with the components it declares and its title. */
export function writeUps(root) {
  const dir = path.join(root, 'docs', 'solutions');
  const out = [];
  let cats;
  try {
    cats = fs.readdirSync(dir, { withFileTypes: true });
  } catch {
    return out;
  }
  for (const c of cats.filter((c) => c.isDirectory()).sort((a, b) => a.name.localeCompare(b.name))) {
    const sub = path.join(dir, c.name);
    for (const f of fs.readdirSync(sub).filter((f) => f.endsWith('.md')).sort()) {
      const abs = path.join(sub, f);
      let body;
      try {
        body = fs.readFileSync(abs, 'utf8');
      } catch {
        continue;
      }
      // Both regexes require at least one list item, so a miss and an empty list coincide: this
      // `continue` fires on exactly the inputs it did when the two parses were hand-written twice.
      const components = parseYamlList(RELATED, body);
      if (components.length === 0) continue;
      const heading = body.split('\n').find((l) => l.startsWith('# '));
      const appliesWhen = parseYamlList(APPLIES, body);
      out.push({
        relPath: path.relative(root, abs),
        title: heading ? heading.slice(2).trim() : f.replace(/\.md$/, '').replace(/-/g, ' '),
        components,
        appliesWhen,
      });
    }
  }
  return out;
}

/**
 * Write-ups whose declared components are named in `text`.
 *
 * Word-boundary matching is the point: `MyProducerManagerFactory` must not match `ProducerManager`,
 * or every file mentioning a common prefix drags in write-ups about something else. JavaScript's
 * `\b` would treat `_` as a word character, so the guards are explicit.
 */
export function match(text, root, { types = null, docs = null } = {}) {
  if (!text || !text.trim()) return [];
  const vocabulary = types ?? javaTypeNames(root);
  if (vocabulary.size === 0) return [];
  const hits = [];
  for (const w of docs ?? writeUps(root)) {
    const named = w.components.filter((c) => {
      if (!vocabulary.has(c)) return false;
      const esc = c.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
      return new RegExp(`(^|[^A-Za-z0-9_])${esc}([^A-Za-z0-9_]|$)`).test(text);
    });
    if (named.length) hits.push({ ...w, named });
  }
  return hits;
}

/** True when the file being written is itself a write-up, or this mechanism's own test fixture. */
export function isSelfReferential(filePath) {
  const p = String(filePath || '');
  return /(^|\/)docs\/solutions\//.test(p) || p.includes('solutions-for-named-components');
}

/**
 * The block handed back to the agent.
 *
 * `docs/compound-engineering.md`: anything that hides items must say how many it hid - a capped list
 * reads downstream as the complete set, and the omission is invisible precisely because the cap
 * worked.
 */
export function render(hits, cap) {
  const shown = hits.slice(0, cap);
  const hidden = hits.length - shown.length;
  const lines = [
    'This text names components that solved problems are already written up against.',
    'Read before deciding, not after review catches it:',
    '',
  ];
  for (const h of shown) {
    lines.push(`  ${h.relPath}`);
    lines.push(`      ${h.title}`);
    lines.push(`      matched: ${h.named.join(', ')}`);
    // `applies_when` is not used for MATCHING - free prose against arbitrary text is the noise this
    // hook exists to cut through. But once a write-up has matched on a component, showing when its
    // author said to read it costs no false positives and hands over the retrieval intent directly.
    for (const w of (h.appliesWhen || []).slice(0, 3)) lines.push(`      applies when: ${w}`);
  }
  if (hidden > 0) {
    lines.push('');
    lines.push(
      `  ${hidden} further write-up(s) also matched and are not listed; ` +
        'grep docs/solutions/ for the component names above.',
    );
  }
  return lines.join('\n');
}
