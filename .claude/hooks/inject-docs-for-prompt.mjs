#!/usr/bin/env node
/*
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

// UserPromptSubmit hook: when a prompt names a mechanism - a class, a snake_case or kebab-case
// name, a path, a backticked span, an issue number - put the titles and paths of the documents
// that carry that name, across EVERY live ref, beside the prompt: each marked when it exists off
// the baseline or has a divergent version elsewhere, capped, and with the command that lists the
// rest.
//
// WHY THIS EVENT. The prompt is where intent is stated; a file the agent touches comes later, and
// by then it has already decided. The words in a prompt are the primary injection trigger (the
// plan's Key Decisions), and the output shape is the prior-art result an agent already knows how
// to read. Same corpus, same query family as the read-time header beside this file
// (inject-docs-divergence.mjs): this one answers "does a document exist about what you are about
// to work on", that one "is the document you just read the only version of itself".
//
// WHY IT SEARCHES EVERY REF. Roughly two thirds of everything under docs/ exists only on branches
// that have not merged, so a working-tree grep answers a narrower question than the one asked
// (docs/solutions/workflow-issues/prior-art-lives-on-branches-2026-09-01.md). The query is
// `matchDocs` in bin/lib/terms.mjs: one `git grep` over the live refs, never a corpus-index
// build, and the divergence marks come from the same `drift` summary the header uses.
//
// BUDGET: 2500 ms cold when it fires, 100 ms cold on the silent path - the plan's R19 figures for
// this delivery. MEASURED 2026-09-03 on this repository (Apple Silicon, warm disk, 559 live refs
// of 612), end to end from stdin to exit with a fresh session each run, median of five:
//   - a prompt naming `ProducerManager` (60 documents, 11 shown - eight frontmatter and heading
//     hits plus the three the body cap keeps - 11 drift summaries): 1550 ms;
//   - a prompt naming `PartitionStateCommittedOffsetIT` (fewer hits, fewer summaries): 1070 ms;
//   - a prompt with no identifier in it: 65 ms, against 50 ms for `node -e 0` on the same host -
//     because only bin/lib/terms.mjs and the hook library are imported before a term survives,
//     and the git-touching modules are loaded dynamically afterwards.
// The `git grep` itself is about 450 ms of the firing figure whatever the term count (fixed
// strings, per terms.mjs's header - an alternation costs 2.6 s on its own), and the drift
// summaries most of the rest; MARK_LIMIT in terms.mjs is the knob if a slower host breaches the
// budget. There is no warm state (KTD5).
//
// ONCE PER SESSION PER STATE (KTD4). The seen key is the path plus its divergence state - on the
// baseline or not, divergent elsewhere or not - so a prompt naming the same class twice in one
// session injects once, and a document whose state changes is news again.
//
// SILENT WHEN NOTHING MATCHES (R12), and silent when a term survives but every hit was already
// shown. The empty answer still says what it covered, but only to the failure record and the
// tool: a prompt is not the place for "searched 559 refs, found nothing".
//
// IT NEVER BLOCKS, AND IT NEVER PRINTS ON FAILURE (R20). Every failure path exits 0 with nothing
// on stdout; the failure is recorded in the tool's cache as `delivery-failures.json` under
// `docs-for-prompt` (KTD13), and bare `inflight docs` prints a one-line notice while the record
// exists. A later success of this delivery clears its entry.
//
// The `UserPromptSubmit` event delivering `additionalContext` was already verified live by
// inject-merge-checklist.sh, which uses the same envelope; docs/agent-harness.md records it.
//
// Self-tested by bin/test-check-docs-hooks.mjs, including the silent twins and a mutant control.

import { readStdin, baseDir, treeContaining, seenStore } from './lib/hook-common.mjs';
import { termsFromPrompt } from '../../bin/lib/terms.mjs';

/** The name this delivery records failures under; `inflight docs` prints it back. */
const DELIVERY = 'docs-for-prompt';

/** Titles shown per prompt; the rest is a count and the command that lists them (KTD6). */
const CAP = 12;

/** The seen key: the path and the two marks, so a change of state is a new fact. */
const stateKey = (h) => `${h.path} ${h.onBaseline ? 'on-baseline' : 'off-baseline'} ${h.divergent ? 'divergent' : 'single'}`;

function renderLine(h) {
  const marks = [];
  if (!h.onBaseline) marks.push('off baseline');
  else if (h.divergent) marks.push('divergent elsewhere');
  const title = h.title ?? '(no title)';
  return `- ${title}  ${h.path}${marks.length ? `  (${marks.join(', ')})` : ''}`;
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
  if (!ev || typeof ev !== 'object' || typeof ev.prompt !== 'string') return;

  // The whole pre-git filter: no identifier, no import of anything that touches git.
  const terms = termsFromPrompt(ev.prompt);
  if (terms.length === 0) return;

  const tree = treeContaining(baseDir(ev));
  if (!tree) return;
  // Every bin/lib call reads git from the process's directory, so the process goes to the tree
  // the prompt was sent from - the payload's cwd, never the session root alone.
  process.chdir(tree);

  const [{ matchDocs }, { sourceFrame }, { clearDeliveryFailure }] = await Promise.all([
    import('../../bin/lib/terms.mjs'),
    import('../../bin/lib/views.mjs'),
    import('../../bin/lib/cache.mjs'),
  ]);

  const m = matchDocs(terms);
  if (!m.ok) throw new Error(m.reason);

  const store = seenStore('docs-for-prompt', String(ev.session_id || ''));
  // Only the marked hits can be keyed on state; the unmarked tail past MARK_LIMIT is counted, not shown.
  const marked = m.hits.filter((h) => h.onBaseline !== null);
  const fresh = marked.filter((h) => !store || !store.has(stateKey(h)));
  const unshownTail = m.hits.length - marked.length + m.truncated;

  // Reached only when the query answered: the record this clears is the one the catch below writes.
  clearDeliveryFailure(DELIVERY);
  if (fresh.length === 0) return;

  const shown = fresh.slice(0, CAP);
  if (store) store.remember(shown.map(stateKey));
  const more = fresh.length - shown.length + unshownTail;
  const body = [
    `${m.hits.length + m.truncated} document(s) across ${m.refsSearched} live ref(s) name ${terms.length === 1 ? 'this term' : 'these terms'}; ${shown.length} shown:`,
    ...shown.map(renderLine),
    ...(more > 0 ? [`+${more} more`] : []),
  ].join('\n');

  const quoted = terms.map((t) => (/^[A-Za-z0-9_./#-]+$/.test(t) ? t : `'${t.replaceAll("'", "'\\''")}'`));
  process.stdout.write(JSON.stringify({
    hookSpecificOutput: {
      hookEventName: 'UserPromptSubmit',
      additionalContext: sourceFrame('terms', terms, body, `node bin/inflight.mjs prior-art --headings ${quoted.join(' ')}`),
    },
  }));
}

try {
  await main();
} catch (e) {
  // Fail open, but not silently everywhere: the agent sees nothing, the cache remembers why.
  try {
    const { recordDeliveryFailure } = await import('../../bin/lib/cache.mjs');
    recordDeliveryFailure(DELIVERY, e && e.message ? e.message : String(e));
  } catch {
    /* the record is a courtesy; a tree where even that fails still gets its prompt */
  }
}
process.exit(0);
