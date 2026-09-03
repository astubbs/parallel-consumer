// Copyright (C) 2026 Antony Stubbs and contributors
//
// THE INFLIGHT TAG VOCABULARY - the sets, the order, and the readers of one note's markers. THE
// SINGLE SOURCE, for both consumers: the session index (`bin/inflight.mjs docs index`, what
// .claude/hooks/inject-recorded-knowledge.sh injects) and bare `docs` import it; the bash gate
// (bin/check-inflight-tags.sh) sources bin/lib/inflight-tags.sh, which is a thin wrapper that runs
// `node bin/lib/inflight-tags.mjs --shell` and evals the variable assignments this file prints.
//
// It used to be two copies - this file a "port" of the shell file, held equal by a parity test -
// and the failure mode of two copies of a closed vocabulary is the worst one this system has: a
// value the gate accepts and the index cannot place files a note under "unmatched", or loses it.
// One source makes that class impossible; the round-trip self-test in bin/test-inflight.mjs
// (`tag-vocabulary-round-trips-through-the-shell-library`) now guards the PRINTER, not the copy.
//
// WHAT EACH VALUE MEANS lives in docs/inflight/AGENTS.md, which owns the vocabulary. This file owns
// which values exist and in which order the index presents them. Adding a value: add it here AND
// describe it in docs/inflight/AGENTS.md in the same commit, and say why the existing values do not
// fit - the gate's doc/lib agreement check catches the omission.
//
// ORDER IS LOAD-BEARING for the index, not the gate: within each partition the values are listed
// by cost of not knowing, signal-integrity classes first (you cannot judge the code through
// instruments that lie, so `misdirection` outranks `blind-spot` and both outrank any product
// defect), and the index emits its groups in exactly this order.
//
// THE PARTITION IS THE POINT - bug impacts and task impacts are separate sets, not one flat list,
// because the index groups them separately: a `bug` carrying `release-gate` once passed a flat-set
// gate and then appeared under "unmatched" in the index.
//
// No git, and no printing or exiting when IMPORTED: pure functions over a note's text. The one
// thing that prints is the `--shell` renderer at the bottom, and only when this file is the
// script Node was started with - never on import, so the index cannot pay for it.

import { realpathSync } from 'node:fs'
import { fileURLToPath } from 'node:url'

import { NOTES_DIR } from './repo.mjs'

// A REGISTER is consulted, never completed - a ranked backlog, a collision list. It has no done
// state, so filing it as a `task` implied a discrete action someone could finish and sorted it
// among things waiting to be done, when it is the thing you READ to decide what to do next.
// Surfaced in its own section at the top of the session index rather than among open work.
export const INFLIGHT_TYPES = ['bug', 'feature', 'task', 'register']

export const INFLIGHT_BUG_IMPACTS = [
    'misdirection', 'blind-spot', 'crash', 'data-loss', 'stall', 'security', 'config-lie', 'reliability', 'throughput',
]

export const INFLIGHT_TASK_IMPACTS = [
    'release-gate', 'coordination', 'stranded-work', 'ci', 'test-debt', 'refactor', 'process', 'deps-debt',
    'security', 'reliability',
]

// A FEATURE MAY CARRY AN IMPACT, and should whenever it addresses one. The point of the tag is that
// work falls out in priority order, not that it is filed under the correct part of speech: a
// commit-failure seam whose motivation is PC shutting down is `feature` + `crash`, and tagging it
// impact-less buries it among cosmetic features. Optional, because a genuinely new capability with
// no problem behind it has an opportunity rather than a consequence. A register may carry one too -
// a collision list whose cost of being unread is a collision.
//
// Derived, not listed a third time. `security` and `reliability` appear in both halves and
// therefore twice in the derived list; the gate tests membership, so the repeat costs nothing.
export const INFLIGHT_FEATURE_IMPACTS = [...INFLIGHT_BUG_IMPACTS, ...INFLIGHT_TASK_IMPACTS]
export const INFLIGHT_REGISTER_IMPACTS = [...INFLIGHT_BUG_IMPACTS, ...INFLIGHT_TASK_IMPACTS]

// LABELS ARE THE THIRD AXIS, AND IT IS A MECHANISM - deliberately neither of the other two. The
// filename prefix says the AREA a note is about; the impact says the CONSEQUENCE of not knowing
// it. Neither can say what a note is about MECHANICALLY, and that is what you search by when you
// sit down to do a piece of work: "show me the concurrency ones" spans bug-, core-, static-,
// deps- and release-, and its consequences are already spread across stall, data-loss, crash and
// reliability.
//
// WHY A CLOSED SET. An open free-text field becomes tag soup within a month and then partitions
// nothing, which is the failure this whole scheme exists to avoid. Add a value the way impacts
// were added - by reading the corpus and finding a group the existing values cannot express.
//
// WHY IT STARTS AT ONE VALUE. Because one is what the corpus currently justifies: a small minority
// of notes are concurrency-shaped, which is the band where a label partitions usefully rather than
// matching nearly everything. A speculative second value would be inventing a group and hoping.
export const INFLIGHT_LABELS = ['concurrency']

/**
 * The order the index presents impacts in, across every type - because a feature that prevents a
 * crash must appear beside the crashes, not after them. Position IS the priority: signal integrity
 * first, then what kills, then what corrupts, then what stops, then what is merely owed.
 */
export const INFLIGHT_IMPACT_ORDER = [
    'misdirection', 'blind-spot', 'crash', 'data-loss', 'stall', 'security', 'config-lie', 'reliability', 'throughput',
    'release-gate', 'coordination', 'stranded-work', 'ci', 'test-debt', 'refactor', 'process', 'deps-debt',
]

/**
 * The shell variables `--shell` prints, in this order - the names bin/check-inflight-tags.sh reads
 * after sourcing bin/lib/inflight-tags.sh. One place, so the printer, the gate and the round-trip
 * self-test cannot disagree about which variable holds which set.
 */
export const SHELL_VARIABLES = {
    INFLIGHT_TYPES, INFLIGHT_BUG_IMPACTS, INFLIGHT_TASK_IMPACTS, INFLIGHT_FEATURE_IMPACTS,
    INFLIGHT_REGISTER_IMPACTS, INFLIGHT_LABELS, INFLIGHT_IMPACT_ORDER,
}

/**
 * The vocabulary as bash assignments, one `NAME="v1 v2 ..."` per line - what the shell library
 * evals. Every value is a tag, so the shell word set is [a-z0-9-]; anything else would need
 * quoting the eval cannot be trusted with, and is refused here rather than emitted as a line that
 * parses into something else.
 */
export function shellAssignments() {
    return Object.entries(SHELL_VARIABLES).map(([name, values]) => {
        const bad = values.find((v) => !/^[a-z0-9-]+$/.test(v))
        if (bad !== undefined) throw new Error(`${name} holds '${bad}', which is not a bare shell word - the vocabulary is [a-z0-9-] only`)
        return `${name}="${values.join(' ')}"`
    }).join('\n')
}

// --- Reading one note's markers - the session index's rules, one function each. ----------------
//
// THE WHOLE MARKER, `-->` INCLUDED, is what every reader below requires. A note that merely
// MENTIONS `inflight-state:` in its prose - one quoting this convention, or the gate's own output -
// was once read as closed and rolled into a "not shown" count, which is exactly the omission the
// index claims its filters cannot make. And `[^>]*` rather than `.*` between the field and the
// close: a greedy match parsed a reason containing `>` that the gate could not cross, so one note
// was gate-green and listed as OPEN at every session start (astubbs#324 review).

const TYPE_RE = /inflight-type:\s*([a-z-]*)\s*-->/
const IMPACT_RE = /inflight-impact:\s*([a-z-]*)\s*-->/
const STATE_RE = /inflight-state:\s*([^>]*)-->/
const LABELS_RE = /inflight-labels:\s*([^>]*)-->/
/** Any state marker at all: its presence is what makes a note NOT open. */
export const STATE_MARKER_RE = /inflight-state:[^>]*-->/
/**
 * DEFERRED OR PARKED, ANYWHERE INSIDE THE MARKER. Anchoring the word to the front of the state
 * meant `parked - deferred` matched neither this nor the open test and two notes fell out of the
 * index entirely; the position of the word carries no meaning, only its presence does. Parked is
 * deferred by ruling: the two words named one disposition, and only one was recognised.
 */
export const DEFERRED_RE = /inflight-state:[^>]*(deferred|parked)[^>]*-->/

const first = (re, text) => {
    const m = re.exec(text)
    return m ? m[1].trim() : ''
}

/**
 * A document's title, by the fallback chain the session index uses: the frontmatter `title:` when
 * the file opens with a frontmatter block (solutions carry one, and their titles are YAML, so the
 * quotes a colon or an apostrophe forces are stripped), else the first `# ` heading, else the
 * filename stem. Never empty, so a document always has a line to be listed on.
 *
 * AN IN-FLIGHT NOTE IS NAMED BY ITS HEADING, frontmatter or not. docs/inflight/AGENTS.md puts the
 * note's markers "after the heading" - the heading is the note's identity - and the bash index
 * always read notes that way. A handoff note carries a ce-handoff frontmatter whose `title:` is a
 * different sentence from its `# ` line, and reading the frontmatter first named that one note
 * differently from every session that had seen it; the equivalence check in
 * bin/test-check-agent-hooks.sh caught it as the only title the old hook listed and this did not.
 */
export function titleOf(text, path) {
    const fm = path.startsWith(`${NOTES_DIR}/`) ? null : /^---\r?\n([\s\S]*?)\r?\n---/.exec(text)
    if (fm) {
        const t = /^title:[ \t]*(.*)$/m.exec(fm[1])
        if (t) {
            const raw = t[1].trim().replace(/^"(.*)"$/, '$1').replace(/^'(.*)'$/, '$1')
            if (raw) return raw
        }
    }
    for (const l of text.split('\n')) if (l.startsWith('# ')) return l.slice(2).trim()
    return path.replace(/^.*\//, '').replace(/\.(md|html)$/, '')
}

/**
 * Everything the index needs to place one note.
 *
 * @param {string} text the note's content
 * @param {string} path its path, for the title fallback
 * @returns {{type: string, impact: string, state: string, labels: string[], open: boolean,
 *            deferred: boolean, title: string}}
 *   `type` and `impact` are the marker's value or '' - not validated here, because an unknown value
 *   is a finding the index reports (its "unmatched" group) rather than an error to throw.
 */
export function classifyNote(text, path) {
    return {
        type: first(TYPE_RE, text),
        impact: first(IMPACT_RE, text),
        state: first(STATE_RE, text),
        labels: first(LABELS_RE, text).split(/\s+/).filter(Boolean),
        open: !STATE_MARKER_RE.test(text),
        deferred: DEFERRED_RE.test(text),
        title: titleOf(text, path),
    }
}

// --- The `--shell` renderer: the one thing here that prints, and only when run directly. --------
//
// COMPARE REALPATHS, NEVER THE SPELLINGS - the guard bin/inflight.mjs's `invokedDirectly` carries,
// with the incident: `process.argv[1]` is the path as typed and `import.meta.url` is resolved
// through symlinks, so on a symlinked tmpdir the two disagree and the body never runs. Fails
// closed on an unresolvable argv, because "was I run directly" is not a thing to crash over.
function invokedDirectly() {
    if (!process.argv[1]) return false
    try {
        return realpathSync(process.argv[1]) === realpathSync(fileURLToPath(import.meta.url))
    } catch {
        return false
    }
}

if (invokedDirectly()) {
    // Any other invocation THROWS rather than setting an exit code: a library here never touches
    // the process's exit (bin/test-inflight.mjs's `library-never-exits-the-process` scans for the
    // text), and an uncaught error is Node's own loud non-zero exit, message on stderr.
    if (process.argv.length !== 3 || process.argv[2] !== '--shell') {
        throw new Error('usage: node bin/lib/inflight-tags.mjs --shell   (prints the tag vocabulary as bash assignments)')
    }
    process.stdout.write(`${shellAssignments()}\n`)
}
