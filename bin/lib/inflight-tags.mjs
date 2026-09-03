// Copyright (C) 2026 Antony Stubbs and contributors
//
// THE INFLIGHT TAG VOCABULARY, IN NODE - the sets, the order, and the readers of one note's markers.
//
// This is a PORT of bin/lib/inflight-tags.sh and of the grouping rules the session index
// (.claude/hooks/inject-recorded-knowledge.sh) applies to them, not a replacement: the gate
// (bin/check-inflight-tags.sh) is bash and sources the shell file, so the shell file stays the
// thing the gate reads until the gate migrates. Two copies of a closed vocabulary is exactly the
// drift the shell file's own header was written against, so the two are held equal by a self-test
// that SOURCES the shell file under bash and prints its variables - `bin/test-inflight.mjs`,
// `tag-vocabulary-matches-the-bash-library`. Not a regex over the file: its feature and register
// impact sets are built by shell expansion of the other two, and a parser would see the expansion
// rather than the values.
//
// WHAT EACH VALUE MEANS lives in docs/inflight/AGENTS.md, which owns the vocabulary. This file owns
// which values exist and in which order the index presents them; the order is load-bearing and the
// shell file's header explains why (signal integrity first, then what kills, then what is owed).
//
// Change a value here AND in the shell file AND in docs/inflight/AGENTS.md in the same commit. The
// parity test catches the first omission; the gate's doc/lib agreement check catches the second.
//
// No git, no printing, no process.exit: pure functions over a note's text.

export const INFLIGHT_TYPES = ['bug', 'feature', 'task', 'register']

export const INFLIGHT_BUG_IMPACTS = [
    'misdirection', 'blind-spot', 'crash', 'data-loss', 'stall', 'security', 'config-lie', 'reliability', 'throughput',
]

export const INFLIGHT_TASK_IMPACTS = [
    'release-gate', 'coordination', 'stranded-work', 'ci', 'test-debt', 'refactor', 'process', 'deps-debt',
    'security', 'reliability',
]

// The shell file builds these by expansion (`"$INFLIGHT_BUG_IMPACTS $INFLIGHT_TASK_IMPACTS"`), so
// they are derived here the same way rather than listed a third time. `security` and `reliability`
// appear in both halves and therefore twice in the derived list, exactly as they do in bash.
export const INFLIGHT_FEATURE_IMPACTS = [...INFLIGHT_BUG_IMPACTS, ...INFLIGHT_TASK_IMPACTS]
export const INFLIGHT_REGISTER_IMPACTS = [...INFLIGHT_BUG_IMPACTS, ...INFLIGHT_TASK_IMPACTS]

export const INFLIGHT_LABELS = ['concurrency']

/** The order the index presents impacts in, across every type. Position IS the priority. */
export const INFLIGHT_IMPACT_ORDER = [
    'misdirection', 'blind-spot', 'crash', 'data-loss', 'stall', 'security', 'config-lie', 'reliability', 'throughput',
    'release-gate', 'coordination', 'stranded-work', 'ci', 'test-debt', 'refactor', 'process', 'deps-debt',
]

/**
 * The shell variables the parity test reads back, in the order it prints them - one place, so the
 * test and this file cannot disagree about which variable holds which set.
 */
export const SHELL_VARIABLES = {
    INFLIGHT_TYPES, INFLIGHT_BUG_IMPACTS, INFLIGHT_TASK_IMPACTS, INFLIGHT_FEATURE_IMPACTS,
    INFLIGHT_REGISTER_IMPACTS, INFLIGHT_LABELS, INFLIGHT_IMPACT_ORDER,
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
 */
export function titleOf(text, path) {
    const fm = /^---\r?\n([\s\S]*?)\r?\n---/.exec(text)
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
