#!/usr/bin/env node
// Copyright (C) 2026 Antony Stubbs and contributors
//
// THE FRONT DOOR for this repo's in-flight tooling. One command an agent has to know, with a help
// list that names the others, instead of N scripts in bin/ that it has to already know to reach for.
//
// WHY A FRONT DOOR AT ALL. The failure this fixes is not that the tools are missing - it is that an
// agent that cannot name the tool uses `git grep` on the working tree and gets a false negative with
// the authority of a completed check. bin/lib/prior-art.mjs's header carries the measurement: 580 of
// the 901 documents under docs/ exist ONLY on unmerged branches, so the habitual search sees a third
// of the corpus. A tool nobody reaches for is indistinguishable from a tool that does not exist.
//
// WHY WE BUILD RATHER THAN ADOPT, for the part this covers. Backlog.md is the one surveyed tracker
// that keeps state in the working tree and reconciles it across branches, so the cross-branch READER
// is genuinely a thing that ships - docs/plans/2026-09-01-001-investigate-beads-comparison.md, §10,
// owns that finding and the correction it forced. What does NOT ship anywhere is the rest of this
// surface, and in Backlog.md's case that is not an omission but a decision: GitHub Issues sync and a
// lifecycle hook system were both proposed there and both closed NOT_PLANNED by its maintainer as
// out of scope. So this front door is deliberately not a tracker. It is the layer above one.
//
// ADDING A TOOL IS ADDING A ROW to COMMANDS below - the same move bin/lib/source-patterns.mjs makes
// for rules. A tool reachable only by knowing its filename is the state this file exists to end, so
// a new tool that is not in this table is not finished.
//
// THIS IS THE ONLY FILE HERE THAT MAY CALL process.exit, and it does so once, at the bottom. The
// libraries return findings; the views render them; the exit code is a fact about a process and
// belongs at the process boundary. A library that exits has decided something that is not its to
// decide - and cannot then be called by a self-test, which is how the first cut of this split was
// caught.
//
// NOT A GATE, and deliberately not named `check-*`: bin/AGENTS.md grants that prefix to the review
// agent by pattern, and this reaches the network through `gh`.
//
// EXIT CODES: 0 ran (whatever it found), 2 cannot run - including a usage error. Self-test:
// bin/test-inflight.mjs.

import { baseline, freshnessWarnings, refTips } from './lib/git.mjs'
import { corpusIndex, drift, findNotes, prsByBranch, stranded } from './lib/notes.mjs'
import { formatDrift, formatFind, formatStranded, formatWarnings } from './lib/views.mjs'
import {
    format, formatHeader, formatSection, formatTail,
    priorArt, summary as priorArtSummary, usage as priorArtUsage,
} from './lib/prior-art.mjs'

/**
 * The registry.
 *
 * `when` is the sentence an agent needs to decide whether this is the tool - it answers "should I
 * reach for this now", which a name alone cannot.
 *
 * `run` takes the remaining argv and an `emit` for streaming, and returns `{ok, reason?}`. It never
 * exits and never decides a code: a search that ran and found nothing is `ok: true`, because "no
 * hits" and "could not look" are different answers and this repo has been bitten by conflating them.
 *
 * A command may instead carry `sub`, a nested registry. That is deliberately how the word "in-flight"
 * is disambiguated rather than by renaming the tool: `inflight note drift` is unmistakably about ONE
 * NOTE, while `inflight stranded` is a question about the whole directory. The product keeps its name
 * and the vocabulary does the work - Antony's call, and the right one, because the collision was only
 * ever between two senses of a word this repo already uses for both.
 *
 * @typedef {{name: string, summary: string, when: string, usage: string,
 *            sub?: Command[],
 *            run?: (args: string[], emit: (s: string) => void) => {ok: boolean, reason?: string}}} Command
 * @type {Command[]}
 */
const COMMANDS = [
    {
        name: 'prior-art',
        summary: priorArtSummary,
        when: 'BEFORE forming any hypothesis, and before proposing anything that sounds new',
        usage: priorArtUsage,
        run: (args, emit) => {
            const byRef = args.includes('--by-ref')
            const terms = args.filter((a) => a !== '--by-ref')
            if (terms.length === 0) return { ok: false, reason: priorArtUsage }

            // Streamed per section in the default view, because a 438-ref search that prints nothing
            // until it finishes reads as a hang. --by-ref cannot stream: a cluster is a statement
            // about every section at once, so it has nothing to say until all of them are in.
            let streamed = false
            const result = priorArt(terms, byRef ? {} : {
                onSection: (section, r) => {
                    if (!streamed) { emit(formatHeader(r)); streamed = true }
                    emit(formatSection(section, r))
                },
            })
            if (!result.ok) return { ok: false, reason: `prior-art: ${result.reason}` }
            if (byRef) emit(format(result, { byRef: true }))
            else emit(formatTail(result))
            return { ok: true }
        },
    },
    {
        name: 'note',
        summary: 'questions about ONE in-flight note, across every branch tip',
        when: 'you have a note or a feature name and need to know where it lives and how it varies',
        usage: `Usage: bin/inflight.mjs note <find|drift> [args...]`,
        sub: [
            {
                name: 'find',
                summary: 'which note is this, and which branches carry it',
                when: 'you know the feature but not the filename - including notes that never reached master',
                usage: `Usage: bin/inflight.mjs note find <fuzzy-name>

Substring match over every in-flight note path that exists on ANY ref, not just the checked-out one.
Measured 2026-09-01: 570 note paths exist across the refs and 165 are on origin/master, so the
working tree can show you under a third of them.

  bin/inflight.mjs note find 857
  bin/inflight.mjs note find quarantine`,
                run: (args, emit) => {
                    const query = args[0]
                    if (!query) return { ok: false, reason: 'note find: give a fuzzy name to match' }
                    const index = corpusIndex()
                    emit(formatWarnings(freshnessWarnings(index.baseline, index.refs.length)))
                    emit(formatFind(findNotes(index, query), query, index))
                    return { ok: true }
                },
            },
            {
                name: 'drift',
                summary: 'how one note differs across every branch tip, and what each branch is',
                when: 'before editing a shared note, and when two branches may disagree about what is open',
                usage: `Usage: bin/inflight.mjs note drift [--all] <path>

Reports only what is DIVERGENT: versions carrying content the baseline has never held. A branch that
simply has not merged recently is not drift - it is behind, it gets further behind every time anyone
edits the note, and it is nobody's finding. For the fork's most-edited note that filter removes 198
of the 274 carrying refs.

Clusters by BLOB, so identical copies are one row and the diff runs once per distinct version rather
than once per ref. Each branch is named by facts only - its PR title, else the title of a note it
carries that the baseline does not, else the branch name. Nothing is summarised or inferred.

--all also lists the behind-only versions.

  bin/inflight.mjs note drift docs/inflight/bug-857-family.md`,
                run: (args, emit) => {
                    const all = args.includes('--all')
                    const path = args.find((a) => a !== '--all')
                    if (!path) return { ok: false, reason: 'note drift: give a note path (see: note find)' }
                    // No corpus index: this is a question about one path, so it asks git that.
                    emit(formatWarnings(freshnessWarnings(baseline(), refTips().length)))
                    emit(formatDrift(drift(path, { prs: prsByBranch(), all })))
                    return { ok: true }
                },
            },
        ],
    },
    {
        name: 'stranded',
        summary: 'notes that exist on a branch and have never reached master',
        when: 'looking for knowledge that will be lost if nobody acts - the stranded-work impact',
        usage: `Usage: bin/inflight.mjs stranded

A note absent from master is not automatically stranded. Three filters run, and the middle one was
expected to do most of the work and did almost none - stated because the wrong prediction is worth
more than the right conclusion:

  present on master now                          not stranded
  its blob lives on master under another path    a rename, proven exactly - removed 1 of 405
  master's HISTORY once had this path            it landed and was git rm'd - removed 40 more

What survives is clustered by ref-set, because one workstream's notes share their refs and listing
them separately buries the finding under its own volume.`,
        run: (args, emit) => {
            const index = corpusIndex()
            emit(formatWarnings(freshnessWarnings(index.baseline, index.refs.length)))
            emit(formatStranded(stranded(index), index))
            return { ok: true }
        },
    },
]

/** Flatten one level, so help and lookup see `note find` as a first-class name. */
const flatten = (cmds, prefix = '') => cmds.flatMap((c) => (c.sub
    ? [{ ...c, path: `${prefix}${c.name}` }, ...flatten(c.sub, `${prefix}${c.name} `)]
    : [{ ...c, path: `${prefix}${c.name}` }]))

const ALL = flatten(COMMANDS)

function help() {
    const width = Math.max(...ALL.map((c) => c.path.length))
    return [
        "bin/inflight.mjs - the front door for this repo's in-flight tooling.",
        '',
        'Usage: bin/inflight.mjs <command> [args...]',
        '       bin/inflight.mjs help [<command>]',
        '',
        'Commands:',
        ...ALL.flatMap((c) => [
            `  ${c.path.padEnd(width)}  ${c.summary}`,
            `  ${' '.repeat(width)}  when: ${c.when}`,
        ]),
        '',
        'Every command exits 0 when it RAN - whatever it found - and 2 when it could not run. An empty',
        'result and a search that never happened are different answers, and nothing here reports them alike.',
    ].join('\n')
}

/** Resolve argv to `{ok, output}`. Pure, so the self-test can drive it without a subprocess. */
function dispatch(argv, emit) {
    const [name, ...rest] = argv
    if (!name) return { ok: false, reason: help() }

    if (name === 'help' || name === '--help' || name === '-h') {
        if (!rest.length) return { ok: true, reason: help() }
        // Longest match first, so `help note drift` beats `help note`.
        const topic = [...ALL].sort((a, b) => b.path.length - a.path.length)
            .find((c) => rest.join(' ').startsWith(c.path))
        if (topic) return { ok: true, reason: topic.usage }
        return { ok: false, reason: `inflight: no such command '${rest.join(' ')}'\n\n${help()}` }
    }

    const top = COMMANDS.find((c) => c.name === name)
    if (!top) return { ok: false, reason: `inflight: no such command '${name}'\n\n${help()}` }
    if (!top.sub) return top.run(rest, emit)

    const child = top.sub.find((c) => c.name === rest[0])
    if (!child) {
        const which = rest[0] ? `'${name} ${rest[0]}'` : `'${name}' on its own`
        return { ok: false, reason: `inflight: no such command ${which}\n\n${top.usage}` }
    }
    return child.run(rest.slice(1), emit)
}

const { ok, reason } = dispatch(process.argv.slice(2), (s) => console.log(s))
if (reason) (ok ? console.log : console.error)(reason)
process.exit(ok ? 0 : 2)
