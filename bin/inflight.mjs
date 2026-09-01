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
 * @typedef {{name: string, summary: string, when: string, usage: string,
 *            run: (args: string[], emit: (s: string) => void) => {ok: boolean, reason?: string}}} Command
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
]

function help() {
    const width = Math.max(...COMMANDS.map((c) => c.name.length))
    return [
        "bin/inflight.mjs - the front door for this repo's in-flight tooling.",
        '',
        'Usage: bin/inflight.mjs <command> [args...]',
        '       bin/inflight.mjs help [<command>]',
        '',
        'Commands:',
        ...COMMANDS.flatMap((c) => [
            `  ${c.name.padEnd(width)}  ${c.summary}`,
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
        const topic = COMMANDS.find((c) => c.name === rest[0])
        if (topic) return { ok: true, reason: topic.usage }
        if (rest[0]) return { ok: false, reason: `inflight: no such command '${rest[0]}'\n\n${help()}` }
        return { ok: true, reason: help() }
    }

    const command = COMMANDS.find((c) => c.name === name)
    if (!command) return { ok: false, reason: `inflight: no such command '${name}'\n\n${help()}` }
    return command.run(rest, emit)
}

const { ok, reason } = dispatch(process.argv.slice(2), (s) => console.log(s))
if (reason) (ok ? console.log : console.error)(reason)
process.exit(ok ? 0 : 2)
