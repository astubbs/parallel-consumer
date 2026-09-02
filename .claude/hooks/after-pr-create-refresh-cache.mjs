#!/usr/bin/env node
// Copyright (C) 2026 Antony Stubbs and contributors
//
// PostToolUse hook: after `gh pr create` succeeds, fold the new PR into the local cache and say so.
//
// WHY THIS EXISTS. `bin/inflight.mjs` caches the repository's PR set, and every command that names a
// branch reads it - `branch`, `note drift`, the tracking-gap detector. A PR created seconds ago is
// absent from that cache, so the tool reports the branch that PR is FOR as having no PR, and the
// detector then tells the agent to write a tracking note for work that is already tracked. Being
// wrong in that direction is the failure that gets a detector ignored.
//
// It is also why the cache can be held for twenty-four hours rather than thirty minutes: the writers
// are the people working in this repository, and each write updates the cache as it happens, so the
// TTL became a backstop for changes made OUTSIDE this machine rather than the mechanism that makes
// the cache correct.
//
// WHY NODE, AND WHY THAT MATTERS MORE THAN THE OPERATOR RULING. The first version of this hook was
// bash for the prefilter, embedded python3 for the payload, and a spawned `node` for the work -
// THREE runtimes to do one thing, and Antony rightly asked what it was for. In Node it IMPORTS
// `cachePr` directly: no subprocess, no JSON round-trip through a shell, and the hook fails the same
// way the library does rather than through an exit code it has to re-interpret.
//
// `bin/lib/source-patterns.mjs`'s `new-shell-script` rule would not have caught this - its scope is
// `^bin/.*\.(sh|bash)$`, and this lives in `.claude/hooks/`. The operator ruling it encodes says
// "new scripts", so the gate is narrower than the rule; that gap is recorded in
// docs/inflight/ci-inflight-next-commands.md rather than silently widened here.
//
// FAILS SILENT, DELIBERATELY. A cache refresh is a convenience and must never break a tool call or
// emit noise when there is nothing to say. Every unreadable payload, every failed refresh, exits 0
// with no output.

import { pathToFileURL } from 'node:url'

import { cachePr } from '../../bin/lib/notes.mjs'

/**
 * The PR this payload created, or null.
 *
 * EXPORTED SO RECOGNITION CAN BE TESTED WITHOUT THE NETWORK. Deciding and acting are separate
 * questions, and only the first is cheap to assert: once the hook calls `cachePr`, a test either
 * hits GitHub and rewrites the real cache, or uses a number that cannot exist - at which point
 * "not recognised" and "recognised but the refresh failed" are both silence, and the case proves
 * nothing. The suite's `cd && gh pr create` case had exactly that shape.
 */
export function prNumberFrom(payload) {
    const command = payload?.tool_input?.command ?? ''
    // Whole words, so `cd x && gh pr create ...` counts. bin/AGENTS.md records that a prefix
    // matcher missed every command shape it existed for, which is why this reads the payload.
    if (!/\bgh\b.*\bpr\b.*\bcreate\b/.test(command)) return null
    if (/--dry-run\b/.test(command)) return null

    const response = payload?.tool_response
    const output = typeof response === 'string'
        ? response
        : ['stdout', 'stderr', 'output', 'content'].map((k) => String(response?.[k] ?? '')).join(' ')

    // gh prints the new PR's URL on success. No URL means nothing was created - a validation
    // failure, a missing base, a PR that already exists - and there is nothing to fold in.
    const url = output.match(/https:\/\/[^\s]*\/pull\/(\d+)/)
    return url ? Number(url[1]) : null
}

export function contextFor(number, result) {
    return `The in-flight tool's PR cache now includes astubbs/parallel-consumer#${number}, folded `
        + `in automatically when you created it (${result.action}; ${result.total} PRs cached).\n`
        + '\n'
        + 'This matters for what you do next: `bin/inflight.mjs branch <ref>` and its tracking-gap '
        + 'detector read that cache, and without the refresh they would have reported the branch '
        + 'you just opened a PR for as having no PR - and told you to write a tracking note for '
        + 'work that is already tracked.\n'
        + '\n'
        + 'You do not need to run anything. `bin/inflight.mjs cache` shows what is held and how '
        + 'old it is.'
}

// Guarded so the matcher above can be imported without the hook reading stdin and acting.
if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
    const stdin = await new Promise((resolve) => {
        let buf = ''
        process.stdin.setEncoding('utf8')
        process.stdin.on('data', (d) => { buf += d })
        process.stdin.on('end', () => resolve(buf))
        process.stdin.on('error', () => resolve(''))
    })

    let payload
    try {
        payload = JSON.parse(stdin)
    } catch {
        process.exit(0) // never break a tool call over a payload we cannot read
    }

    const number = prNumberFrom(payload)
    if (number === null) process.exit(0)

    let result
    try {
        result = cachePr(number)
    } catch {
        process.exit(0)
    }
    if (!result?.ok) process.exit(0)

    process.stdout.write(JSON.stringify({
        hookSpecificOutput: {
            hookEventName: 'PostToolUse',
            additionalContext: contextFor(number, result),
        },
    }))
}
