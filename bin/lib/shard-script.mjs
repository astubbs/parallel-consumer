// Copyright (C) 2026 Antony Stubbs and contributors
//
// The integration shard partition as bin/ci-integration-test.sh ships it - read from the script, so a
// Node gate cannot drift from what CI actually selects. Two gates need the same list (the drift
// checker and the coverage gate) and each had its own copy of this regex until the simplify pass on
// astubbs/parallel-consumer#442 caught it; the marker line is `readonly HEAVY_CLASSES="a,b,c"`, and
// a future change to that shape now has one place to land.

import { readFileSync } from 'node:fs'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'

export const SHARD_SCRIPT = join(dirname(fileURLToPath(import.meta.url)), '..', 'ci-integration-test.sh')

/**
 * The named heavy set, or `null` when the script cannot be read or carries no marker - callers decide
 * whether that is "could not run" (the drift checker) or "assume none" (the coverage gate).
 */
export function heavyClassesFromScript(script = SHARD_SCRIPT) {
    let body
    try { body = readFileSync(script, 'utf8') } catch { return null }
    const m = body.match(/readonly HEAVY_CLASSES="([^"]*)"/)
    return m ? m[1].split(',').filter(Boolean) : null
}
