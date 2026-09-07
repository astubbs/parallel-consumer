// Copyright (C) 2026 Antony Stubbs and contributors
//
// "Am I the script being run, or a module being imported?" - answered by comparing REALPATHS, never
// the spellings. `process.argv[1]` is whatever the caller typed: a symlink, a relative path, a path
// with `..` in it. `fileURLToPath(import.meta.url)` is the resolved file. Comparing the two strings
// says "not invoked directly" for a symlinked front door and the script silently does nothing while
// exiting 0 - which is exactly what bin/inflight.mjs shipped once, and why its guard (the model for
// this one) carries a symlinked-path negative control in bin/test-inflight.mjs.
//
// Shared here because the second and third copies (the two integration-shard gates) both started
// life with the spelling comparison. A guard that has to be imported cannot be mis-copied.

import { realpathSync } from 'node:fs'
import { fileURLToPath } from 'node:url'

/** True when the module at `metaUrl` (pass `import.meta.url`) is the file Node was asked to run. */
export function invokedDirectly(metaUrl) {
    if (!process.argv[1]) return false
    try {
        // realpathSync throws on a path that does not exist; deciding whether to run is not a
        // thing to crash over, so an unresolvable path means "not invoked directly".
        return realpathSync(process.argv[1]) === realpathSync(fileURLToPath(metaUrl))
    } catch {
        return false
    }
}
