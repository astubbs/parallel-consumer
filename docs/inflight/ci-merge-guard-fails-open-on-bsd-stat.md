# The outstanding-work merge guard fails open on macOS, in the one case it exists to catch

<!-- inflight-type: bug -->
<!-- inflight-impact: misdirection -->

`.claude/hooks/check-merge-outstanding-work.sh` reads each background task's mtime with GNU
`stat -c %Y`. BSD `stat` rejects `-c`, so on macOS the guard silently allows every merge it was
written to refuse:

```bash
mtime="$(stat -c %Y "$f" 2>/dev/null || echo 0)"
[ "$mtime" -eq 0 ] && continue
```

`stat` exits non-zero, stderr is discarded, `|| echo 0` yields `0`, and the next line `continue`s -
so the live task file the `find` just located is dropped, `live_tasks` stays empty, and the hook
exits 0 printing nothing, which the harness reads as ALLOW.

**This is not one of the fail-opens the hook declares.** Its header lists them deliberately - "no
session id, no scratch dir, no python3, unparseable JSON" - and every one of those means *there is
nothing to check*. This one means *there was something to check and it was discarded*: the guard
finds the evidence of in-flight work and then throws it away. A merge that the guard was built to
refuse is permitted, and nothing in the output distinguishes that from a clean pass.

The failure is total on macOS rather than intermittent: every case fails, because `-c` is never
accepted. `bin/test-check-agent-hooks.sh` reports 10 failures there, all `expected DENY, got
ALLOW`; on Linux the same suite is green.

## Why no one saw it

`bin/test-check-agent-hooks.sh` is **not referenced by any workflow** - `grep -rn
'test-check-agent-hooks' .github/workflows/` returns nothing. It runs only when an agent runs it by
hand. All 29 CI jobs are `ubuntu-latest`, where `stat -c` works, so the platform that would fail is
the one CI never uses and the suite that would catch it is one CI never runs.

## Scope

Master state, not a PR's. The identical line is on `origin/master`, and the file is absent from
astubbs#57's diff - that PR touches `check-upstream-map-merged.sh` only. It surfaced during a
babysit run of astubbs#57 on macOS, which is why it is recorded here rather than there.

## What it costs

The guard exists because `astubbs#31` merged about ten minutes before a spawned agent finished the
broker-level reproduction of `confluentinc#909` - the exact gap that PR's own description declared
open. Its header argues a checklist could not have prevented it, because the agent's question was
"is this PR ready?" while the thing it needed to recall was a background task started an hour
earlier. On macOS that argument no longer holds up, and an agent merging from a Mac has the same
exposure the hook was written to close, while appearing to be protected by it.

## Fixing it

Two parts, and the second matters more than the first:

- Read the mtime portably - try `stat -c %Y` and fall back to `stat -f %m`, or use `find -newermt` /
  `perl -e 'print (stat($f))[9]'`.
- **Fail closed when the mtime cannot be read.** A file matched the session's `tasks/*.output` glob;
  being unable to date it is not evidence that nothing is running. The current `continue` treats an
  unreadable clock as proof of quiescence, which is the same inversion the hook's own header warns
  about in its `ListAgents` caveat.

Running the self-test suite in CI would have caught this on the first commit, but only if it ran on
a non-Linux runner too; a Linux-only job reproduces the blind spot exactly.
