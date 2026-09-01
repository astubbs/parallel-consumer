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

The suite **is** wired into CI - `repo-hygiene.yml` runs `bash bin/test-check-agent-hooks.sh`, added
by astubbs#299. It runs on every push. What it never does is run anywhere `stat -c` is rejected:
every CI job is `ubuntu-latest`, so the harness that would catch this executes constantly and always
on the one platform where the bug is invisible. A green tick here means "GNU stat works", which is
not the question.

That makes it worse than an unwired suite, not better: the coverage exists, reports green, and is
blind by construction. Running it on a macOS runner - or any BSD `stat` - is what would close it.

<!-- post-merge: checked-begin -->
An earlier draft of this note claimed the suite was referenced by no workflow at all. That was
wrong, and wrong in an instructive way: the grep behind it was run in the repo's **main checkout**,
which was sitting on a different branch that lacks the wiring, rather than in the worktree holding
the branch under test.
<!-- post-merge: checked-end --> `AGENTS.md` forbids working in the main checkout precisely because its HEAD moves under
you; here it turned a one-line verification into a confidently published falsehood.

## Scope

<!-- post-merge: checked-begin -->
Master state, not a PR's. The identical line is on `origin/master`, and the file is absent from
astubbs#57's diff - that PR touches `check-upstream-map-merged.sh` only. It surfaced during a
babysit run of astubbs#57 on macOS, which is why it is recorded here rather than there.
<!-- post-merge: checked-end -->

## What it costs

The guard exists because `astubbs#31` merged about ten minutes before a spawned agent finished the
broker-level reproduction of `confluentinc#909` - the exact gap that PR's own description declared
open. Its header argues a checklist could not have prevented it, because the agent's question was
<!-- post-merge: checked -->
"is this PR ready?" while the thing it needed to recall was a background task started an hour
earlier. On macOS that argument no longer holds up, and an agent merging from a Mac has the same
exposure the hook was written to close, while appearing to be protected by it.

## Fixing it

Two parts, and the second matters more than the first:

- Read the mtime portably, and **do not write it as `stat -c %Y || stat -f %m`.** A blind fallback is
  the same silent-wrong-answer bug one layer up: GNU `stat -f` is `--file-system`, so on Linux that
  second arm *succeeds* and returns a number about the filesystem, not the file. The fallback fires
  whenever the first arm fails for any reason, and what it hands back is wrong rather than absent.
  Branch on the platform explicitly (`uname`), or use one implementation that behaves the same
  everywhere - `perl -e 'print((stat($ARGV[0]))[9])'` is already a dependency-free option here.
- **Fail closed when the mtime cannot be read.** A file matched the session's `tasks/*.output` glob;
  being unable to date it is not evidence that nothing is running. The current `continue` treats an
  unreadable clock as proof of quiescence, which is the same inversion the hook's own header warns
  about in its `ListAgents` caveat.

Running the self-test suite in CI would have caught this on the first commit, but only if it ran on
a non-Linux runner too; a Linux-only job reproduces the blind spot exactly.
