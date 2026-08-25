# The branch-to-PR lookup exists three times, and should exist once

<!-- inflight-type: task -->
<!-- inflight-impact: refactor -->
<!-- inflight-state: deferred - until the shared hooks library from astubbs#357 is on master -->

`.claude/hooks/check-history-rewrite.sh`, `.claude/hooks/remind-inflight-on-push.sh` and
`.claude/hooks/check-merge-outstanding-work.sh` each carry their own copy of the same python3 block:
derive the slug from `origin`, refuse to fall back to an unqualified `gh`, run
`gh pr list -R <slug> --head <branch>` under a timeout, and report `found` / `none` / `failed`
apart from each other. `bin/check-pr-ready.sh` does the same job in bash, against a hardcoded slug -
the convention its `bin/` neighbours use.

**Why it was copied rather than extracted.** A guard that cannot source its helper fails OPEN, which
is the argument astubbs#341 makes by name against moving repeated hook logic into `bin/lib/` - and
these three are guards. There is nowhere better yet: the shared hooks library `hook-common.sh`
arrives with astubbs#357 under `.claude/hooks/lib/` and does not centralise this lookup today, so
extracting now would mean inventing a second shared location for hooks.

**What to do once that library is on master.** Move one copy into it behind a fail-closed source
guard - the loader in `bin/check-file-refs.sh`, which errors loudly rather than continuing without
`bin/lib/node-gate.sh`, is the shape to copy - and delete the other two. The self-test arms under
"the PR lookup, in the three hooks that make one" in `bin/test-check-agent-hooks.sh` already
exercise all three hooks, so they are the regression net for that move: they must stay green without
being edited.

Do not fold `bin/check-pr-ready.sh` into the same helper on that pass. `bin/` scripts name the repo
with an env-overridable constant (`bin/check-pr-analysis-surfaces.sh`,
`bin/check-branch-self-reference.sh`) rather than deriving it from `origin`, and the two conventions
differ on purpose - a hook runs wherever the agent happens to be, a gate runs against this repo.

**Count a FOURTH copy, and two prior shapes, before extracting anything.** Whoever does the fold-in
inherits more than the three hooks in this tree:

- `.claude/hooks/inject-branch-context.sh`, arriving with astubbs#350, already has an `origin_slug()`
  doing this - and it is the copy where the `file://` local-clone case was found and fixed, so it is
  ahead of the three here rather than behind them. Reconcile against it; do not assume the newest
  copy is the most correct one.
  <!-- file-refs: N/A - that hook is the fourth copy and it is on astubbs#350, not in this tree; naming a file before it lands is what this note is for -->
- `bin/check-quarantine-owners.sh` solved the neighbouring half - telling a confirmed absence apart
  from a transient `gh` failure - in bash, in `gh_query()`, classifying to `MISSING` or `TRANSIENT`.
  It is not a drop-in: it wraps `gh pr view <number>`, which exits non-zero for *both* outcomes and
  so must read stderr, whereas `gh pr list --head` exits 0 with empty output for a real absence and
  non-zero only on failure. Its slug derivation also predates the `file://` fix. Read it for the
  shape of the problem, not for code to lift.
