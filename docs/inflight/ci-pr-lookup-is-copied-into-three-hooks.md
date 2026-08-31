# The branch-to-PR lookup exists three times, and should exist once

<!-- inflight-type: task -->
<!-- inflight-impact: refactor -->

`.claude/hooks/check-history-rewrite.sh`, `.claude/hooks/remind-inflight-on-push.sh` and
`.claude/hooks/check-merge-outstanding-work.sh` each carry their own copy of the same python3 block:
derive the slug from `origin`, refuse to fall back to an unqualified `gh`, run
`gh pr list -R <slug> --head <branch>` under a timeout, and report `found` / `none` / `failed`
apart from each other. `bin/check-pr-ready.sh` does the same job in bash, against a hardcoded slug -
the convention its `bin/` neighbours use.

**Why it was copied rather than extracted, and where that argument stops.** A guard that cannot
source its helper fails OPEN, which is the argument astubbs#341 makes by name against moving
repeated hook logic into `bin/lib/`. astubbs#357 then landed `.claude/hooks/lib/hook-common.sh` and
drew the line in code rather than in prose: its two non-blocking reminders source that library
behind `[ -r "$hook_lib" ] || exit 0`, which is fail-open and therefore harmless for something that
only advises, while every hook that actually refuses a tool call still inlines. So the dividing line
is refusing-guard against advisory-reminder, not hook against gate - and all of these are
`PreToolUse`, so the event type does not tell you which is which.

That reclassifies the four copies, and two of them are ready now:

- `.claude/hooks/remind-inflight-on-push.sh` is advisory and **already sources `hook-common.sh`**, so
  the fail-open objection does not apply to it at all. Its copy can move into that library today.
- `bin/check-pr-ready.sh` is a gate, where a failure to load is visible rather than silently
  permissive. See the caveat below before folding it in with the others.
- `.claude/hooks/check-history-rewrite.sh` and `.claude/hooks/check-merge-outstanding-work.sh` both
  refuse, so astubbs#341 still covers them. Moving these two needs a **fail-closed** source guard -
  the loader in `bin/check-file-refs.sh`, which errors loudly rather than continuing without
  `bin/lib/node-gate.sh`, is the shape to copy - and that is a deliberate change to their failure
  mode, not a refactor. Decide it explicitly rather than inheriting it from the other two.

The self-test arms under "the PR lookup, in the three hooks that make one" in
`bin/test-check-agent-hooks.sh` already exercise all three hooks, so they are the regression net for
any of these moves: they must stay green without being edited.

Do not fold `bin/check-pr-ready.sh` into the same helper on the same pass. `bin/` scripts name the
repo with an env-overridable constant (`bin/check-pr-analysis-surfaces.sh`,
`bin/check-branch-self-reference.sh`) rather than deriving it from `origin`, and the two conventions
differ on purpose - a hook runs wherever the agent happens to be, a gate runs against this repo.

**Count a FOURTH copy, and two prior shapes, before extracting anything.** Whoever does the fold-in
inherits more than the three hooks named above:

- `.claude/hooks/inject-branch-context.sh` already does this in `def origin_slug`, reasoned at
  "THE REPO IS DERIVED FROM `origin`" - and it is the copy where the `file://` local-clone case was
  found and fixed, so it is ahead of the three here rather than behind them. Reconcile against it;
  do not assume the newest copy is the most correct one.
- `bin/check-quarantine-owners.sh` solved the neighbouring half - telling a confirmed absence apart
  from a transient `gh` failure - in bash, in `gh_query()`, classifying to `MISSING` or `TRANSIENT`.
  It is not a drop-in: it wraps `gh pr view <number>`, which exits non-zero for *both* outcomes and
  so must read stderr, whereas `gh pr list --head` exits 0 with empty output for a real absence and
  non-zero only on failure. Its slug derivation also predates the `file://` fix. Read it for the
  shape of the problem, not for code to lift.

**The INPUT to the lookup was wrong too, and that half is now fixed - it adds a fourth duplicated
thing this dedup task inherits.** Full incident and mechanism:
[`a-hook-processes-own-directory-describes-the-session-not-the-command-2026-08-31.md`](../solutions/workflow-issues/a-hook-processes-own-directory-describes-the-session-not-the-command-2026-08-31.md).
In short - every copy derived the branch from the hook process's own directory (the session's, not
the guarded command's), fixed by deriving it from the push refspec, then a leading `cd`, then the
payload's `cwd`, then the hook's own directory as a labelled last resort. That adds a **fourth**
duplicated thing: the refspec rule now exists twice for the same fail-open reason the lookup itself
is triplicated - `hook_push_head_ref` in `.claude/hooks/lib/hook-common.sh` (bash) and
`push_head_ref` inside `.claude/hooks/check-history-rewrite.sh` (python). The derivation rule now has
**three** consumers in total (the two above plus `.claude/hooks/pre-commit-gate.sh`'s own directory
derivation), and only the advisory push hooks that already source `hook-common.sh` can share
`hook_push_head_ref` - the two that refuse still have to inline, for the reason astubbs#341 gives
above. Whoever folds the three lookups together folds this in too.
