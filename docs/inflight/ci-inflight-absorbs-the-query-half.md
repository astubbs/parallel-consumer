# The query half of the harness belongs in `inflight`, not in seven bash scripts

<!-- inflight-type: task -->
<!-- inflight-impact: ci -->

<!-- post-merge: checked-begin -->
**A hook or gate should decide POLICY. It should not know how to READ THE CORPUS.** Several do both
today, in bash, each with its own copy of the reading. `bin/inflight.mjs` now owns the reading -
[`ci-node-query-client.md`](ci-node-query-client.md) proposed it and the cross-branch half landed in
astubbs/parallel-consumer#400 - so the migration is a subtraction from those scripts rather than
new work.
<!-- post-merge: checked-end -->

<!-- post-merge: checked -->
Surveyed 2026-09-01, on astubbs/parallel-consumer#400. Numbers are line counts, not estimates of
what migrates: each script keeps its policy and loses only its reading.

## What migrates, and why each one

<!-- post-merge: checked-begin -->
| Script | Lines | The query inside it |
|---|---|---|
| `.claude/hooks/inject-branch-context.sh` | 654 | a branch's commits, handoff notes, PR body and comments - `branchFacts()` already answers most of it |
| `.claude/hooks/inject-recorded-knowledge.sh` | 437 | **Migrated** (astubbs/parallel-consumer#419): the three corpus areas are rendered by `bin/inflight.mjs docs index` and the hook keeps only the framing and the non-corpus sections. Its own cross-ref `git grep` and its `for-each-ref` enumeration are gone with it |
| `.claude/hooks/check-merge-outstanding-work.sh` | 364 | reads notes and `gh` to decide whether background work is live |
| `bin/check-branch-self-reference.sh` | 360 | whether a note names the branch or PR it sits on |
| `.claude/hooks/remind-inflight-on-push.sh` | 239 | what a PR's own note still says is open - `note find` plus a read |
| `bin/check-inflight-tags.sh` | 169 | parses every note, then fails. Already shares `bin/lib/inflight-tags.sh` with the index - the split is right and is done in bash |
| `bin/issue-index.sh` | 127 | GitHub to tree; the tunnel's first half, and it should share the tool's cache |
<!-- post-merge: checked-end -->

<!-- post-merge: checked-begin -->
**The session index went first, and it gained the capability this paragraph predicted.** It was
branch-scoped and said so apologetically - *"this list is what the CURRENT BRANCH carries, and that
is not all of it"*. It is **corpus-scoped** now (astubbs/parallel-consumer#419, unit U6 of
[the docs context query plan](../plans/2026-09-03-001-feat-inflight-docs-context-query-plan.md)):
the hook calls `bin/inflight.mjs docs index`, which renders the three areas from the refs, and the
language-proxy cluster that an agent on master could not learn existed is one heading naming its
branch set, under the in-flight area. What did NOT move is the framing and the sections outside
the corpus - the repo-level registers, ideation, the test-hardening audits - which the hook still
reads from the working tree by design (the plan's R17). An equivalence check in
`bin/test-check-agent-hooks.sh` runs the pre-migration hook at a pinned commit against the current
checkout and asserts every title it listed is in the new index.

**The widened-corpus defect this note owned in the hook went with the scan.** The hook's own
`git for-each-ref ... refs/heads refs/remotes/origin` - the narrow enumeration the tool had
dropped, which made its cross-ref count silently exclude tags and `refs/backup` - was not widened,
it was deleted: the hook no longer enumerates anything, and the command's first lines state the
ref set searched and the archival split. That was the argument here and it held.

**Two things the migration found that this note now records.** The front door called
`process.exit()`, which on macOS drops stdout still queued on a pipe, so any page over 64 KiB read
through `$(...)` arrived cut at exactly 65536 bytes with exit 0 - the hook lost its plans section
before anything noticed; it sets `process.exitCode` now and a self-test holds the line. And the
index's cold cost is the `corpusIndex` build, one `git ls-tree` per ref, which on a loaded host sits
at the 8 s session-start budget; a build that deduplicated refs by their `docs/` tree object would
cut most of those calls, and is the lever if the budget is breached.
<!-- post-merge: checked-end -->


## Also queued: the low-disk warner, which is not a query at all

Antony's call, and it does not fit the table above - `.claude/hooks/warn-low-disk.sh` (471 lines)
reads *the machine*, not the corpus. It belongs here for the other reason this note exists: it is
several hundred lines of bash doing structured work that Node does natively.

- **What it actually is**: two filesystem readings (`df -Pk` plus an `awk` column parse) and, on
  macOS, a sparse-image size read through `stat -f` and a settings-JSON scrape. The `awk` programs
  exist to parse fields and JSON - both free in Node - and `bin/AGENTS.md` already rules that a bash
  script reaching for a second language is the signal to change language.
- **What must survive the move**: it is a `PreToolUse` hook on *every* `Bash` call, so its cost is
  the design constraint, not an afterthought. Its header commits to a measured budget - seven
  short-lived processes on the healthy Linux path, 8-10ms - and **any port has to re-measure that
  and publish the new number**, because the whole argument for running it unfiltered is that it is
  cheap. Node's own startup is the risk that argument has not had to face before.
- **Its open items travel with it**, and one is a portability bug the move would delete outright:
  [`ci-low-disk-warner-open-items.md`](ci-low-disk-warner-open-items.md) records a `df` with no
  timeout (a hung NFS mount freezes the session; `timeout` is GNU-only, which is why it is still
  open), a hardcoded `/var/lib/docker` that ignores `daemon.json`'s `data-root`, and no supported
  way for a user to turn the warner down.
- **Sequencing**: after the query migrations above, not before. Those subtract duplicated *reading*
  from scripts that keep their policy; this is a whole-script port, and it is the one place in the
  harness where a regression is measured in milliseconds on every single tool call.

## What does NOT migrate

Copyright, CVE, OSS Index, shell hazards, source patterns, the rename tooling, mutation and the build
scripts. They are gates over the working tree and read no corpus. Leaving them alone is the point of
the dividing line above.

## The open decision

**`.github/scripts/*.js` and `bin/*.mjs` are both Node and share nothing.** Four CI gates with
one-to-one self-tests on one side, the tool on the other; `file-ref-gate.js` reads the whole tree and
ratchets against `origin/master`, which is the same fan-out the tool now owns. Merging them means CI
gates importing from `bin/lib/`, which couples the CI lane to local tooling. **Not yet** - decide it
after the hook migrations have proved the libraries stable.

Counting the bash this sits against: 23,666 lines under `bin/` and `.claude/hooks/`, against ~1,700
in `bin/*.mjs` and 2,515 in `.github/scripts/` (self-tests included). Node is the default for new
scripts by operator ruling; this is what the existing surface looks like.

## The register family, and the edges nobody can query

**The generated registers should be `inflight` subcommands, not separate scripts.** Same shape in
each: generate, commit, check staleness - three generators, three staleness stories, and
`docs/todo-index.md` is the repo's own cautionary case of a committed generated file rotting until a
reviewer caught it.

| Register | Generator | Edges it already holds |
|---|---|---|
| `docs/inflight/issue-index.md` | `bin/issue-index.sh` | **none** - nodes only, which is [`ci-issue-index-has-no-edges.md`](ci-issue-index-has-no-edges.md)'s whole complaint |
| `docs/todo-index.md` | `bin/todo-index.sh` (`--check` fails when stale) | marker to file |
| `docs/quarantined-tests.md` | `bin/quarantine-lane-report.sh`, `bin/lib/quarantine-common.sh` | note to PR, via `Owner: PR astubbs#NN`, already reconciled against GitHub by `check-quarantine-owners.sh` |
| `src/docs/development/upstream-map.yaml` | hand-maintained | fork to upstream PR - and nothing checks the fork side |

**So edges already exist in four formats, and none of them can be queried together.**

### The tool that already builds the link graph, then throws it away

`.github/scripts/file-ref-gate.js` has `citationsIn`, `treeFrom`, `resolves` and `danglingRefs`: it
walks every citing file in the tree on every PR, resolves each citation to a path - and reports only
the **dangling** ones. The doc-to-doc and doc-to-file edge set is computed on every PR and discarded.
Reusing that resolver is a better starting point than any cache, because it cannot go stale.

### Not an embedded graph database

- **The recorded constraint forbids it.** [`ci-node-query-client.md`](ci-node-query-client.md): "No
  daemon, no socket, no lockfile. Plain `.js` run by `node`, no bundler and no npm dependencies... the
  property that rules out a compiled binary rules out a package with an install step just as firmly."
- **It is the architecture the survey already rejected.** Beads generation 2 moved state out of the
  working tree into a side ref, and
  [the comparison](../plans/2026-09-01-001-investigate-beads-comparison.md) §1's verdict was that it
  fails *silently* rather than as a merge conflict - a branch's notes and its code disagreeing with
  nothing going red.
- **The edges split in two, and only one half needs storing.** In-tree edges - note to doc, doc to
  file, note to issue number, registry to PR - are derivable from the tree and travel with the branch,
  so they should not be cached at all; that is the argument that removed the corpus cache from
  `bin/lib/notes.mjs`. GitHub edges - which PR closes which issue, issue to issue, a comment saying
  the fix is elsewhere - are not derivable locally and share a rate limit with every parallel session.
  **Those are the only ones a throwaway cache is for**, and `ci-issue-index-has-no-edges.md` already
  states its discipline: cache *structure*, never *status*.
- **Ask for a third hop before paying for a database.** Note to issue to PR is two hops, which is a
  join. The graph is a `Map` built at runtime from a live tree scan joined to a cached GitHub slice.

## Two git traps to encode, wherever this work lands

<!-- post-merge: checked -->
Both cost real time on astubbs/parallel-consumer#400, and both fail by answering a *different*
question than the one they appear to answer - so neither looks like a failure.

- **`git branch -d`'s "not fully merged" is measured against the CURRENT HEAD**, not against the
  branch you merged into. It refused three deletions whose every commit was demonstrably present in
  the target, because the checkout was on a stale master. The real question is
  `git merge-base --is-ancestor <branch> <target>`.
- **`git cherry` compares patch-ids, and a squash-merge changes them.** It reported 11 of 12 commits
  absent from master when the content had landed weeks earlier through a squashed PR. To ask whether
  work landed, check the *artifacts*, not the patch-ids.

A helper for the first was written into `bin/lib/git.mjs` and removed unused before merge - a
function nobody calls is documentation with a maintenance cost. The knowledge belongs here until
something needs it.

## Not this note

What the tool should *grow* - a per-branch view, Claude session ownership, the tracking-gap detector
and branch relationships - is [`ci-inflight-next-commands.md`](ci-inflight-next-commands.md). This
note owns only the migration of query logic out of the bash scripts listed above.

## Delete when

Each row above has either migrated or been ruled out in writing, and the `.github/scripts` decision
has been taken either way.
