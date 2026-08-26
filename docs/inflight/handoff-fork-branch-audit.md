---
artifact_contract: "ce-handoff/v1"
created_at: "2026-08-20T11:34:33Z"
title: "Fork branch audit - findings landed, the check itself unbuilt"
summary: "109 of the fork's own branches are accounted for by no inventory; the finding and its method are committed, the recurring check that would keep the answer true is not written."
keywords: ["branch-audit", "archaeology", "orphan-branches", "upstream-map", "inflight", "presentation-branch", "pyallel-consumer"]
cwd: "/Users/astubbs/github/parallel-consumer/.claude/worktrees/fork-branch-archaeology"
resume_focus: "Build the branch-accounting check, and correct the upstream.md entry that over-claims"
repository: "astubbs/parallel-consumer"
repo_root_sha: "713c7468d5ecda55a2190d38e698cda017e91259"
branch: "docs/fork-branch-archaeology"
head: "854ee239c1b49ad23a44fc009e6a1b59453e3e8f"
worktree_path: "/Users/astubbs/github/parallel-consumer/.claude/worktrees/fork-branch-archaeology"
---

# Fork branch audit - what is established, and what is not built

<!-- inflight-type: task -->
<!-- inflight-impact: stranded-work -->

## Objective

A branch carrying the code behind the project's most-linked artifact turned out to be named in no
ledger. The owner's question was the useful one - not "rescue that file", but *"How can a branch be
invisible to every inventory we keep, and what else have we missed?"* This branch answers the
question and records the method. **It does not build the thing that would keep the answer true.**

The owner's stated intent for this branch: hand it on so a fresh session can pick it up.

## Current state

**Complete and committed** (`854ee239c`, one commit ahead of `origin/master`, **not pushed**):

- `docs/inflight/process-fork-branch-archaeology.md` - the whole finding. Read this first; everything
  below is orientation for it, not a substitute.

**Not started:** the recurring check, and the `docs/upstream.md` correction. Both are named in that
file's "What this wants" section.

Nothing else on this branch. No code, no scripts, no CI wiring.

## What is established, and how

Counts as of 2026-08-20, on `origin` (= `astubbs/parallel-consumer`; `upstream` is
`confluentinc/parallel-consumer`):

The counts, and the commands that re-derive them, are in
[`process-fork-branch-archaeology.md`](process-fork-branch-archaeology.md) and are not repeated here -
that file owns them, and a second copy would drift from it.

**The benign explanation was tested and does not hold.** "Not merged" is computed by ancestry and
squash-merges break ancestry, so these could have been branches whose content landed years ago. A
per-branch tree comparison against master by file basename says otherwise: after subtracting the nine
files common to more than half the branches (the baseline master deleted wholesale - `Jenkinsfile`,
`RELEASE.adoc`, `EnumCartesianProductTestSets.java`, `JavaEnvTest.java`, `StringTestUtils.java` and
four more), **all 109 still carry branch-specific classes present nowhere on master**.

**The claim is deliberately narrow: "109 branches nobody has an account for", not "109 lost
treasures."** Two caveats are stated in the committed file and should not be dropped when the finding
is repeated - basename matching over-counts renames, and much of this was tried and abandoned
deliberately, which is a perfectly good account that simply nobody wrote down.

## The mechanism, which is the transferable part

Four inventories exist. Three were correctly scoped to something that excluded the branch:
`src/docs/development/upstream-map.yaml` maps fork↔**upstream** issues and PRs and this branch has
neither; `docs/inflight/branch-package-rename-sweep.md` enumerated from `gh pr list` because its job
was keeping open PRs mergeable; `docs/inflight/branch-stale-and-diagnostic.md` is a hand-written
list, not an enumeration.

The fourth is the near miss and the reason this is worth a check rather than a one-off sweep:
**`docs/upstream.md` line 344 carries an entry titled "Orphan branches never attached to a PR"** -
exactly the right check, pointed at the wrong remote. It swept `upstream`; the trigger branch exists
only on `origin`. It reads as covering orphan branches generally while covering five branches on one
of two remotes, which is worse than not having looked, because it closes the question.

Common shape: **every audit was seeded from a list of interesting things and worked outward; none
started from the complete set of refs and worked inward.** Same defect as searching only
`--state open`.

## What the next session would build

From the committed file's "What this wants":

1. **A check, not a sweep.** Enumerate `origin` and `upstream` refs and require every one to be
   accounted for by exactly one of: merged, has a PR of any state, named in a document, or tagged as
   archived. Report the unaccounted set, and make absence fail loudly rather than read as coverage.
2. **Correct `docs/upstream.md` line 344** so it says which remote it covered.

The repo already has the pattern to copy: `bin/check-*.sh` is a family of exactly this kind of gate,
and **`bin/check-file-refs.sh` is the closest model** - read its header comment, which explains why
a documentation-integrity check exists at all and why it delegates to the same module CI uses rather
than reimplementing the rule. `.github/workflows/maven.yml` is where such checks are wired.
`scripts/upstream-sweep.sh` and `scripts/upstream-map.py` are the existing upstream-facing tooling
and are the natural place for the `upstream` half.

Design question left open deliberately, because it is a judgement call and not a detail: an archive
tag is one of the four proposed accounting routes, and no tag convention exists yet. The owner has
separately decided that `origin/presentation` will be archived under an `archive/` tag prefix and
then deleted - see the related work below. Whoever builds the check should settle whether that
prefix is the convention it recognises.

## Already surfaced, unread, and directly relevant to live work

**`upstream/pyallel-consumer` is a prior Python client** - `confluentinc#443` (Robbie-Palmer,
"feature: Python Support", CLOSED unmerged) plus `confluentinc#539` ("Automatically Publish Python
Package to PyPi", MERGED). confluentinc#443 **is** tracked in `upstream-map.yaml` under the `sweep-2023-long-tail`
entry, characterised in a single line as having "attacked the same goal from the client side" - but
**the branch itself has not been read**, while a Python client is being written right now on
`feats/proxy-requirements` (astubbs#293, astubbs#242). confluentinc#539 appears in no document at all.

This is prior art in the strongest sense and it is the highest-value single follow-up on this branch,
independent of whether the check ever gets built.

Also unverified: `upstream` has 41 branches and the ruled-out entry accounted for 5 as orphans,
implying the other ~36 each had a PR. Nobody has confirmed that implication.

## Related work, deliberately not stacked here

`feats/classic-vertx-demo` (worktree `.claude/worktrees/classic-vertx-demo`, one unpushed commit
`4babb1414`, off `feats/proxy-requirements`) is where this was found. It carries
`docs/inflight/branch-classic-comparison-demo.md` - the design ledger for a per-language comparison
demo, and the plan to rescue `Demo.java` from `origin/presentation` @ `ffda9c6a3`.
<!-- file-refs: N/A - that path lands on feats/classic-vertx-demo, deliberately not on this branch; the paragraph below records why the split is kept -->

**The split is intentional and should be preserved:** the audit is a repo-wide concern with nothing
to do with the language proxy, and stacking it would gate a master-level finding behind an unrelated
feature PR. The two entries reference each other by filename rather than by markdown link, precisely
because they land on master through different branches.

## Fragile local state - machine-local, not committed

Both worktrees hold **unpushed** commits and neither branch exists on the remote. Nothing has been
pushed at any point in this session; the owner gates the first push of any branch.

The scratch data behind the counts lives under
`/private/tmp/claude-501/-Users-astubbs-github-parallel-consumer/9c564e47-960b-4208-b88c-6ba27ef6cd34/scratchpad`
(`origin-branches.txt`, `pr-heads.txt`, `merged.txt`, `old-undocumented.txt`, `master-files.txt`).
It is regenerable from the commands above and should be treated as disposable, not as evidence.

## Failed approaches - do not retry these shapes

- **The interactive shell here is not bash**, despite the tool name. `time`/error output came back in
  zsh form (`(eval):5: command not found: timeout`), `timeout` is absent, and three separate
  shell-loop scans produced **zero output and no error** before this was noticed - roughly 25 minutes
  lost. The successful scans are Python driving `git` through `subprocess`. Do that.
- **`comm -23 - file`** reading the left operand from stdin is the specific construct that hung.
- **Whole-path comparison against master is too noisy to mean anything.** The package rename
  (`io/confluent` -> `bz/stub`) and years of restructuring make ~45-80 paths per branch read as
  "absent". Basename comparison plus subtracting the shared baseline is what produced signal; the
  intermediate whole-path numbers are in the transcript and are worthless.

## Verification performed

The counts and the content check were run and are reproducible by the commands above.
`docs/upstream.md:344` was read directly rather than recalled. `confluentinc#443` and
`confluentinc#539` were confirmed through `gh pr list` against `confluentinc/parallel-consumer`, and
confluentinc#443's presence in `upstream-map.yaml` was confirmed by reading the `sweep-2023-long-tail` entry.

**Not verified:** that the ~36 non-orphan upstream branches each had a PR; the contents of
`upstream/pyallel-consumer`; and whether any of the 109 branches' work is genuinely worth recovering
- that question was explicitly not asked.
