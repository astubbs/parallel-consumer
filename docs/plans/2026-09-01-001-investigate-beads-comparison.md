# Beads compared with the harness this repo already built

**Status:** Desk comparison only. **Nothing was installed and nothing was run.**
**Written:** 2026-09-01
**Executes:** [`docs/inflight/process-beads-evaluation.md`](../inflight/process-beads-evaluation.md) - the
documentary half of it. The runnable probes that note carries are untouched, and are restated in §6.

---

## 0. Every claim here is documentation-derived, and that is a deliberate exception

[`docs/agent-harness.md`](../agent-harness.md)'s standing rule is that claims about harness behaviour
are **tested, not read off the documentation** - a rule that exists because that file's own first
version asserted four things about Claude Code that turned out false, each with a design already
built on top. This document breaks that rule on purpose and on instruction: the desk pass is cheap,
and the research below suggests it may be decisive on its own, which would make a spike unnecessary
rather than merely premature.

So: **nothing below was observed.** Sources, with their dates, because the dates matter more than
usual here:

- `gastownhall/beads` README, read 2026-09-01 - **generation 2**, Dolt-backed.
- `ianbull.com/posts/beads/`, dated 2026-01-24 - **generation 1**, SQLite + JSONL.
- Better Stack's guide - generation 1; it still gives the install as a clone of `sourcegraph/beads`.
- The prior-art survey already recorded in
  [`process-adopt-external-harness.md`](../inflight/process-adopt-external-harness.md), 2026-08-19.

**The reading caution that governs everything after this.** Beads has had two architectures, they
differ in precisely the place this repo cares about, and the third-party writeups that dominate
search results describe the older one. A comparison assembled from the top results compares us
against a Beads that no longer exists. The project has also changed hands - `steveyegge` ->
`gastownhall`, with one guide still naming `sourcegraph` - so provenance is worth establishing
before trusting any single page.

---

## 1. Where the state lives - the question that may end this

| | **Generation 1** (to ~Jan 2026) | **Generation 2** (current) |
|---|---|---|
| Store | SQLite at `.beads/beads.db`, gitignored | Embedded Dolt at `.beads/embeddeddolt/` |
| In-tree artefact | `.beads/issues.jsonl`, **committed** | Same path, demoted to *"an export for viewers and interchange, not the source of truth or a backup"* |
| How it moves between machines | Ordinary git - *"No central server, git IS the database"* | `bd dolt push` / `bd dolt pull` against **`refs/dolt/data`** on the git remote |
| Collision strategy | Hash-based IDs (`bd-a1b2`) | Hash IDs plus Dolt cell-level merge and native branching |

**Generation 2 takes the tracker's state out of the working tree.**
[`docs/inflight/AGENTS.md`](../inflight/AGENTS.md) is built on a property that follows from the state
being ordinary files: a note travels on the branch with the work that produced it, and stops being
true when that branch does. A store that lives in a side ref with its own branching does not have
that property - checking out a different branch would not change what `bd` shows unless something
explicitly moves the Dolt branch too, and nothing in the documentation says anything does.

**Generation 1 does have the property, and its author disowns the architecture.** Yegge, quoted in
ianbull's writeup, on the design where the JSONL is committed: *"a crummy architecture (by pre-AI
standards) that requires AI in order to work around all its edge cases where it breaks."* That is
said about the **more** git-native of the two.

**The two failure modes are not the same failure, and the second is the worse one here.**

- Generation 1 fails **loudly**: one appended file that every PR touches. That is the exact shape
  that split `docs/inflight/` into a directory on 2026-08-04, when the single file appeared in 26 of
  the last 30 master commits and unrelated PRs conflicted purely because their notes were adjacent.
  A JSONL append log merges better than prose did, so this is a real improvement on what we had
  then - but it is an improvement on the problem we already solved.
- Generation 2 fails **silently**: there is no conflict because the data is not on the branch at
  all. A branch's notes and the branch's code can disagree with nothing going red. This repo's
  entire doctrine is organised around the principle that a silent miss costs more than a loud one -
  it is why `misdirection` sits at the top of the impact scale, and why the harness exists at all.

**Unresolved, and not answerable from the documentation:** whether `bd` tracks the git branch
automatically, whether `bd dolt push` is expected per-branch, and what a merge of two git branches
that both changed issues actually produces. §6 carries these.

---

## 2. What Beads gives us

| Capability | Our equivalent today |
|---|---|
| `bd ready` - tasks with no open blockers | **None.** We order by consequence, not by blockedness; `inflight-state: blocked` only *excludes* a note from the session index. |
| Dependency graph, typed links (`blocks`, `parent-child`, `related`, `discovered-from`; `relates-to`, `duplicates`, `supersedes`, `replies-to`) | Prose cross-links between notes, and `depends on astubbs/parallel-consumer#N` in PR bodies behind the PR-dependency gate. **A machine-queryable graph is genuinely absent.** |
| `bd prime` / `bd remember` - context injection and persistent memory | `.claude/hooks/inject-recorded-knowledge.sh` and `inject-branch-context.sh`. Ours is the same idea, sourced from files rather than a store. |
| `bd compact` - semantic "memory decay" summarising old closed tasks | **None.** The anti-inflation duty currently rests on a person noticing the index has got long - `docs/inflight/AGENTS.md` says so in as many words. **This is the strongest single thing Beads has that we do not.** |
| Hash IDs preventing collisions across agents and branches | One file per item, named by a human. Collisions are impossible by construction, so this is not a gap. |
| `bd setup claude`, `bd hooks install` | Our own hooks, written for this repo's rules. See §5. |

---

## 3. What of ours it could replace

By **surface**, not by file - the question is which job stops needing doing.

| Surface | Verdict |
|---|---|
| The note format and its three tag axes | **Partly, and lossily** - see §4. |
| `bin/check-inflight-tags.sh` and `bin/lib/inflight-tags.sh` | **Replaced in kind**: a schema makes a validator redundant. Only worth having if the vocabulary survives the move, which is the same question as the row above. |
| The impact-ordered session index in `inject-recorded-knowledge.sh` | **Partly.** `bd prime` injects, but ordering by priority is not ordering by consequence. |
| `bin/issue-index.sh` and `docs/inflight/issue-index.md` | **Not addressed.** No GitHub Issues import or sync is documented in either generation. |
| `inject-branch-context.sh` - the branch's own commits, handoff notes, PR body and PR comments | **Not addressed.** Its inputs are git and GitHub, not a tracker. |
| `remind-inflight-on-push.sh`, `check-merge-outstanding-work.sh`, `check-squash-subject.sh`, the `bin/check-*.sh` family | **Not addressed.** These fire on git and tool events and enforce this repo's rules. |
| `docs/solutions/`, `docs/plans/`, `docs/refactoring.md`, `docs/todo-index.md` | **Not addressed.** Different purposes, deliberately separated. |

**So adoption is not a replacement; it is an addition with a partial overlap.** The tracker core is
the replaceable part. Everything that makes this harness *fire* - hooks bound to git commands and
tool calls, gates bound to CI - is untouched, because it is about delivery rather than storage, and
that is where the effort here has actually gone.

**Partial replacement is the dangerous outcome, not the safe middle.** It means two places to look,
and the parent note already names the sharper version of the risk: adopting the tooling without the
conventions gets gates enforcing rules nobody agreed to, which is worse than no gates.

---

## 4. What we have beyond Beads

**A consequence vocabulary, where Beads has a ranking.** Ours is `inflight-type` (what kind of item),
`inflight-impact` (what it costs you to not know), `inflight-labels` (the mechanism), plus the
filename prefix (the area) - four things, three of them independent by design, and
`docs/inflight/AGENTS.md` owns the argument for why none collapses into another. Beads offers type,
`P1`-`P4`, status and a tag bag. The two are not the same kind of object: **`P1` is a position in a
queue; `misdirection` is a statement about what is wrong with your instruments.** A priority field
cannot answer "what does it cost me to not know this", and the ordering that puts signal integrity
above everything - because acting on a false green is worse than acting on nothing - has nowhere to
live in an ordinal scale.

**Plain files, no daemon.** Our store is read by `grep`, by every agent regardless of tooling, and by
whatever reads files in five years. Generation 2 runs a background daemon and a Unix socket. That is
a real capability difference in Beads' favour for queries, and a real dependency in ours.

**The GitHub surface.** `issue-index.md` puts the tracker inside `git grep`'s reach; the
`upstream-mirror` pairing maps this fork to confluentinc's issues; `inject-branch-context.sh` puts a
branch's own PR body and comments in front of whoever inherits it. None of that is a tracker
feature, and none of it is addressed.

**Two claims inherited from the parent note, and their status.** *"The conventions are not in the
code"* - supported: nothing in any documented Beads feature encodes "a filename must never carry
state", or "all non-deferred work happens before any deferred work", or the weight-axis boundary
between `docs/inflight/` and `docs/refactoring.md`. *"The self-test density is unmatched"* - **not
testable from documentation**; carried to §6.

---

## 5. Hazards, before anyone runs it

- **`bd init` "generates or updates `AGENTS.md` by default"** and installs agent integrations unless
  `--skip-agents` or `--stealth` is passed. This repo's `AGENTS.md` is the hand-curated router with
  its own doctrine about what may go in it. **Run `bd init` only in a throwaway clone.**
- **`bd hooks install` / `bd setup claude` write agent hooks.** `docs/agent-harness.md` records that
  this repo's tracked `.claude/settings.json` silently overwrites a local one exactly once, with no
  conflict and nothing to recover from. A second writer to that file needs the same care.
- **The daemon and Unix socket are unexamined against this repo's shape** - many worktrees and
  several agent sessions running at once against one clone.
- **The install path.** The project's primary documented route is `curl -fsSL ... | bash`. Given the
  change of ownership and the CVE/ossindex lane this repo runs, prefer a pinned, isolated build.

---

## 6. What the desk pass cannot settle

The probe list for a spike, if one is authorised. **These are the claims that need `bd` to have run**,
and nothing in §1-§5 should be read as having answered them.

1. **Does `bd` follow the git branch?** Two branches, different issues, `git checkout` between them.
   This is the deciding question and the one the documentation is silent on.
2. **What does a two-branch merge produce under generation 2** - and is `bd dolt push` per-branch?
3. **Round-trip fidelity**: import real notes and check whether type, impact, labels and area survive,
   or collapse into one priority plus a tag bag.
4. **Is `bd prime`'s ordering configurable** to put signal integrity first, or is it priority-ordered?
5. **Is there a GitHub bridge that the documentation omits?** `bd --help` and a grep of the source.
   The answer un-gates or re-gates the link-graph work in `ci-issue-index-has-no-edges.md`, which is
   on branch `docs/inflight-gh-link-graph` and has not merged.
6. **What `bd init` actually writes to `AGENTS.md`** - in a throwaway clone, diffed.
7. **The self-test-density claim**, which needs Beads' own test suite read rather than its docs.

**The install decision is already made and recorded**, so a later session does not re-litigate it:
`GOBIN=<scratchpad>/bin go install github.com/gastownhall/beads/cmd/bd@latest`, using mise's Go
1.26.5. Nothing on `PATH`, nothing global, deleted with the scratchpad, and the module version pinned
and auditable. **Explicitly not `curl | bash`**, for the reasons in §5.

---

## 7. Provisional reading - not a verdict

The verdict belongs to
[`process-adopt-external-harness.md`](../inflight/process-adopt-external-harness.md) and is not
written there, because a documentary pass has not earned one. On the documentation alone:

- **Beads is not a replacement for this harness.** It replaces the tracker core and leaves every
  delivery mechanism - the hooks, the gates, the GitHub surface - untouched.
- **Generation 2 moves away from us specifically**, by taking the state out of the working tree. If
  probe 1 confirms it, that is close to disqualifying on its own, and for a reason that has nothing
  to do with feature count.
- **One thing is worth stealing regardless of the decision**: compaction. We have no answer to index
  inflation beyond a person noticing, and that is a real gap Beads has already thought about.
- **The GitHub link-graph work is provisionally un-gated.** No GitHub import is documented, so
  `ci-issue-index-has-no-edges.md` is probably building something Beads was never going to provide.
  Probe 5 is what makes that safe to rely on.
