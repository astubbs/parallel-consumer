# Parallel Consumer - Agent Context

Project context for AI coding agents (Claude Code, Copilot, Cursor, etc.).

## How to write this file

Every agent session loads this file whole, whatever the task. That makes it a **router**: the rules
that bind every agent, plus a complete map to the topic docs that hold everything else. Its real
cost is not tokens but **attention** - each rule competes with every other one for compliance, and a
rule buried in a long file is followed less reliably than the same rule in a short one. Never
weakening a test protects the product; the mirror-title format does not; a file long enough to
flatten that difference has already failed.

**The test for whether something belongs here is: does an agent need this whatever it is doing?**
If yes, it goes here. If it only matters once you are already in a topic - releasing, chasing a
CI failure, writing a mirror - it goes in that topic's doc and gets a row in the table below. No
check can make this call; it is a judgement for whoever writes and reviews the change.

**For anything situational, the deciding question is what catches a miss:**

- **A gate or script enforces it** (copyright headers, issue references, the quarantine registry,
  the PR checklist, PR dependencies) - safe to relocate. Forget the rule and CI tells you, so this
  file needs only the rule in one line and the name of its enforcer.
- **Nothing enforces it** (worktree ownership, the commit subject format, "a PR never adds a
  changelog entry", keeping the upstream manifest in sync) - it stays here even when situational,
  because the failure is silent and the agent will not know to go looking. Most rules in this repo
  exist precisely because something went wrong with no check to catch it.

**It grows by accretion, and the history says so**: `git log --numstat -- AGENTS.md` is a column of
additions with almost no deletions, mostly from learning-capture commits. Rules get added when
something goes wrong and are never retired once they stop earning their place. So **retiring and
relocating is part of adding** - if you are here to write a rule, you are also here to ask whether
an older one has been superseded, absorbed by tooling, or is now stated twice.

**Writing a rule:**

- An entry is **the rule, a one-line why, and a citation**. Detail lives where it can be looked up:
  the topic doc, the dated plan, `docs/solutions/`, the PR, or the enforcing script's own header
  comment.
- **Cite incidents, never retell them.** If the story behind a rule has no durable home yet, write
  it into `docs/solutions/` first and link it from there.
- **Never state a fact twice** - duplicates drift apart. Cross-reference whichever doc owns it, and
  **name the owner in the pointer**: "X owns this; what is here is the part that binds every
  session". A pointer that only lists adjacent topics reads as *further detail*, so a reader who
  found a complete-looking rule here stops - and then edits the copy instead of the original. The
  doc it routes to states the same contract from its side, so either entrance reveals the owner.
- **Do not pre-empt misreadings.** A rule needing three paragraphs to defend it against
  misinterpretation is a rule that needs rewriting.
- **Before you move or rename any labelled block, grep the whole repo for its text** -
  `grep -rn '<exact label text>' . --exclude-dir=.git --exclude-dir=target` - not just `docs/`.
  Headings are the obvious case, but **bold sub-item labels get cited too**, and those citations
  live in `pom.xml`, shell-script headers, workflow YAML and javadoc as often as in markdown.
  Nothing checks any of them, so a stale one is silent: the split that produced this file moved a
  bold label, `Copyright rules for this fork`, and left six files pointing at it.

**Backstops, if the judgement above is slipping.** `wc -l AGENTS.md` past ~400 lines means
something situational has crept in; each of these fires earlier and names its own fix: a section
outgrowing about a screen (move it to a topic doc, add a table row, keep rule plus pointer); a
rule's backstory longer than the rule (move the story to `docs/solutions/`, cite it); the same fact
in two places (collapse into the owner, cross-reference); a rule now enforced by a script (keep the
rule, name the enforcer, delete what the enforcer's header already explains); routine conflicts on
unrelated PRs (the file is doing too many jobs).

## Where things live (read this before concluding something isn't tracked)

Documentation is split by *purpose*, enforced by convention only - so the commonest mistake is not
misreading a doc but **never opening it**. Check this table before concluding some category of work
is untracked (a whole triage doc was once written duplicating `docs/refactoring.md`, because only
`docs/inflight/` was grepped).

**Topic docs** - the detail behind the rules in this file:

| Document | Read it when |
|---|---|
| [`docs/testing.md`](docs/testing.md) | Writing or debugging tests: suite split, **why a run prints nothing and the flag that fixes it**, the ambient probe autopsy, the quarantine lane, the chaos suite, shared test utilities |
| [`docs/ci.md`](docs/ci.md) | CI is red, or you are changing a workflow: what each workflow does, the self-hosted lanes, how to fetch a failed job's log |
| [`docs/investigating.md`](docs/investigating.md) | Past the prior-art checks and into diagnosis: control arms, instrumentation traps, reporting rates |
| [`docs/compound-engineering.md`](docs/compound-engineering.md) | Asking whether a piece of work is *finished* - the failure-to-mechanism loop, what "done" means beyond green, and the three techniques `investigating.md` does not own |
| [`docs/issue-references.md`](docs/issue-references.md) | Writing any reference to an issue or PR - the full convention and the gate |
| [`docs/citations.md`](docs/citations.md) | Repairing a citation that no longer resolves, in a plan or solution write-up you may not rewrite |
| [`docs/copyright.md`](docs/copyright.md) | Adding, renaming or extracting a file: which header it gets and why |
| [`docs/releasing.md`](docs/releasing.md) | Cutting a release, or generating its changelog section |
| [`docs/upstream.md`](docs/upstream.md) | Work that maps to upstream: the manifest, commit trailers, issue mirrors, the sweep |
| [`docs/self-hosted-runner.md`](docs/self-hosted-runner.md) | Setting up or operating the self-hosted highcpu runner |
| [`docs/agent-harness.md`](docs/agent-harness.md) | Adding a rule you need agents to follow *reliably* - which layers fire on their own, and which are merely available |
| [`docs/merge-checklist.md`](docs/merge-checklist.md) | Getting a PR ready to merge - what to offer the author, including the squash message and reorganising the commits |
| [`bin/AGENTS.md`](bin/AGENTS.md) | Writing or changing a script in `bin/` - the shell conventions, including the ones no check enforces |
| [`docs/inflight/AGENTS.md`](docs/inflight/AGENTS.md) | Adding or editing a note in `docs/inflight/` - what may live there, and when to delete it |

**A directory with its own `AGENTS.md` owns the rules for what goes in it - read it before you write
there, not after review catches you.** The table above routes the ones that exist today; `find . -name
AGENTS.md` is the check that it is still complete. This rule is here because the routing was complete
and got missed anyway: astubbs#206 added a `docs/inflight/` note describing work that PR itself landed,
which the directory's first rule - track only what is currently OPEN - forbids. Routing is necessary
but not sufficient: the nested `CLAUDE.md` bridges described in
[`docs/agent-harness.md`](docs/agent-harness.md) are what make a directory's `AGENTS.md` arrive when
a file in it is touched, rather than waiting to be opened.

**Where work and knowledge are recorded:**

| Document | Owns | Explicitly NOT for |
|---|---|---|
| **`AGENTS.md`** (this file) | Rules that bind every agent, and the map above | Work items of any kind; anything only one topic needs |
| **`STRATEGY.md`** (repo root) | What the product is and why: target problem, the client-side guiding choice, who it is for, success metrics, tracks under investment | A roadmap or feature list. It is a *claims* document nothing tests - work that falsifies a claim must update it; the branches that will are named in `docs/inflight/pr-strategy-doc-merge-triggers.md` |
| **`docs/inflight/`** | *Transient* cross-branch state, **one file per item**, named `<category>-<slug>.md` (`bug-`, `test-`, `ci-`, `deps-`, `pr-`, `branch-`, `release-`, `parked-`, `next-`). Rules in [`docs/inflight/AGENTS.md`](docs/inflight/AGENTS.md) | A backlog. A file is deleted when its work lands - and never a committed index file, which every PR would edit |
| **`docs/refactoring.md`** | The deferred-work backlog: internal refactors grouped by file, **breaking changes queued for the next major** (release-gated section), and the **triage of `TODO`/`FIXME`/`XXX` markers** | In-flight work; anything already started |
| **`docs/todo-index.md`** | Generated inventory of every marker in the tree (`bin/todo-index.sh`, `--check` fails when stale) | Priorities - deliberately unsorted; triage goes in `refactoring.md` |
| **`docs/quarantined-tests.md`** | CI-enforced registry of quarantined tests and, when one exists, their owning fix PR (unowned entries are legal, flagged advisory) | Tests that merely flake - quarantine requires evidence: a diagnosis, or a recorded sighting ledger proving it is master-state |
| **`docs/test-hardening/`** | Dated audits of tests that do not run, do not assert, or were never written - per-test evidence and the commit that disabled each one | A live or generated registry - each audit is point-in-time; triage goes in `refactoring.md` |
| **`CONCEPTS.md`** (repo root) | Shared domain vocabulary whose meaning here is project-specific (produce/commit lock pair, *dirty*, shard, in-flight work). Entries stand alone - no file paths, class names or current config values | A spec, an architecture doc, or general programming vocabulary |
| **`docs/solutions/`** | Write-ups of problems already **solved**, by category, with YAML frontmatter (`module`, `tags`, `problem_type`) for searching | Open problems |
| **`docs/plans/`** | Dated plan and investigation documents for one piece of work | Durable reference - a plan goes stale once its work lands |
| **`src/docs/development/upstream-map.yaml`** | **Source of truth** for fork↔upstream mapping: fork branch/PR → upstream **PR**, with status; plus a cache of *frozen* upstream **issue** facts | Editorial opinion, and the live state of an upstream issue - that belongs to its fork mirror |
| **`src/docs/development/upstream-pr-analysis.adoc`** | Editorial analysis of upstream PRs: rankings, verdicts, merge order | Facts - when it and the manifest disagree, the manifest wins |
| **`CHANGELOG.adoc`** | Release notes, regenerated at release time | Per-PR entries of any kind - see [Changelog](#changelog) |

Rule of thumb - and the axis is **weight**, not when the work happens:

- **A refactor too small to deserve its own note** → [`docs/refactoring.md`](docs/refactoring.md). One
  or two lines, no owner, no tags, no state. It is a lightweight list of things that should be tidied,
  and nothing about it says *when*.
- **Anything needing context, evidence, tracking or a decision** → `docs/inflight/`, one file per item,
  tagged. Including work decided to happen **later**: a note may carry
  `inflight-state: deferred - <what it waits on>` and still belongs here. Deferred is a schedule, not
  an exile - and it is why "later → refactoring.md" was wrong.
- **Already settled** → [`docs/solutions/`](docs/solutions/) for the knowledge. **Not
  `CHANGELOG.adoc`** - see [Changelog](#changelog): a PR never adds an entry, the file is generated at
  release time from commit messages, so "put it in the changelog" is an instruction nobody may follow.

**When a `refactoring.md` line outgrows a line or two - it needs a decision, has a blocker, or has
evidence worth keeping - promote it to a note and delete the line in the same commit.** Neither file
may state it twice. The stale-arrival guard is the worked example: a one-line tidy-up in
`refactoring.md` until it turned out to be blocked on a null-safety decision, at which point it became
`docs/inflight/core-stale-arrival-guard-needs-a-null-safety-decision.md`.

This wording replaces "happening now → inflight; should happen later → refactoring.md", which stopped
being true the moment `docs/inflight/` gained deferred notes: 34 of them are "later" work and none of
them belong in `refactoring.md`.

### Cite by anchor, never by line number

**Never cite a `file:line`.** An unrelated commit inserting lines above the target invalidates the
citation while the file and the section are both intact, so it still reads as valid and nothing
checks it. Cite the path plus the smallest distinctive greppable string - an identifier, a flag, a
config key, a quoted literal; a long quotation is brittle the other way, breaking on a reword. Run
the grep before you commit the citation.

**The path half is now enforced: `bin/check-file-refs.sh` fails a cited path that does not exist**,
across the whole tree, and the `PR Checklist` workflow runs the same module - so deleting a file
also fails the PR that leaves citations behind. The anchor half is still yours: a gate can only tell
you the file is there, never that your quoted string is still in it.

Repairing one that has already gone stale in a dated record is its own procedure, because those
documents may not be rewritten to match today's code - [`docs/citations.md`](docs/citations.md)
**owns that procedure**.

## `gh` defaults to the WRONG repo here - fix it before your first command

This fork has two remotes - `origin` → `astubbs/parallel-consumer` (**where the work happens**) and
`upstream` → `confluentinc/parallel-consumer` - and `gh` prefers `upstream` when nothing says
otherwise, so a bare `gh pr list`, `gh pr view 259`, `gh run view` or `gh pr create` addresses
**confluentinc**. **Run this once per clone**; it writes `remote.origin.gh-resolved` into the shared
git config, so one run covers every worktree:

```bash
gh repo set-default astubbs/parallel-consumer
```

Verify with `gh repo view --json nameWithOwner -q .nameWithOwner`, which must print exactly
`astubbs/parallel-consumer`. Keep that one *unqualified* - it tests what the bare default resolves
to, so `-R` would defeat the check.

**The failure is usually silent, which is what keeps this rule here.** The damaging case is not the
command that errors but the one that *succeeds* against the wrong repository - above all the
merged-PR prior-art search below, which returns upstream's history and reads as "no prior art".
Worked incident, and the rest of the false-negative class:
[`docs/solutions/workflow-issues/compound-tooling-breaks-in-worktrees-and-forks-2026-08-07.md`](docs/solutions/workflow-issues/compound-tooling-breaks-in-worktrees-and-forks-2026-08-07.md).

The config is local and uncommitted, so CI runners, other machines and fresh agent sandboxes all
start without it. Two habits survive that: **qualify `gh` in anything you write down** - docs,
scripts, a handoff prompt - and **when you genuinely mean upstream, name it**,
`-R confluentinc/parallel-consumer`. Reading an upstream issue is normal here; what must never be
accidental is *which* repo answered. The same ambiguity bites a bare `#NNN` in prose -
[Issue references](#issue-references).

## Before you investigate anything

Do all six checks **before** forming a hypothesis, and say in your write-up what each returned -
including "nothing". Prior art tells you the method that settled the last question of this shape,
and the traps that voided earlier experiments.

| Check | Command |
|---|---|
| Prior investigations | `ls docs/plans/`, then grep them |
| Solved problems | `grep -rl <mechanism> docs/solutions/` |
| In-flight state | `ls docs/inflight/`, `grep -rl <mechanism> docs/inflight/` |
| Open PRs (collision check) | `gh pr list -R astubbs/parallel-consumer`, then `gh pr diff <n> -R astubbs/parallel-consumer --name-only` |
| **Merged** PRs, by file | `gh pr list -R astubbs/parallel-consumer --state merged --limit 100 --json number,title,files --jq '.[] \| select(.files[]?.path \| test("<ClassName>")) \| "\(.number) \(.title)"'` |
| Issues, `--state all` | `gh issue list -R astubbs/parallel-consumer --state all --limit 300` - fork issues *and* `upstream-mirror` ones; read the upstream original, not the mirror's summary |

- **The titles are already in your context**, injected at session start by
  `.claude/hooks/inject-recorded-knowledge.sh` - so "I did not know it existed" is not available as
  an excuse, and the check costs one grep against a list you have been handed.
- **Grep the mechanism, not the symptom.** The failing test's name is the weakest search term
  available. Search the class, the lock, the option, the exception, the log line.
- **`--state open` is a collision check, not a prior-art search.** The PR that already solved
  something in your file is, by definition, merged; the issue documenting it is usually closed.
  Searching only the open list produces false confidence, which is worse than not looking.

Once you have a hypothesis, [`docs/investigating.md`](docs/investigating.md) carries the method for
settling it: **a fix that works is not evidence of the cause.**

## Read the commits you inherit

The same rule one step earlier: read the record before you build on it, not before you ship.

Whenever your base moves under you - cutting a worktree from a master that advanced, merging master
in mid-flight, rebasing, replaying, or picking a branch back up - run
`git log --oneline <old-base>..<new-base>` and read the **bodies** of anything touching your area.
You inherit decisions, constraints, and sometimes instructions addressed to your branch. A green
build proves the code still compiles; it proves nothing about whether the ground under your design
moved.

Three things hide there, and none announce themselves: an instruction to your branch; a decision
that reshapes your work (a renamed module, a new document naming the project's approach); and an
argument against what you are about to do. **When you override an inherited decision, record the
reasoning you are overriding, where you override it** - or the next reader takes your change for an
oversight and reverts it.

Commit bodies are load-bearing here, because release notes are generated from the log - so the
most consequential sentence in a commit is often nowhere near its subject. Reading a dozen of them
costs seconds.

Worked example and method:
[`docs/solutions/workflow-issues/read-the-commits-you-inherit-2026-08-10.md`](docs/solutions/workflow-issues/read-the-commits-you-inherit-2026-08-10.md).

## IN FLIGHT: rename your branch BEFORE you merge master

One such inherited instruction, live now: the fork is moving `io.confluent.*` to `bz.stub.*` with
`bin/rename-packages.sh`, branch by branch. It binds **any** agent merging **any** branch until no
open branch predates the rename - then delete this section. Reasoning, measurements and the task
inventory are in the script's own header and in
`docs/plans/2026-08-11-001-refactor-package-rename-plan.md`, which arrives with
astubbs/parallel-consumer#277 - the tooling merges first, so on master the plan lands after this
section does.

- **Run `bin/rename-packages.sh` on your branch, then merge master.** Both sides then agree on where
  every file lives and what it is called, so the merge is ordinary.
- **A clean merge is not evidence you were safe to skip it.** Merging renamed master into an
  un-renamed branch reported **zero conflicts** and silently applied the streams module's ArchUnit
  edit into the *mutiny* module's file - git paired the five near-identical
  `TestConventionsArchTest.java` files across modules. Renamed on both sides, that case surfaces as
  a rename/rename conflict on the right file, for a human to resolve.
- **Sweep with `grep -rnE 'io[\\./]*conflu'`, never `grep -rn "io\.confluent"`.** Three files encode
  the package as an escaped regex and one misspells it, so the habitual sweep reports success
  without `bin/lib/quarantine-common.sh` even appearing in its output.
- **Assert the renames git RECORDED *and* their pairing - a bare R-count reads a mis-paired rename
  as healthy.** `bin/rename-packages.sh` asserts both; if you moved anything by hand, do both by
  hand.
- **Confirm the mutation lane scored mutants rather than trusting its tick.**
  `bin/ci-mutation-test.sh` exits **0** printing "nothing to mutate, skipping" when its package
  regex is stale, which is indistinguishable from a pass in the job summary.

## Overview

Parallel Consumer is a Java library for concurrent message processing from Apache Kafka with a
single consumer, keeping ordering guarantees (by partition or key) without raising partition
counts. This is a community-maintained fork of the no-longer-maintained
`confluentinc/parallel-consumer`, published to Maven Central as `bz.stub.parallelconsumer`.

## Build Requirements

- **JDK 17** (the project uses Jabel to compile Java 17 source to Java 8 bytecode)
- **Docker** (integration tests - TestContainers spins up Kafka brokers)
- **Maven via wrapper** (`./mvnw`) - do not use system Maven

## How to Build

```bash
bin/build.sh                 # quick local build (compile + unit tests)
bin/ci-unit-test.sh          # unit tests only (no Docker needed)
bin/ci-integration-test.sh   # integration tests only (requires Docker)
bin/ci-build.sh              # full CI build, Kafka version matrix (push-to-master CI)
bin/ci-build.sh 3.9.1        # full CI build against one Kafka version
bin/performance-test.sh      # performance tests (substantial hardware)
```

## Module Structure

| Module | Purpose |
|--------|---------|
| `parallel-consumer-core` | Core library - consumer, producer, offset management, sharding |
| `parallel-consumer-vertx` | Vert.x integration for async HTTP |
| `parallel-consumer-reactor` | Project Reactor integration |
| `parallel-consumer-mutiny` | SmallRye Mutiny integration (Quarkus) |
| `parallel-consumer-examples` | Example implementations for each module |

## Key Architecture Decisions

- **Jabel cross-compilation**: Java 17 source, Java 8 bytecode (`--release 8` restricts the API
  surface). The Mutiny module overrides to 17 - its real runtime floor; its pom carries the full
  reasoning, including why the build cannot detect it.
- **Offset encoding**: custom offset-map encoding (run-length, bitset) in Kafka commit metadata
  tracks in-flight messages.
- **Sharding**: messages distribute to processing shards by key or partition for ordering.

## Testing

[`docs/testing.md`](docs/testing.md) **owns this topic** - suite mechanics, the quarantine lane, the
chaos suite, the ambient probe - and wins where the two disagree. Four rules bind regardless:

- **⚠️ Be EXTREMELY careful modifying tests to make them pass, especially under
  parallelism/stress.** A test failing under concurrent load may be exposing a **real main-code bug
  that only manifests under stress**. Never loosen a timeout, weaken an assertion, add a retry, or
  serialize a test until you have determined *which* it is: test-infra contention (e.g. one shared
  overloaded broker) or a genuine concurrency bug. Prefer diagnostics that separate them - give the
  test an uncontended broker: passes → contention; still fails → investigate the code, do not mask
  it. Say in the commit/PR which cause you established and how. Loosening deadlines to go green
  hides exactly the bugs this library exists to prevent. **When a broker integration test fails,
  read its `AMBIENT PROBE AUTOPSY` block before diagnosing by hand** - and check the
  probe's thresholds before believing a clean one.
- **A flake fails the build - there is no retry, deliberately.** The CI scripts no longer pass
  `-Dsurefire.rerunFailingTestsCount=2`: it retried failures into green runs and hid three flakes no
  ledger knew about, one of them a regression of an already-fixed one. **Do not restore it to get a
  build green** - the lever is `@Quarantined` with evidence: a diagnosis, or a sighting ledger
  ([`docs/quarantined-tests.md`](docs/quarantined-tests.md)), which relocates the signal where a retry destroys it, and
  nothing enforces this. Background:
  [`docs/solutions/workflow-issues/ci-retries-hid-flakes-from-the-ledger-2026-08-07.md`](docs/solutions/workflow-issues/ci-retries-hid-flakes-from-the-ledger-2026-08-07.md);
  the flakes it uncovered are open in
  [`docs/inflight/test-untracked-ci-flakes.md`](docs/inflight/test-untracked-ci-flakes.md).
- **Reuse test utilities - search before you add.** Extend the shared helpers rather than writing a
  parallel one; a drifted copy of topic-creation logic once became a flaky-CI source. Where they
  live and what they cover: [`docs/testing.md`](docs/testing.md). Check `docs/solutions/` before
  solving a familiar-feeling problem.
- **Quarantine is master-state, not PR-state.** A test red on only one PR is that PR's problem, not
  a quarantine candidate - and nothing enforces this, so it is on you. The rest of the quarantine
  discipline is in [`docs/testing.md`](docs/testing.md).
- **A test that never runs is not a passing test, and nothing goes red to tell you.** Tests that are
  disabled, assumption-skipped, assert nothing, or were never written are recorded in dated audits
  under [`docs/test-hardening/`](docs/test-hardening/) - per-test evidence and the commit that
  disabled each one; the current one is
  [`inactive-tests-audit-2026-08-08.md`](docs/test-hardening/inactive-tests-audit-2026-08-08.md).
  Read the newest before re-enabling, deleting, or rewriting a dark test - the reason it went dark
  is usually already established there. Each audit is point-in-time: add a new dated one rather than
  editing an old one.

Unit tests are surefire (`src/test/java/`); integration tests are failsafe and need Docker
(`src/test-integration/java/`).

## Code Style

- **Lombok** used extensively (builders, getters, logging); IntelliJ Lombok plugin required.
- **EditorConfig** enforced - 4-space Java indent, 120-char lines.
- **Google Truth** for test assertions, with JUnit 5 and Mockito.
- **License headers** are enforced by `bin/check-copyright-headers.sh`, and there is no tool that
  writes them - which header a new, modified, renamed or extracted file gets depends on its
  provenance, and [`docs/copyright.md`](docs/copyright.md) **owns that call**. Two rules bind
  before you get there: do not
  touch an existing file's header unless that commit also changes the file substantively, and never
  bump a copyright year as an incidental or standalone change.

## Changelog

**In a PR the changelog is never added to.** No new entries, and no `== Unreleased` section - a
shipped section is finished, and the in-flight section belongs to the release-time generator. There
is no window in which a PR contributes an entry, and **the `PR Checklist` gate does not enforce
this** - it checks citations, so it will happily pass an entry the policy forbids.

**The one edit a PR may make is correcting a factual error in text already there** (astubbs#198 is
the model: an entry claimed a dependency version the pom had moved past). The test is whether you
are *changing an existing claim to be true* (allowed) or *adding information about a change* (the
generator's job).

Write commit messages that can feed that generator - see [Commits](#commits).
[`docs/releasing.md`](docs/releasing.md) **owns the rest** - how generation works, the state of each
section - and wins where the two disagree.

## Issue references

The fork's issue numbers sit entirely inside upstream's range, so an unqualified `#NN` is a coin
flip - `#29` and `#114` mean different things in each repo.

- **Name the repo**: `astubbs#NN` or `confluentinc#NN` in prose, the fully qualified
  `astubbs/parallel-consumer#NN` in anything **posted to GitHub**, where the short form does not
  auto-link. Never `upstream #NN` - it names a relationship, not a repository, and the gate flags it.
- **Resolve the number in both repos before choosing the prefix.** The gate checks that a reference
  *names* a repo, not that it names the right one, and **a wrong reference that resolves is worse
  than a broken one**. Cite both numbers, fork first: `(astubbs#119, confluentinc#857)`.
- **`Fixes astubbs#167` closes nothing** - closing keywords need `astubbs/parallel-consumer#167`.
- **Run `bin/check-issue-refs.sh` before you push.** It calls the same gate module CI does, so the
  rule cannot drift; a red run is always real. Both scan the PR body when one is reachable; before
  a PR exists, the body stays CI's to catch.

[`docs/issue-references.md`](docs/issue-references.md) **owns this topic** - the threshold, the
exemptions, the reasoning - and wins where the two disagree.

## Commits

**`.gitmessage` is the template** - `git config commit.template .gitmessage` once per checkout.
Nothing lints commit messages, so all of this is on you.

- **Subject: `type(scope) #NNN: subject` - the trailing `(#N)` slot belongs to the squash-added PR
  number, never an issue.** GitHub appends `(#123)` on squash-merge, so a title ending `... (#41)`
  merges as `... (#41) (#123)`: two bare numbers with no way to tell issue from PR. Citing the issue
  at the front matches Apache Kafka and this repo's pre-fork history. For an upstream issue, word it
  (`fix(core) confluentinc#909: subject`) since a bare `#909` autolinks to the fork's own 909;
  prefer the fork mirror's number when one exists. **The same rule governs PR titles** - on
  squash-merge the merged subject *is* the PR title.
- **`(scope)` is optional** and only earns its place when it narrows usefully - `(core)`,
  `(producer)`, `(changelog)`. A directory name is not a scope.
- **Bodies feed the release notes**: what changed, what it changed for a user, plus the diagnosis,
  the experiment and the rejected alternatives.
- **Branch names encode the upstream number**: `bugs/857-...`, `fix/909-...`,
  `cherry-pick/893-...`, `upstream-pr-905`. It keeps the mapping greppable.
- Upstream-related commits carry DEP-3 provenance trailers -
  [`docs/upstream.md`](docs/upstream.md).

## PR Discipline

- **Before merging a fix, look for other instances of the same defect - and say what you found,
  including "none".** A fix that removes today's instance invites tomorrow's. Once you can name the
  defect *class* rather than the symptom, grep for its shape: the pattern, the API being misused.
  State which candidates you checked and dismissed, not just the hits - "none found" is only worth
  reading if it says where you looked, and ruling one out is a real result (astubbs#220 is the
  worked example). Do this at merge prep, once the class is understood; doing it mid-diagnosis just
  widens the investigation.
- **Before merging, recommend a merge strategy - and say why**, and **offer** to write the squash
  message and to re-cut the commits into atomic units rather than doing either silently. Keep the
  recommendation to a line or two: write the squash message where it is used, not into the
  conversation, unless asked for it.
  [`docs/merge-checklist.md`](docs/merge-checklist.md) **owns this** - why the choice matters to the
  generated release notes, the three strategies and when each applies, and the reset-to-merge-base
  trap that silently reverts master.
- **Closing something as superseded: link both directions, and link a durable anchor.** Name the
  successor from the closed PR *and* the predecessor from the successor - a reader arrives from
  whichever side they know about, and a one-way link strands the other half. If the successor does
  not exist yet, cite the tracking issue rather than a branch: a branch name is not a link, says
  nothing about whether the work landed, and nobody comes back to upgrade it. astubbs#30 said "will
  land as a fresh PR" for a month while astubbs#57, the PR in question, never mentioned its
  predecessor at all.
- **Keep the PR title and body in sync with what the PR actually covers.** Re-check before
  requesting review and before merge. Update only on *material* drift - whole workstreams missing,
  wrong specifics, scope outgrowing the title. Do not churn the description for cosmetic wording.
- **Open PRs from the template and complete its checklist honestly.**
  `.github/PULL_REQUEST_TEMPLATE.md` is NOT auto-applied when a PR is created non-interactively
  (e.g. `gh pr create -R astubbs/parallel-consumer --body-file`), so base the body on it and resolve
  every box: check it `[x]`, or mark it `N/A - <reason>`. The `PR Checklist` gate fails a
  human-authored PR when the checklist is missing entirely *or* any box is left unchecked without an
  `N/A`, so dropping the template is not a bypass. Only real bot authors are exempt.
- **Ask for the automated review when the PR is ready - it does not run on push.** Two routes, and
  they are not interchangeable: comment **`@claude review this`** when you want findings that
  mechanically block the merge (only that route can open inline review threads), or dispatch
  `claude-code-review-dispatch.yml --ref master -f pr=<number> -f focus="<steer>"` when you want a
  steer or the packaged review procedure. Enforced by the required `claude-review` check, so
  **a red `claude-review` on a PR nobody has reviewed yet is the expected state, not a fault**,
  and never something to fix by editing the gate. What exactly satisfies that check is stated in
  one place only - [`docs/ci.md`](docs/ci.md), "The gate asks..." - along with which route to
  reach for and why `--ref master` is required. Do not restate the rule here; it has drifted
  before.
- **Respond to review comments IN-THREAD and resolve the thread when addressed.** Reply to the
  specific review comment, NOT as a separate top-level PR comment - a summary comment leaves the
  original conversation unresolved and can block merge on "unresolved conversations". When a finding
  is fixed, reply in-thread with the fix plus commit SHA and mark the thread resolved
  (`gh api graphql ... resolveReviewThread`). Leave a thread open only when it genuinely needs the
  author's decision, and say so in the reply.
- **After opening a PR, follow up on the duplication reports.** The duplicate-code and
  file-similarity checks post comments flagging new clones. Read them and remove duplication
  introduced by *this* PR before it merges; clones that already existed on the base branch are out
  of scope.
- **Stacked PRs: put `depends on astubbs/parallel-consumer#N` in the description**, one line per
  parent, kept current if the chain changes. The PR-dependency gate blocks the child until every
  parent merges. Write the owner/repo form, not the bare `depends on #N` the action also accepts:
  the issue-reference gate reads the body too, and a bare number below the threshold fails it. Both
  forms are equally understood by `dependencies-action` (`partialLinkRegex`), so nothing is lost.

## Worktree ownership

**Never do any work in the main checkout. Every task gets a worktree.** The main clone at the repo
root is shared mutable state - several agent sessions run against it at once, so its HEAD can move
between two of *your own* commands. Work only under `.claude/worktrees/<name>`, and reach a task by
`cd`-ing into its worktree. `git worktree list` tells you which one holds a branch; create one if
none does.

**Reaching for `git checkout <branch>` is the tell that you are in the wrong directory** - and it is
how the rule gets broken silently. Git refuses to check out a branch another worktree already holds,
so the command *fails*; if you piped it into `tail`/`head`, the pipeline still exits 0 and a
following `&& git rebase ...` runs against whatever branch you were really on. On 2026-08-06 that
rebased an unrelated PR's branch by accident. Two habits prevent it: change directory rather than
branch, and never pipe a git command whose failure must stop an `&&` chain (or test
`${PIPESTATUS[0]}`).

- **`.worktree-owner` marker** - each worktree holds one at its root describing `owner`, `status`,
  `branch`, `pr`, and a brief `work:` line. It is local-only (git-ignored, never committed). Write
  or update it when you claim, hand off, or finish a worktree.
- **`bin/worktree-status.sh`** - prints every worktree with its marker fields plus live process
  holders (via `lsof`), the "who's on what" view the UI lacks. Run it before starting parallel work.
- **Before deleting a worktree**, verify it is safe: no live `lsof` holder, no uncommitted changes,
  and its branch content merged or preserved. A marker `status: merged - SAFE TO DELETE` records
  that verification; `git worktree lock --reason "..."` makes git refuse removal.
- The higher-level map of what each branch or worktree is *for* lives in `docs/inflight/` (the
  `branch-` and `pr-` files).

## Refactoring backlog

Deferred internal refactors live in [`docs/refactoring.md`](docs/refactoring.md) - see the table
above for what it owns, including `TODO`/`FIXME`/`XXX` triage and the release-gated breaking-change
queue. When you notice one, drop a `// TODO(refactor): <one line>` marker at the spot
(`grep -rn "TODO(refactor)" --include=*.java` lists them) and, if it warrants context, add an entry
to the doc - **do not start a parallel list**. Promote an item to a branch or PR only when you
actually start it; if it maps to an upstream issue, link it rather than duplicate it.

## Upstream tracking

Work that maps to an upstream PR must have an entry in `src/docs/development/upstream-map.yaml`,
updated **at every lifecycle transition of your own work, in the same commit that causes it**.
Nothing automated checks the fork side of that mapping, so a stale entry passes every check and
quietly rots. An upstream *issue*'s live status is owned by its fork mirror, never the manifest.

[`docs/upstream.md`](docs/upstream.md) **owns this topic** - the manifest schema and what it may
cache about an upstream issue, the mirrors, the commit trailers, the sweep. The above is only the
part that binds every session; where the two disagree, that doc wins.
