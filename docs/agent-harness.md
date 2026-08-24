# The agent harness - making rules fire instead of hoping they are read

**Owns the layer map** - which mechanisms fire on their own, what each can and cannot do, and how to
add to them. `AGENTS.md` carries the pointer and nothing else, because this only matters once you
are already writing a rule and wondering whether anyone will read it. Not a style guide: the rules
themselves live in `AGENTS.md` and the topic docs; this describes the machinery that delivers them.

Claims about harness behaviour here are **tested, not read off the documentation** - every one below
that says "verified" was checked by running it against a live session, and the commands are named so
you can repeat them. That habit is not decoration: the first version of this file asserted four
things about Claude Code that turned out to be false, and each one had a design built on top of it.

**This file is deliberately open for additions.** Everything here exists to meta-program the agents
working on this repo, and the set is nowhere near complete - see *Worth adding* at the end. If you
find yourself writing a rule into a document and wondering whether anyone will read it, that is the
signal to come here and give it a mechanism instead.

## `.claude/hooks/` is runtime programming, not tooling

The framing that should govern every change in here: **the hooks are how the agent is programmed at
runtime.** Not a lint layer bolted on the side - the mechanism by which behaviour is actually
determined at the moment it matters, when nobody remembers the rule.

That is not a metaphor about documentation being important. It is a statement about *when* each
layer fires. A rule in a document only takes effect if someone opens the document and thinks to
apply it. `docs/merge-checklist.md` was injected into the very turn in which astubbs#31 was merged
with work still outstanding, and it did not help - a checklist prompts for the things you think to
check against it, never for the thing you have forgotten you are waiting on. The hook that now
catches that case fires whether or not anyone remembers it exists.

Four consequences, and they are why the sections below are as strict as they are:

- **A hook is production code.** It gets a header naming the trap and the incident that produced it,
  self-tests in `bin/test-check-agent-hooks.sh`, and a **negative control** for each - break the
  guarded thing, watch it go red, restore. Rule 3 below says so, and this harness once shipped
  without applying that rule to itself: the suite printed `FAIL` and exited `0`.
- **Fail open on your own bugs.** A guard that blocks when it is itself broken jams the tool call
  shut, which is worse than the mistake it was written to prevent.
- **Remove an arm rather than scope it** when its claim cannot be made honest. The merge guard's
  live-build arm was deleted, not narrowed: scoping it would have blinded the guard to the very case
  it existed for.
- **A hook that only reaches the shapes you thought of is a documented bypass.** Match tokens, not
  substrings; basenames, not exact strings. Both merge guards here still miss
  `gh -R owner/repo pr merge`, and that is recorded rather than quietly tolerated.

## The problem it solves

Every convention in this repo was already written down, correctly, before this harness existed. They
were still missed - by humans and by agents - because **a document only fires when somebody chooses
to open it**. The specific failure that prompted this: an agent added a note to `docs/inflight/`
describing work its own PR was landing, which `docs/inflight/AGENTS.md` forbids in its first rule.
The rule existed, was correct, and was linked from the root `AGENTS.md`. It was simply never read.

The lesson generalises: **if a rule must not be missed, it cannot live only in a doc an agent chooses
to read.** It needs a layer that fires on its own.

## What actually loads, and what does not

Verified against Claude Code **2.1.223**. This is the part most people get wrong:

| File | Loaded? |
|---|---|
| `CLAUDE.md` (repo root) | **Yes** - automatically, every session |
| `CLAUDE.md` (subdirectory) | **Yes** - lazily, when a file in that directory is read or written |
| `CLAUDE.local.md` | Yes, after `CLAUDE.md` at the same level |
| `~/.claude/CLAUDE.md` | Yes, across all projects |
| **`AGENTS.md` (any location)** | **NO. Never auto-loaded.** |

`AGENTS.md` is the portable, tool-neutral convention other agents use, and this repo keeps its rules
there. Claude Code does not read it. The bridge is a `CLAUDE.md` alongside each `AGENTS.md`
containing `@AGENTS.md`, which imports it:

- `CLAUDE.md` -> imports root `AGENTS.md`
- `bin/CLAUDE.md` -> imports `bin/AGENTS.md`, arriving when you touch a script
- `docs/inflight/CLAUDE.md` -> imports `docs/inflight/AGENTS.md`, arriving when you touch a note

**The nested ones are the interesting half.** They load *at the moment you work in that directory* -
which is exactly the "inject the right prompt at the right time" that a routing table in a doc
cannot do. Adding a nested `AGENTS.md` without its `CLAUDE.md` sibling means Claude Code never sees
it.

### Import or symlink?

Both work, and git handles both. A symlink is stored as mode `120000` with the target path as its
blob, so `ln -s AGENTS.md CLAUDE.md` commits and clones fine.

| | Symlink | `@AGENTS.md` import, kept as a pure stub |
|---|---|---|
| Drift between the two | impossible - one file | **impossible - the stub has no content to drift** |
| Windows without Developer Mode | **silently broken**: `core.symlinks` defaults false, and the file checks out as plain text containing the literal string `AGENTS.md` | fine |
| Claude-only content | impossible | possible, but see below |

This repo uses the **import**, and the reason is worth stating precisely, because the obvious version
of the argument is wrong. The usual objection to two files is drift - but a stub containing *only*
`@AGENTS.md` has nothing to drift *with*. Drift is a cost of putting rules in the stub, not a cost of
the stub existing. So keep them pure: every `CLAUDE.md` here is an import and a sentence about why
the file exists, and nothing else. On those terms the import strictly dominates - same zero drift,
and no silent Windows breakage.

That is also why the "Claude-only content" row is not the advantage it looks like. Rules written into
a `CLAUDE.md` are invisible to Codex and every other agent reading `AGENTS.md`, so taking that option
is how the two copies start diverging in the first place.

Neutrality is unaffected either way: `AGENTS.md` stays the tool-neutral source, Codex and other
agents read it directly, and `CLAUDE.md` is only the adapter that makes Claude Code see it.

## The layers

| Layer | Fires when | Injects context? | Can block? | Binds |
|---|---|---|---|---|
| Root `CLAUDE.md` | session start | yes | no | Claude Code |
| Nested `CLAUDE.md` | file in that dir is touched | yes | no | Claude Code |
| `SessionStart` / `UserPromptSubmit` hooks | session start / each prompt | **yes** | `UserPromptSubmit` only | Claude Code |
| `PreToolUse` hook | before a matched tool call | **yes**, per tool call | **yes** | Claude Code |
| Git hook (`core.hooksPath`) | `git commit`, `git push` | no | **yes** | **everyone** |
| CI gate | push / PR | no | **yes** | everyone, authoritatively |

Two properties decide where a rule belongs:

- **A hook's stdout is not the injection channel; `additionalContext` is.** An earlier version of
  this file said "`PreToolUse` cannot inject context - it can allow, deny, or ask, nothing else",
  and both hook headers cited that as the reason they were built the way they are. The first half is
  true and the conclusion is false. **Verified against 2.1.223**: a `PreToolUse` hook printing
  `{"hookSpecificOutput": {"hookEventName": "PreToolUse", "permissionDecision": "allow",
  "additionalContext": "<marker passphrase>"}}` and nothing else caused the model to report
  receiving that passphrase verbatim, delivered as a `PreToolUse:Bash hook additional context`
  system-reminder alongside the tool call. A `permissionDecisionReason` on a deny reaches the model
  too. What *is* true is that raw stdout is discarded - the JSON envelope is mandatory.
- **So choose the event by WHEN it fires, not by what it can carry.** That is why the merge
  checklist is still on `UserPromptSubmit`: `PreToolUse` fires per tool call, so the checklist would
  arrive stapled to whichever command ran next, over and over, and never at the moment the merge
  strategy is being chosen. `UserPromptSubmit` fires when the human states the intent. Same
  capability, different instant, and the instant is the whole point.
- **Only git hooks and CI bind non-Claude actors.** Anything that must hold for a human, a different
  agent, or a cron job cannot live in `.claude/`.

### `if` goes on the HOOK, not on the matcher group

The most expensive thing in this file, because it fails silently and it fails *open-ended*. A
`PreToolUse` entry has a matcher group (`matcher`, `hooks`) and, inside it, hook objects (`type`,
`command`, `if`). **`if` is only honoured on the hook object.** Put it on the group and it is
silently dropped - no warning, no parse error - and the hook then runs on **every** call to the
matched tool.

That is not merely wasteful. A gate that ends `|| exit 2` blocks the tool call when it fails, so
with a misplaced `if` a single red gate takes away *every* Bash command in the session, including
the one that would fix the gate. Verified both ways against 2.1.223, with a hook that always exits
non-zero:

| `if` position | `claude -p "...run: echo MARKER_OK"` |
|---|---|
| on the matcher group | "The command did not run - it was blocked before execution by a `PreToolUse` hook" |
| on the hook object | `MARKER_OK` |

And the positive control, which is the half that is easy to forget - after moving `if`, a
`git commit` prompt still fires the gate, so the fix filtered the hook rather than disabling it.

### ...and `if` matches a PREFIX, so only one of the two hooks can use it

`if: "Bash(gh pr merge *)"` fires only when the command *starts* with `gh pr merge`. Every other
shape the merge guard exists for - `/usr/local/bin/gh pr merge ...`, `echo ready && gh pr merge ...`
- never reached it. That is worse than a plain gap: `bin/test-check-agent-hooks.sh` asserted those
shapes were denied, and they were, *by the script* - which the harness was never going to invoke for
them. **A self-test can only prove what the script does; whether the script is reached is a
different question, and it needs asking separately.**

So the two hooks are registered differently, on purpose:

| Hook | `if` | Why |
|---|---|---|
| `check-squash-subject.sh` | **none** - runs on every Bash call | It can only ever allow, or deny a real `gh pr merge`. A `grep` for `merge` in the payload rejects the overwhelming majority before python starts, so the cost is a shell test. |
| `check-merge-outstanding-work.sh` (astubbs#324) | **none** - runs on every Bash call | Same reasoning as the squash guard, and the same shapes must reach it: `echo ready && gh pr merge ...` is exactly the case a prefix `if` would miss. A cheap `*merge*` pre-filter skips the interpreter on everything else; the decision itself is tokenised with `shlex`, so `gh pr comment --body "run gh pr merge later"` is not a merge. It watches this session's background TASKS only - it deliberately does not scan the process table for builds. |
| `pre-commit-gate.sh` | `Bash(git commit *)` | It runs the gates and can `exit 2`. Firing it on every Bash call is the outage described above - and it must stay prefix-matched anyway, because it gates *the session's* repository, which is only the right one when the command has no `cd` in front of it. |

The `git commit` case that `if` therefore misses (`cd sub && git commit`) is covered by
`.githooks/pre-commit`, which git runs inside the target repository. That is the layering working
as intended, not a hole - see *Known gaps*.

## What is wired up today

**`.githooks/pre-commit`** - runs the fast read-only gates (~1.5s total): copyright headers, issue
references, docs data, shell sigpipe, quarantine registry, action versions. Enable per clone, once:

```
git config core.hooksPath .githooks
```

One clone's config covers every worktree of that clone. Bypass with `git commit --no-verify`, which
is deliberately easy - a gate people cannot skip when they have a reason is a gate they disable
permanently. CI remains the authority; this is its fast mirror.

It distinguishes **failed** from **could not run**. `bin/check-issue-refs.sh` exits 2 when `node` is
absent and `bin/check-docs-data.sh` exits 2 without Python 3 or PyYAML, and blocking a commit for
that would teach everyone to bypass the hook, taking the real violations with it. Soft exits warn;
only genuine violations block. Keeping the soft list right means reading each gate's exit codes -
the `check-docs-data.sh` entry was written without one, so a contributor missing PyYAML (which
nothing in this repo installs) was hard-blocked from committing anything at all.

It also reports a gate that is **present but not executable**, rather than skipping it the way it
skips a gate a branch simply does not have. A lost exec bit otherwise reads as "this branch predates
that check" and the gate stops running with nobody told - a silent miss, in a tool built to stop
silent misses.

**What it reads is the working tree, not the index.** That gap is documented in the hook's own
header and listed under *Known gaps* below; it is an open decision, not an oversight.

**The three `CLAUDE.md` bridges** - `CLAUDE.md`, `bin/CLAUDE.md`, `docs/inflight/CLAUDE.md`, each a
pure `@AGENTS.md` import. They are **tracked**, which took a `.gitignore` change: a bare `CLAUDE.md`
rule there (the one whose comment begins "A `CLAUDE.md` is ignored BY DEFAULT") dated from when
these were personal scratch files, so all three were ignored and existed only on the author's
machine. Everything looked correctly wired locally and would have
merged as a no-op - `git ls-files | grep -c CLAUDE.md` returned **0**. The three paths are now
negated individually rather than with a blanket `!CLAUDE.md`; the reasoning is in `.gitignore`
itself, next to the rule.

**`.claude/settings.json`** - five hooks, and the file is **tracked**. `.gitignore` excludes
`/.claude/*` by contents rather than excluding the directory, with a comment anticipating exactly
this; the negations `!/.claude/settings.json` and `!/.claude/hooks/**` open that door. Personal
grants stay in `settings.local.json`, still ignored.

- `PreToolUse` on `Bash`, `if` `Bash(git commit *)`, runs `.claude/hooks/pre-commit-gate.sh`, a
  wrapper around the same pre-commit script. Belt-and-braces: it catches the agent even in a clone
  where `core.hooksPath` was never set, which is the likely state of a fresh worktree on a new
  machine. The wrapper exists so the hook can **read the payload and honour `--no-verify`** - the
  original inline `pre-commit || exit 2` could not see the command it was gating, which left the
  agent with no escape hatch at all while the pre-commit header promises an easy one. It exits 2
  with the failing gate's output on stderr, so the model is told *why* rather than just "no".
- `PreToolUse` on `Bash`, **with no `if`** - it runs on every Bash call and filters itself - runs
  `.claude/hooks/check-squash-subject.sh`, which refuses a `--subject` that would drop or misstate
  the PR number. It carried `if: Bash(gh pr merge *)` until review pointed out that a prefix match
  misses every shape it exists for (`/usr/local/bin/gh pr merge`, `echo x && gh pr merge`); see
  *`if` matches a PREFIX* above for the reasoning and the measured cost of removing it. Because it
  now sees every command, it only matches `gh` in **command position**, so `echo gh pr merge ...`
  is text rather than a merge.
- `PreToolUse` on `Bash`, **with no `if`**, same self-filtering shape - runs
  `.claude/hooks/check-merge-outstanding-work.sh`, which refuses a `gh pr merge` while this
  session's background tasks are still writing output. A green PR is not a finished PR when a
  subagent is mid-way through work that belongs in it; merged anyway, that work becomes a second
  PR and the first one's description goes stale on master the moment it lands. The override
  (prefix the merge command with `MERGE_DESPITE_OUTSTANDING_WORK=1`) and the stated limits - a
  stalled agent writes nothing and is not detected; `bash -c` wrapping and REST-API merges are not
  seen - are documented in the hook's own header.
- `UserPromptSubmit` runs `.claude/hooks/inject-merge-checklist.sh`, which puts
  `docs/merge-checklist.md` in front of the agent when a prompt looks like merge prep - "squash",
  "rebase", "ready to merge", "tidy up the commits" and friends. It never blocks; the point is to
  inject the thought at the decision, not to gate anything. Matching is deliberately broad on verbs
  and narrow on nouns: a false positive costs a few hundred tokens, a false negative costs the thing
  it exists to prevent.

- `SessionStart` runs `.claude/hooks/inject-recorded-knowledge.sh`, which lists the **titles** of
  every `docs/solutions/` write-up, the open items in `docs/inflight/`, and the size of
  `docs/plans/`. Titles only, once per session, no bodies - the length tracks the corpus, so no
  line count is promised here.

  It exists because the prior-art check in `AGENTS.md` is the one most often skipped, and skipping
  it is **invisible**: an agent that never learns a document exists cannot notice it is missing, so
  it rediscovers the problem and the work looks like progress the whole way. That is not
  hypothetical - astubbs/parallel-consumer#320 spent three rounds designing a fix for the
  duplication scanners' scope, a week after the diagnosis and the prescribed fix were written into
  `docs/solutions/workflow-issues/duplication-scanners-do-not-look-where-agents-duplicate-2026-08-12.md`,
  which names both CI jobs and the exact config line.

  Titles only, deliberately. The failure is not knowing the document EXISTS; once a title is in
  context the agent's own grep does the rest, and a hook that injected bodies would cost per session
  what the whole corpus costs to read. It is the clearest case in this file of the distinction the
  whole harness turns on: the rule was there, was read, and was not run - so it became a mechanism.

The checklist itself is a plain doc, not embedded in the hook, so Codex and anything else reading
`AGENTS.md` gets the same words from the same file. Only the delivery is Claude-specific - and the
hook injects the file's bytes with a one-line pointer, not a summary of them, because a summary is a
second copy in the one place nobody would think to check for drift.

**`bin/test-check-agent-hooks.sh`** - the negative control for the hooks, feeding each one
crafted payloads and asserting its verdict. It is what rule 3 below asks for, and the harness
shipped its first version without it: a review then found six defects in one 25-line parser, four
letting the exact mistake it was named after through and two hard-blocking legitimate merges. Every
one is a case in that file, and the suite goes red against the old parser.

## Adding to it

1. **Decide what the rule needs.** To be *known* -> a `CLAUDE.md`. To be *enforced* -> a hook or a
   gate. To bind humans too -> git hook or CI, never `.claude/`.
2. **Prefer the cheapest layer that actually fires.** A nested `CLAUDE.md` costs nothing and loads at
   exactly the right moment.
3. **Give it a negative control.** This repo's standing rule: a check that has never failed proves
   nothing. Make it go red on purpose before you trust it - `bin/AGENTS.md` says the same about
   `test-check-*.sh`.
4. **Keep pre-commit under a couple of seconds.** A slow hook gets `--no-verify`'d by habit and then
   protects nothing. Slower checks belong in CI only.
5. **Write down what you verified**, especially harness behaviour - the auto-load table above was
   wrong in three different ways when guessed at, and right only once checked against the docs.

## Known gaps

- **Pre-commit gates read the working tree; the commit records the index.** Stage a clean hunk and
  leave an unrelated dirty one and you are blocked for content you are not committing; stage a
  violation and fix it unstaged and the violation is gated green. Documented in the hook header
  rather than fixed: the usual remedy, `git stash push --keep-index` around the run, can destroy
  uncommitted work if the hook dies mid-run, and a pre-commit hook that can eat your changes is
  worse than one with a stated blind spot. CI reads the pushed commit and has no such gap.
  **Open decision** - the alternatives are a stash with a robust trap, gating `git diff --cached`
  instead of the tree (which several of these gates cannot do, being whole-tree scans), or leaving
  it as is.
- **`core.hooksPath` cannot be committed.** A fresh clone has no hooks until someone runs the config
  command. The `PreToolUse` hook covers Claude Code in that window; nothing covers a human.
- **The `PreToolUse` `if` matches the command as written.** `Bash(git commit *)` does not fire on
  `cd sub && git commit ...`. The git hook covers that case; the Claude-side belt-and-braces does
  not.
- **Nothing enforces that a nested `AGENTS.md` has its `CLAUDE.md` bridge.** A check could;
  see below. Until then the `.gitignore` negation is the only place the question is asked - which is
  why the three bridges are enumerated there rather than blanket-negated.
- **Tracking `.claude/settings.json` silently overwrites the local one it replaces - once.** Git
  refuses to clobber an *untracked* file and clobbers an *ignored* one without a word, and this file
  was ignored in every clone until it became tracked. So the first pull past that commit replaces
  any local `.claude/settings.json` with the shared version: no conflict, no warning, and nothing to
  recover from, because the old contents were never in git. Verified against a scratch clone rather
  than reasoned about. This is unfixable from inside the repo - git resolves the checkout before any
  hook here runs - and it is a **one-time** hazard: it only bites a clone that predates the change.
  The mitigation is to move anything local into `.claude/settings.local.json`, which stays ignored,
  before pulling; the `.gitignore` comment says so at the point someone reads it.
  <!-- file-refs: N/A - the file is git-ignored by design, so it is absent from every checkout -->

## Settled by testing, so nobody re-opens them

- **Sub-agent hooks DO fire.** Whether `.claude/settings.json` hooks apply to agents spawned via the
  Agent tool is not stated in the Claude Code docs, and this file previously said to assume they may
  not. **Verified against 2.1.223**: with a `PreToolUse` hook logging every payload, a `claude -p`
  told to spawn a sub-agent to run `echo SUBAGENT_RAN_THIS` and to run no bash itself produced a log
  containing exactly that command. Inheritance is real, which cuts both ways - it is why the `if`
  bug above was so damaging, since the blast radius included every sub-agent too.
  Put load-bearing rules in the git hook anyway, for the reason in the layer table: it binds every
  process that runs `git`, not just this tool.

## Worth adding

Open list - add to it, or take from it:

- A gate asserting every `AGENTS.md` has a sibling `CLAUDE.md` importing it, so a future nested
  convention cannot be invisible to Claude Code the way `docs/inflight/AGENTS.md` was.
- A `SessionStart` hook surfacing repo state an agent otherwise has to think to ask for: open PRs
  needing an LGTM, worktrees with uncommitted work, gates currently red on master.
- A `PreToolUse` deny on `git push --force` / `git rebase` against shared branches, which several
  skill definitions already forbid in prose.
- A `UserPromptSubmit` hook injecting the current PR's review state, so "is this LGTM'd" never has to
  be asked.
- Pre-push rather than pre-commit for the slower gates, keeping commits fast while still catching
  things before they reach CI.
