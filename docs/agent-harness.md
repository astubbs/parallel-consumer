# The agent harness - making rules fire instead of hoping they are read

How this repo gets agents to do things **reliably**. Not a style guide: a map of the mechanisms that
execute without anyone choosing to invoke them, what each can and cannot do, and how to add to them.

**This file is deliberately open for additions.** Everything here exists to meta-program the agents
working on this repo, and the set is nowhere near complete - see *Worth adding* at the end. If you
find yourself writing a rule into a document and wondering whether anyone will read it, that is the
signal to come here and give it a mechanism instead.

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
| `PreToolUse` hook | before a matched tool call | **no** | **yes** | Claude Code |
| Git hook (`core.hooksPath`) | `git commit`, `git push` | no | **yes** | **everyone** |
| CI gate | push / PR | no | **yes** | everyone, authoritatively |

Two properties decide where a rule belongs:

- **`PreToolUse` cannot inject context.** Its stdout never reaches the model. It can allow, deny, or
  ask - nothing else. A rule you want the agent to *know* goes in a `CLAUDE.md` or a context-injecting
  hook; a rule you want *enforced* goes in `PreToolUse` or a git hook.
- **Only git hooks and CI bind non-Claude actors.** Anything that must hold for a human, a different
  agent, or a cron job cannot live in `.claude/`.

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
absent, and blocking a commit for that would teach everyone to bypass the hook, taking the real
violations with it. Soft exits warn; only genuine violations block.

**`.claude/settings.json`** - two hooks, and the file is **tracked**. `.gitignore` excludes
`/.claude/*` by contents rather than excluding the directory, with a comment anticipating exactly
this; the negations `!/.claude/settings.json` and `!/.claude/hooks/**` open that door. Personal
grants stay in `settings.local.json`, still ignored.

- `PreToolUse` on `Bash` matching `git commit *` runs the same pre-commit script. Belt-and-braces:
  it catches the agent even in a clone where `core.hooksPath` was never set, which is the likely
  state of a fresh worktree on a new machine.
- `UserPromptSubmit` runs `.claude/hooks/inject-merge-checklist.sh`, which puts
  `docs/merge-checklist.md` in front of the agent when a prompt looks like merge prep - "squash",
  "rebase", "ready to merge", "tidy up the commits" and friends. It never blocks; the point is to
  inject the thought at the decision, not to gate anything. The checklist's two standing asks are to
  **offer** to write the squash message and to **offer** to reorganise the commits into cohesive
  units. Matching is deliberately broad on verbs and narrow on nouns: a false positive costs a few
  hundred tokens, a false negative costs the thing it exists to prevent.

The checklist itself is a plain doc, not embedded in the hook, so Codex and anything else reading
`AGENTS.md` gets the same words from the same file. Only the delivery is Claude-specific.

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

- **Sub-agent hook inheritance is undocumented.** Whether `.claude/settings.json` hooks apply to
  agents spawned via the Agent tool is not stated in the Claude Code docs. Assume they may not, and
  put anything load-bearing in the git hook, which binds every process that runs `git`.
- **`core.hooksPath` cannot be committed.** A fresh clone has no hooks until someone runs the config
  command. The `PreToolUse` hook covers Claude Code in that window; nothing covers a human.
- **Nothing enforces that a nested `AGENTS.md` has its `CLAUDE.md` bridge.** A check could;
  see below.

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
