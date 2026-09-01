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
| `PostToolUse` hook | after a matched tool call | **yes**, per tool call | no - it already ran | Claude Code |
| Git hook (`core.hooksPath`) | `git commit`, `git push` | no | **yes** | **everyone** |
| CI gate | push / PR | no | **yes** | everyone, authoritatively |

**`PostToolUse` is the layer for "you just did X, now go and check Y".** It cannot block - the call
has happened - so it is only worth using when the thing to say could not have been said earlier.
`after-push-check-ci.sh` is the case that earned it: CI does not exist until the push lands, and the
window that matters is between the push and the agent moving on. Its `additionalContext` works the
same way as `PreToolUse`'s and is verified by `bin/test-check-agent-hooks.sh` rather than assumed.

The reason it exists is worth knowing before adding a second one: a required duplication check went
red on astubbs#267 with its finding posted **nowhere** - GitHub rejected the inline annotation - so
the only record was a job log nobody opened, and a later push cleared the red without fixing the
duplication ([`docs/inflight/ci-duplication-report-can-fail-to-post.md`](inflight/ci-duplication-report-can-fail-to-post.md)).
No earlier layer can carry that: at prompt time there is no push to talk about.

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
| `pre-commit-gate.sh` | `Bash(git commit *)` | It runs the gates and can `exit 2`. Firing it on every Bash call is the outage described above - and it must stay prefix-matched anyway, because it gates *the session's* repository, which is only the right one when the command has no `cd` in front of it. **It self-filters as well**, exiting 0 when the payload holds no commit, because the `if` is a belt the script must not hang its trousers on - see below. |

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

**`.claude/settings.json`** - fourteen hook scripts across sixteen registrations, and the file is
**tracked**. The entries below are the ones whose design decisions are worth recording here;
`remind-inflight-on-push.sh` and `check-history-rewrite.sh` carry theirs in their own headers.
The count is stated because it drifted: this said "five" while the file registered seven, which is
the same silent staleness the rest of this document exists to prevent. `.gitignore` excludes
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
  It also **decides for itself** whether the payload contains a commit, rather than trusting the
  `if` to have filtered for it - and finding a commit means finding it wherever the shell would run
  one, `then`, `do`, `{` and `!` included, or the self-filter turns a scope fix into an exemption.
- `PreToolUse` on `Bash`, **with no `if`** - it runs on every Bash call and filters itself - runs
  `.claude/hooks/check-squash-subject.sh`, which refuses a `--subject` that would drop or misstate
  the PR number. It carried `if: Bash(gh pr merge *)` until review pointed out that a prefix match
  misses every shape it exists for (`/usr/local/bin/gh pr merge`, `echo x && gh pr merge`); see
  *`if` matches a PREFIX* above for the reasoning and the measured cost of removing it. It finds the
  merge with a regex over the raw command, **not** a command-position scan - this file claimed the
  opposite until it was run: an `echo` whose argument spells out a subject-overriding merge is
  denied, and the near-miss case that reads like a command-position test passes only because its
  quoting makes `shlex` raise. Erring towards denying is the safe direction for a guard that can
  only refuse a merge, but do not build on the stronger claim. See *Known gaps*.
- `PreToolUse` on `Bash`, **with no `if`** - runs `.claude/hooks/warn-low-disk.sh`, which warns when
  either disk this project fills is running low, and **never blocks**. It exists because a fan-out of
  eleven per-language demo agents took the host volume to 8.8 GiB free of 926 GiB in about an hour
  and took the Docker VM's virtual disk with it - one agent's build died outright, two others pruned
  under each other - and nothing warned, because the session had started with plenty of room.
  `SessionStart` would therefore have reported all clear; the only instant that can see what the last
  command left behind is just before the next one. It has no `if` for the same reason the squash
  guard has none: `Bash(docker *)` would miss `cd demo && docker compose up` and every wrapper
  script, which is most of how containers actually get built here. It buys the right to run on every
  call by forking a handful of short-lived commands and no `docker` CLI - the hook's own header owns
  the measured figure, and states why it is not repeated here - and by saying nothing at all unless a
  threshold trips, then at most once per ten minutes **per session** unless the band worsens.

  **The throttle is keyed per session, not per user, and that decides who the warning is for.**
  One stamp per UID meant the first agent to notice silenced every other concurrent session for
  the window - in the incident above, ten of the eleven could not have been told while they were
  the ten still filling the disk. So every session is warned, and the message tells each agent to
  **report the situation and suggest a reclaim, never run one**: the operator is the gate against
  duplicate effort, which the same incident shows is a real cost and not a hypothetical - two of
  those agents pruned under each other. Keying is best-effort and degrades to the shared stamp
  when no `session_id` reaches the hook, because the failure this hook refuses is going silent.

  Two properties are worth keeping in mind if you change it. **It must never exit non-zero**: a disk
  warner that blocked `Bash` on a full disk would remove the commands needed to clear the disk, which
  is the outage described under the misplaced-`if` trap above. And **Docker Desktop's disk image is a
  high-water mark** - a sparse file that grows and never shrinks, so pruning 17 GB does not shrink it
  by a byte; that is why a cheap always-on trigger is confirmed by a cached `docker system df` before
  anything is said, and why the correction applies only to the sparse-image reading and not to
  Linux's live filesystem one. It is a dev-machine tool by design: `.claude/` binds Claude Code
  sessions only, so it can never run in CI, and CI runners are reaped anyway.

- `PreToolUse` on `Bash`, **with no `if`**, same self-filtering shape - runs
  `.claude/hooks/check-merge-outstanding-work.sh`, which refuses a `gh pr merge` while this
  session's background tasks are still writing output. A green PR is not a finished PR when a
  subagent is mid-way through work that belongs in it; merged anyway, that work becomes a second
  PR and the first one's description goes stale on master the moment it lands. The override
  (prefix the merge command with `MERGE_DESPITE_OUTSTANDING_WORK=1`) and the stated limits - a
  stalled agent writes nothing and is not detected; `bash -c` wrapping and REST-API merges are not
  seen - are documented in the hook's own header.
- `PreToolUse` on `Bash`, **with no `if`**, same self-filtering shape - runs
  `.claude/hooks/check-upstream-map-merged.sh`, which refuses a `gh pr merge <N>` while an entry in
  `src/docs/development/upstream-map.yaml` naming that PR still says `status: pr-open`. The manifest
  is meant to be written to `merged` on the branch, BEFORE the merge - there is no observable instant
  where that is untrue, and doing it afterwards means a commit straight to master that nobody
  remembers to make. It gates on the status rather than on mere mention, so it is silent once the
  entry is right: a guard that fires on correct behaviour teaches people to route around it. Fails
  open on every uncertainty - no PyYAML, unparseable manifest, no manifest in the CWD, no PR number
  on the command line. Deliberately disposable: it exists only until the last upstream link is
  closed out, and then it is one file to delete.
- `PreToolUse` on `Bash`, **with no `if`** - runs `.claude/hooks/remind-inflight-on-push.sh`, which
  reminds you at PUSH time what this PR's own inflight note still lists as open. Push, not commit and
  not merge: commits are too frequent for a note that runs to dozens of lines, and the merge guard
  above is the backstop that fires when re-opening the work is already expensive. It emits
  `additionalContext` and never denies. Its own header owns the reasoning.
- `PreToolUse` on `Bash`, **with no `if`**, same self-filtering shape - runs
  `.claude/hooks/remind-master-drift-on-push.sh`, which reports the commits `origin/master` has
  gained that this branch does not have, and whether any of them touch files the branch is changing.
  It answers the question "Read the commits you inherit" poses and nothing else was putting in front
  of anyone: not *how far* the branch has diverged - `docs/inflight/AGENTS.md` rightly says never to
  write down what `git rev-list --left-right --count` can answer - but *whether anything relevant
  has landed*, which needs the subjects. It never says to merge: batching several master merges is
  often right, and the failure it exists to prevent is deciding without looking. Throttled on
  master's SHA rather than a clock, so the same tip is reported once per branch and a master that
  moves reports again immediately; the one clock is a floor on how often it fetches, so a push loop
  cannot become a fetch loop. It **fetches** before reading, because a stale `origin/master`
  under-reports, which is the exact failure it exists to prevent.
- `SessionStart` **and** `PreToolUse` on `Bash` - runs
  `.claude/hooks/check-branch-behind-its-own-remote.sh`, the mirror image of the drift hook above:
  that one watches the base moving under you, this one watches **your own branch** moving under you.
  At session start it runs `git fetch --all --prune`, throttled on a stamp file, so every ref the
  session goes on to read is real; on a `git merge` or `git rebase` it **refuses** while
  `origin/<branch>` holds commits the checkout does not, and names them.
  `BRANCH_FRESHNESS_OVERRIDE=1` is the documented override, honoured both as a genuinely exported
  variable and as a **token** of the parsed command - never as a raw-payload substring, which review
  showed any prose mentioning the variable could satisfy, including the agent-written `description`
  field, on a guard whose own deny message teaches that exact string.
  `BRANCH_FRESHNESS_FETCH_FLOOR` (default 300s) is the SessionStart fetch throttle, and exists so
  the self-test can drive both sides of it.
  **It exempts two things on purpose, and a guard that blocks its own remedy is why**: merging or
  rebasing onto `origin/<this-branch>` (or `@{upstream}`/`FETCH_HEAD`) is the reconciliation it asks
  for, and `--abort`/`--continue`/`--skip`/`--quit` are the way out of a conflicted tree. The first
  version denied both - which on `master` meant denying `git merge origin/master` every time master
  advanced.
  It denies where the other push hooks only report, and the line is the one this document already
  draws: a situation with no wrong answer gets `additionalContext`, and merging onto a ref you know
  is behind its published tip is not that - the result cannot be pushed without discarding somebody
  else's commits, so no outcome was wanted. `git push` is deliberately not an arm, because git
  refuses that case itself and legibly. Measured cost of not having it, on 2026-08-26: a session
  fetched `origin/master`, never fetched astubbs#205's own two-week-stale ref, re-did a package
  rename of 239 files, resolved 43 conflicts on top, and learned at the rejected push that all of it
  was already published. Its own header owns the incident.
- The push detection and the portable `stat` both live in `.claude/hooks/lib/hook-common.sh`, shared
  by the two push hooks. Each had been got wrong once in a way that made a hook *silently stop
  working* - `git -C <path> push` unmatched, `stat -c` unavailable on BSD - and a second copy hides
  the next such bug until somebody re-runs the same experiment on the same platform.
- `PreToolUse` on `Bash`, **with no `if`** - runs `.claude/hooks/check-history-rewrite.sh`, one of
  the two guards here that **refuse**: it stops a force-push, rebase, amend or any other ref-moving
  command while a review is in flight, because a rewrite orphans inline review threads and destroys
  the incremental diff the reviewer works from. It names what would actually be lost rather than asking
  "are you sure?", and `REWRITE_HISTORY_CONFIRMED=1` is the documented override. Its own header owns
  the rest, including the full list of ref-moving shapes it reaches.
- `PreToolUse` on `Bash`, **with no `if`** - runs `.claude/hooks/check-shallow-history.sh`, the other
  guard that **refuses**: it denies a depth-dependent history query - a range, an ancestry test, a
  whole-history walk - while the clone is shallow, because such a query does not error, it *answers*,
  from the truncated graft. `SHALLOW_HISTORY_ACCEPTED=1` is the override. It is per-command rather
  than per-session because the `shallow` file lives in the shared `--git-common-dir`, so one sibling
  agent's depth-limited fetch re-shallows every worktree, including one that unshallowed itself a
  minute earlier. Its own header owns the rest, including why `git status` and `git log -1` are left
  alone.
- `UserPromptSubmit` runs `.claude/hooks/inject-merge-checklist.sh`, which puts
  `docs/merge-checklist.md` in front of the agent when a prompt looks like merge prep - "squash",
  "rebase", "ready to merge", "tidy up the commits" and friends. It never blocks; the point is to
  inject the thought at the decision, not to gate anything. Matching is deliberately broad on verbs
  and narrow on nouns: a false positive costs a few hundred tokens, a false negative costs the thing
  it exists to prevent.

- `PostToolUse` on `Bash` runs `.claude/hooks/after-push-check-ci.sh`, the only registration on that
  event. Why it has to be there rather than any earlier layer is above, under `PostToolUse`; it is
  listed here so the registry is not silent about an event the rest of the file never uses.
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

**`.claude/hooks/inject-branch-context.sh`** - the branch's own record, put in front of whoever is
about to work on it: the commits between the merge base and `HEAD` with each body's non-empty line
count, the `docs/inflight/` and `docs/plans/` notes only this branch has, the `.worktree-owner`
marker, and the open PR's number, title, body size, **comment count per author** and review count.
Names, counts and pointers - never bodies, the same cheapness contract `inject-recorded-knowledge.sh`
states, and for the same reason: the failure being fixed is not knowing the record exists.

It exists because `AGENTS.md`'s inherit rule had one trigger - *your base moved* - and the other one
was unwritten: *you were handed a branch.* On 2026-08-24 five agents were dispatched, one per open
PR, each given that PR's changed-file list and none of its commits, body or comments. Every one of
the five had a decision in its body that a simplify pass reverses on sight, and in
astubbs/parallel-consumer#341's case the decisive text was a **PR comment posted after the body** -
which is why this hook counts comments *by author* rather than merely noting that a body exists.

**Registered at three points, and the third is the one that works.** All three measured against
2.1.231; the reasoning for each is in *Settled by testing* below:

| Registration | Reaches | When, measured |
|---|---|---|
| `SessionStart` | a session opened in the worktree | **before its first tool call** |
| `PreToolUse`, tool `Agent`/`Task` | the **dispatcher** | alongside the dispatched agent's *result* |
| `PreToolUse`, payload carries `agent_type` | the **subagent itself** | alongside its first tool result |

The middle row is the honest limitation, and the emitted block says so rather than implying the
dispatch was vetted: a `PreToolUse` hook **cannot alter the call it fires on**, because the model
composed that tool call before the hook ran. It reaches the dispatcher in time to judge what came
back and to compose dispatch N+1 - four further agents, in the incident above - and not in time to
fix dispatch N.

The third row is what actually closes that incident, and it exists because of a **negative result**:
`SessionStart` does not fire for an agent spawned via the Task tool, so without this registration a
subagent could receive branch context by no route at all. It is throttled to once per `agent_id`,
with the stamp file named after that id verbatim so the shell prologue can test for it without
hashing - `shasum` differs between GNU and BSD, and the python spawn it skips measures 27ms on every
tool call the subagent makes, against 6ms for the shell bail.

Both `PreToolUse` rows are **one** registration with `matcher: "*"`, self-filtering on the payload -
the same shape `check-squash-subject.sh` uses, and for the same reason. A matcher of `Task` alone
would miss the subagent row entirely.

**A degraded read is LOUD, never short.** A section that cannot be built says `COULD NOT BE BUILT` or
`UNKNOWN` and names the reason, instead of being omitted - because a shorter block that reads
complete is indistinguishable from a healthy one, which is this hook's own failure signature. That is
measured here rather than assumed: `inject-recorded-knowledge.sh` uses GNU-only `xargs -r`, and under
a BSD `xargs` its Registers section drops from 13 entries to 4 while closed notes get relabelled as
mis-tagged. That defect belongs to astubbs/parallel-consumer#341's class and is fixed there, not
here. Distinguishing a *confirmed* absence from a failure matters just as much in the other
direction: `gh` exits non-zero for "this branch has no PR" exactly as it does for offline, so the
no-PR case is read off stderr and reported as a fact - otherwise every fresh branch prints an alarm,
and an alarm that is always on gets scrolled past.

`gh` is the only network call, on a path that fires per dispatch and per subagent: bounded at 5s,
cached 10 minutes per repo and branch, and the repo slug derived from `git remote get-url origin`
rather than left to `gh` - a bare `gh` here answers **confluentinc/parallel-consumer**, and the
damaging case is the command that *succeeds* against the wrong repository. The bound is python's
`subprocess` timeout, not `timeout(1)`, which is GNU coreutils and absent on macOS.

**`bin/test-check-agent-hooks.sh`** - the negative control for the hooks, feeding each one
crafted payloads and asserting its verdict. It is what rule 3 below asks for, and the harness
shipped its first version without it: a review then found six defects in one 25-line parser, four
letting the exact mistake it was named after through and two hard-blocking legitimate merges. Every
one is a case in that file, and the suite goes red against the old parser.

The disk hook's cases are worth reading before adding a hook of your own, because it has the failure
mode every warn-only hook shares: **its correct behaviour on a healthy machine is to print nothing,
which is byte-identical to it being broken, unregistered, or not running at all.** So its silent case
is pinned to thresholds of zero rather than to a healthy disk, and pairs with forced cases proving the
same call path can be made to speak. An earlier version left the thresholds at their defaults, which
made the suite a function of how much free space the machine happened to have - three cases flipped
to failing mid-session when the host dropped below the default warn line. A self-test for a disk
warner must not itself depend on the disk.

**Still unverified for this hook: whether the harness reaches it at all.** The `claude -p` reachability
check that settled that question for the other three could not be run when it was added. That is the
separate question flagged under *`if` matches a PREFIX* - a self-test can only prove what a script
does - and it stays open until someone runs it.

## Adding to it

1. **Decide what the rule needs.** To be *known* -> a `CLAUDE.md`. To be *enforced* -> a hook or a
   gate. To bind humans too -> git hook or CI, never `.claude/`.
2. **Prefer the cheapest layer that actually fires.** A nested `CLAUDE.md` costs nothing and loads at
   exactly the right moment.
3. **Give it a negative control - and an ADJACENT case it must ignore.** This repo's standing rule:
   a check that has never failed proves nothing. Make it go red on purpose before you trust it -
   `bin/AGENTS.md` says the same about `test-check-*.sh`.

   **The red control alone is not enough, and a session's worth of evidence says so.** Every gate
   that misbehaved on 2026-08-19/20 failed on **scope**, never on logic: `pre-commit-gate.sh` fired
   on read-only commands that were not commits; `check-pr-ready.sh` read only the first matching
   `pr-<n>-*.md` when two existed; the inflight `inflight-state` regex was greedy, so the gate and
   the session index disagreed about the same note; `chaos-test.sh`'s EXIT trap swallowed the exit
   code and would have rendered a real chaos RED as green. **Not one was wrong about what to check.**
   A red control proves the gate CAN fire; it says nothing about whether it is looking at the right
   thing.

   So pair every red control with a **near-miss that must stay green** - the thing one character away
   from what you are catching. `bin/test-check-agent-hooks.sh` already does this where it was learned
   the hard way (`a commit MESSAGE mentioning push does not fire`, `gh pr comment --body "run gh pr
   merge later"`), and those cases exist because the first drafts matched substrings and blocked
   them. Write the near-miss when the gate is new, not after it has been routed around.
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
  not. **And it does not filter reliably in the other direction either** - verified against 2.1.231,
  the same registration lets a COMPOUND command through to the hook: a `for` loop with a nested `if`
  and a command substitution reached an always-deny hook and was blocked, while a plain `echo` was
  correctly filtered out. That is the misfire this harness has now been bitten by twice. Treat `if`
  as a cheap filter for the common case and nothing more - `pre-commit-gate.sh` decides for itself
  whether the payload holds a commit.
- **`check-squash-subject.sh` matches its merge anywhere in the command, not in command position.**
  An `echo` that merely names one is denied. Unfixed on purpose: the hook can only ever refuse a
  merge, so over-matching costs a spurious deny carrying an actionable message, while under-matching
  costs the thing it exists to prevent. The sibling guards (`check-merge-outstanding-work.sh`,
  `remind-inflight-on-push.sh`, `check-history-rewrite.sh`) scan every token for the same reason.
  Only `pre-commit-gate.sh` tracks command position, because only it can exit 0 on a miss - which is
  exactly how it came to have a blind spot worth fixing.
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

## NOT settled by testing - the nested cascade

`parallel-consumer-core/src/main/java/bz/stub/parallelconsumer/AGENTS.md` is placed at a **package
root**, above the three sub-packages its rules are about - `state`, `metrics`, `internal`. That
placement assumes a nested `CLAUDE.md` loads for a file in a **subdirectory**, not only for a file
directly in its own directory. The layer table above says "file in that dir is touched", which does
not answer it, and the two existing bridges (`bin/`, `docs/inflight/`) both sit directly above their
files, so neither tests the question.

**Evidence it probably cascades**, short of a test: the root `CLAUDE.md` loads for work anywhere in
the tree, so ancestors clearly participate. Whether the root is special-cased is the open part.

**How to settle it:** edit a file in `.../parallelconsumer/state/` in a fresh session and check
whether the package-root rules arrive. If they do not, the fix is three bridges instead of one - the
content is identical and the cost is duplication, which is why one was tried first.

Until then this is an assumption, and a rule that does not arrive is a rule that does not exist.

## Settled by testing, so nobody re-opens them

- **Sub-agent hooks DO fire.** Whether `.claude/settings.json` hooks apply to agents spawned via the
  Agent tool is not stated in the Claude Code docs, and this file previously said to assume they may
  not. **Verified against 2.1.223**: with a `PreToolUse` hook logging every payload, a `claude -p`
  told to spawn a sub-agent to run `echo SUBAGENT_RAN_THIS` and to run no bash itself produced a log
  containing exactly that command. Inheritance is real, which cuts both ways - it is why the `if`
  bug above was so damaging, since the blast radius included every sub-agent too.
  Put load-bearing rules in the git hook anyway, for the reason in the layer table: it binds every
  process that runs `git`, not just this tool.

- **`SessionStart` does NOT fire for a sub-agent.** The complement of the row above, and the more
  surprising half. **Verified against 2.1.231**: a `SessionStart` hook logging every payload fired
  exactly once, `source=startup`, for a `claude -p` that dispatched a sub-agent which then ran two
  Bash calls - and those Bash calls fired the `PreToolUse` hook carrying the **same `session_id`** as
  the parent. A sub-agent does not get a session; it borrows the dispatcher's. So any context an
  agent must have *before it starts* cannot be delivered to a sub-agent by `SessionStart`.
- **A hook CAN inject into a sub-agent's own context, keyed on `agent_type`.** The route the previous
  point closes off, reopened. A sub-agent's tool-call payload carries `agent_id`, `agent_type` and
  `effort` on top of the usual fields; **verified against 2.1.231**, a `PreToolUse` hook that emitted
  `additionalContext` only when `agent_type` was present had that text quoted back by the sub-agent
  itself, while the dispatcher in the same run saw nothing of it. It arrives with the sub-agent's
  *first tool result*, so it is early rather than pre-emptive, and it repeats on every subsequent
  call unless the hook throttles per `agent_id`.
- **The Task tool's real `tool_name` is `Agent`, and matchers are regexes.** **Verified against
  2.1.231**: the payload for a Task dispatch reports `"tool_name": "Agent"`, yet matchers of `Task`,
  of `Agent`, of `Task|Agent` and of `*` all fired for it, and the injected text is delivered to the
  model labelled `PreToolUse:Agent hook`. Match on whichever you like; do not assume the payload will
  say `Task`.
- **`additionalContext` reaches the CALLER, not the callee, and it arrives with the RESULT.**
  **Verified against 2.1.231** with markers in both directions: the dispatcher quoted the string back
  and reported it arriving after the sub-agent's tool call had completed, while the sub-agent asked
  the same question answered that it had seen nothing. A `PreToolUse` hook on a dispatch therefore
  cannot pre-empt that dispatch - it can only inform what the caller does next. Choose the event for
  *when* it fires, and then be honest in the text about what that instant can and cannot promise.
- **Injected text that reads like an instruction gets flagged as prompt injection.** Not a harness
  behaviour but a reliable model one, and it shapes how these hooks must be written: probes whose
  `additionalContext` said "repeat this string verbatim" were quoted back with an unprompted warning
  that the content looked like an injection attempt and should be treated as untrusted. State the
  provenance and keep the register factual - `inject-branch-context.sh` opens by naming the script
  and the settings file it is registered in, and says it is a report rather than an instruction.

## Worth adding

Open list - add to it, or take from it:

- A gate asserting every `AGENTS.md` has a sibling `CLAUDE.md` importing it, so a future nested
  convention cannot be invisible to Claude Code the way `docs/inflight/AGENTS.md` was.
- A `SessionStart` hook surfacing repo state an agent otherwise has to think to ask for: open PRs
  needing an LGTM, worktrees with uncommitted work, gates currently red on master. **Partly done** -
  `inject-branch-context.sh` covers the current branch's own PR and its worktree marker. What is left
  is the cross-repo view: *other* PRs awaiting an LGTM, *other* worktrees with uncommitted work, and
  master's gate state. Different query, different cost, so it stays on the list. Picking a branch
  back up is also the cheapest moment to learn master has moved under it, so
  `remind-master-drift-on-push.sh` would earn its keep there too - it is on push only because that is
  where its sibling already had a tested detector, not because session start was ruled out.
- A `PreToolUse` deny on `git push --force` / `git rebase` against shared branches, which several
  skill definitions already forbid in prose.
- A `UserPromptSubmit` hook injecting the current PR's review state, so "is this LGTM'd" never has to
  be asked. **Superseded for the counting half; the verdict half is still open.**
  `inject-branch-context.sh` already reports how many reviews a PR carries and who wrote them, at
  three events. What nobody reports is whether any of them is an *approval* - the question that entry
  was really about, and the one that matters here, since this repo requires a human LGTM that no
  review count answers.
- Pre-push rather than pre-commit for the slower gates, keeping commits fast while still catching
  things before they reach CI.

The disk hook's cases are worth reading before adding a hook of your own, because it has the failure
mode every warn-only hook shares: **its correct behaviour on a healthy machine is to print nothing,
which is byte-identical to it being broken, unregistered, or not running at all.** So its silent case
is pinned to thresholds of zero rather than to a healthy disk, and pairs with forced cases proving the
same call path can be made to speak. An earlier version left the thresholds at their defaults, which
made the suite a function of how much free space the machine happened to have - three cases flipped
to failing mid-session when the host dropped below the default warn line. A self-test for a disk
warner must not itself depend on the disk.
