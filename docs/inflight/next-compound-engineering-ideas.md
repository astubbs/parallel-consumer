# Compound-engineering ideas from the language-proxy fan-out

**A running ledger, kept as the work happens, to be ranked and selected at the end** (owner's
instruction, 2026-08-14 — and the owner asked to be reminded when that point arrives). These are
candidate *practices*, not product work: things this project did that would compound if turned into
a skill, a check, a dispatch convention, or a rule other projects could adopt.

Add to it as the fan-out continues. Do not rank here — ranking is the end-of-work step, with the
owner.

## The strong ones

**1. Falsification by fresh context.** Give an agent with no prior context *only* the specification
and the generated artifacts, and have it build a working client. Every question it cannot answer
from the documents is, by definition, a specification defect — the defect list is the deliverable,
and the implementation is thrown away. Done here as the specification probe: 12 defects found, none
of them things the authors would have thought to look for. Generalises to any API, protocol, runbook
or onboarding doc. The discipline that makes it work is refusing to read the reference
implementation, and *recording each time you were forced to*.

**2. Convergence as a validity signal.** When two independently-dispatched agents, unaware of each
other, hit the *same* defect, it is systematic rather than a quirk of one author. That happened
twice here (the unimplementable client-side status; the drain-versus-negotiation contradiction) and
turned two "maybe I misread it" notes into decisions the specification then made. Cheap to exploit:
dispatch N independent implementers, then diff their complaint lists rather than their code.

**3. The reference is the least-audited code in the system.** The Java reference client — written
first, reviewed twice — was the only client that declared its capabilities wrongly. Both later
foreign clients got it right unprompted, because they read the specification while the reference
predated the rule. **New implementations audit the original**, and a fan-out is therefore a review
mechanism, not only an output multiplier.

**4. Hand known defects forward instead of serialising on the fix.** The obvious sequencing was
"repair the docs, then write the clients". Instead each client agent was handed the known
corrections inline while a separate agent repaired the docs concurrently. Same result, no waiting,
and the fixes were validated by five implementations while being written. The general rule: a known
defect is a *prompt input*, not a blocker.

**5. Structure the corpus so parallel agents can never share a file.** One data file per module,
filename equal to the artifact, merged by the gate; per-language notes in their own files; shared
surfaces created up front by a single seeding pass so every later agent only ever *adds* files
inside its own directory. Eight agents ran concurrently in one worktree with zero merge conflicts.
The failure this prevents is not conflict resolution — it is two agents silently overwriting each
other's edit to the same list.

**6. Every dispatched check must be proven able to fail.** "Introduce the defect, watch it go red,
revert" as a standing clause in every agent prompt. It caught decorative checks repeatedly here, and
it is the direct antidote to this repo's most recurrent documented failure class — a check that
reports success without having run.

## Worth capturing, smaller

**7. Order parked findings by when they detonate, not by severity.** Review findings were parked
unfixed at the owner's call; the one placed first was a P2 that would arm itself the moment another
in-flight unit landed. It did, hours later, and was fixed in one line because the note said so. A
parked-findings list sorted by severity would have buried it.

**8. Land additive metadata before the gate that freezes it arms.** The protocol's per-language
placement options were added while the breaking-change gate was still unarmed; `buf` later confirmed
every one of them reads as breaking. Generalises: naming, namespacing and identity decisions are
free before a freeze and permanently expensive after, so sweep for them *at* the freeze.

**9. Dispatch conventions that keep agents honest.** Two mechanical ones learned the hard way:
subagents that background a long build stop instead of resuming (tell them to wait synchronously),
and parallel agents in one worktree share `target/`, so a root build's `clean` destroys siblings'
output (scope every dispatched build to its own module).

**10. The git index is shared state between parallel agents, and `git add` is a trap.** File
ownership was split so no two agents edited the same file — but every agent in a worktree shares one
*index*, so `git add <mine> && git commit` commits whatever anyone else happens to have staged at
that moment.

**It happened twice in one night, and the second was not small.** First an orchestrator's
documentation commit swallowed a language agent's pending file deletion; then an entire language
client — its source, tests, build files and data fragments — landed inside a *different* language's
commit. Both times the content was correct and only the attribution was wrong, which is precisely
what makes it dangerous: nothing fails, nothing is red, and the history reads as deliberate forever
after. A future bisect lands on a commit whose message describes none of what it contains.

Three fixes, in ascending order of how much they actually fix:

- **Discipline:** `git commit -- <paths>` always, never `git add` then commit. For a new file,
  `git add <paths> && git commit -- <paths>` — the pathspec on the *commit* is what bounds it.
  Cheap, and it belongs in every dispatch prompt; but it is compliance, so it will fail sometimes.
- **A hook** that refuses a commit whose staged set is wider than the paths it names. Mechanical,
  but needs the agent to declare its intent somewhere the hook can read.
- **Real isolation:** one worktree per agent, which is what worktrees exist for and what this repo's
  own ownership rule already says. It was skipped here for a real reason — agents on one branch
  share a build tree and a branch cannot be checked out twice — so the honest statement is that
  running N agents in one worktree buys coordination and *pays for it in shared mutable state*: one
  index, one `target/`, one HEAD. Know which you are choosing.

**11. A fan-out doubles as a design review of the API it mirrors.** Each language's idioms
interrogate the shared surface: no-exception languages test whether outcomes are really values,
single-threaded runtimes test whether the concurrency model is really the engine's, forked-process
runtimes test whether the client is really stateless. Questions the reference language cannot ask
itself.
