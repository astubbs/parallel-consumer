# Compound-engineering ideas from the language-proxy fan-out

**A running ledger, kept as the work happens, to be ranked and selected at the end** (owner's
instruction, 2026-08-14 — and the owner asked to be reminded when that point arrives). These are
candidate *practices*, not product work: things this project did that would compound if turned into
a skill, a check, a dispatch convention, or a rule other projects could adopt.

Add to it as the fan-out continues. Do not rank here — ranking is the end-of-work step, with the
owner.

**Sibling ledger**: [`next-compound-ideas-from-the-engine-performance-work.md`](next-compound-ideas-from-the-engine-performance-work.md)
does the same for the engine-performance work. Same instruction, same rule - rank both together at the
end rather than separately.

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

**11. In a fan-out, simplify by dimension — and the target is divergence, not duplication.** The
obvious structure for a cleanup pass is one agent per module, and it is the wrong one: a per-module
agent cannot see that its port-line scanner is the seventh copy, because seeing that requires reading
the other six. But handing one agent all N implementations is expensive and beyond what a single
context holds well. **Parallelise by question instead of by module** — one agent per dimension (how
each client spawns the sidecar, how each declares capabilities, how each implements the queue rules,
what each does on close), reading a narrow slice across all N. Cross-cutting by design, still
parallel, still cheap.

The reframe underneath it: N implementations of the same thing is *expected* in a fan-out, so
duplication is the wrong thing to hunt. **Divergence is** — N clients doing one thing N different
ways, where only one way is right. Most of it cannot be deduplicated anyway, since the languages
differ; what is being harvested is consistency of design, naming and semantics.

Two consequences: **order the cross-cutting pass first**, because per-module cleanups otherwise
optimise toward N local shapes and drift further apart, so the divergence is paid for twice; and note
that any sub-family sharing a runtime (here the JVM clients) is the exception where real code sharing
is possible, which is worth checking explicitly rather than assuming the languages are all equally
separate.

**12. Make a checker's coverage exhaustive, so an unknown case fails instead of being skipped.** The
copyright checker read `.java` and silently ignored everything else, so seven new languages, every
workflow, and eleven upstream-derived files modified without their attribution notice were all
"passing". The rewrite classifies **every tracked file** into enforced-types or exempt-paths, and
**anything matching neither is a violation** naming the file and both tables. The difference matters
most at exactly the moment a project grows: an extension list rots the instant someone adds a
language quietly, while an exhaustive classifier fails the build until a human decides which set the
new thing belongs to. Pairs with a self-test that **generates** its red and green case per entry by
reading the checker's own table — a hand-maintained list of cases drifts in the one direction nobody
notices, toward fewer cases than the checker has branches.

Corollary found the same day: **a check can be present, run, and still be testing the wrong thing.**
The old header check asked whether a window of text contained a word, so two files that *explained
the copyright policy in prose* read as claiming that copyright. Wrong-shaped checks are harder to
find than absent ones, because their green is indistinguishable.

**13. Seed reviewers with the strategy's known risks, not just the diff.** A reviewer given a diff
finds defects in the diff. A reviewer told *what this way of working tends to get wrong* also checks
whether the approach itself is going astray — and in a fan-out that is where the expensive mistakes
live. Concrete example from this project: "make sure the multi-language clients' tests are testing
the right things and not overlapping more than they need to with the core tests" is a risk no
line-by-line review would surface, because every individual test looks reasonable; only someone
holding the strategy can see the duplication building.

So a review dispatch should carry the identified risks of the *method* alongside the scope of the
work: what layer each kind of test belongs in, which duplication is deliberate mirroring and which is
drift, which decisions are settled and must not be re-litigated. Cheap to add to a prompt, and it
changes what comes back — the same reviewers then report on the shape of the work rather than only
its contents.

**14. A test named for a property, that cannot detect that property, is worse than no test.** It
stops anyone looking. Three instances in one session: Rust shipped an overflow test that passed
against the very defect it was named for, because it overflowed with one oversized wave — which the
wrong bound also rejects. Two conformance scenarios stayed green while deliberately sabotaged: one
because the runner exited so fast it killed its own bad report in flight, the other because the
engine dispatched both shards in a single wave regardless, making the sabotage invisible. None was
found by reading; all three were found by *doing* the red-then-green step (idea 6). The compounding
move is to treat "prove it red" as the moment you discover what your test actually asserts, not as
paperwork after writing it.

**15. Mirror the shape the existing API already chose, not the shape the first caller needed.** The
clients modelled one-record-in-one-outcome-out, because that is what the first client wanted. Core's
own API is batch-shaped — it hands the user a context of records, and a batch size of one yields a
context of one — so single-record was always the degenerate case of an API that already existed. The
cost of getting this backwards is paid N times: adding batching now changes the user-facing signature
in every language. Before fanning out an interface, check whether the thing being wrapped already
generalised it.

**16. A whole-tree gate blocks every agent, and that is a feature.** The shared pre-commit hook
validates the entire tree, so while any agent left work half-done every other agent was blocked. The
tempting reading is that this is a hazard of parallelism; the owner's correction was better — it is a
signal that real work is missing, and the response is to fix your own violations and retry, never to
bypass and never to fix someone else's files to get past it. Convergence then happens on its own,
usually within minutes. What makes this work is that the failures name their owner, so "mine or
theirs" is a cheap question.

**17. Externalise decisions into the repo before the context that holds them is gone.** Most of a
long session's value is in decisions that were never code: why a shape was chosen, what was rejected,
what a gate is really protecting. Written into notes *as they are made*, a fresh agent inherits them
by reading; carried in a context window, they die with it. The test is whether the next step could be
executed by someone who was not here — and if not, the missing piece gets written down before
anything else. It also makes handoff timing a non-question, which is the real payoff.

**18. A fan-out doubles as a design review of the API it mirrors.** Each language's idioms
interrogate the shared surface: no-exception languages test whether outcomes are really values,
single-threaded runtimes test whether the concurrency model is really the engine's, forked-process
runtimes test whether the client is really stateless. Questions the reference language cannot ask
itself.
