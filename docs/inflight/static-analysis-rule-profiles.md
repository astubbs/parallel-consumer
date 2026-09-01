# Two analysis profiles: full rules on new code, a small enforced set on all of it

<!-- inflight-type: register -->
<!-- inflight-impact: ci -->

**A rule switched off because legacy code trips it is switched off for new code too.** That is the
flaw in a single global rule set, and it is what the per-rule registries were quietly paying: rules
like `USBR_UNNECESSARY_STORE_BEFORE_RETURN` or `CT_CONSTRUCTOR_THROW` are off not because they are
wrong, but because the existing tree violates them. Every line written since then has been unguarded
by a rule nobody actually disagreed with.

Two profiles fix that.

| Profile | Scope | Contents | A failure means |
|---|---|---|---|
| **old** | whole tree, every build | only rules that are clean today, plus rules genuinely WRONG for this codebase | you broke something everywhere |
| **new** | the PR diff | the full rule set of every engine | the code you just wrote violates a rule the legacy tree also violates |

The registries keep their job and lose most of their entries. What stays in a registry is the
genuinely-wrong set - `EI_EXPOSE_REP` against this library's DI composition, `PREDICTABLE_RANDOM` in
tests, `SLF4J_FORMAT_SHOULD_BE_CONST` against deliberately computed log formats. What leaves is
everything currently off only because old code trips it, which moves to profile **new** and starts
being enforced on the code being written now.

## Why this is not the baseline we just deleted

It is the same *idea* as the SpotBugs baseline, and the objection to that baseline was never
ratcheting. It was that the mechanism was invisible: regenerated on every push to master, keyed on
class plus method plus bug type, recording nothing about what it swallowed, and defeated wholesale by
the package rename because every key changed at once. See
[`static-spotbugs-latent-findings.md`](static-spotbugs-latent-findings.md).

The difference is that a profile is **declared**, not accumulated. Profile old is a written list.
Profile new is "everything else", computed fresh each run against the diff, with nothing stored and
nothing to go stale.

The repo already ratchets this way three times, so this is a fourth instance of an established
pattern rather than a new idea: `newFindings(current, base)` in `.github/scripts/file-ref-gate.js`,
whose header states the principle outright - *a red result on a branch means that branch broke
something, not that it inherited something* - plus the duplicate-detection lane's `compare_with_base`
and the PIT lane, which already scopes to classes changed against the PR base.

## One gate, not one per engine

Every engine here can emit findings with a file and a line: SpotBugs in XML, javac and Error Prone as
warnings, `forbidden-apis` in its report, ShellCheck in JSON. So the diff filter belongs in **one**
module that all of them feed, not five per-engine ratchets that drift apart. That is the same
argument `file-ref-gate.js` makes for being a sibling module sharing a shape rather than a rule
bolted into its neighbour.

For the compile-time engines this needs **no second build**: run every check at *warning*, capture
the findings, and fail on the ones inside the diff. Only a base-versus-head comparison would need
building twice, and the diff filter makes that unnecessary.

## Three things that will bite, stated before they do

- **Moved code reads as new.** A refactor that relocates existing code lights up its findings as if
  they were written today. Unavoidable with diff scoping. Expect it during the God-class
  decomposition specifically, which is the largest planned move in the tree.
- **Some findings have no line.** Class-level findings from SpotBugs and Error Prone cannot be
  diff-scoped at all. They belong in profile old or nowhere; there is no third option.
- **Line scoping misses findings reported away from the edit.** A change on line 40 can cause a
  finding reported at line 300. This is the reason to want file scoping, and the reason it is
  blocked below.

## Scoping granularity: lines now, files later, piecemeal

Profile new is scoped to changed **lines**, and that is a size problem rather than a design
preference. `AbstractParallelEoSStreamProcessor` is 1533 lines; scoping by file would mean any PR
touching it inherits every latent finding in it, which is a red build for reasons the author did not
cause - the exact failure this whole scheme exists to avoid.

**File scoping is strictly better and the trigger is file size, so it can be taken one file at a
time.** As each file comes down to a reviewable size it can be promoted to file scoping on its own,
without waiting for the decomposition to finish. `docs/refactoring.md`, under "Decompose the God
class", carries this as a consequence of that work.

Promotion list - a file moves here once it is small enough that inheriting its findings is
reasonable:

| File | Status |
|---|---|
| *(none yet)* | The first promotions come out of the God-class decomposition. |

## Every tool's registry carries a ranked "turn these on next"

A flat list of disabled rules with triggers answers *when*, and nobody ever reads it as a work order.
So each registry also carries a **top 5 to turn back on whole-tree, ranked**, with a stated criterion
rather than a vibe: **value divided by cost**, where value is how close the rule sits to a defect
class this repo has actually paid for, and cost is sites plus judgement needed.

The ranking is a claim, so it is written to be argued with. Each list also says what was
**deliberately left out and why** - usually "largest and least valuable" or "gated on other work, so
effort now is effort twice."

Underneath it, the same registry groups its entries by **what clearing them takes** rather than when:
mechanical with no judgement, read-the-sites-then-decide, blocked on other work, or permanent. That
is the difference between an afternoon and a design decision, and it is not derivable from a count.

Current lists: [`static-spotbugs-rule-registry.md`](static-spotbugs-rule-registry.md) (5),
[`static-error-prone-rule-registry.md`](static-error-prone-rule-registry.md) (3),
[`static-shell-lint-severity-tiers.md`](static-shell-lint-severity-tiers.md) (5),
[`static-forbidden-apis-parallelstream.md`](static-forbidden-apis-parallelstream.md) (1).

**The length of the list is not five.** It is however many entries are genuinely worth doing next:
forbidden-apis has one because it arrived with eight findings and all eight were fixed, and Error
Prone has three because below them the list is javadoc and formatting. Padding a ranking with things
nobody should do next is what makes rankings ignored.

**A worked case for why the ranking is not just triage.** fb-contrib's `RU_INVOKE_RUN` and Error
Prone's `DoNotCall` both landed on `ReactorTest` calling `new Thread(...).run()`, which
`docs/test-hardening/inactive-tests-audit-2026-08-08.md` had already examined and ruled a defensible
library scratchpad. Two independent engines disagreed with a human audit, the defect was real, and it
is now fixed - so both exclusions are deleted rather than tiered. A manual audit reading for one
question ("does this test assert anything?") will not see a second one ("does this thread ever
start?"). That is the argument for running the engines at full strength somewhere, which is what the
new profile is for.

**Any registry added later carries the same two sections.** A tool whose disabled set has no ranked
next-five is a tool whose backlog nobody will ever start.

## Rules every rule registry follows

**This section is the owner.** Each tool's registry restates only what is specific to that tool and
points here for the rest - the SpotBugs and Error Prone registries had near-identical copies of these
three, which is how two lists of the same contract start disagreeing about it.

- **Off requires an entry.** A rule switched off without a row in its registry is the failure the
  registries exist to prevent - it is indistinguishable from a rule that never fired.
- **An entry needs a trigger, not a date.** "When the options rework happens" is a trigger. "Later"
  is not: nothing ever arrives to make "later" true, so the entry becomes permanent by default rather
  than by decision.
- **The off set only shrinks.** Promoting a rule to the enforced tier deletes its row, and a registry
  is deleted outright once nothing is left switched off. A registry that grows is a registry
  recording a retreat.

## What is done, and what is not

**The classification is done. The wiring is not, deliberately.**

Every suppressed rule in every registry now carries a `profile:` marker - `old` or `new`. Nothing
about enforcement changed: the engines still run whole-tree with the same suppressions, so this PR's
behaviour is identical with and without the markers. What the markers buy is that the split is
recorded **while somebody has the context to make it**, which is the perishable half. Deciding that
`EI_EXPOSE_REP` is wrong-for-this-codebase while `CT_CONSTRUCTOR_THROW` is merely blocked-by-legacy
takes knowing why each was switched off; wiring a diff filter does not.

Reading the markers:

- `profile: old` - stays off **everywhere, permanently**. The rule is wrong for this codebase, not
  merely inconvenient. These are the registry's real content.
- `profile: new` - off only because the existing tree trips it. **Belongs on for new code** and will
  be, the day the gate exists. Every one of these is currently unguarded on code being written today,
  which is the cost this scheme is paying until the follow-up lands.

**The gate is a separate PR** and is not in scope here. Building it means one filter module, a report
path per engine, and each engine switched to non-failing so the gate owns the verdict. Until then
`profile: new` is an intention with a written list behind it rather than an enforced thing, and this
note is the tracking.

**The clearest single example of what is being lost meanwhile:** `forbidden-apis` cannot ban
`parallelStream()` because three legacy call sites exist. Under profile new it would be banned on new
code from the first day, with those three untouched -
[`static-forbidden-apis-parallelstream.md`](static-forbidden-apis-parallelstream.md) has the
signature ready. That entry stops being blocked work and becomes a wiring task.
