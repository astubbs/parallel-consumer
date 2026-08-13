---
title: "A mechanical reference sweep rewrote a quoted log line - verbatim artifacts are the fourth class no exemption protects"
date: 2026-08-07
category: workflow-issues
module: tooling
problem_type: workflow_issue
component: development_workflow
severity: medium
applies_when:
  - Running any mechanical sweep that rewrites pattern matches across tracked files
  - Editing docs that quote log output, error text or a transcript verbatim
  - Making a long-lived branch pass the issue-reference gate after merging master up
  - Reading a script's substitution count as evidence the rewrite was correct
  - "Choosing between a code-span escape and the `issue-refs: N/A` opt-out"
  - Deleting an in-flight ledger because the work it tracked is finished
related_components:
  - documentation
  - tooling
tags:
  - issue-ref-gate
  - mechanical-sweep
  - verbatim-quotes
  - code-spans
  - false-success
  - diff-review
  - fork-issue-numbers
---

# A mechanical reference sweep rewrote a quoted log line - verbatim artifacts are the fourth class no exemption protects

## Context

This fork enforces a house convention through CI: any `#NNN` below `QUALIFY_BELOW` on a
branch-**added** line must name its repo - `astubbs#119` for this fork, `confluentinc#857` for the
original. The threshold is `1000` (`.github/scripts/issue-ref-gate.js:24`), because the fork's issue
numbers sit entirely inside upstream's range, so a bare number is a coin flip rather than a broken
link. `AGENTS.md` records the measurement that settled it: of 51 numbers cited across one PR's files,
48 existed in both repos meaning different things.

The gate is textual and deliberately narrow:

- it looks only at added lines of a patch, one line at a time (`issue-ref-gate.js:97-99`);
- it skips whole files listed in `EXEMPT_PATHS` (`issue-ref-gate.js:27-36`, checked at `:96`);
- it strips everything already unambiguous before matching - markdown links, HTML anchors, asciidoc
  link macros, bare URLs, **backtick code spans**, and the owner-qualified forms
  (`issue-ref-gate.js:56-85`, code spans specifically at `:76`);
- what survives is matched by `/(?<![\w\/#])#(\d+)\b/g` (`issue-ref-gate.js:102`) and flagged when
  `n < limit` (`:104`).

`bin/check-issue-refs.sh` is the local mirror and *calls the same module* rather than reimplementing
it, so the local answer cannot drift from CI's. Its own header says "NO SECOND COPY OF THE RULE".

The situation that produced this learning: PR astubbs#29 (the confluentinc#857 deadlock fix) is a
long-lived branch that predates the gate. Merging master up brought the gate in, and the branch
failed it with 28 bare references on lines it adds. The fix was a one-off Node script that rewrote
only branch-added lines and - correctly - reused the gate's own exported `stripQualified()` so the
sweep and CI agreed on what "unqualified" means.

27 of the 28 were genuine references and were rewritten correctly. **One was not a reference at
all.** Inside `docs/BUG_857_INVESTIGATION.md`, a fenced code block quoted a real diagnostic log line
emitted during the stall:

    #857-poll: runState=RUNNING, pausedForThrottling=false, assignment=0

The script rewrote it to `confluentinc#857-poll: ...` - a string no program has ever emitted. The
`#857-poll` tag was temporary instrumentation that has since been removed from the code
(`grep -rn "857-poll" --include="*.java"` returns nothing at the current tree), so that quotation
was the **only surviving record** of what the instrumentation printed. The falsification was caught
by reading the resulting diff line by line, not by any check - the script's own output was a
cheerful count of successful substitutions.

## Guidance

**1. Before a mechanical text sweep touches a line, classify what the line *is*, not just what it
matches.** The prior ledger for this exact task already said "classify each before rewriting it".
This case adds the class it missed: a line can be *text a program produced* rather than *text a
human wrote*. Log output, stack traces, error messages, CLI transcripts, captured HTTP bodies,
golden-file fixtures - anything reproduced because it is what something actually said. These are
**verbatim quoted artifacts**, and editing one does not improve it, it forges it. A qualified
reference is more informative than a bare one; a "corrected" log line is *less* informative than the
real one, because it is false.

**2. For this gate specifically, the escape hatch is an inline code span, not the opt-out.**
`stripQualified` removes backtick spans in *any* file (`issue-ref-gate.js:76`) - it is not gated by
`isExempt`, which is a separate per-file check at `:96`. So moving a quoted artifact from a fenced
block to an inline span both silences the gate and keeps the text byte-identical. This is the
intended mechanism, and it is already load-bearing elsewhere: the retired ledger noted that the
`AGENTS.md` convention-shown-by-example survives the gate *only* because it sits in backtick spans,
with no file exemption behind it.

Do **not** reach for `issue-refs: N/A - <reason>` in the PR body (`issue-ref-gate.js:45`). That
opt-out exists for a flagged reference that genuinely needs no qualifier; using it to turn a red
gate green while leaving corrupted text in place is exactly the gate-gaming this repo's rules
forbid. It also only works in CI - `bin/check-issue-refs.sh` does not read the PR body and will keep
failing locally.

**3. When you silence a gate on a quoted artifact, pay the information back in prose.** The gate
fired for a real reason: a reader seeing `#857` cannot tell which repo it means. Suppressing the
check does not answer that question - the surrounding prose has to.

**4. A script's success line is not verification. Read the diff.** "27 reference(s) qualified across
10 file(s)" was useless twice over. It counts substitutions, and every falsification is also a
substitution. It was also *wrong*: the gate finds 28 references on those lines, and the script was
counting changed lines rather than references - the `#188/#189` line carries two. Nobody noticed,
because a count nobody can check against anything is not evidence of anything. The only check that
could have caught the falsification was a human reading each changed line and asking "is this
sentence still true?"

**5. Fenced code blocks give a mechanical sweep no protection at all - do not assume otherwise.**
The gate has no fence tracking whatsoever; it iterates patch lines individually at `:97`. A patch is
a list of lines, and a fence opener three lines up is invisible. Any tool that consumes diffs shares
this blindness. If a quoted artifact must survive a sweep, an **inline** span is the protection; a
fence is not.

**6. Retired ledgers take their warnings with them - promote the warning before you delete the
ledger.** The knowledge that an earlier sweep - "77 refs", in the ledger's own count, which is a
count of distinct numbers rather than of rewrites - got three classes wrong lived only in
`docs/inflight/next-qualify-remaining-refs.md`, and that file was deleted by the commit that
finished the sweep (`docs: qualify every issue reference tree-wide`, PR astubbs#219) on the entirely
reasonable grounds that in-flight files do not outlive their work. The *task* was finished; the
*lesson* was not. It is now unfindable by any grep of the working tree. **When closing out an
in-flight ledger, move any trap it documents into a durable location first.** The three classes are
restated below so this document does not repeat the mistake.

**7. Watch for references the gate structurally cannot see - green is not the same as unambiguous.**
The lookbehind `(?<![\w\/#])` at `issue-ref-gate.js:102` excludes any `#` preceded by a word
character, a `/`, or another `#`. The exclusion exists to avoid matching URL paths and
`owner/repo#N`, and the consequence is that a reference written `#188/#189`, `PR#270` or `GH#193` is
invisible to the gate *permanently* - before qualification and after.

Note which half of that class is larger. `\w` catches far more than `/` does, and it is the half a
naive grep for the `/` shape will never surface. Search the mechanism as the regex actually defines
it, in PCRE rather than ERE: `grep -rnP '[A-Za-z0-9_.-]#[0-9]{1,3}'` alongside
`grep -rnP '/#[0-9]{1,3}'`. Read the line, not the exit code.

## Why This Matters

A wrong reference that resolves is worse than a broken one, because nothing looks amiss - that is
the gate's own founding argument (`issue-ref-gate.js:8-12`). A falsified quotation is the same
failure one level up. A broken link announces itself; a plausible-looking log line that no program
ever emitted does not, and a future reader will debug against it, grep for it, or cite it.

The specific loss here was total and silent. `#857-poll` was temporary instrumentation, since
removed from the source. Once the quotation was rewritten, *nothing anywhere* recorded what the
instrumentation actually printed - not the code, not the logs, not the doc. The corruption would
have been permanent, and it would have looked exactly like a diligently-maintained investigation
note.

The class generalises well beyond this gate. Every mechanical text sweep - a rename, a lint autofix
over comments and docs, a link rewrite, a terminology pass, a spelling correction - runs the same
risk the moment it touches a file that quotes something. The durable form of the rule is "do not
edit text that is quoted because it is what a program said"; issue-reference qualification is only
the instance that caught us.

And the meta-point compounds: this is the **fourth** failure class found by mechanical sweeps
against this one gate. The first three were found once, written into a ledger, and then deleted with
it.

## When to Apply

Apply this before running **any** script that rewrites text across a repository - not only
issue-reference sweeps. Specifically:

- **Before writing the sweep**: enumerate the file types in scope and ask which of them quote
  external output. `docs/` investigation notes, `CHANGELOG` entries, README troubleshooting
  sections, test fixtures and golden files, and issue/PR mirrors are the usual carriers.
- **When a long-lived branch merges master and inherits a gate it predates.** This is the archetypal
  trigger: the branch is large, the violation count is high enough that hand-editing feels wasteful,
  and a script feels proportionate. That is precisely when the diff stops being read.
- **When a gate fires on something you are about to conclude "isn't really a reference".** That
  conclusion is right often enough to be dangerous. It has two correct endings - move it into a code
  span, or exempt the file - and one wrong one, which is rewriting it anyway so the gate stops
  complaining.
- **Before deleting an in-flight ledger** because its work is done. Ask what it knows that the tree
  will not know without it.
- **When reviewing a sweep PR.** The review question is not "are the substitutions correct?" but "is
  every changed line still a true statement?" These differ exactly on quoted artifacts.

It does *not* apply to ordinary prose references, which is the whole point: 27 of the 28 hits in
this sweep were fine, and mechanical rewriting was the right tool for them. The guidance is about
the reading pass afterwards, not about abandoning automation.

## Examples

### The falsification, and the fix

Before the sweep - a fenced block in `docs/BUG_857_INVESTIGATION.md` quoting real output:

    Diagnostic logging in the poll loop during the aggressive test stall:
    ```
    #857-poll: runState=RUNNING, pausedForThrottling=false, assignment=0
    ```

What the script produced - the gate is satisfied, the artifact is a forgery:

    confluentinc#857-poll: runState=RUNNING, pausedForThrottling=false, assignment=0

What was committed instead - text restored byte-for-byte, moved to an inline span, and the reference
the gate wanted supplied by the prose around it:

    Diagnostic logging in the poll loop during the aggressive test stall, tagged `#857-poll` after
    confluentinc#857 (a temporary marker; it is no longer in the code):

    `#857-poll: runState=RUNNING, pausedForThrottling=false, assignment=0`

Verified against the gate at the current tree - fenced is flagged and would be rewritten, inline is
stripped at `issue-ref-gate.js:76`:

    "#857-poll: runState=RUNNING, ..."      => ["#857"]
    "`#857-poll: runState=RUNNING, ...`"    => []

### The reference the gate cannot see

`MultiInstanceRebalanceTest.java` carried `Originally created for #188/#189, ...`. Running the
gate's own matcher over that line:

    "Originally created for #188/#189, re-enabled for #857 investigation."  => ["#188", "#857"]

`#189` is **absent**. This is not, as first believed, a case of qualifying `#188` and thereby hiding
its neighbour: `#189` was never visible to the gate at all, and qualifying `#188` would have left a
permanently gate-invisible ambiguous reference. It was reworded by hand.

**Other live instances of this defect class in the tree.** The `/#` shape gives seven hits, four of
them in non-exempt files. Searching the *whole* lookbehind rather than just its `/` term adds two
more that no `/#` grep would ever surface - which is the point of grepping the mechanism as the
regex defines it:

| File:line | Reference | Found by |
|---|---|---|
| `docs/inflight/bug-857-family.md` | `astubbs#29/#31` | `/#` |
| `docs/plans/2026-07-30-002-feat-chaos-pain-suite-phase1-plan.md` | `astubbs#29/#80` | `/#` |
| `docs/plans/2026-07-31-001-feat-chaos-w4-cooperative-variant-plan.md` | `confluentinc#857/#29` | `/#` |
| `docs/solutions/test-flakiness/parallel-integration-tests-flaky-under-concurrency-2026-07-28.md` | `confluentinc#857/#29` | `/#` |
| `parallel-consumer-core/src/main/java/bz/stub/parallelconsumer/state/ProcessingShard.java:40` | `This is addressed in PR#270.` | `\w#` |
| `parallel-consumer-core/src/test/java/bz/stub/parallelconsumer/ParallelEoSStreamProcessorPauseResumeTest.java:23` | `(see {@code GH#193})` | `\w#` |

All six return `[]` from the gate's own matcher and all six sit in non-exempt files. Ruled out as
correct-by-exemption: `src/docs/development/upstream-map.yaml`, `upstream-pr-analysis.adoc` and
`CHANGELOG.adoc` (which carries `PR#530`) all have the shape but are in `EXEMPT_PATHS`.

### The three prior classes, restated so they are not lost again

From the retired `docs/inflight/next-qualify-remaining-refs.md`. Recover it with
`git log --all -- docs/inflight/next-qualify-remaining-refs.md`, then
`git show <sha>^:<path>`. Its own words: *"A previous mechanical sweep rewrote 77 refs and got three
classes wrong"*:

| Class | Why rewriting it was wrong | What protects it now |
|---|---|---|
| `CHANGELOG.adoc` entries below 0.6.0.0 | its header already declares those numbers upstream | file exemption, `issue-ref-gate.js:29` |
| every number in `upstream-map.yaml` | upstream by construction | file exemption, `issue-ref-gate.js:31` |
| an `AGENTS.md` convention shown **by example** | rewriting the example changed the documented rule | **nothing but backtick code spans** (`:76`) |

The ledger's own emphasis is worth preserving verbatim: *"So when you rewrite prose that shows a
reference by example, check it is in a code span; nothing else will save it."*

**Verbatim quoted artifacts are the fourth class**, and like the third they have no file exemption -
a code span is the only thing standing between them and the next sweep.

One caution on that recovered text: `EXEMPT_PATHS` has four entries, not two. The other two are
`upstream-pr-analysis.adoc` (`:33`) and the gate's own `issue-ref-gate.test.js` (`:35`), exempt for
different reasons again - "exempt" and "a class a sweep got wrong" are not the same set.

**And the ledger's own recommended command carries one of its own traps**, which is the sharpest
argument in this document. It tells the next author to find candidates with
`git grep -nE '\bupstream (PR |issue )?#[0-9]+'` - but `\b` is not POSIX ERE, so that command
silently returns zero and reads as "nothing left to do". This reproduces: an audit of *this*
document hit it again, running a `\b` grep, getting nothing, and having to redo it with `-P`. A
warning written by someone who had just been bitten still shipped the bite.

### Classes an earlier sweep had to hand-treat (session history)

The tree-wide sweep that preceded this one hit several more classes that never made it into a
durable doc. Recorded here because they are the same question in different clothes:

- **Numbers that are not references at all** - author ordinals annotating log excerpts ("run #1",
  "produce #1/#2", "NUDGE #1/#2") were qualified by the mechanical pass, then reworded to plain
  numbers so the shape could not recur.
- **A fixture whose meaning is its bareness** - the changelog gate's test fixture asserts that a
  *bare* `#NN` is not a citation. Qualifying it would have destroyed the thing it tests, so it was
  moved above the ambiguity threshold as an obviously-fake number instead. This is the closest
  sibling in the tree to the log-line corruption, and unlike the rest of this list it is durable
  evidence rather than session recollection.
- **Load-bearing string literals a program consumes** - `@Tag("#355")` in Java tests is a *test
  selector*, not prose. The author grepped `pom.xml`, `*.yml` and `*.sh` to confirm nothing selected
  on the tag before renaming it. This is the nearest precedent to the log-line corruption: the
  question "is this string consumed or emitted by a program?" was asked once, for `@Tag`, and never
  generalised into a rule.
- **Closing keywords must stay bare** - `Fixes astubbs#167` closes nothing, silently, because
  `astubbs#167` is not valid GitHub cross-reference syntax.
- **Files CI itself validates** - qualifying comments inside `.github/workflows/claude-code-review.yml`
  tripped the action's own workflow-modification guard, permanently blocking a required check.
- **Regex-engine traps that produce silent zeros** - `git grep -E '\bupstream'` matched nothing
  because `\b` is not POSIX ERE. Nearly accepted as "zero occurrences" before re-testing with `-P`
  found 31 across 14 files. Verify a pattern works before trusting a zero.

### Hazards in the sweep script itself, for whoever writes the next one

The script did the important thing right - it required the gate module and called
`gate.stripQualified` and `gate.isExempt`, so it and CI agreed on "unqualified". Three things it got
wrong or risky:

1. **A guard that silently did nothing.** It called `gate.NOT_A_REF_TEST(line)` behind an existence
   check. `NOT_A_REF` is *not exported*, so the guard was permanently `undefined` and the gate's
   own false-positive filter (javadoc `{@link #close()}`, method refs like `#poll(`,
   `issue-ref-gate.js:39-43`) never applied. Checked for damage in this sweep's diff: **none** - no
   added line matched a `NOT_A_REF` pattern. But it would have failed silently the day one did. If
   you reuse an internal from another module, assert it exists rather than testing for it.
2. **The threshold was re-typed as a literal `1000`** instead of using the exported
   `gate.QUALIFY_BELOW`. The gate's own header calls the threshold "an assumption with a deadline" -
   it is expected to move. Import the constant.
3. **Added lines were identified by content, not position.** Any identical line elsewhere in the
   same file is rewritten too, whether or not the branch touched it. Harmless here; not harmless in
   a file with repeated boilerplate.

## Related

- `docs/solutions/workflow-issues/compound-tooling-breaks-in-worktrees-and-forks-2026-08-07.md` -
  sibling case of a tool reporting success while producing a wrong result.
- `docs/solutions/workflow-issues/copyright-header-rules-for-fork-2026-04-21.md` - same root cause
  in a different guise: an automated pass modifying a verbatim, externally-owned string as a side
  effect of unrelated work.
- `AGENTS.md`, "Issue references" (lines 384-504; the relevant paragraph is 414-421) - already
  carries this principle scoped to one artifact type: *"Leave a quoted upstream title intact and
  append the number instead"*, at 418-419, immediately followed by *"This is style, not
  enforcement"*. That is why a sweep facing a fenced log line had no rule to consult. Worth
  generalising from quoted titles to all verbatim quoted artifacts, and worth promoting out of
  "style".
- `docs/inflight/next-qualify-remaining-refs.md` - **deleted** at PR astubbs#219; its three-class
  taxonomy is reproduced above.
