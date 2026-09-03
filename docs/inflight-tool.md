# `bin/inflight.mjs` - worked examples

The front door for querying this repository across **every ref**, rather than the working tree. Run
`node bin/inflight.mjs help` for the command list and `help <command>` for one command's usage; this
document is the part help text cannot carry - what the answers look like on real questions, and why
the working-tree version of each answer is wrong.

**The measurement the whole tool rests on:** 580 of the 901 documents under `docs/` exist ONLY on
branches that have not merged, and 570 in-flight note paths exist across the refs against 165 on
`origin/master`. A working-tree `grep` therefore answers a narrower question than the one asked, and
returns a false negative carrying the authority of a completed check.

## Finding out whether a failing test is already known

The case that produced this document. `PartitionStateCommittedOffsetIT.committedOffsetRemoved[1]`
failed in CI on a PR that changed no Java at all.

```
node bin/inflight.mjs prior-art --headings committedOffsetRemoved
```

Three hits, and the third is the one that matters:

| Where | What it says | On master? |
|---|---|---|
| `docs/plans/2026-08-05-001-investigate-committedoffset-latest-reflake.md` | it re-flaked after astubbs#80 un-quarantined it | yes |
| `docs/solutions/test-flakiness/latest-reset-nudge-race-committedoffsetremoved-2026-07-30.md` | a *previous* instance, SOLVED - an offset-reset race in the test harness | yes |
| `docs/inflight/test-load-tightness-flakes.md` | live entries dated 2026-09-01 tracking **this exact test and parameter**, still re-flaking | **no - 16 refs** |

A working-tree search finds the first two and concludes the flake is fixed. The ledger saying it is
*actively re-flaking* is on sixteen branches and none of them is master. That is the failure mode the
tool exists for, caught on its own pull request.

**`--headings` is why this is readable.** The same term unscoped returns thousands of body-text hits;
scoped to headings it returns what documents are *about*. Reach for it first on a broad term.

## Before editing a note several branches share

```
node bin/inflight.mjs note drift docs/inflight/bug-857-family.md
```

Reports only what is **divergent** - versions carrying content the baseline has never held - and
names each branch by its PR title, else the title of a note it carries that the baseline does not,
else its own name. Sizes are against each branch's merge-base, so the number says what that branch
*added*, not how far master has moved since.

The filtering is most of the value: for that note, 198 of the 274 carrying refs are merely behind.
Reporting them would bury the two dozen that actually differ.

## Reading a document as the corpus holds it, not as your checkout does

```
node bin/inflight.mjs docs show docs/inflight/bug-857-family.md
```

The file in your working tree is *one* version of that note. On the fork's most-shared note there
are two dozen others on live branches, each carrying content the baseline has never held, and
`cat` shows none of that: it shows the copy your branch happens to hold, and says nothing about
whether that copy is the baseline's, your branch's own edit, or one of the two dozen. That is the
incident the divergence header exists for - a session edited a stale copy of a note and every
working-tree read answered for that copy without saying so.

The read-time hook prints one line about this whenever a file under `docs/inflight/`,
`docs/solutions/` or `docs/plans/` is read. `docs show` is the same query at full size, and the
page has three parts, in the order an agent needs them:

```
docs/inflight/bug-857-family.md from origin/master - the baseline
docs context: divergence header for docs/inflight/bug-857-family.md
=== divergence: docs/inflight/bug-857-family.md ===
  25 divergent versions on 131 live refs carry content origin/master has NEVER held; 614 refs searched (561 live, 53 archival)
  this copy is the baseline's version (origin/master)
  preserved, not in flight: 1 version held only by tag refs - backup/pr57-pre-split
  largest 3 versions, by what each added:
    +600 -9        experiment/857-deadlock-control-arm-do-not-merge, origin/experiment/857-deadlock-control-arm-do-not-merge
        adds: "## Commit mode decides which defect can explain a sighting", ...
    ...
  the rest: bin/inflight.mjs note drift docs/inflight/bug-857-family.md
more: bin/inflight.mjs docs show docs/inflight/bug-857-family.md --ref experiment/857-deadlock-control-arm-do-not-merge

--- docs/inflight/bug-857-family.md @ origin/master ---
# Bug 857 family
...
```

**The first line names the ref shown, and the choice is a rule, not a guess.** The baseline when it
carries the path; otherwise the first live ref carrying it, in sorted order, so two agents asking on
two machines get the same copy. `--ref <ref>` picks any other, and the header then describes *that*
copy's state - the baseline's version, that branch's own edit, or branch-only.

**Divergence is the only claim the header makes.** It never says a version is newer. It says how many
distinct versions exist on live refs, which branches and pull requests carry the largest, how much
each added against its own merge-base, and *what* - the headings it added, or its first added line
when it added no heading. Evidence, ordered by size, and the command for the rest.

**Archival refs are searched and reported, never shown by default.** A version held only by a tag
or a `refs/backup` ref is *preserved*: that is where this repository parks work before a re-cut, and
serving it as the document would present history as the live copy. The header names it by ref kind;
`--ref` reaches it when that is what you want.

**An empty answer says what it covered.** A path on no ref prints the ref set it searched - size, and
the live-versus-archival split - because "on none of 614 refs" is a result and a blank line is not. A
path outside the three corpus areas says so and claims nothing, since the query is not defined there.

```
node bin/inflight.mjs docs header docs/inflight/bug-857-family.md
```

The header alone - byte for byte what `docs show --header-only` prints. It is the "more" command the
read-time hook names under its one-line summary, and the pull form for an agent on a host without
hooks: run it before acting on a document, and you have seen what the hook would have shown.

## Finding work that will be lost if nobody acts

```
node bin/inflight.mjs stranded
```

Notes that exist on a branch and have **never** reached the baseline, clustered by the set of refs
carrying them - one workstream's notes share their refs, so listing them per path buries the finding
under its own volume.

Three filters run first, and the middle one was expected to do most of the work and did almost none:
a note present on the baseline now; a note whose blob lives there under another name (a rename,
proven exactly); and a note the baseline's history once held, which landed and was `git rm`d when its
work closed. What survives is genuinely unlanded.

## Locating a note you can only half-name

```
node bin/inflight.mjs note find 857
```

Substring match over every note path that exists on any ref - including the ones that never reached
master, which is most of them. Use it to get the path that `note drift` wants.

## Deciding whether now is the moment to break up an oversized class

```
node bin/inflight.mjs refactor-window
```

`docs/refactoring.md` says its entries are to be picked up "when things are quiet", and until this
command nothing could evaluate that. The entries aged instead: its `AbstractParallelEoSStreamProcessor`
entry records the class at 1533 lines, and the command reports what it is now.

<!-- issue-refs: exempt-begin - verbatim command output; the tool prints the bare number GitHub gives it -->

```
  nearest to workable: work-manager (1.9x over)
  furthest away:       work-container (3.5x over)

  BUSY    abstract-parallel-eos-stream-processor   largest +1047, threshold 480 - 2.2x over
          on origin/feats/hasten-micro-mvp - PR #392 (OPEN)
          land that branch first, or wait
          2406 lines, up 872 over the last 180 days
          live refs carrying it under none of its 2 configured paths: 28
          live refs carrying it that could not be measured (no merge-base with the baseline?): 1
```

<!-- issue-refs: exempt-end -->

Four things in that output are the whole design.

**The number is the largest single divergence, not how many branches touch the file.** Measured
2026-09-02: `PartitionState` had dozens of live branches with an open pull request diverging from it
and the largest of those divergences was **eight lines**. A count calls that file blocked with
nothing in its way. A maximum is also immune to two artefacts a count is not - a branch counted
twice as `feats/x` and `origin/feats/x`, and a stack of branches sharing a base.

**It names the branch, because waiting is not the only option.** When the window is shut, the line
above says which single branch to land in order to open it. That is the difference between a verdict
and a lead.

**The last line is the one that stops this lying to you.** A candidate is configured with every path
it is known by, and that count is the live refs carrying it under *none* of them. It is not
decoration: this fork's package rename is in flight, so a candidate genuinely lives at
`bz/stub/...` and `io/confluent/...` at the same time, and a path the config was never told about
would otherwise read as quiet - the exact false negative the rest of this tool exists to prevent.

**The ordering is the answer to "what next".** Candidates are listed by distance to open, nearest
first, so the top row is the one to start and the bottom is the one furthest from ever being
startable. The growth line beside each says whether the problem is getting worse while nobody takes
the moment - derived from git on every run, never recorded, so it cannot rot the way the 1533 above
did.

A working-tree answer cannot be given at all here. The question is about other branches by
definition, and 160 of the refs carrying that class carry it only under its pre-rename path.

`--if-open` prints nothing when the signal ran and nothing is open - the form the two hooks use. It
still prints when anything **failed**, because a hook whose correct output is silence cannot be told
from a hook that is broken. Thresholds are per candidate, live in `bin/refactor-candidates.json`,
and retuning one is an ordinary commit.
## Asking when a test started failing, without re-running anything

`bin/inflight.mjs codecov` reads Codecov's API, which holds the **outcome and wall-clock of every
individual test, per commit, per branch** - for far longer than a CI log is retained. That answers a
question nothing else here can: not *is this test red now*, but *when did it change, and has it done
this before*.

**No token and no setup.** This repository is public, so the API answers unauthenticated. The tool
works from a fresh agent sandbox, from CI, and from a machine that has never run `gh auth login`.

```
node bin/inflight.mjs codecov test MultiInstanceHighVolume
```

```
bz.stub.parallelconsumer.integrationTests.MultiInstanceHighVolumeTest::multiInstance
    pass        51.4s  3f54f02  ci/quarantine-report-status-changes
    pass        54.7s  957d583  ci/quarantine-report-status-changes
  -> 2 runs, all pass.
```

When the outcome does change across those commits, the tool says so and **refuses to interpret it**:
the same evidence fits a flake and fits a regression that landed between the two, and deciding which
is the reader's job. That is the same line [`docs/quarantined-tests.md`](quarantined-tests.md) draws
when it refuses to quarantine on a failure rate alone.

**Where this replaces manual work.** A quarantine entry needs sighting evidence, and that ledger is
currently assembled by hand from CI logs that expire - the reason
[`docs/testing.md`](testing.md) warns that a flake seen on a PR must be recorded before that PR
merges. `codecov flaky` lists every test recorded with more than one outcome, from history that
outlives the logs.

```
node bin/inflight.mjs codecov slow 3
```

```
    266.6s  ...WorkManagerLincheckTest::stressMustNotRediscoverTheCheckpointThreeTear  [lincheck]
    106.0s  ...RunLengthEncoderTest::testSimultaneousWithOverflowErrors(TypeKind)[2]   [unit]
     58.1s  ...PartitionStateCommittedOffsetIT::committedOffsetRemoved(OffsetResetStrategy)[1]  [integration]
```

**These durations are not a benchmark, and the distinction is load-bearing.** They are wall-clock on
a shared GitHub runner, moved by runner contention, Docker pulls and broker startup. The library's
throughput is a figure the performance test *computes about the library*, on a controlled arm, and
`bin/check-throughput-regression.mjs` owns that comparison. Feeding runner wall-clock into it would
reintroduce exactly the noise those control arms exist to remove. Use `codecov slow` for "this test
owns four minutes of every run"; never for "is the library faster".

**What an empty answer means here.** Codecov only knows tests whose suite has uploaded results, and
only since that upload was turned on. So "no flakes recorded" is a narrower claim than "no flakes",
and every one of these commands says so in its own output rather than leaving the reader to assume
otherwise.

## If it does not answer your question, change it - that is the expected use

**You are encouraged to patch this tool when it does not fit.** Not to work around it, not to write
a one-off script beside it, and not to file a note asking somebody else to. If you needed an answer
and had to get it another way, the tool has a gap and you are the person holding the evidence of what
that gap is - which is the part nobody can reconstruct later.

`bin/inflight.mjs`'s own header states the shape: **adding a tool is adding a row** to its `COMMANDS`
registry. A command is a `name`, a `summary`, a `when` (the sentence that tells a reader whether this
is the tool for what they are doing), a `usage`, and a `run` that returns `{ok, reason?}`. The
libraries under `bin/lib/` return findings, the views render them, and only the front door exits. A
tool reachable only by knowing its filename is the state this front door exists to end, so **a new
command that is not in that registry is not finished**.

Three things to keep when you extend it, each of which exists because getting it wrong here has
already produced a confidently wrong answer:

- **"Found nothing" and "could not look" are different answers.** A search that ran and matched
  nothing is `ok: true` with an empty result and a sentence saying what was searched. `ok: false` is
  reserved for could-not-run. Exit 0 means it ran; exit 2 means it could not.
- **Say what an empty result does NOT prove.** Every command here narrows the claim in its own
  output - which refs it covered, which history the data starts at. That sentence is the difference
  between a negative result and a false negative wearing the authority of a completed check.
- **Split the analysis from the fetching.** Keep the logic pure over its inputs and put the network
  or git call in a thin wrapper, so `bin/test-inflight.mjs` can drive the real logic on fixtures. A
  check that needs the network is a check that gets deleted the first time CI is offline - and every
  check there carries a negative control proving the mutant goes red.

**Migrate the shell scripts into it as you go.** `bin/` is overwhelmingly shell against a handful of
Node modules (`ls bin/*.sh | wc -l` against `ls bin/*.mjs bin/lib/*.mjs | wc -l`), and the split is
not a design - it is chronology. The rule is opportunistic, not a project:
when you are already working on a script that ANSWERS A QUESTION - `worktree-status.sh`,
`issue-index.sh`, `todo-index.sh`, `quarantined-test.sh` - move it behind the front door rather than
extending it in place. Do not migrate the ones that *do* something: `build.sh`, `deploy.sh`,
`ci-*.sh`, `rename-packages.sh` and the `check-*.sh` gates are fine as shell, and `bin/AGENTS.md`
owns their conventions. The distinction that matters is query versus action, not language.

## What the exit codes mean

**0 means it RAN, whatever it found. 2 means it could not run.** Every command distinguishes these,
and the distinction is load-bearing rather than decorative: two P0 defects found while building this
were both cases where a failure rendered as a confident empty result. "Nothing, across 436 refs" is a
result; a blank line is not, and neither is a search that never happened.
