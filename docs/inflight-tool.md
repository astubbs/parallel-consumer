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
