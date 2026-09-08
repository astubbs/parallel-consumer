# `AmbientProbeExtensionTest`'s headroom tests are a flake, and they are red on master

<!-- inflight-type: bug -->
<!-- inflight-impact: misdirection -->

Three headroom tests fail most of the time, on master, with a **varying** number failing per run:

- `headroomIsSilentWithoutADeadlineAndWithoutAMeasurement`
- `headroomIsReportedOnAPassingTestToo`
- `headroomOutcomeComesFromTheWatcherPhaseNotTheEndOfTheTestMethod`

All three fail the same way: the assertion expects one `PC-DEADLINE-HEADROOM` line for the test and
finds **two**, one `outcome=PASSED` and one `outcome=FAILED`. So a headroom line is recorded from
more than one phase - which is exactly what
`headroomOutcomeComesFromTheWatcherPhaseNotTheEndOfTheTestMethod` exists to forbid. The test names
its own defect.

Reproduce on master, no Docker needed:

```bash
./mvnw --batch-mode -pl :parallel-consumer-core -am test -Dtest=AmbientProbeExtensionTest \
  -DfailIfNoTests=false -Dsurefire.failIfNoSpecifiedTests=false
```

## It is a FLAKE, and two earlier readings of it here were wrong

Recorded because both wrong readings were reached from real runs, and the next person will reach
them the same way:

1. **"Deterministic, 3/3."** Three consecutive isolated runs on the same tree gave **3, 2, 3**
   failures. The count varies between runs, so any single run - green or red - establishes nothing
   on its own. The first version of this note said "reproducibly rather than intermittently" off
   three same-count runs.
2. **"astubbs#29 fixes it."** One full-suite run after merging that branch came back green (571 run,
   0 failed) and was written up as a fix. Subsequent isolated runs on that same merged tree fail
   again. **That green run was luck, not a fix.** Do not read a single green suite as evidence here
   - which is the whole reason this note carries an impact of `misdirection`.

## Why it matters beyond its own three assertions

This is the class that owns the **ambient probe** - the instrument every broker integration failure
is supposed to be read through, and the one the root `AGENTS.md` tells you to consult before
diagnosing by hand. A self-test that flaps means its verdicts are not currently trustworthy. It also
means any branch running the core unit suite inherits one to three failures it did not cause, which
is how a real regression gets waved through as "that pre-existing thing".

## What has NOT been established

- **Whether CI is red on this.** The recent master runs listed by
  `gh run list -R astubbs/parallel-consumer --branch master` are Claude Code, Dependabot, mutation
  and publish jobs; no unit lane was inspected. If CI is green while this flaps locally, the gap
  between them is the more interesting defect and should be chased first.
- **The mechanism.** Two headroom lines for one test implies the extension records from both a
  watcher phase and an after-each phase under some interleaving, but that has not been traced.
- **A bisect.** `git log` names `66a9a35e0` (astubbs#381) as the last commit to touch the headroom
  code. That is where to start, not a cause.

Not quarantined here: quarantine wants a diagnosis or a sighting ledger, and this note is neither
yet - it is the sighting that should become one.
