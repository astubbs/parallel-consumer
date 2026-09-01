# `AmbientProbeExtensionTest` fails 3/3 on master, deterministically

<!-- inflight-type: bug -->
<!-- inflight-impact: misdirection -->

Three headroom tests fail on a clean `origin/master` worktree, reproducibly rather than
intermittently:

- `headroomIsSilentWithoutADeadlineAndWithoutAMeasurement`
- `headroomIsReportedOnAPassingTestToo`
- `headroomOutcomeComesFromTheWatcherPhaseNotTheEndOfTheTestMethod`

All three fail the same way: the assertion expects one `PC-DEADLINE-HEADROOM` line and finds **two**
for the same test, one `outcome=PASSED` and one `outcome=FAILED`. So the extension records a headroom
line from more than one phase, which is exactly what
`headroomOutcomeComesFromTheWatcherPhaseNotTheEndOfTheTestMethod` exists to forbid - the test is
naming its own defect.

Reproduce on master, no Docker needed:

```bash
./mvnw --batch-mode -pl :parallel-consumer-core -am test -Dtest=AmbientProbeExtensionTest \
  -DfailIfNoTests=false -Dsurefire.failIfNoSpecifiedTests=false
```

**Established by control arm, not inference.** Found while running the full core unit suite on a
feature branch, and initially suspected to be that branch's doing.
<!-- post-merge: checked - the branch is deliberately not named; what matters is the method, and the
     conclusion is about master either way -->
It is not: `AmbientProbeExtensionTest.java` and `AmbientProbeExtension.java` were byte-identical to
`origin/master` there, the same three names failed with and without that branch's changes, and a
detached worktree cut straight from `origin/master` reproduces 3/3.

**Impact is `misdirection` because of what it does to everything else.** This is the class that owns
the ambient probe - the instrument every broker integration failure is supposed to be read through,
and the one `AGENTS.md` tells you to consult before diagnosing by hand. A red self-test on the
instrument means its verdicts are not currently trustworthy, and it also means any branch running the
core unit suite inherits three failures it did not cause and must spend time excluding, which is how
a real regression gets waved through as "that pre-existing thing".

**What is NOT established:** whether CI is red on this too. The recent master runs listed by
`gh run list -R astubbs/parallel-consumer --branch master` are Claude Code, Dependabot, mutation and
publish jobs; no unit lane was inspected. If CI is green while this fails locally, the gap between
them is the more interesting defect and should be chased first.

Arrived with `66a9a35e0` (astubbs#381) by `git log` on the file, which is the last commit to touch
the headroom code - not bisected, so treat that as where to start rather than as the cause.
