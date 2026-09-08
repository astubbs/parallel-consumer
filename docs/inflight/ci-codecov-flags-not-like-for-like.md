# Coverage gates are flag-scoped now; what is left is one upload pattern and an unexplained `unit` history

<!-- inflight-type: bug -->
<!-- inflight-impact: misdirection -->
<!-- inflight-vetted: 2026-09-08 - applied: shrunk. The first open item - "the fix cannot be verified by the change that makes it" - is discharged and now reads as history, and the FILE SETS sighting that was its evidence is compressed to the part that still bears on the upload glob. The two remaining items are the shared jacoco glob and master's unexplained two-band `unit` history, re-premised on the jumps having stopped; checked: `gh pr checks 475 -R astubbs/parallel-consumer` shows both per-flag gates passing on a PR with no Java, `maven.yml`'s `test`-matrix collector still finds both jacoco patterns for every suite and its own comment names this note as owning that question, and the `codecov/project/unit` status of the last dozen master commits sits inside 78.72-78.98% from abd1d392c onward with every band jump before it -->

`codecov/project` compared a PR total against a base total built from a **disjoint set of flags**.
The master `build` job uploaded everything under one `default` flag; a PR runs the `test` matrix and
uploads `unit`, `integration`, `performance`, `lincheck`, `chaos`, and never `default`.

**Observed on a PR touching zero Java**: Files 90 -> 90, Lines 4822 -> 4822, Branches 463 -> 463,
reported **-3.53%**. Nothing had changed; the two sides counted different things. The check was new
enough that nothing had caught it, and it would have gone red on every PR from then on.

<!-- post-merge: checked-begin -->
**Both halves are fixed in astubbs/parallel-consumer#400.** The master job now uploads `unit` and
`integration` separately - the split the pom's `report` and `report-integration` executions already
make - and `codecov.yml` gates per flag rather than on the overall total. Both sides of each gate
come from the same `-Pci` profile and the same `-Dexcluded.groups=performance,chaos,quarantined,lincheck`,
so a drop there is a real drop.
The whole-repository number is `informational: true`: five flags on a
PR against two on master cannot be made honest by tuning a threshold, so it is reported as a trend
and never gates. `carryforward` is off explicitly, because it is the reflexive fix for this and the
wrong one - it merges a stale measurement into a run that did not produce it, masking genuine drops.
Validated with codecov's own `POST /validate` before landing.

**The flags now compare against a base that carries them**, which was this note's first open item:
`build` is `push`-only, so the new flags did not exist on any base until the split had landed on
master and that job had run, and a red or no-data `codecov/project` in that window was the expected
state rather than a regression. astubbs/parallel-consumer#475 - a pull request with no `.java` in its
diff - shows `codecov/project/unit` and `codecov/project/integration` both green against a base
carrying both flags. That item is discharged. Two things are not.
<!-- post-merge: checked-end -->

## Still open: every suite uploads both jacoco patterns, not just its own

Unverified, and recorded rather than changed. The `test` matrix has one shared collector that finds
both `*/target/site/jacoco/jacoco.xml` and `*/target/site/jacoco-it/jacoco.xml` for every suite, so
the `integration` job (which runs `clean verify -DskipUTs=true`) may also upload a unit report with
no exec data behind it. Codecov merges reports within a flag by union, so an empty report should not
un-cover anything - which is why this was left alone rather than guessed at. Narrowing it means a
per-suite `files` value in the matrix, and three of the five suites (`performance`, `lincheck`,
`chaos`) would need their report shape established first rather than assumed. `maven.yml`'s collector
step carries a comment naming this note as the owner of that question, so the two do not drift.

<!-- post-merge: checked-begin -->
**One sighting is consistent with it and settles nothing**, which is why it is kept in one paragraph
rather than as a section. On astubbs/parallel-consumer#105 - whose diff carried no Java at all when
the sighting was taken - `codecov/project/unit` reported the head as carrying more files and more
lines than a base that `git merge-base --is-ancestor` proves is contained in that head, which no diff
can produce. So the two sides were measuring different file SETS. It is *consistent with* the glob
putting files in a PR's `unit` flag that master's `unit` upload never carried, but the sighting
inspected no uploaded report and named no file: it does not establish which side is wrong, whether
the glob is the cause, or whether some asymmetry between `ci-unit-test.sh` and `ci-build.sh`'s
surefire half explains it. Settling it needs the per-suite `files` work above, or a diff of the two
flags' file lists.
<!-- post-merge: checked-end -->

## Still open: master's own `unit` history ran in two bands, and the mechanism was never found

The fix above claims both sides of the `unit`/`integration` gates come from the same profile and
exclusions, "so a drop there is a real drop"; `codecov.yml` carries the identical sentence in its own
comments. Master's own `codecov/project/unit` history contradicted that for `unit` specifically -
`integration` was not examined.

**The shape**: pull the `codecov/project/unit` status for a run of consecutive master commits
(`gh api repos/astubbs/parallel-consumer/commits/<sha>/status`, or `node bin/inflight.mjs codecov`)
and, over the stretch before astubbs/parallel-consumer#464, the percentage does not walk - it sits in
one of two stable bands roughly three points apart and jumps between them from one master commit to
the next, with no correlation to what that commit changed. `440b9bd9` and `cc36b64b` are master
commits where that drove the check to a genuine `failure` state on a commit that introduced no
coverage regression: the very next master commit lands back in the other band. A gate whose own text
says a drop on it is real, going red on master against master, is the falsification.

**The jumps have stopped, and that is not the same as the mechanism being found.** Since
abd1d392c - astubbs/parallel-consumer#464, which keyed push runs per SHA so no master run is
superseded and every master commit uploads - the same status command shows a single band with
sub-0.2% deltas, and every band jump on record predates it. That is suggestive of the truncated-base
class rather than proof of it: nobody has run the files/lines comparison that would say. **The cheap
check is now retrospective**: take one same-band and one cross-band master pair from before
abd1d392c and pull each commit's whole-report totals (`curl .../commits/<sha>/ | files, lines,
coverage`). Files and lines moving in lockstep with the percentage points at a file-set mismatch;
staying fixed while the percentage jumps points elsewhere, most likely at a run-to-run difference in
which tests actually executed, since `unit` is JVM-forked and per-class.

**What it cost while it ran.** `codecov/project/unit` is not a required status check, so none of it
blocked a merge. The cost was upstream of blocking: astubbs/parallel-consumer#444 added no files
under `src/main/java` and still read a real `unit` drop while the project-wide total rose, purely
because its base sat in the other band from its head. A gate that flips on roughly every other master
commit, independent of content, cannot be told apart from a genuine unit-coverage regression by
looking at the number alone - the noise band is wide enough to hide a real drop of the same size.
Until the mechanism is named, "so a drop there is a real drop" stays an unsafe reading of this gate
for `unit`, however quiet it has been since.

## Delete when

The upload-pattern question above has been settled either way, and master's two-band `unit` history
has a named mechanism rather than an absence of recurrences.
