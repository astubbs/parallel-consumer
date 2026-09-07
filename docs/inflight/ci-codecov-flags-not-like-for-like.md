# Coverage gates are flag-scoped now; what is left is the total and one upload pattern

<!-- inflight-type: bug -->
<!-- inflight-impact: misdirection -->

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
so a drop there is a real drop. The whole-repository number is `informational: true`: five flags on a
PR against two on master cannot be made honest by tuning a threshold, so it is reported as a trend
and never gates. `carryforward` is off explicitly, because it is the reflexive fix for this and the
wrong one - it merges a stale measurement into a run that did not produce it, masking genuine drops.
Validated with codecov's own `POST /validate` before landing.
<!-- post-merge: checked-end -->

## What is still open

**The fix cannot be verified by the change that makes it.** `build` is `push`-only, so the new flags
do not exist on the base until this lands on master and that job runs. Until then a PR compares
against a `default`-flagged base and the flag gates have nothing to compare with. **A red or
no-data `codecov/project` on the first PRs after this merges is the expected state, not a
regression** - it clears once master has re-uploaded under the new flags. The first post-merge PR is
the real test of all of this, and nobody has run it yet.

**Every suite uploads both jacoco patterns, not just its own** - unverified, and recorded rather
than changed. The `test` matrix has one shared upload step with
`files: '**/target/site/jacoco/jacoco.xml,**/target/site/jacoco-it/jacoco.xml'`, so the
`integration` job (which runs `clean verify -DskipUTs=true`) may also upload a unit report with no
exec data behind it. Codecov merges reports within a flag by union, so an empty report should not
un-cover anything - which is why this was left alone rather than guessed at. Narrowing it means a
per-suite `files` value in the matrix, and three of the five suites (`performance`, `lincheck`,
`chaos`) would need their report shape established first rather than assumed.

## Sighting, 2026-09-07: the two sides still count different FILE SETS

<!-- post-merge: checked-begin -->
Observed on astubbs/parallel-consumer#105, check `codecov/project/unit`. That PR is the cleanest
possible probe for this, and the reason is worth stating before the numbers: **its diff contains no
Java at all** - a surefire `runOrder` property, one exempt path in a shell script, three documents
and ten `.surefire-pc-unit-times` data files. So any difference the comparison reports is the
comparison's, not the change's.

Reproduce the shape from that PR's codecov comment; the figures are deliberately not copied here,
because they move at every re-upload and a stale one reads as current:

- The base codecov chose was **an ancestor of the PR head**, provable with
  `git merge-base --is-ancestor <base> <head>`. Everything in the base is therefore in the head.
- Codecov nonetheless reported the head as carrying **more files and more lines than that base** -
  double-digit files, hundreds of lines. With no Java in the diff and the base contained in the
  head, no diff can produce that. **The two sides are measuring different file sets**, which is the
  same defect as the `default`-versus-suite-flags case above, surviving the flag split.
- `codecov/patch` passed and the whole-repository `project` number **rose**. Only the flag-scoped
  `unit` gate was red, and it was red on a PR that changed no code.
- `integration`, `chaos` and `performance` each showed **no base value at all** - codecov renders
  them `(?)` - so `unit` was the only flag with two sides to compare.

**Which half of this note that supports.** The first open item - "the fix cannot be verified by the
change that makes it", expecting red-or-no-data on the first PRs after the split lands. Master has
since run and uploaded, because `unit` had a base to compare against; `integration` did not, so that
item is **partly** discharged and partly still live. The prediction that it "clears once master has
re-uploaded under the new flags" did not hold for `unit`: master re-uploaded, the flag compared, and
it was still wrong.

**Which half it does NOT test, and must not be read as testing.** The shared-upload-glob mechanism -
`files:` globbing both jacoco patterns for every suite - remains **unverified**. A file-set
mismatch is *consistent with* that glob putting files in a PR's `unit` flag that master's `unit`
upload never carried, but this sighting inspected no uploaded report and identified no specific
file. It does not establish which side is wrong, whether the glob is the cause, or whether some
other asymmetry between `ci-unit-test.sh` and `ci-build.sh`'s surefire half explains it. Settling
that still needs the per-suite `files` work described above, or a diff of the two flags' file lists.
<!-- post-merge: checked-end -->

## Delete when

A PR after this has merged shows `codecov/project/unit` and `codecov/project/integration` comparing
against a base that carries those flags, and the upload-pattern question above has been settled
either way.
