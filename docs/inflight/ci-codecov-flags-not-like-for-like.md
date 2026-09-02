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

## Delete when

A PR after this has merged shows `codecov/project/unit` and `codecov/project/integration` comparing
against a base that carries those flags, and the upload-pattern question above has been settled
either way.
