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

## 2026-09-03: a partial base report, which reads the same as a drop

Third sighting of the same *shape*, and a different cause from either half above, so it goes here
rather than being explained by them.

`codecov/project/unit` went red on a PR whose Java changes were test-only and whose patch coverage
Codecov itself reported as 100%: the unit flag read a large negative delta, and the project total
read a large positive one at the same time. The base commit's whole report was SHORT - fewer files
and fewer lines than both the master commit before it and the PR head, on a master commit that
touched only `pom.xml` and docs, so no file could legitimately have left the report.

**Read the base report before believing a flag delta**, rather than reasoning from the percentage:

```bash
curl -s https://api.codecov.io/api/v2/github/astubbs/repos/parallel-consumer/commits/<sha>/ \
  | python3 -c 'import sys,json; t=json.load(sys.stdin)["totals"]; print(t["files"], t["lines"], t["coverage"])'
```

A base with fewer files than the commit before it is a truncated upload, and every flag comparison
against it is meaningless in both directions. It self-corrects on master's next full upload, which
is why nothing was changed for it - but a red that clears by itself is exactly the kind that gets
attributed to the PR that happened to be open.

## Sighting, 2026-09-07: the two sides still count different FILE SETS

<!-- post-merge: checked-begin -->
Observed on astubbs/parallel-consumer#105, check `codecov/project/unit`. That PR is the cleanest
possible probe for this, and the reason is worth stating before the numbers: **its diff contained no
Java at all** when the sighting was taken - a build property, a shell-script line, some documents and
some checked-in data files. So any difference the comparison reports is the comparison's, not the
change's. (That PR has since been re-scoped to documentation only, which does not weaken the probe -
it strengthens it.)

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

<!-- post-merge: checked-begin - names astubbs/parallel-consumer#431 as the source of a recorded sighting, in the past
     tense; reads the same once that PR has landed -->
## 2026-09-07: master's own `unit` history falsifies "so a drop there is a real drop"

The `<!-- post-merge: checked -->` block above claims both sides of the `unit`/`integration` gates
come from the same profile and exclusions, "so a drop there is a real drop." `codecov.yml` carries
the identical sentence in its own comments. Master's own `codecov/project/unit` history contradicts
that, for `unit` specifically - `integration` was not examined here.

**The shape**: pull the `codecov/project/unit` status for a run of recent master commits
(`gh api repos/astubbs/parallel-consumer/commits/<sha>/status`, or `node bin/inflight.mjs codecov`)
and the percentage does not walk - it sits in one of two stable bands roughly three points apart and
jumps between them from one master commit to the next, with no correlation to what that commit
changed. `440b9bd9` and `cc36b64b` are two masters commits where this pattern drove the check to a
genuine `failure` state - reproduce with the status command above - on commits that did not
introduce a coverage regression: the very next master commit lands back in the other band. A gate
whose own text says a drop on it is real, going red on master against master, is the falsification.

**Not the same sighting as the two related ones already on file, and neither settles it.**
astubbs/parallel-consumer#431's `ed6b8f461` documents a **verified, narrower** cause - a single base
report short on files and lines because its upload was partial - which explains a one-off red on a
PR compared against a specific truncated master commit. The FILE SETS sighting above finds that a PR
and its base disagree on `unit`'s file set even though the base is a git ancestor of the head, and
leaves the cause **explicitly unverified** - a candidate (the shared jacoco upload glob) named but not checked
against an actual file list. Neither is a same-commit-pair, master-vs-master comparison: both are a
PR against one base at one point in time.
A stable two-band oscillation across many consecutive
master pushes, with no relation to diff content, is a different shape from either, and this note
records it rather than folding it into theirs.

**The mechanism for this shape is NOT established.** Nothing here or in the two related sightings
pins down *why* master's own `unit` upload lands in one of two bands rather than drifting
continuously. The cheapest next check: for one same-band master pair and one cross-band master
pair, pull each commit's whole report totals (the `curl .../commits/<sha>/ | files, lines, coverage`
line from the astubbs#431 sighting works for this) and see whether files/lines move in lockstep with
the percentage jump (points at the same file-set-mismatch class as the two sightings above) or stay
fixed while the percentage still jumps (points elsewhere - most likely a run-to-run difference in
which tests actually executed, since `unit` is JVM-forked and per-class). Nobody has run that
comparison yet.

**What it costs.** `codecov/project/unit` is not in the repository's required status checks, so none
of this blocks a merge. The cost is upstream of blocking: astubbs/parallel-consumer#444 is a PR
whose diff added no files under `src/main/java` and still read a real `unit` drop while the
project-wide total rose, purely because its base happened to sit in the other band from its head -
<!-- post-merge: checked -->
the same shape as the PR the FILE SETS sighting above describes. A gate that flips on
roughly every other master commit, independent of content, cannot be told apart from a genuine
unit-coverage regression by looking at the number alone - the noise band is wide enough to hide a
real drop of the same size. "So a drop there is a real drop" is no longer a safe reading of this
gate for `unit` until the mechanism above is found and fixed.
<!-- post-merge: checked-end -->

## Delete when

A PR after this has merged shows `codecov/project/unit` and `codecov/project/integration` comparing
against a base that carries those flags, and the upload-pattern question above has been settled
either way.
