# The similarity engine has no way to accept duplication that is correct

<!-- inflight-type: bug -->
<!-- inflight-impact: misdirection -->

`dups: similarity` (`astubbs/duplicate-code-detection-tool`, pinned in `maven.yml`) compares whole
files pairwise and fails above 80%. It has **no allowlist**: no `ignore_files`, no accepted-pairs
baseline, no glob. `ignore_directories` is the only exclusion and it takes directories.

## Why that is a defect rather than a missing nicety

Its only baseline is **the base branch, computed live**. So identical files are accepted or rejected
purely by which side of a merge they sit on.

<!-- post-merge: checked-begin -->
The worked example: when astubbs/parallel-consumer#326 added `TestConventionsArchTest` wrappers to
three example modules, they failed at 84.3% while the four already on master (core, vertx, reactor,
mutiny) passed at ~84% - because those four were not an *increase*. Same files, same similarity,
opposite verdicts. Once astubbs#326 landed, its three joined the baseline and stopped being flagged,
which is the whole problem: nothing about the code changed, only which side of the merge it sat on.
<!-- post-merge: checked-end -->

ArchUnit opts a module in **only** through its own two-line wrapper pointing `@AnalyzeClasses` at
that module's packages, so every wrapper in the repo is near-identical by construction. There is no
way to write one that is not. This recurs on **every new module**.

### The same live baseline fails a branch for being BEHIND, on files it never touched (2026-09-07)

<!-- post-merge: checked-begin -->
A second consequence of the same root cause, and it does not need a new module. The check reported a
`+11.5` increase between `JStreamParallelEoSStreamProcessor` and `JStreamParallelStreamProcessor` on
astubbs/parallel-consumer#431, which changes neither file. Master had rewritten both under
astubbs#122 and left them *less* alike; that branch still carried the pre-rewrite pair, so the same
unchanged files read as an increase against a base that had moved under them. Merging master cleared
it, and the diff of those files against master was empty either side of the merge.
<!-- post-merge: checked-end -->

**The tell is a flagged pair the PR's own diff does not contain** - `git diff origin/master...HEAD
--name-only` naming neither file. Reach for a master merge before reading the report as a finding;
the tool is comparing two trees, not two versions of your change.

The full report's >80% rows are a separate thing and stay green: the per-module wrappers above sit at
80-87% against each other on both sides, so base suppression covers them, and only the increase
table fails a build.

## Why the obvious workarounds are all wrong

<!-- post-merge: checked -->
- **Exclude the example modules** - tried on astubbs#326 and reverted. Each wrapper shares its
  directory with that module's real app test (`CoreAppTest`, `VertxAppTest`, `ReactorAppTest`), and
  those five example apps solve the same problem five ways, so they are the *most* likely place for
  genuine duplication. The exclusion blinds the metric to the one thing in examples worth measuring.
- **`only_code: true`** (strip comments before analysis) makes it WORSE: the wrappers' *code* is what
  is identical, so removing the differing javadoc raises their similarity.
- **Lower `fail_above`** - blinds the metric everywhere to fix one file class.
- **Make it advisory** - it has a demonstrated catch. On astubbs#325 this same engine flagged 83%
  between the two drain control arms; the fix stopped two controls drifting apart. `dups: clones` is
  density-based and would not have caught it: two near-identical 40-line files barely move a 0.13%
  figure. Downgrading a check that works, because it lacks an allowlist, treats a missing feature as
  a reason to stop enforcing.

## The fix

Add an accepted-duplication input to the tool - `ignore_files` taking paths or globs, or a checked-in
accepted-pairs baseline. **The action is `astubbs/duplicate-code-detection-tool`, this project's own
fork**, so this is available rather than blocked on a third party.

An accepted-pairs baseline is the better shape: it records *which* duplication was reviewed and
accepted, so a NEW pair between the same files still fails. A path ignore silences the file forever,
including duplication nobody has looked at.

## Related

- `.github/workflows/maven.yml` - the job, whose own comment already makes this argument about
  `bin/`: check/test-check twins are similar by design, and flagging them "yields findings that are
  all correct and all unactionable, which is how a check gets ignored."

## Delete when

The tool can express accepted duplication and `maven.yml` uses it.
