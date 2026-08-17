---
title: A check that reports success without having run
date: 2026-08-11
category: workflow-issues
module: build-system
problem_type: workflow_issue
component: development_workflow
severity: high
root_cause: config_error
resolution_type: tooling_addition
applies_when:
  - Adding or reviewing any CI job, Maven plugin, or shell guard meant to gate something
  - A check depends on a network service, or on a credential or token that can expire
  - Reading a green check on a PR that edited the check's own configuration
  - Designing a guard for a tool whose own failure mode is to exit zero
  - Deciding whether a green build is evidence that a scan, lint, review, or audit happened
symptoms:
  - A build is green while the vulnerability scan it runs has been 401ing on every module
  - "A gating parameter (`fail=true`) is set and the build still succeeds when the service is unreachable"
  - A review check reports success on a run where the reviewer refused to start, or posted nothing
  - A managed workflow fails on every run, including on the default branch, and nobody notices
tags:
  - ci
  - silent-failure
  - fail-open
  - guard-design
  - maven
  - ossindex
  - false-negative
---

# A check that reports success without having run

## Context

Seven checks in this repository are on record as having gone unread while the thing they were meant to
catch went unchecked. Six reported **success**; the seventh reported failure on every run and nobody
looked. Several were recognised as the same thing within a single day, which is what turned a
recurring annoyance into a named class.

They come in two shapes, and keeping them apart matters because it changes the question you have to
ask. Most **never looked** - the scanner 401ed, the review action refused to start, the copyright
script could not resolve a fork point. Two **looked and then declined to enforce what they found**:
Maven spotted the missing plugin version and degraded it to a `[WARNING]`, and the review action
completed a review it never posted. So the diagnostic is not "did it run", which clears both of those,
but **did its verdict reach anything that acts on it**.

None of them was lying about its result. Each faithfully reported "I did not find a problem" - some
having never looked, some having looked and let it go. That is what makes the failure mode expensive:
a check that goes red when broken gets
fixed, and a check that goes green when broken is worse than no check at all, because it also removes
the prompt to add a working one.

| Check | What it did when it could not run | Where it stands |
|---|---|---|
| `ossindex-maven-plugin` on `validate` | HTTP 401 on every module - the API stopped serving anonymous component-report requests - logged as `[WARNING]`, build green, scan never happened | External guard, `bin/check-ossindex-audit.sh` (see below) |
| The same plugin with `fail=true` | Still green. `fail` covers *"vulnerable components were found"*, not *"the request failed"* | No plugin setting exists; the external guard is the only answer |
| `claude-code-action` posting no review | Ran green for months without posting a single review, because the plugin stops before commenting unless `--comment` is passed | Gated by `bin/check-review-posted.sh` |
| `claude-code-action` workflow-validation skip | Refuses to run when the workflow file differs from the default-branch copy; logged *"Exiting due to workflow validation skip"*, exited 0, PR sat mergeable with a green check and no review (astubbs/parallel-consumer#124) | Same gate - and it deliberately fails such PRs |
| `bin/check-copyright-headers.sh` | Exits 0 with a warning when there is no fork point (shallow clone) | `COPYRIGHT_CHECK_REQUIRE_FORK_POINT: "1"` in CI |
| Maven's own model validation | Degrades a plugin declared without a version to a `[WARNING]` that scrolls past in a long log | `requirePluginVersions` enforcer rule (astubbs/parallel-consumer#259) |
| GitHub's managed *Automatic Dependency Submission (Maven)* | Failed on every run it ever made, `master` included - repo-wide, not any one PR's doing - and went unnoticed because it is not a required check | Turned back off, with the re-enable trigger written down |

The first six fail open. The last fails closed and was ignored anyway, which is the same outcome by a
different route: **a check nobody gates on is a check nobody reads.**

## Guidance

### The rule already has a home; this is the class it belongs to

[`docs/investigating.md`](../../investigating.md) already states the half that matters most:

> **A guard added with a fix must be verified by negative control.** Break the thing it guards and
> confirm it fails deterministically. An assertion nobody has seen fail is decoration.

[`docs/ci.md`](../../ci.md) states it again for the review job, and
[`docs/releasing.md`](../../releasing.md) states it a third time for the `PR Checklist` changelog gate
(*"Do not read a green gate as compliance with the no-entries rule"*). This doc exists because those are
three separate statements of one class, and the class is worth naming so the next instance is
recognised before it costs anything.

### 1. For every check, ask what it does when it *cannot* run

Not "what does it do when it finds a problem" - that is the case every tool documents. The question
is the third state. A check has three possible outcomes and most tools expose only two:

```
found nothing wrong   -> pass
found something wrong -> fail
could not look at all -> ???      <- this is what decides whether the check is real
```

If the tool collapses the third state into the first, the tool cannot be trusted alone.

`ossindex-maven-plugin` is the clean example, and the part that surprised us is worth stating plainly:
**`fail=true` does not fix it.** Verified directly - forced on, no credentials, cold client cache:

```
[WARNING] Failed to fetch component-reports
org.sonatype.ossindex.service.client.transport.Transport$TransportException:
    Unexpected response; status: HTTP/1.1 401 Unauthorized
[INFO] BUILD SUCCESS
```

The plugin exposes no setting that makes an unreachable scanner fatal. Its green tick is never, by
itself, evidence that it ran.

### 2. When the tool cannot fail, put the decision outside the tool

Both guards here are the same move: a small script, run after the tool, that answers *did this
actually happen* using evidence the tool leaves behind.

- `bin/check-review-posted.sh` matches **this run's** id in a posted comment's `[View job]` link, so a
  comment citing this run is proof this run posted something. It states its own limit: it proves a
  comment exists, not that its contents are a good review.
- `bin/check-ossindex-audit.sh` (added by astubbs/parallel-consumer#279) reads the Maven log and the
  exported reports, with a split the plugin cannot express:

```
scan did not run           -> exit 1   (red: the CHECK is broken - nothing was learned about the tree)
scan ran, found nothing    -> exit 0
scan ran, found problems   -> exit 2   (red: the TREE has an advisory nobody has looked at)
```

**Two different reds, and holding them apart is the point.** They demand opposite responses - exit 1
sends you to the token or the lane and tells you nothing about the dependencies; exit 2 says the lane
worked and there is something to triage. Collapsed into one undifferentiated red, the reader can no
longer tell whether the scanner had anything to say. When both are true at once, exit 1 wins: findings
from a scan that cannot be proven to have happened are not evidence, in either direction.

**Findings were NOT fatal in the first version, and why that changed is the more useful half.** They
were rendered and exited 0, because the tree carried a standing backlog and a job that goes red on
every PR for known debt is ignored inside a week - which would have cost the exit-1 leg its audience
too. astubbs/parallel-consumer#281 retired that backlog: every item is now either fixed or an explicit
`excludeVulnerabilityIds` entry in the root pom carrying a stated retirement condition. That inverts
the argument rather than overruling it. On a tree whose known debt is already excluded *with reasons*,
a finding is by construction something nobody has looked at, so there is no standing red left for
people to learn to ignore. The general rule survives intact - **a red that fires on known debt means
nothing** - and what changed is the tree, not the rule.

The escape hatch for a false positive, and this scanner produces them, is that same exclusion list.
`bin/check-cve-exclusions.sh` polices it so a suppression added in a hurry cannot quietly become
permanent: it is the identical move applied one level up, to the audit's own escape hatch.

### 3. Prefer structural evidence from the artifact over string evidence from the log

The obvious guard for the audit was to grep the Maven log for `Failed to fetch component-reports`.
That was **rejected as the sole leg during implementation**, because a log-string guard dies silently
the day the vendor rewords its message - the exact class of rot the guard exists to catch. A guard
built out of the failure mode it is guarding against is not a guard.

What was built leans on the artifact instead. On a 401 the plugin still *writes* `ossindex-report.json`
- it writes `{ }`. So "the file exists" proves nothing and "the file has a non-empty `reports` map"
proves the scan happened. Three fail-closed legs, independent of one another:

1. **negative, on the log** - the failure marker anywhere means it did not scan;
2. **positive, structural** - every exported report must carry a non-empty `reports` map;
3. **positive, coverage** - one exported report per module the log says it checked.

**Legs 2 and 3 are only fail-closed on a cold client cache.** A warm cache serves full,
correct-looking reports without the run ever contacting the service, so a non-empty `reports` map is
proof *this run* scanned only if the job never restores or caches the OSS Index report directory. That
precondition is part of the guard, not a footnote to it; the trap in full is in section 5.

Leg 1 is cheap and catches today's common case, so it is kept. **Leg 2 is the one that survives a
wording change upstream**, and it is the leg a token expiry trips. The rule is not "never match a log
string" - it is **never let a log string be the only leg.**

### 4. Test the guard's silent case explicitly

`bin/test-check-ossindex-audit.sh` (alongside the guard) includes a case
asserting that empty reports fail **with no failure line in the log at all**. That test is what makes
leg 2 load-bearing: remove leg 2 and only that case goes green, and that case is precisely the shape
the vendor's next reword will produce.

### 5. Prove it by making it fail

The evidence that established the guard was a controlled failure, not a successful run: expired token,
client cache cleared, and then

```
[INFO] BUILD SUCCESS      # Maven's verdict
maven exit = 0            # the shell's verdict
guard  exit = 1           # the only component that noticed
```

Three signals, one dissenting. Watching a guard pass tells you nothing - the broken version passes
too.

**The cache is part of this method, not a footnote.** The OSS Index client keeps an on-disk report
cache, and a warm cache returns full, correct-looking results even against a bogus base URL and an
expired token. It invalidated two negative controls before anyone noticed. Clear it before any
reachability experiment, and never cache it in CI - a cached report would let the job pass its own
did-it-actually-scan guard on a run that never contacted the service. The general form of that trap
is the sibling doc below.

### 6. Where the third state cannot be made fatal, make it *visible*

Not every instance gets a guard script.

- `bin/check-copyright-headers.sh` cannot resolve a fork point on a shallow clone. Locally that is a
  warning, because a developer's clone is legitimately shallow sometimes; CI sets
  `COPYRIGHT_CHECK_REQUIRE_FORK_POINT: "1"` and the same state becomes a hard failure. **One
  environment variable is enough** when the state is only ambiguous in one of the two environments.
- Maven's malformed-model warning was made fatal by adding `requirePluginVersions` to the enforcer
  rules, so that whole warning class cannot recur silently.
- The managed dependency-submission workflow cannot be edited at all - it is not in
  `.github/workflows`. It was switched back off rather than left permanently red-and-ignored, with the
  condition for re-enabling it written down.

## Why This Matters

A silent-green check does two kinds of damage, and the second is the expensive one:

1. The thing it was meant to catch goes uncaught. Here, dependency vulnerability scanning was off for
   an unknown period, and PRs merged with a green `review` check and no review.
2. **It occupies the slot.** Nobody adds a vulnerability scanner to a project that already has a green
   one. The false signal is not neutral - it actively suppresses the fix.

The `fail=true` finding is why this needs writing down rather than reasoning out. The intuition that a
gating flag converts a broken check into a red one is wrong for at least one widely used plugin, and
it is wrong in the direction that feels safe. Ask what the flag actually gates on, then test it.

Two near neighbours are worth knowing, because they produce the same green tick from a different
mechanism - the check runs and returns the wrong answer, rather than not running at all, and the
observable is identical:

- `bin/check-shell-sigpipe.sh` exists because a `bin/*.sh` piping into `grep -q` under `pipefail` can
  report failure *because* it matched, silently inverting the script's answer. The producer has to
  still have more than a pipe buffer left to write when `grep -q` exits, so small inputs pass forever
  and the inversion surfaces only once real data grows past the threshold - which is what makes a
  quick reproducer look like a refutation.
- The mirror image, one line away in a workflow: `mvn ... | tee log` **without** `set -o pipefail`
  takes `tee`'s exit status, so a Maven `BUILD FAILURE` leaves the step green. Piping a build's
  output anywhere is enough to lose its verdict.

## When to Apply

- **Adding any check.** Before merging it, describe its behaviour when the network, the credential, or
  the input is missing. If that answer is "passes", it needs an external guard or it does not count as
  a check.
- **Any credential with an expiry.** A token that expires in six months turns a working check into a
  silently green one on a date nobody has in their calendar. This is the most likely future cause of
  the defect recurring here.
- **Reviewing a PR that edits a check's own configuration** - exactly the condition under which the
  review action skips and reports success.
- **Reading a green check as evidence in an argument** - a PR description, a release note, a security
  claim. "CI is green" is a claim about CI, not about the code.

## Examples

The negative control, reduced to its essentials:

```bash
# Cold cache is required - a warm client cache serves full results against a bogus
# endpoint and invalidates the experiment.
rm -rf ~/Library/Application\ Support/Sonatype/Ossindex

./mvnw --batch-mode test-compile -Dossindex.skip=false -Dossindex.fail=true
# [WARNING] Failed to fetch component-reports   (once per module)
# [INFO] BUILD SUCCESS
```

And the guard run over that same log, in outline - note the first line, because "no vulnerabilities
found" and "no scan happened" render identically everywhere else:

```
## :x: OSS Index audit did not run

**This is not a vulnerability finding.** The scan did not happen, so an empty result means nothing.

- the plugin logged "Failed to fetch component-reports" - the scanner was unreachable, or rejected our credentials
- N of N exported report(s) hold zero component reports - that is the shape a 401 leaves behind
```

A guard for this class has to say *which* kind of red it is.

## Related

- [`negative-results-need-an-instrument-that-could-have-said-yes.md`](negative-results-need-an-instrument-that-could-have-said-yes.md)
  - the sibling. This doc is about checks that pass without running; that one is about *investigations*
  that return a negative because the tool was pointed at nothing. The OSS Index client cache appears in
  both, from the two different angles.
- [`compound-tooling-breaks-in-worktrees-and-forks-2026-08-07.md`](compound-tooling-breaks-in-worktrees-and-forks-2026-08-07.md)
  - a tool that *ran* but looked in the wrong place and returned empty; this doc covers a check that
  never ran and returned green. Same genus, different species.
- [`../test-flakiness/vacuous-await-condition-brokerpoller-backpressure-2026-07-31.md`](../test-flakiness/vacuous-await-condition-brokerpoller-backpressure-2026-07-31.md)
  - the same class inside a test: an await condition that was satisfied before the system reached the
  state it was meant to prove, so green runs passed vacuously.
- [`../../ci.md`](../../ci.md) - the review gate and the sigpipe check, with the reasoning for both.
- [`../../investigating.md`](../../investigating.md) - *verify a guard by negative control*, and
  *verify your instrumentation actually reached the run*.
- [`../build-errors/maven-multi-module-plugin-and-resolution-traps.md`](../build-errors/maven-multi-module-plugin-and-resolution-traps.md)
  - the Maven mechanics behind two rows of the table: the profile-scoped plugin version that only
  warns, and the `validate`-builds-nothing trap that breaks the managed dependency submission.
- `docs/inflight/ci-ossindex-lane-reassessment.md` - whether the lane still earns its keep now that
  GitHub can submit Maven transitive dependencies itself.
- astubbs/parallel-consumer#259 - `requirePluginVersions`, and the audit turned off-and-visible rather
  than on-and-hoping.
- astubbs/parallel-consumer#279 - the CI job, `bin/check-ossindex-audit.sh`, and the exclusion-expiry
  guard beside it.
- astubbs/parallel-consumer#281 - retiring the standing backlog, which is what let findings become
  fatal.
