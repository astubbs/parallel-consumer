---
title: "Do not filter, truncate or quieten the output you are about to use as evidence"
date: 2026-08-10
category: test-issues
module: parallel-consumer-connect
problem_type: best_practice
component: development_workflow
severity: high
applies_when:
  - "Running a build or test command whose output will be cited as proof that something ran"
  - "Reaching for a pipe into tail, head or grep because the raw output is long"
  - "Adding -q, --quiet, or a reduced log level to a command you are about to report on"
  - "Claiming that specific named tests, arms or surefire executions ran - not merely that the build passed"
  - "Reviewing a verification claim whose only supporting artefact is an exit code or a truncated excerpt"
  - "Verifying a refactor did not silently drop a test execution from the build"
symptoms:
  - "A verification claim names specific tests, but the captured output could never have shown them"
  - "The visible tail of a build shows one execution's result and nothing about the others"
  - "A quiet build passes and there is nothing in the log that names which tests executed"
  - "A results grep matches deliberate error-path fixtures and makes a passing run look alarming"
  - "The run is over, the output is gone, and the claim about it cannot be rechecked without re-running"
tags:
  - evidence
  - verification
  - build-output
  - shell-pipelines
  - surefire
  - reporting
  - false-confidence
related_components:
  - testing_framework
  - tooling
---

# Do not filter, truncate or quieten the output you are about to use as evidence

## Context

`parallel-consumer-connect` proves that patching Kafka Connect's `WorkerSinkTask` does not regress
Connect itself, by running Kafka's own `WorkerSinkTaskTest` twice in one build: once against a stock
arm whose classpath substitutes an intentionally empty main-classes directory, and once against the
generated patched class. Both arms write their surefire XML to separate report directories
(`parallel-consumer-connect/pom.xml:32-33`), and a third, explicitly selected execution compares the
two report sets against a checked manifest of test identities
(`WorkerSinkTaskRegressionReportsVerifier.java:33-60`, which requires exactly
`EXPECTED_TEST_COUNT` identities on each side and refuses a zero-discovery arm at `:110-113`).

The value of that harness is entirely in *both arms actually running*. A refactor that silently
dropped one surefire execution would leave the build green and the claim false. So "both arms ran"
is a claim that has to be re-earned on every run that touches the module.

In one session on astubbs/parallel-consumer#240 that claim was made three times, and all three times
the evidence had been filtered before it was read. Each was a different mechanism, and in every case
the underlying run was genuinely fine - it was the *claim about the run* that was unsupported.

1. **Piping through `tail`.** The build was run as `mvn ... | tail`. The visible tail ended in
   `Tests run: 2 ... BUILD SUCCESS`. Two is the count for the comparison execution, which runs last;
   the two arms' own `Tests run:` lines had scrolled past and were destroyed by the pipe. The tail
   was compatible with both arms running, with one arm running, and with neither - it simply could
   not distinguish them. Re-running with the full log captured to a file showed every surefire
   execution and both arms' results.

2. **`-q` (quiet).** A later run used `mvn -q`, which suppresses `[INFO]` output and with it the
   `Tests run:` lines entirely. Exit code 0 was real evidence that the build passed. It was not, and
   could not be, evidence of *which* tests executed, so the specific claim - "both regression arms
   still execute after this refactor" - stayed unsupported until the run was repeated without `-q`.

3. **A grep that matched the wrong thing.** A results grep matched `ERROR` lines emitted by test
   fixtures that deliberately exercise failure paths, so a passing run read as alarming, while the
   `Tests run:` lines that would have settled the question were filtered out. The filter both
   invented a problem and hid the answer.

## Guidance

**Name the claim before choosing the filter.** All three failures share one shape: the filter was
picked for the convenience of *reading* the output, before anyone had said what the evidence needed
to contain. `tail` is a reasonable way to see how a command ended. It is not a way to see what a
command did. Decide which sentence you are going to write - "the build passed", "both arms ran",
"`WorkerSinkTaskTest` executed against the patched class" - and only then decide what to keep.

**Redirect the full output to a file, then grep the file.**

```bash
mvn -pl parallel-consumer-connect test > /tmp/connect-build.log 2>&1; echo "exit=$?"
grep -n "Tests run:" /tmp/connect-build.log
```

Two things follow that a pipe cannot give you. The exit status is the build's own, not a filter's.
And the artefact survives, so the next question - "which arm was the failure in?", "did the verifier
execution run at all?" - is answered by grepping the file again instead of by re-running a build you
have already paid for. Keep the log until the claim it supports is no longer being relied on.

**Make the grep match the positive evidence, not the absence of failure.** `grep -i error` answers
"is anything alarming here", which is a different question from "did the thing I claimed run".
Search for the artefact the claim is about:

```bash
grep -nE "Tests run:.*WorkerSinkTaskTest|worker-sink-task-(stock|patched-disabled|report-verifier)" /tmp/connect-build.log
```

A grep for failure text returning nothing is the same class of non-evidence as a clean probe whose
detectors could never have fired - it is consistent with success and with the run never happening.

**Exit status answers "did it pass". It never answers "what ran".** Those are different claims with
different evidence. Zero is a real, useful signal and it is the *only* thing a quiet run gives you.
When the sentence you are about to write names specific tests, arms, executions or counts, the
evidence has to name them too.

**Treat `-q`, `--quiet` and log-level reductions as filters.** They are less visibly a filter than a
pipe, which is exactly why they slip through: nothing in `mvn -q` looks like it is discarding
anything, and the missing lines leave no gap where they used to be. Quiet flags are for runs you are
not going to report on.

**This is already the house recipe for CI logs - generalise it to local runs.** `AGENTS.md:314-327`
tells you to fetch a failed job's log to a file with `gh api ... > /tmp/job.log` and *then* grep it
for `Tests run:` and `<<< FAILURE`. The reason it is written that way is the same reason it applies
locally. The only difference is that a CI log can be re-fetched and a local run cannot.

## Why This Matters

**The failure mode is not carelessness about running things. It is confidence about reporting
them.** Every one of the three runs above was genuinely executed, and every one of them passed. What
went wrong each time happened afterwards, in the sentence written about the run. That is worth
stating plainly because it defeats the obvious self-check: "did I actually run it?" is answered yes,
honestly, and the answer is irrelevant to the defect.

**A verification you cannot show is a verification you did not do.** The reader of a claim cannot
distinguish "I ran it and saw both arms" from "I ran it and saw a tail that was compatible with both
arms". Neither, a week later, can the person who wrote it. The evidence is what makes the claim
recheckable; destroying it converts a verified statement into a remembered impression, and
impressions do not survive contact with a reviewer.

**The cost asymmetry is extreme.** A redirect costs one shell token and some disk. Reconstructing a
destroyed run costs the whole build again, and it costs it at the worst moment - during review, when
someone has challenged the claim and the branch has moved on. Optimising the *reading* of output at
the expense of the *keeping* of it trades a few seconds now for a full re-run later.

**Green builds are where this hides.** A filtered failure gets noticed, because something downstream
breaks. A filtered success is never questioned, so a harness that quietly stopped running one of its
two arms can survive indefinitely behind a series of honestly-reported green runs.

## When to Apply

- Before piping any command whose output will be quoted, summarised, or used to support a claim.
- Whenever a claim names specific tests, executions, arms, counts or configurations - the evidence
  must name them too.
- After any refactor of build configuration, surefire executions, profiles or CI scripts, where the
  risk is not a failing test but a *missing* one.
- When adding `-q`, `--quiet`, `--no-transfer-progress` or a reduced log level to a command in a
  verification step, as opposed to a routine one.
- When choosing a grep pattern for results: check it against the positive artefact you expect, not
  only against the error text you hope is absent.
- During review of someone else's verification claim - ask what output they had, and whether it
  could have contained the thing they said it showed.

## Examples

### The same family: a wrapper that silently removes what you were relying on

This repo already treats two other members of this class as serious enough to write down, and one of
them is enforced in CI. They are worth reading together, because the shared lesson is that a
convenience wrapper removes something without leaving a mark:

- **The pipe destroys the exit status.** `AGENTS.md:694-700` records that `git checkout <branch>`
  fails when another worktree holds the branch - but piped into `tail`/`head` the pipeline still
  exits 0, so a following `&& git rebase …` runs against the wrong branch. That actually rebased an
  unrelated PR's branch on 2026-08-06. Do not restate that rule; it is written there.
- **The pipe destroys the answer.** `bin/check-shell-sigpipe.sh`, run by the `sigpipe` job of
  `.github/workflows/repo-hygiene.yml`, fails any `bin/*.sh` that pipes into `grep -q` under
  `pipefail`, because `grep -q` exits on first match and the resulting SIGPIPE reports *failure
  exactly when the pattern matches*, once enough output follows. `shellcheck` does not detect it
  (`AGENTS.md:343`).
- **The pipe destroys the content.** This document.

Three different things a pipe can take from you - status, answer, content - and in each case the
command line still looks like it did the work, because it did. Only the part you were going to rely
on is gone.

### The defect: a tail that could not have shown what was claimed

```bash
mvn -pl parallel-consumer-connect test | tail        # visible: "Tests run: 2 ... BUILD SUCCESS"
```

The claim being supported was that Kafka's `WorkerSinkTaskTest` ran twice, once per arm. `Tests run:
2` is the comparison execution (`WorkerSinkTaskRegressionTest`'s two cases), which runs last and is
therefore the only one a tail can see. Both arms' results were above the window. The output was
consistent with the claim and equally consistent with its opposite.

### The fix: capture, then interrogate

```bash
mvn -pl parallel-consumer-connect test > /tmp/connect-build.log 2>&1; echo "exit=$?"
grep -nE "Tests run:|worker-sink-task" /tmp/connect-build.log
```

The grep names what the claim names, so the output either supports the sentence or visibly does not.
The log stays on disk, so the follow-up question does not cost another build.

### The check that would have caught all three

Ask, before writing the sentence: *could the output I kept have looked exactly like this if the thing
I am claiming had not happened?*

- Under `| tail`: yes - the arms' lines are off-screen either way.
- Under `-q`: yes - the arms' lines do not exist either way.
- Under `grep -i error`: yes - the deliberate error fixtures produce those lines either way, and a
  missing arm produces no error at all.

Three yeses, three unsupported claims. The question is cheap, it is asked after the run rather than
before, and it does not require knowing what went wrong - only what the output was capable of
distinguishing.

## Related

- [A restart assertion satisfiable by pre-crash data proves nothing](a-restart-assertion-satisfiable-by-pre-crash-data-proves-nothing.md) -
  the sibling failure. There the *assertion* could not fail; here the *evidence* is destroyed before
  it is read. Both produce a confident green report about something nobody checked, and both are
  fixed by making the uninformative outcome impossible rather than by looking harder at it.
- [Await conditions that are vacuously true before the system reaches its initial state mask unsatisfiable assertions](../test-flakiness/vacuous-await-condition-brokerpoller-backpressure-2026-07-31.md) -
  the same family again, in a condition rather than a report.
- [Assert on the statistic that states your claim, not the one that sounds most rigorous](../best-practices/choose-the-statistic-that-states-the-claim.md) -
  the positive form of "name the claim first": pick the measurement that states what you mean, then
  read it, rather than picking a convenient one and describing whatever it says.
- [A control arm must vary exactly one term, not a term and whatever it silently derives from it](../best-practices/control-arms-vary-exactly-one-term.md) -
  a control arm is only as good as the output you keep from it; a filtered control arm is not a
  control arm.
- [Fresh work needs an independent reviewer, and the tail is what momentum skips](../best-practices/fresh-work-needs-independent-review.md) -
  filtered evidence is invisible to the author and obvious to a reviewer, who has no memory of the
  run to fill the gap with.
- [Patch a dependency's internals at build time instead of vendoring or forking it](../architecture-patterns/patch-a-dependency-at-build-time-without-vendoring-it.md) -
  the technique whose regression harness is the subject of the claims above.
- `AGENTS.md:314-327` - the CI-log recipe (fetch to a file, then grep for `Tests run:` and
  `<<< FAILURE`) that this generalises to local runs.
- `AGENTS.md:694-700` and `:343` - the exit-status and SIGPIPE members of the same class, already
  documented and CI-enforced.
- astubbs/parallel-consumer#240 - the PR this was learned on.
