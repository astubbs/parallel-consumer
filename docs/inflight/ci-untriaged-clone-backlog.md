# Clones nobody has ever looked at

<!-- inflight-type: bug -->
<!-- inflight-impact: blind-spot -->
<!-- inflight-state: deferred - after v6, tooling investment -->


Both duplication engines scanned four Java module directories until
astubbs/parallel-consumer#320 widened them to the whole repo. Everything outside those four
directories has therefore **never been triaged** - not once, not by anyone.

What the widening reported, base and head identical, so all of it pre-existing:

| | before (four Java dirs) | after (whole repo) |
|---|---|---|
| PMD CPD clones | 24 | **27** |
| jscpd clones | 68 | **70** |

Five clones appeared purely from looking somewhere new. They are **not** introduced by any PR - the
`max increase vs base` guard reads +0.00%, because master carries them too.

## What is not known

**Where they are.** The PR comment reports totals only. Enumerating them means running the engines
locally; the CI job's own configuration in `.github/workflows/maven.yml` (the `dups: clones` job)
is the source of truth for how to invoke them, and the action it uses installs both.

That is deliberately not written down here - it is a command's answer, and it will be stale within a
week. What is written down is the part no command knows: **that nobody has ever read them**, and
that the count changed for a reason unrelated to code quality.

## Why this is worth a look rather than a shrug

The newly-scanned areas are `bin/`, `.github/`, the five `parallel-consumer-examples/*` modules and
the docs. Two of those are where duplication has actually hurt this repo:

- the gate module's base-tree reader was copy-pasted between a workflow and a script, ~22 lines,
  and both engines reported clean because neither was looking there
  ([`docs/solutions/workflow-issues/duplication-scanners-do-not-look-where-agents-duplicate-2026-08-12.md`](../solutions/workflow-issues/duplication-scanners-do-not-look-where-agents-duplicate-2026-08-12.md));
- the example modules are near-copies of each other **on purpose**, so some of the five are expected
  and should be dismissed explicitly rather than silently - "checked, it is the examples" is a
  result worth recording.

## The trap to avoid

Do not fix a clone because a tool named it. The examples exist to be read side by side, and the
`check-*.sh` / `test-check-*.sh` pairs share a header, an argument shape and an exit-code convention
deliberately. Extracting a shared helper from either would make the code worse and the check noisier.
Triage means deciding, per clone, whether the duplication is a defect or the design - and saying
which, so the next person does not re-open it.
