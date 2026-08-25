---
title: "GNU-only shell constructs do not error on BSD - they return a wrong answer, and the gate reports success"
date: 2026-08-25
category: workflow-issues
module: build-system
problem_type: workflow_issue
component: development_workflow
severity: high
status: "SOLVED for the sites found - the harness was swept, and a real macOS run replaced the reasoning that had stood in for it. The remaining latent site and the missing CI lane are tracked in `docs/inflight/ci-bsd-portability-gaps.md`."
applies_when:
  - Writing or reviewing any script under `bin/` or `.claude/hooks/`
  - A gate passes on Linux and you have not run it on macOS
  - Reasoning about BSD behaviour from documentation instead of executing it
  - A self-test suite reports success and you have not checked that it ran
  - Choosing a regex flavour, an in-place edit, or a way to pass a list into awk
tags:
  - portability
  - bsd
  - macos
  - false-negative
  - shell
  - gates
  - observability
---

# GNU-only constructs fail silently on BSD

## Context

This repo's agent harness is shell: gates under `bin/`, guards under `.claude/hooks/`. It is
developed and CI-tested on Linux with GNU coreutils, and run by contributors on macOS with BSD
userland. Over a dozen defects of one shape have now been found in it.

**The shape is not "a flag that errors on the other platform".** That case is harmless - it exits
non-zero and somebody fixes it. The damaging case is the construct that is *accepted* and means
something different, or that fails in a way the surrounding script does not check. The script keeps
running and produces a confident wrong answer.

Four instances, all measured on Darwin 25.5.0 with system bash 3.2.57:

| Construct | What BSD does | What the script then reported |
|---|---|---|
| `sed -n 's/\([a-z]\+\)/\1/p'` | `\+` and `\|` are GNU BRE extensions; the expression matches nothing | `bin/check-inflight-tags.sh` extracted **0** documented impacts instead of 17, and printed "N note(s) valid" - half the check dead, success reported |
| `awk -v x="$(printf '1\n2')"` | rejects a newline in a `-v` assignment, diagnostic on stderr, **exit still 0** | `bin/rename-packages.sh` got an empty frozen-line set, so every file carrying a freeze marker was dropped from the rewrite list and it printed "already applied, nothing to do" over an untouched tree |
| `source missing.sh \|\| { echo; exit 2; }` under `set -e` | on bash 3.2 a failed `source` is fatal; the `\|\|` arm never runs | `bin/check-file-refs.sh` and `bin/check-issue-refs.sh` died **exit 1 with empty output** - their code for "violations found", so a missing helper accused the tree |
| `sed -i 's/x/y/' f` | `-i` takes the suffix as the NEXT argument, so the script becomes the suffix | fixture edits silently did not happen; the self-tests then failed against a gate that was fine |

A fifth, adjacent: an apostrophe inside a comment inside `$( ... )` stops bash 3.2 parsing the file
at all. `bin/test-check-branch-self-reference.sh` exited 2 before any case ran, so its 31 green
cases on Linux were never evidence about the platform its `mapfile` fix was written for.

## Guidance

**Reasoning about BSD from documentation is not a substitute for running it, and it gets the sign
wrong in both directions.** The sweep that preceded this run recorded `xargs -r` as a live defect,
measured by simulating BSD `xargs` on Linux. macOS `xargs` **accepts** `-r`; its man page says the
flag exists for GNU compatibility and does nothing. The same sweep flagged bare `mktemp -d` as
probably broken; it works. Two asserted findings, both false, both argued carefully. Meanwhile the
four real defects above were not on the list at all.

**Prefer the construct both implement, rather than branching on platform.**

- Regex: `sed -E` with a POSIX extended regex. Never `\+`, `\?` or `\|` in a basic regex.
- In-place edit: read through a temp file and `mv`. There is no `sed -i` spelling that means
  in-place on both - GNU wants the suffix attached, BSD wants it as the next argument.
- Passing a list into awk: choose a separator that is not a newline. `-v` cannot carry one on BSD.
- Loading a helper under `set -e`: test `[ -r "$f" ]` **before** `source`, never `source ... ||`.
  On bash 3.2 the failed source is fatal and the guard after it is unreachable.

**When you must branch, probe - never fall back.** `stat -c %Y || stat -f %m` is the same defect one
level up: on GNU, `stat -f` exits 1 *while printing six lines of prose to stdout*, which then flows
into the arithmetic. Probe once, pick a branch, and validate the value's shape rather than trusting
the exit code.

**The bash on macOS is 3.2, from 2007.** It is not merely "old bash": it lacks `mapfile`, and its
`$( ... )` parser does not skip comments, so an apostrophe or an unbalanced paren in a comment
inside a substitution breaks the file. `/bin/bash -n <file>` over every tracked script is a cheap
sweep and catches that whole class in one pass.

**Check that the self-test RAN, not that it passed.** Three of the defects above lived inside test
harnesses, where the failure mode is a suite that reports success having tested nothing - or, for
the parse error, one that never starts. A test count that has not changed is not proof; on this run
the count was zero and nothing said so.

## Verification

The whole `bin/` suite now passes on macOS - 15 of 15 `test-check-*.sh`, plus
`bin/test-rename-packages.sh` and `bin/test-todo-index.sh` - against 11 of 15 and 7 failures
respectively at the start. Every fix has a control arm on a detached `origin/master` taken at the
same moment, so "this was already broken" and "I broke this" are distinguished by measurement rather
than by assumption. All 62 tracked shell scripts parse under bash 3.2.

**No CI lane runs any of this on macOS**, so the whole class is still caught only by somebody
happening to work on a Mac. That is the open item.
