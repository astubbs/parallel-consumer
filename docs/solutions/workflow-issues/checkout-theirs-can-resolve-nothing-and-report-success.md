---
title: "`git checkout --theirs` can resolve nothing at all and still report success"
date: 2026-09-07
category: workflow-issues
module: tooling
problem_type: workflow_issue
component: development_workflow
severity: high
root_cause: wrong_api
resolution_type: workflow_improvement
applies_when:
  - Merging master into a long-lived branch across the `io.confluent.*` -> `bz.stub.*` rename
  - Resolving conflicts in bulk with `git checkout --theirs` or `--ours`
  - Merging a branch whose parent was squash-merged, producing add/add conflicts
  - Sweeping a working tree for leftover conflict markers before committing a merge
  - Merging in a repo that holds a family of near-identical per-module files
symptoms:
  - "`javac` reports `class, interface, enum, or record expected` at several low line numbers of one file"
  - A conflict-marker sweep with `grep '<<<<<<< HEAD'` returns nothing while markers are present
  - "`git checkout --theirs` exits without complaint and `git ls-files -u` still lists the path"
  - A patch or `git apply` step fails after conflicts were apparently resolved
  - An ArchUnit test compiles and passes while pointing at another module's packages
tags:
  - merge-conflict
  - rename-rename
  - conflict-markers
  - package-rename
  - long-lived-branch
  - silent-failure
  - git
  - archunit
related_components:
  - build-system
  - testing_framework
---

# `git checkout --theirs` can resolve nothing at all and still report success

## Context

A long-lived branch was caught up with `origin/master` across this fork's `io.confluent.*` ->
`bz.stub.*` package rename. Both sides had already been renamed - the procedure
`bin/rename-packages.sh` requires, under its header block `RUNNING THIS ON EVERY OPEN PR BRANCH IS
MANDATORY, NOT A CONVENIENCE` - so the conflicts were overwhelmingly mechanical: paths that had
moved on both sides, with our side contributing nothing but the rename itself.

The bulk resolution was sound in principle. For each conflicted path, our stage-2 blob was
reverse-normalised (`bz.stub` -> `io.confluent`) and compared against the merge-base blob at its
pre-rename path; where they matched, our side had added nothing and master's version could be taken
wholesale. The mistake was in *how* master's version was taken: `git checkout --theirs -- <path>`
followed by `git add -- <path>`.

Nothing failed. Not the checkout, not the `add`, not the marker sweep that followed. The first
signal was `maven-compiler-plugin` reporting `class, interface, enum, or record expected` at a
handful of low line numbers in one test file - the line numbers of conflict markers that had been
staged as source.

**This was the second occurrence of the class, not the first.** A prior session on 2026-09-02, in
the quarantine-report worktree, hit the same flag on add/add conflicts inherited from a
squash-merged parent: after `git checkout --theirs -- <paths>` a follow-up patch application failed,
and that session's own diagnosis was that the index still had the paths unmerged. Different conflict
class, same flag, same silent no-op. That observation was never written down anywhere, which is why
it had to be rediscovered.

## Guidance

**Resolve by explicit ref, not by side.** This is the headline. Instead of asking git for "their"
version of a conflicted path - which is a question the index may have no answer to - name the ref and
the path you actually want:

```bash
git show "origin/master:<path>" > "<path>" && git add -- "<path>"
# equivalently, and already used elsewhere in this repo:
git checkout origin/master -- "<path>" && git add -- "<path>"
```

An explicit ref cannot silently resolve to nothing. If the path is absent on that ref, the command
fails loudly. `--theirs` has no such property, because it reads a stage that may not exist at the
path you named.

The repo already knew this technique and never wrote it down as a rule. `git checkout origin/master
-- ...` appears in `bin/rename-packages.sh` (in the `BRINGING AN OPEN BRANCH ACROSS` recipe), in
`docs/solutions/workflow-issues/red-proof-old-code-new-tests-2026-08-18.md`, and in
`docs/inflight/branch-package-rename-sweep.md` - each time as a way to *fetch a file from master*,
never as the way to *resolve a conflict*. A 2026-08-26 session resolving this very arch-test family
used it, having first classified each file as rename-only; this session arrived at the `git show`
form independently. Two sessions converging on the same technique without either writing the rule is
what this document exists to stop.

**Never treat a `--theirs`/`--ours` exit as evidence of resolution.** After any bulk resolution
pass, before you commit:

```bash
git ls-files -u                                              # must be empty
grep -rln '^<<<<<<<' . --exclude-dir=.git --exclude-dir=target   # must be empty
```

The first catches "left unmerged in the index" (the add/add instance). The second catches "written
into the worktree and staged anyway" (the rename/rename instance). Neither is optional, because the
two failure modes are disjoint - the rename/rename case leaves the index *clean* and the content
broken.

**Anchor the marker sweep on `^<<<<<<<` as a prefix.** Never on the word `HEAD`, never on exactly
seven brackets, never with a trailing space. A rename/rename conflict writes eight brackets and a
`:path` suffix; a directory-rename conflict writes seven brackets and a `:path` suffix. Both were
reproduced for this write-up (see Examples). `grep '<<<<<<< HEAD'` matches neither. Sweeps used in
prior sessions here were of the form `grep -n '^<<<<<<< \|^=======$\|^>>>>>>> '` - seven characters,
one of them with a trailing space - and would have missed this too.

**Expect the eight-bracket form to be mistaken for a typo.** A 2026-08-26 session saw such a marker
sequence, called it "malformed", and initially explained it away as prose quoting conflict markers.
It reads like a mistake, so it gets waved past. It is not a mistake; it is what git writes when the
two sides disagree about the path as well as the content.

**Verify the whole near-identical family, not just the file that broke.** Where a repo holds one
near-identical file per module, git's rename detection can pair them across modules. A mis-paired
file is not merely wrong - it can *compile and pass*, which means nothing at any stage of the build
will tell you. Prove every member byte-identical to the ref you meant to take, and prove each one's
own identity matches its own path.

**Run the loop through `bash -c`, and check it errored rather than found nothing.** A prior session's
sweep loop was written in fish syntax, errored, and had to be re-run - a sweep that errors is
indistinguishable from a sweep that found nothing, which is the same silent-false-negative shape as
everything else in this document.

## Why This Matters

`AGENTS.md` carries the rule that came out of the previous `--theirs` incident, under the PR
Discipline bullet whose text begins **"`--theirs`/`--ours` take the whole file; a conflict is one
hunk."** That rule is correct for the ordinary case and it is exactly the wrong preparation for this
one. It teaches you to fear the flag taking **too much**. On a rename/rename conflict the flag takes
**neither side** - and a reader primed for over-application will check what was lost, find nothing
missing, and move on.

That is also what distinguishes this write-up from its neighbour,
`docs/solutions/workflow-issues/theirs-took-the-whole-file-and-the-repair-stopped-at-the-tests-2026-08-18.md`.
The two are not duplicates and should be read as a pair:

- **That doc: the flag resolved the conflict and took more than you asked for.** The damage is
  *deletion*, invisible in a diff-vs-base because a merge that takes the other side renders as
  nothing at all.
- **This doc: the flag resolved nothing and reported success.** The damage is *non-resolution*,
  invisible because the index looks clean or the file looks staged.

Two independent silent-failure modes came out of this incident, and they fail at different distances
from the mistake:

**A staged file with markers in it.** Caught only by the compiler, and only for languages with one.
Here it was Java, so `javac` produced `class, interface, enum, or record expected` and the merge was
stopped. In markdown, YAML, `.adoc` or a shell script - where this repo's merges *actually* conflict
- nothing would have gone red. That is precisely the incident recorded in
`docs/inflight/ci-conflict-marker-gate.md` ("**A merge was committed and pushed with most of one
file still inside an unresolved conflict, and nothing went red.**"), and the gate that note proposes
still does not exist: `bin/check-conflict-markers.sh` is absent from `bin/`.
**That note's own
description of the markers - "`git` writes `<<<<<<<`, `=======` and `>>>>>>>`" - is the seven-
character form only.** Whoever writes that gate must match a *prefix*, or it will ship blind to the
exact conflict class that motivated this document.
<!-- file-refs: N/A - names the gate that note PROPOSES, which deliberately does not exist yet -->

**A mis-paired file that compiles.** Caught by nothing at all. `TestConventionsArchTest` exists once
per module, and the copies differ only in their `package` declaration and the value of
`@AnalyzeClasses(packages = "...")` - verified by reading the reactor and examples-core copies, which
are otherwise byte-identical down to the javadoc. Rename detection paired reactor's against
examples-core's, and mutiny's against examples-vertx's. A file that survives that pairing still
compiles, still runs, still goes green - while pointing ArchUnit at another module's packages. The
module it was supposed to cover is silently unanalysed and no test fails to say so. This is the same
mis-pairing `bin/rename-packages.sh` warns about in its header ("silently applied the PR's edit to
the streams module's ArchUnit test INTO THE MUTINY MODULE'S FILE"); what is new is that renaming
*both* sides converts it from silent data loss into a conflict that `--theirs` then fails to
resolve.

Note also that the count of that family stated in `AGENTS.md` has drifted from the tree, so do not
trust any written number for it. `find . -name TestConventionsArchTest.java -not -path '*/target/*'`
is the answer.

## When to Apply

- **Any merge that crosses a rename** - the package rename above, a module move, a file
  reorganisation. Rename detection is what produces the conflict class, so the risk scales with how
  much moved.
- **Any merge into or out of a branch whose parent was squash-merged.** The squash makes the
  parent's content arrive as new, producing add/add conflicts - the 2026-09-02 instance.
- **Any repo holding a family of near-identical per-module files** - arch tests, module-local test
  conventions, per-module logback or `pom.xml` fragments, a generated file replicated per module. Ask
  the question directly: *is there more than one file in this tree that looks almost exactly like the
  one I just resolved?* If yes, verify all of them.
- **Any bulk conflict resolution**, whatever the technique. The two verification commands cost
  seconds; the mis-paired-arch-test failure mode costs a silently unanalysed module for as long as
  nobody looks.

Not needed for a single hand-edited conflict hunk in a file you read afterwards - the point of the
rule is bulk work, where nobody reads the result.

## Examples

### The marker forms, reproduced


Both forms below were produced from a scratch repository with git 2.51.2, using `git merge-tree
--write-tree --messages` so no refs or commits were involved. A true rename/rename - the same base
file renamed to two different paths on the two sides - gives **eight** brackets and a `:path`
suffix, and writes the identical conflicted content to **both** destination paths:

```
CONFLICT (rename/rename): x/A.java renamed to x/B.java in <side1> and to x/C.java in <side2>.
<<<<<<<< <side1>:x/B.java
package bz.stub.pc.reactor;
========
package bz.stub.pc.examples.core;
>>>>>>>> <side2>:x/C.java
```
<!-- file-refs: N/A - synthetic fixture paths from a throwaway repo used to reproduce the marker shape -->

The real incident's markers had the same shape with branch names as labels:
<!-- file-refs: N/A - x/A.java, x/B.java and x/C.java are synthetic fixtures in a throwaway repo, not paths in this tree -->

```
<<<<<<<< HEAD:parallel-consumer-reactor/src/test/java/bz/stub/parallelconsumer/reactor/TestConventionsArchTest.java
package bz.stub.parallelconsumer.reactor;
========
package bz.stub.parallelconsumer.examples.core;
>>>>>>>> origin/master:parallel-consumer-examples/parallel-consumer-example-core/src/test/java/bz/stub/parallelconsumer/examples/core/TestConventionsArchTest.java
```

A *directory*-rename conflict on the same shape of input gives **seven** brackets - and still a
`:path` suffix:

```
CONFLICT (file location): a/Old.java renamed to a/TestConventionsArchTest.java in <side1>, inside a
directory that was renamed in <side2>, suggesting it should perhaps be moved to
b/TestConventionsArchTest.java.
<<<<<<< <side1>:a/TestConventionsArchTest.java
package bz.stub.parallelconsumer.reactor;
=======
package bz.stub.parallelconsumer.examples.core;
>>>>>>> <side2>:b/TestConventionsArchTest.java
```
<!-- file-refs: N/A - synthetic fixture paths from a throwaway repo used to reproduce the marker shape -->

So neither the bracket count nor the presence of a `:path` suffix is a reliable discriminator. The
only stable signal is the marker at the start of a line, matched as a prefix.
<!-- file-refs: N/A - the a/ and b/ paths above are the same throwaway-repo fixtures -->

### Why `--theirs` has nothing to give


The index for that reproduced rename/rename conflict:
<!-- file-refs: N/A - the index listing below names the throwaway-repo fixture paths -->

```
100644 <blob-base>  1  x/A.java     <- base, at the base path
100644 <blob-side>  2  x/B.java     <- ours, at OUR path
100644 <blob-side>  3  x/C.java     <- theirs, at THEIR path
```
<!-- file-refs: N/A - synthetic fixture paths from a throwaway repo used to reproduce the marker shape -->

Stages 2 and 3 are recorded at **different paths**. `--theirs` means "write stage 3 of the path I
named"; for `x/B.java` there is no stage 3 to write. Per this session's observation the command did
not stop the pipeline, and the following `git add` staged the marker-laden worktree file, resolving
the index around broken content. (The exact exit code was not measured at the time and this
repository's history-rewrite hook blocks `git checkout` in a scratch repo, so that half was not
re-derived here - the index shape above, which is the mechanism, was.)
<!-- file-refs: N/A - x/B.java is the throwaway-repo fixture named in the listing above -->

### The failing and working sweeps

```bash
# WRONG - matches neither the seven- nor the eight-bracket path-labelled form
grep -rn '<<<<<<< HEAD' .

# WRONG - seven characters, and the trailing space rules out the ':path' forms
grep -n '^<<<<<<< \|^=======$\|^>>>>>>> ' <file>

# RIGHT - prefix-anchored, bracket-count agnostic
grep -rln '^<<<<<<<' . --exclude-dir=.git --exclude-dir=target
```

### The fix

```bash
git show "origin/master:<path>" > "<path>" && git add -- "<path>"
```

### Verifying the near-identical family

Run it through `bash -c` if your shell is fish, and treat any output at all - including an error
from the loop itself - as a failure:

```bash
for f in $(find . -name TestConventionsArchTest.java -not -path '*/target/*' | sed 's#^\./##'); do
  diff <(git show "origin/master:$f") "$f" >/dev/null || echo "DIFFERS: $f"
done
```

Then check identity as well as content - each file's `package` declaration and its
`@AnalyzeClasses(packages = ...)` value must match its own module path, not a sibling's:

```bash
grep -rn '@AnalyzeClasses' --include=TestConventionsArchTest.java .
```

Byte-identical-to-master is the strong check; the `@AnalyzeClasses` check is the one that survives a
future where the file legitimately differs from master.

### Where this happened

The merge was performed on the branches behind astubbs/parallel-consumer#105 and
astubbs/parallel-consumer#106. The prior instance of the same defect class, on add/add conflicts
inherited from a squash-merged parent, is recorded here from a 2026-09-02 session's own diagnosis
and has no other durable home.
