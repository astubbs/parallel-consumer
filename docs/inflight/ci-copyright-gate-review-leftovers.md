# The copyright gate: what widening it past Java left open

<!-- inflight-type: task -->
<!-- inflight-impact: ci -->

<!-- post-merge: checked-begin -->
`bin/check-copyright-headers.sh` used to look only at Java, and reported `255 java files, 0
violations` - a clean tree of one file type out of a dozen. astubbs#338 widened it to the whole
tree, fixed the 41 files that turned up, and closed five ways the widened gate still went green on
a file it should have failed. What is below is what that review pass turned up and did **not**
close. Delete this note when these are resolved.
<!-- post-merge: checked-end -->

## Policy calls, not defects

- **`.claude/*` is exempt, and that exempts seven hand-written hook scripts** which already carry
  correct headers. Narrowing the exemption is verified green (416 files, 0 violations). Whether
  unshipped agent tooling owes a notice is a call for the author, not something the gate can decide.
- **The grandfathering rule is stated twice**, in the scanner and in `docs/copyright.md`. Trimming
  either increases divergence from the language-proxy stack the scanner was extracted from.
- **`AGENTS.md` is past its own backstop.** That file declares "`wc -l AGENTS.md` past ~400 lines
  means something situational has crept in"; it is now 531.

## Two heuristics the review flagged and this work did not change

- **Only the FIRST notice line is placement-checked.** `syntax_violation()` inspects
  `HDR_LINES[$NOTICE_IDX]`, the topmost `Copyright (C)` line, so a dual header's
  `Modifications Copyright` line is never tested for being inside a comment. The shebang and
  `<?xml ?>` halves cannot be reached this way - a later line is by definition below a first line
  that already passed - so the live case is a mods line written outside the comment syntax. It is
  not silent, which is why it is recorded rather than fixed: the file breaks for its own toolchain
  (`Modifications: command not found` in shell, a compile error in Java, an unparseable document in
  XML). Worth closing when the scanner is next opened.
- **The same-line Confluent-claim test can false-positive on header prose.** A fork-original file
  putting `Copyright (C)` and `Confluent` on one physical line reads as a Confluent claim. No file
  does today, and the failure is a red build rather than a silent pass, so this is the deliberate
  trade-off the same-line design already argues for in the script - noted so the next reader does
  not re-derive it.

## The defect class is wider than this gate: a glob narrower than the claim

<!-- post-merge: checked-begin -->
A gate whose glob covers less than its summary line claims reports a clean subset as a clean tree.
astubbs#338 fixed one instance. Two more stand:
<!-- post-merge: checked-end -->

- **`bin/check-shell-sigpipe.sh` globs `bin/*.sh` and `.claude/hooks/*.sh`, one directory level
  each**, so `bin/lib/`, `scripts/` and the two repo-root scripts are outside it. A scope gap rather
  than live instances, and both halves of that are worth keeping: of the five excluded files, four
  carry no `set -o pipefail` at all, and `scripts/upstream-sweep.sh`'s `grep -qx` reads a file
  argument rather than a pipe, which is not the pattern that bites.
- **`.github/scripts/file-ref-gate.js` reads citations from `.md`/`.adoc`/`.txt`/`.html` only**,
  which is why a dangling path inside a `.sh` was invisible. It already grew once, for `.html`, so
  this is the second recurrence and nothing records the class.

Three instances is enough to be predictive - this wants a `docs/solutions/` entry, not a third
one-off fix.
