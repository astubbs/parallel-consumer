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
