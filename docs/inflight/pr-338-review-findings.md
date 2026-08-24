# astubbs#338 - the copyright gate: what the review left open

<!-- inflight-type: task -->
<!-- inflight-impact: ci -->

Findings from the simplify-and-review pass on this branch that astubbs#338 does **not** close. The
five gate holes it found *were* closed on the branch; what is below is what remains. Delete this
note when these are resolved, not when the PR merges.

## Decisions for the author

- **The PR body omits the `AGENTS.md` rule the second commit ships**, while ticking "Title & body
  reflect the final content". A GitHub write, so it was not made here.
- **`AGENTS.md` is past its own backstop.** That file declares "`wc -l AGENTS.md` past ~400 lines
  means something situational has crept in"; it is now 531. The new entry also *retells* its
  incident rather than citing it, against the same file's "cite incidents, never retell them".
- **`.claude/*` is exempt, and that exempts seven hand-written hook scripts** which already carry
  correct headers. Narrowing the exemption is verified green (416 files, 0 violations). Whether
  unshipped agent tooling owes a notice is a policy call, not a defect.
- **The grandfathering prose is stated twice**, in the scanner and in `docs/copyright.md`. Trimming
  either increases divergence from the language-proxy origin the scanner was extracted from.

## The defect class is wider than this gate: a glob narrower than the claim

astubbs#338 fixes one instance - a gate reporting `255 java files, 0 violations`, which reads as a
clean tree and was a clean tree of one file type. Two more instances stand:

- **`bin/check-shell-sigpipe.sh` scans `bin/*.sh` only.** Reported as a scope gap rather than live
  instances: four excluded files carry no `set -o pipefail` at all, and `scripts/upstream-sweep.sh`
  uses `grep -qx` against a file argument.
- **`.github/scripts/file-ref-gate.js` reads citations from `.md`/`.adoc`/`.txt`/`.html` only**,
  which is why a dangling path inside a `.sh` was invisible. It already grew once, for `.html`, so
  this is the second recurrence and nothing records the class.

Three instances is enough to be predictive - this wants a `docs/solutions/` entry, not a third
one-off fix.

## Pre-existing, unrelated to this PR

`bin/soak-test.sh` still passes `-Dlicense.skip`, which `docs/copyright.md` says no longer exists.
