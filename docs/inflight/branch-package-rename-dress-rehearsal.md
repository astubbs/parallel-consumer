# Package-rename dress rehearsal — plan written and reviewed, not executed

Branch `docs/plan-package-rename-dry-run`, pushed, **no PR by decision** — this is a rehearsal, not a
change to land. The plan itself is
[`docs/plans/2026-08-12-001-refactor-package-rename-dry-run-plan.md`](../plans/2026-08-12-001-refactor-package-rename-dry-run-plan.md)
on that branch. Read it first; this file is only what the plan and the commands cannot tell you.

Sibling entry: [`branch-package-rename.md`](branch-package-rename.md) is the rename project's ledger
and stays the canonical entry. This file covers the rehearsal specifically and should be `git rm`'d
when the rehearsal is done, with anything durable folded into the ledger.

## State

Plan complete: written, reviewed by five reviewers, corrected across three commits. **No
implementation unit has run.** No rehearsal branch exists, no rename has been performed anywhere.

## The decision blocking U1

**Where does U7's procedure block land?** The plan says astubbs#280's branch, because U2 merges
astubbs#280 into `rename-stage`, and that is what makes the tooling arrive carrying its own
instructions. The owner has since said no PRs will be opened for the rehearsal. Committing to
astubbs#280's *existing* branch is not opening a PR, so these are probably compatible — but it was
asked and never answered. The alternative, authoring the block on the rehearsal branch, weakens the
test: what U4 follows would not be what a real branch receives.

## Wrong paths already walked — do not retry these

- **"Branches pick the script up by updating from master."** Once the rename is on master a stale
  branch needs the script *before* it can merge master, and merging master is the operation the
  script exists to prevent. Acquisition must not be a merge. It also cannot be a cherry-pick:
  astubbs#280 has eleven non-merge commits across the tooling files, so no single commit suffices.
  It is `git checkout <ref> -- bin/rename-packages.sh bin/check-copyright-headers.sh`, **both files** —
  taking only the first leaves a checker that cannot resolve provenance across the move, and
  astubbs#277 §3 measured that as 197 violations.
- **"Re-running the script brings a drifted branch current."** False for half the change classes, and
  verified against the code rather than reasoned about. `PATH_SCAN_ERE` is built by
  `build_path_scan_ere` from the **old side of `PKG_MAP` only**, so an already-renamed tree matches
  nothing and the run exits 0 at `already applied, nothing to do`. Self-healing holds only for
  old-side `PKG_MAP` additions and exclusion-list *removals*. **A destination change is a second
  migration, not a rule edit** — and no procedure for one exists.
- **Pinning a sha for the fan-out instruction.** Freezes the instructions, which must evolve, while
  doing nothing about the transformation rules, which must not drift.

## Line anchors in `bin/rename-packages.sh` verified 2026-08-12

On `tooling/package-rename-script` at `dc87cff0`. Approximate — grep the name rather than trust the
number; this repo's own convention is to cite by greppable anchor, not by line (astubbs#283).

| Anchor | What is there | Why it matters |
|---|---|---|
| `check_prose_guards` ~`:1330` | runs **before** the dry-run exit ~`:1332` | a bare `--dry-run` aborts with `FAIL:` and never prints the work set; `--defer-prose` is required on dry runs |
| dirty-tree refusal ~`:1347` | sits *after* the prose guard | the prose correction must be committed before re-running, or exit 2 |
| `build_path_scan_ere` ~`:506` | derives discovery from the old side only | the reason re-running an already-renamed tree is a no-op |
| `already applied, nothing to do` ~`:1305` | exit 0 | the not-self-healing case is indistinguishable from a correct no-op |
| `SELF_BASENAMES` ~`:352` | excludes the script from its own rewrite, by basename | the script survives renaming the tree it lives in |
| `retarget_copyright_manifest` ~`:944` | targeted edit to the newpath half only | the checker is frozen from the bulk rewrite but still moves with the rename |

## What the rehearsal will not tell you

astubbs#260 is the only counterparty, and it touches **none** of the five near-identical
`TestConventionsArchTest.java` files that git mis-pairs. So the rehearsal never exercises the
mis-pairing case astubbs#277 §4.5 measured. A clean result does not cover it.
astubbs#266/268/269/271 do touch those files.

Also unmeasured: whether a branch merged in the *wrong* order can be recovered, and whether recutting
a branch onto renamed master is a viable alternative to rename-then-merge.

## Environment

No JDK on the owner's current machine, and they intend to migrate rather than install one. The plan's
units run on git and bash alone, but `--skip-readme-regen` is then mandatory, which **excuses
README.adoc from the completeness check** — a real hole, recorded in the plan as a named gap rather
than absorbed as a caveat. Deferred to the new machine: astubbs#277 §6's verification — the mutation
lane observed scoring mutants, an ArchUnit rule deliberately broken and seen going red, a real
compile, and README.adoc regenerated.
