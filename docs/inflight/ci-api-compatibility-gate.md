# `api: breaking` - the Java API-compatibility gate

The gate exists and runs; the **policy around it is still open**. Plan and evidence:
[`docs/plans/2026-08-18-001-ci-java-api-compatibility-gate-plan.md`](../plans/2026-08-18-001-ci-java-api-compatibility-gate-plan.md).

## What is decided

japicmp, wired behind an opt-in `api-compat` Maven profile, driven by `bin/check-api-breaking.sh`
and pinned by `bin/test-check-api-breaking.sh`. Baseline is the `0.6.0.0-SNAPSHOT` on Maven Central
that `publish.yml` redeploys on every green master push. Design copied from
`bin/check-proto-breaking.sh` (astubbs#242): self-arming grace branch, self-test first, exit 2 for
cannot-run.

## What is open, and blocks nothing yet

- **It is not a required status check**, deliberately - a required context no run has produced blocks
  every PR whose base predates it. Arming is a follow-up **after** the job is on master. This is the
  same state `tooling: package rename` is in.
- **What should happen on red** is undecided: hard block, advisory, or block-unless-recorded. Today
  the job fails on a break but gates nothing, so the practical answer is "advisory" by accident
  rather than by choice.
- **The public/internal boundary is inherited, not chosen.** Everything `public` outside
  `**.internal.**` counts, which sweeps in `.state`, `.offsets` and `.metrics`. Nobody has decided
  those are public API; the gate currently asserts they are.

## Trap worth knowing before touching this

The published baseline carries the **same version string** as the working tree, so a coordinate-based
comparison resolves both sides to the local artifact and reports `No changes.` green forever. Two
further variants (a failed rebuild leaving a stale jar) do the same. All three were hit while building
this and all three exited 0. The script refuses to run rather than pass when it cannot tell the two
sides apart; do not "simplify" that away. Class:
[`a-check-that-reports-success-without-having-run.md`](../solutions/workflow-issues/a-check-that-reports-success-without-having-run.md).
