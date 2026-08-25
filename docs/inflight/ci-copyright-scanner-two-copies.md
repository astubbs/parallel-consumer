# Two open branches rewrote the copyright scanner from the same base

<!-- inflight-type: task -->
<!-- inflight-impact: coordination -->

`bin/check-copyright-headers.sh` exists on `master` in its Java-only form, and **two open lines of
work have independently rewritten it from that same base**. Whichever lands second inherits a large
conflict in that one file, and resolving it the wrong way is silent.

## Look at astubbs#331, not the branch stacked above it

**`gh pr diff astubbs/parallel-consumer#340` shows a one-line change to this script and is
misleading.** The rewrite lives on `feats/polyglot-demos` (astubbs#331); astubbs#340 is stacked on
top of it and adds only the `*.options|hash` row. A reader who checks the topmost PR sees a trivial
diff and concludes there is nothing to reconcile - which is the same shape of false negative this
whole class of finding is about. Diff the blob, not the PR.

<!-- post-merge: checked-begin -->
Measured at the time of writing (merge base `0e21b903d`): master to astubbs#338 is +609/-59, master
to astubbs#331 is +476/-40, astubbs#331 to astubbs#340 is +1/-0, and astubbs#331's copy differs from
astubbs#338's by +203/-89 across ~24 hunks.
<!-- post-merge: checked-end -->

## Why the resolution is not symmetric

<!-- post-merge: checked-begin -->
astubbs#338 is the later of the two and is the extraction of the other's work, so its copy strictly
supersedes. Everything astubbs#331's copy has that astubbs#338's lacks is precisely the code
astubbs#338 replaced on purpose:
<!-- post-merge: checked-end -->

- `HAS_FORK_HOLDER`, which tested for the holder's **name** rather than a **notice** - the hole that
  let a header reading only `@author Antony Stubbs and contributors` pass green in any language;
- the inline `hash`-only shebang check, since replaced by the shared `shebang_below_header()`, so
  the older copy still leaves the gap open for `slash`-style files (`.js`, `.mjs`);
- `grandfathered()` failing **open** on an unreadable blob, and plain `git ls-files` rather than
  `-c core.quotePath=false`, so non-ASCII paths are still silently skipped there;
- `fp_path_of()`, removed when the fork-point lookup was reworked;
- the prose exemption's **retired false reason**, "the notice would render into the document".

<!-- post-merge: checked-begin -->
None of the five gate-hole fixes are present in the older copy. The polyglot rows (`*.go`, `*.rs`,
`*.swift`, `*.kt`, `*.c`, `go.mod`, `*.mjs`) are byte-identical in both, and `*.options|hash` - the
one row the stacked branch adds - **is already in astubbs#338's copy too**, so there is nothing left
to re-apply afterwards.
<!-- post-merge: checked-end -->

**So the resolution is: take the newer copy for this file entire.** Merging the other direction, or
hand-resolving hunk by hunk, reinstates a scanner that reports a clean tree while not doing its job -
the exact failure the widening exists to end, and one that goes green rather than red.

The self-test, `bin/test-check-copyright-headers.sh`, is untouched by the other branches, so this
one script is the whole of the conflict.

Delete this note when the second of the two lands.
