# Two open branches rewrote the copyright scanner from the same base

<!-- inflight-type: task -->
<!-- inflight-impact: coordination -->

`bin/check-copyright-headers.sh` exists on `master` in its Java-only form, and **two open branches
have independently rewritten it from that same base**. Whichever lands second inherits a large
conflict in that one file, and resolving it the wrong way is silent.

<!-- post-merge: checked-begin -->
Measured at the time of writing (merge base `0e21b903d`): master to astubbs#338 is +609/-59, master
to astubbs#340 is +477/-40, and the two copies differ from each other by +202/-89 across ~24 hunks.
<!-- post-merge: checked-end -->

## Why the resolution is not symmetric

<!-- post-merge: checked-begin -->
astubbs#338 is the later of the two and is the extraction of the other's work, so its copy strictly
supersedes. Everything astubbs#340's copy has that astubbs#338's lacks is precisely the code
astubbs#338 replaced on purpose:
<!-- post-merge: checked-end -->

- `HAS_FORK_HOLDER`, which tested for the holder's **name** rather than a **notice** - the hole that
  let a header reading only `@author Antony Stubbs and contributors` pass green in any language;
- the inline `hash`-only shebang check, since replaced by the shared `shebang_below_header()` that
  covers `slash` too;
- `fp_path_of()`, removed when the fork-point lookup was reworked;
- the prose exemption's **retired false reason**, "the notice would render into the document".

None of the five gate-hole fixes are present in the other copy, and the polyglot rows
(`*.go`, `*.rs`, `*.swift`, `*.kt`, `*.c`, `go.mod`, `*.mjs`) are byte-identical in both, so nothing
is lost by taking the newer file whole.

**So the resolution is: take the newer copy for this file entire, then re-apply only genuinely new
entries.** Merging the other direction, or hand-resolving hunk by hunk, reinstates a scanner that
reports a clean tree while not doing its job - the exact failure the widening exists to end, and one
that goes green rather than red.

Delete this note when the second of the two lands.
