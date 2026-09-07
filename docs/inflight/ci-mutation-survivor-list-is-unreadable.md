# The mutation lane's most valuable output lands where nothing can read it

<!-- inflight-type: task -->
<!-- inflight-impact: ci -->

**The lane now works and produces a real survivor table - and that table is written only to
`$GITHUB_STEP_SUMMARY`, which the REST API does not expose.** So the one output worth acting on is
the one output no tool, script or agent can retrieve. Recovering four survivors from a merged PR
cost a ~25-minute local PIT re-run and a hand-written `mutations.xml` parser.

**This is not news to the repo, which is the point.** `bin/check-pr-analysis-surfaces.sh` - the tool
whose whole job is "read the findings on your own PR" - already lists this under **"SURFACES THIS
SCRIPT CANNOT READ - open these by hand"**, naming the PIT survivor table specifically. The note has
been sitting there waiting for the lane to start producing something worth reading. It now does.

## Where exactly it is lost

`bin/ci-mutation-test.sh` builds a complete, sorted, de-duplicated table into `$SURVIVORS` - status,
`file:line`, and the mutation description, with HTML entities decoded and the noisy fully-qualified
paths stripped. It is well-formed and ready to consume. It is then emitted through `summary()`,
which appends to `$GITHUB_STEP_SUMMARY` and nowhere else. The job step itself says so out loud:
`"PIT scored mutants - see the job summary for the survivor list."`

Grep `summary "#### Survived"` in that script for the site.

## Three fixes, cheapest first - none of them implemented yet

1. **Echo the table to stdout as well.** One line. `gh run view --log` then carries it, and it lives
   as long as the log does. Fixes exactly the failure that produced this note.
2. **Upload `mutations.xml` and the HTML report as an artifact.** There is already
   `actions/upload-artifact@v7` precedent in the same workflow (the chaos suite uses it). This is the
   strongest of the three: complete, machine-readable, and it makes survivor sets diffable across
   runs. It also makes the table's deliberate 50-row cap harmless, since the full data is always
   recoverable.
3. **Emit `::warning file=...,line=...::` annotations for survivors on lines the PR wrote.** The one
   with real leverage: survivors would appear on the Files Changed tab *and* become readable by
   `bin/check-pr-analysis-surfaces.sh`, which already parses annotations - closing the gap that
   script documents rather than adding a fourth place to look.

**Recommended: 1 + 2 together.** Both are pure additions with no behaviour change and no reviewer
noise. **3 is deliberately held back**: it changes what every reviewer sees on a lane that is
non-gating on purpose, so the noise question deserves its own decision rather than arriving folded
into a plumbing fix.

## Why this matters more than a missing convenience

A survivor is not self-evidently a test gap. It can equally be an **equivalent mutant** - unkillable
because an invariant no test states makes the two versions behaviourally identical. Telling those
apart is the judgement the reader has to apply, and the whole value of the lane. A reader who cannot
retrieve the list cannot apply it, so the lane's output degrades to a single number that says
"88%" and nothing about what the 12% is.

The worked example is in [`test-partitionstate-mutation-survivors.md`](test-partitionstate-mutation-survivors.md):
of the survivors on astubbs#344's own new code, the interesting one is almost certainly equivalent
rather than missing.
