# The streams module's `sources` jars are warmed HERE, and nowhere else yet

<!-- inflight-type: bug -->
<!-- inflight-impact: misdirection -->

`parallel-consumer-streams` (astubbs#255) fetches Kafka's `sources` and `test-sources` classifier
jars through `dependency:unpack` `<artifactItems>`, which `dependency:go-offline` does not resolve -
so they were fetched live from Maven Central inside `generate-sources` on every lane, in exactly the
phase where the region-dependent read timeout in
[`docs/solutions/build-errors/maven-central-timeout-azure-west-regions-2026-04-21.md`](../solutions/build-errors/maven-central-timeout-azure-west-regions-2026-04-21.md)
bites. It presented as Unit **and** Integration red at `unpack (unpack-kafka-streams-sources)` with
zero tests run, so the lane called "Unit Tests" named a subsystem that never reached compilation.

<!-- post-merge: checked-begin -->
**Fixed by astubbs/parallel-consumer#379**, which added the `Warm the Kafka sources jars the streams
module unpacks` step to `prepare-deps` in `.github/workflows/maven.yml` - so wherever that step is
present, this is closed. **That step's own comment is the durable owner** of why go-offline
misses an artifactItem, why the warm names coordinates rather than building the module, and what the
two guards are for; this note does not restate it.
<!-- post-merge: checked-end -->

## What the sightings established, for any branch still exposed

<!-- post-merge: checked-begin -->
The defect was observed on two independent rungs of this stack -
astubbs/parallel-consumer#394 and astubbs/parallel-consumer#395 - which is what made it master-state
rather than one PR's problem. Three findings from those sightings outlive the fix, because they
describe what an *unwarmed* branch is still living with:

- **It is every lane that builds this module, not one lane occasionally.** Unit and Integration went
  red together on the same run, because they share the runner's route to Central.
- **It is per-run, not per-branch, so a re-run is a coin flip and not a fix.** The referenced
  write-up's "re-running does not reliably help" proved to be exactly the right strength: two
  consecutive runs reproduced the identical failure at the same execution on the same artifact, and a
  third then passed every lane. A green run is luck, and says nothing about the next one.
- **It has nothing to do with the diff, and a control arm settles that rather than arguing it.** A
  **markdown-only commit** - no Java, no pom, no workflow - reproduced the failure on both lanes
  immediately after a green run. There is no reading of that in which the change under review is
  implicated.

If you are on an unwarmed branch looking at a red Unit lane on this module and wondering what you
broke: the answer is the paragraph above, and the fix is to merge astubbs/parallel-consumer#379
forward rather than to re-run.
<!-- post-merge: checked-end -->

## What is still open

<!-- post-merge: checked-begin -->
**Every branch carrying `parallel-consumer-streams` without that step still has the unwarmed
workflow**, and stays exposed until it merges astubbs/parallel-consumer#379 forward. The candidates
are
`for r in $(git for-each-ref --format='%(refname:short)' refs/remotes/origin); do git cat-file -e "$r:parallel-consumer-streams/pom.xml" 2>/dev/null && echo "$r"; done`,
minus those that already contain the step - `git grep -l 'Warm the Kafka sources jars' <ref> --
.github/workflows/maven.yml`.

**A branch carrying the pre-fix copy of this file collides add/add on this path** when it merges
astubbs/parallel-consumer#379 forward: **take the version that names the step**, not the one that
calls the fix a candidate. The two rungs that hit it, `feats/ks-streams-task-lifecycle` and
`feats/ks-streams-error-surfacing`, resolved it that way and are no longer exposed; the instruction
stands for any rung above them that has not yet merged forward.
<!-- post-merge: checked-end -->

**`test-kafka-compat` is the one job whose Kafka version falls outside the warm.** It is `if: false`
today; re-enabling it re-opens this for that lane only.

## Delete when

No open branch builds the streams module without that step - at which point nothing here is both
true and unowned by the workflow comment.
