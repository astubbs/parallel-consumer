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

## What the exposure measured, before the warm landed

Three properties were established on the rungs that met it, and they are what the *shape* of this
class looks like anywhere else it appears - so they are kept here rather than lost with the fix.

- **It is every lane that builds the module, not one.** On astubbs/parallel-consumer#395 the Unit and
  Integration lanes went down together on the same run, because they share the runner's route to
  Central.
- **It is per-run, not per-branch.** Across that PR the lanes went red, red, green, red. A re-run is
  a coin flip and a green run says nothing about the next one - which is the already-recorded
  strength of the upstream write-up's "re-running does not reliably help", read as written rather
  than as "never".
- **The attribution is settled by a control arm, not argued.** The run after the green one carried a
  **markdown-only commit** - no Java, no pom, no workflow - and reproduced the identical failure on
  both lanes. There is no reading of that in which the change under review is implicated.

## What is still open

<!-- post-merge: checked-begin -->
**Every branch carrying `parallel-consumer-streams` without that step still has the unwarmed
workflow**, and stays exposed until it merges astubbs/parallel-consumer#379 forward. The candidates
are
`for r in $(git for-each-ref --format='%(refname:short)' refs/remotes/origin); do git cat-file -e "$r:parallel-consumer-streams/pom.xml" 2>/dev/null && echo "$r"; done`,
minus those that already contain the step - `git grep -l 'Warm the Kafka sources jars' <ref> --
.github/workflows/maven.yml`.

**`feats/ks-streams-task-lifecycle` carries its own copy of this file**, written before the fix and
describing the defect as unfixed. It already contains `feats/ks-streams-fork-machinery`'s pre-fix
commits, so merging astubbs/parallel-consumer#379 forward collides add/add on this path: **take the
version that names the step**, not the one that calls the fix a candidate. The same collision waits
on every sibling rung cut from that base, and the same resolution applies.
<!-- post-merge: checked-end -->

**`test-kafka-compat` is the one job whose Kafka version falls outside the warm.** It is `if: false`
today; re-enabling it re-opens this for that lane only.

## Delete when

No open branch builds the streams module without that step - at which point nothing here is both
true and unowned by the workflow comment.
