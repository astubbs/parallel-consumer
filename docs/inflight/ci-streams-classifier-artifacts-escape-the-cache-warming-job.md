# The streams module's `sources`/`test-sources` artifacts are not pre-warmed, so every run of it fetches from Central live

<!-- inflight-type: bug -->
<!-- inflight-impact: misdirection -->

<!-- post-merge: checked-begin -->
`parallel-consumer-streams` (astubbs#255). First seen as a red Unit lane on the task-lifecycle rung,
astubbs/parallel-consumer#394, which had **nothing to do with the tests** on it. Seen again, with the
same signature, on the error-surfacing rung stacked above it, astubbs/parallel-consumer#395 - two
independent branches, which is what makes it master-state rather than one PR's problem.
<!-- post-merge: checked-end -->

## What it looks like

The Unit lane goes red with the streams module `FAILURE` and every other module `SUCCESS`, at
`maven-dependency-plugin:unpack (unpack-kafka-streams-sources)`:

```
Could not transfer artifact org.apache.kafka:kafka-streams:jar:sources:3.9.2
from/to central (https://repo1.maven.org/maven2/): Read timed out
```

**No test ran.** The module dies in `generate-sources`, so a reader looking for a failing test finds
none, and the lane's name says "Unit Tests". That is the misdirection: the signal names the wrong
subsystem.

## Why the cache-warming job does not cover it

`prepare-deps` in `.github/workflows/maven.yml` warms the cache with `dependency:go-offline`, which
resolves the **declared dependency graph**. This module additionally fetches artifacts by explicit
`artifactItems` in two `dependency:unpack` executions - Apache Kafka's `sources` and `test-sources`
classifiers - and those are not part of that graph. So they are downloaded live from Maven Central on
every run of this module, in exactly the phase where the region-dependent timeout class already
documented in
[`docs/solutions/build-errors/maven-central-timeout-azure-west-regions-2026-04-21.md`](../solutions/build-errors/maven-central-timeout-azure-west-regions-2026-04-21.md)
bites.

That write-up's own conclusion is the relevant one: **re-running does not reliably help**, because the
runner is often reassigned to the same region, and the fix that worked was pre-warming so nothing is
fetched from Central during the build. This module is the one place that was left outside the fix.

**That prediction has now been tested and held.** A fresh push on the error-surfacing rung re-ran the
lanes and reproduced the identical failure, at the same execution, on the same artifact - and this
time it took the **Integration lane down with the Unit lane**, on the same run. So the exposure is not
"one lane occasionally", it is *every lane that builds this module*, and re-running is not a
workaround anyone should keep reaching for. It is a blocker for any PR touching this module, and it
cannot be cleared from inside one.

## Candidate fix

Add the classifier artifacts to what `prepare-deps` pulls, so the warm cache actually contains
everything a build needs. `dependency:go-offline` will not do it on its own; the honest options are
running the module's `generate-sources` phase in the warming job, or a `dependency:get` per artifact
item. Whichever is chosen, the check that it worked is that the streams module's `unpack` executions
log a cache hit rather than a `Downloading from central` line.

## Until it is fixed, what a blocked PR should do

**Not keep re-running.** Say in the PR that the red lane is this, name this note, and let the
module's own lane stand as the evidence the code is healthy: run the module's whole `test` phase and
its integration lane locally, and read the counts out of the report directories. A local run has the
artifacts in `~/.m2` already, which is exactly why it never reproduces there - and exactly why
"green locally" is a real signal about the code and no signal at all about this.

## Delete when

A streams-module CI run shows the unpack executions resolving from the warmed cache rather than
downloading from Central.
