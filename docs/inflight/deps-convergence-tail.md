# Three dependency divergences block `dependencyConvergence`

<!-- inflight-type: task -->
<!-- inflight-impact: deps-debt -->

`maven-enforcer`'s `requireUpperBoundDeps` is **on** - it is the rule that catches the dangerous
case, where resolution settles on a version lower than something in the graph asks for. That is the
shape that made `wiremock-jre8`'s ASM 9.4 pin kill Lincheck's transformer while it reported success.

Its stricter sibling `dependencyConvergence` is **off**, and this note is why. It fails on *any*
version split, including ones where the higher version wins safely.

## What it found when it was switched on

Immediately, and this one is fixed: `pl.tlinkowski.unij.api` 0.1.3 pulled `slf4j-api` 1.7.28 against
the 2.0.18 declared directly in the parent pom. Maven's nearest-wins had already resolved to 2.0.18
so nothing was broken, but a silent version split is exactly the shape that bites later. Excluded at
the `unij` dependency, since the API is declared directly and there is no second opinion to
reconcile.

Behind it sit three more, all in Kafka's transitive tree, measured on the same run:

| Artifact | Resolved |
|---|---|
| `org.xerial.snappy:snappy-java` | 1.1.10.5 |
| `org.slf4j:slf4j-api` | 1.7.36 |
| `com.github.luben:zstd-jni` | 1.5.6-4 |

## Why they are not fixed here

Each needs either an exclusion or a managed pin, and pinning a version *inside* Kafka's tree is a
decision with a blast radius - `reference_jackson_wiremock_coupling` is the standing example in this
repo of a global pin breaking a module nobody was thinking about. The three are worth doing as their
own change with the compatibility question answered per artifact, not as a side effect of switching
a lint rule on.

**Turning the rule back on is the definition of done for this note**, and it is a one-line change in
the parent pom's enforcer rules next to `requireUpperBoundDeps`.
