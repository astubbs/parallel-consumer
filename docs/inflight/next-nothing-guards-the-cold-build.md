# A cold clone builds fine - but only `validate` as a terminal goal does not, and nothing guards it

Investigated 2026-08-17 after `./mvnw validate` failed on `feats/proxy-requirements` with a
dependency-resolution error that never mentions CVEs. **The first draft of this note assumed cold
builds were broken. Measurement says otherwise, and the conclusion reversed.**

## Measured, not reasoned: a cold repository builds

```
./mvnw -Dmaven.repo.local=<throwaway> package -DskipTests
```

**BUILD SUCCESS**, all 30 modules, from an empty local repository - about 130s of module time plus
downloads. So the property that matters - *clone it, `mvn package` or `mvn test`, it works* - **holds
today**. `test` follows by the same mechanism and needs strictly less.

**Why it works, and why the failure looked worse than it is.** Maven walks each module through its
whole lifecycle in reactor order. By the time it reaches `parallel-consumer-proxy-conformance` and
runs *that module's* `validate`, every dependency has already been built in the same session, so the
reactor supplies them and nothing is fetched. Under a bare `./mvnw validate` **no module ever gets
past `validate`**, so nothing is ever produced, and any inter-module dependency has to come from the
repository instead.

## So what is actually wrong

**Only `validate` as a terminal goal**, and only from a cold repository. `ossindex-maven-plugin`'s
`audit` goal is bound to `validate` in the root pom (grep `audit-dependencies`) and must resolve the
full test-scope tree to scan it - which, with nothing built, means the repository.

Two things follow, and they point in opposite directions from the first draft:

- **Do not move the audit's phase.** It is not breaking the developer workflow, and moving it would
  trade a real fail-fast guard for a fix to a diagnostic invocation. The earlier draft recommended
  testing `compile`; that recommendation is withdrawn.
- **The error message is still bad.** It names a missing SNAPSHOT artifact and never mentions the CVE
  audit that demanded it - the silent-cause shape recorded elsewhere in
  [`docs/solutions/`](../solutions/). Worth a line in the build docs rather than a build change:
  *a bare `validate` needs a populated repository; use `package` or `test`*.

**Control arm, so the boundary is not guessed**: hiding the genuinely-used
`parallel-consumer-proxy-client-java-grpc` jar from a warm `~/.m2` reproduces the identical failure in
`parallel-consumer-proxy-client-java-harness`. So terminal-`validate` fragility belongs to the phase
binding, not to any one module's dependencies.

Separately fixed on astubbs/parallel-consumer#293: the conformance suite declared its Kotlin and Scala
edges as `jar` while never using their classes, so those two could never resolve at all - a module
nobody consumes a jar from never gets installed. That was its own defect and is not this one.

## The real gap: nothing keeps the cold build working

**Every developer machine and every CI runner has a warm repository**, so a regression here would be
invisible for months and would surface as a new contributor's first command failing.

The measurement above is a point-in-time result, and the repo's own rule is that an invariant worth
having is worth a check that fails the build. **A cold-repository lane is the fix** - build with
`-Dmaven.repo.local=` a throwaway directory and assert BUILD SUCCESS. It is cheap enough to be real:
the whole reactor packaged in roughly two minutes of module time from empty.

Scheduled rather than per-PR is probably right, since it re-downloads the world each run and the
failure mode it guards is slow-moving. That choice is open.
