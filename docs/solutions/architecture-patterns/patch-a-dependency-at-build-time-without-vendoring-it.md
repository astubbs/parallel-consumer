---
title: Patch a dependency's internals at build time instead of vendoring or forking it
date: 2026-08-31
category: architecture-patterns
module: build
problem_type: architecture_pattern
component: tooling
severity: high
applies_when:
  - You need to change behaviour inside a third-party library that has no extension point where you need one
  - The code you need to reach is package-private, final, or otherwise not designed to be subclassed or intercepted
  - Reflection or a bytecode agent can observe the internals but cannot change the control flow you care about
  - You want the size of your change to be a reviewable, re-derivable number rather than a whole vendored tree
  - You are about to copy third-party source into the repository and are looking for a cheaper option
tags:
  - build-time-patching
  - classpath-shadowing
  - vendoring
  - maven
  - control-arm
  - apache-license
  - kafka-streams
---

# Patch a dependency's internals at build time instead of vendoring or forking it

## Context

`parallel-consumer-streams` needs to change how Kafka Streams hands records to a processor chain, and
everything load-bearing lives in `org.apache.kafka.streams.processor.internals` - package-private,
explicitly not an API, and offering no seam to inject through. Reflection can read those fields but
cannot restructure the dispatch loop, and a `KafkaClientSupplier` swap sits below the point where
Streams serialises, so it gains nothing.

Three routes were open, and two were rejected:

- **Vendor the classes into the repository.** Rejected by the repo owner. Beyond taste, it drags in a
  copyright-gate provenance class and a `NOTICE` change to make committed ASF source legal and
  CI-clean, and a vendored copy drifts *silently* into a runtime `NoSuchMethodError` when the upstream
  version moves.
- **Fork and publish the library.** It works locally and is unresolvable on a CI runner, so the branch
  could never go green. It also means owning a full release, test and signing cycle for someone else's
  project on every upstream release.
- **Unpack the published sources, apply a tracked patch at build time, and compile the result into
  your own module.** This is what shipped.

The third option is the reusable technique. What follows is the mechanism, and then the practices
without which the mechanism is *silently* wrong.

## Guidance

### The mechanism

Four build steps, all using plugins the build already had. Every one of them is in
`parallel-consumer-streams/pom.xml`, which carries the reasoning at each step.

1. **Unpack the named classes from the published sources jar, twice.**
   `maven-dependency-plugin:unpack` at `generate-sources` (execution `unpack-kafka-streams-sources`),
   with `<classifier>sources</classifier>` and `<includes>` limited to a named list. One copy is
   *pristine* - the regeneration baseline - and one is the working copy that gets patched.
2. **Apply the tracked patch, failing the build on any rejected hunk.** `exec-maven-plugin` running
   `parallel-consumer-streams/bin/apply-patch.sh`, which dry-runs before applying, because a
   half-applied patch produces a tree that compiles but is not the thing anyone reviewed.
3. **Add the generated directory as a compile source root.** `build-helper-maven-plugin:add-source`
   (execution `add-patched-kafka-sources`), so the patched sources compile into the module's own
   `target/classes`.
4. **Let classpath order do the rest.** `target/classes` precedes the dependency jar, so the patched
   classes win while every sibling still loads from the jar. Same package name and same classloader
   means package-private access into the jar's internals works without a fork.

Only the `.patch` is tracked. No third-party source enters the repository, and the patch's line count
is the honest answer to "how much had to change" - `bin/regen-patch.sh` prints it.

### Wiring details that cost real time to find

- **Bind the patch step to the phase *after* the unpack, not the same phase.** Within one phase Maven
  orders executions by plugin declaration order in the *effective* pom. Here the root pom declares
  `exec-maven-plugin` before `maven-dependency-plugin`, so a same-phase binding would run the patch
  before the sources exist. Hence `unpack` at `generate-sources` and `apply-patch` at
  `process-sources`.
- **Choose the output directory deliberately.** `target/generated-sources` looked natural and was
  wrong: the root pom adds that whole directory as an *integration-test* source root for every module,
  which compiles the patched classes a second time into `target/test-classes` - and that precedes
  `target/classes` on the surefire classpath. Two copies, and the winner is an accident. The module
  uses `target/kafka-patched`, set by the `kafka.patched.dir` property.
- **Name the class set; do not discover it.** The `patched.classes` property lists the files
  explicitly, with a comment for each one the compiler would never have demanded - here
  `RecordCollectorImpl`, which is constructed outside the class being changed yet has non-concurrent
  maps written from other threads. A named set also gives you a stop-threshold: if the list has to grow
  past roughly a dozen, the sprawl *is* the answer, and you are building a fork by instalments.
- **Promote the dependency's runtime-scope dependencies to compile scope.** `jackson-databind` is
  `runtime` in the `kafka-streams` pom, so the jar resolves it but the generated sources will not
  *compile* without it. Declare it module-local and explicitly versioned, never in root
  `dependencyManagement`, or you break unrelated modules - here pinning it globally breaks WireMock in
  `parallel-consumer-vertx`.
- **Expect the enforcer to notice what you just did to the dependency graph.** Promoting
  `jackson-databind` to a *newer* version than the library resolves puts `jackson-annotations` on two
  different versions, and `RequireUpperBoundDeps` fails the build - correctly. Declaring the second
  artifact at the same version is the fix; it is not optional just because no code imports it.
- **The library's test fixtures may need the same treatment.** Kafka's `InternalMockProcessorContext`
  reads and writes the very fields the patch thread-confines, so it gets its own unpack, its own patch
  and `add-test-source`. Without it, the upstream suite in practice 3 below cannot construct its
  subject at all.

### The practices that make it safe

**1. Prove the shadowing before believing any downstream result.** This is the single most likely false
positive available. If the jar's copy wins, every behavioural test still passes - it is simply testing
the unmodified library against itself, beautifully.
`parallel-consumer-streams/src/test/java/bz/stub/parallelconsumer/streams/ShadowedClassLoadingTest.java`
asserts it directly rather than inferring it from behaviour: each generated class loads from a code
source containing `/classes/` and not `.jar`; a deliberately un-generated sibling still loads from the
jar, which is what makes this shadowing rather than a fork; and every generated class shares both a
package name and a *classloader* with the jar-resident ones, because different classloaders split the
package and break package-private access even when the names match.

Pick that un-generated sibling for sharpness, not convenience: the best one is the class you are most
likely to add to the set next, because then the assertion has to change, visibly, on the day the set
does.

**2. Start with an empty patch as the control arm.** Byte-for-byte the released sources, compiled
through the same pipeline, must behave identically. Without that baseline, a later failure cannot be
attributed between the technique and the change, and the whole verdict is worthless. `apply-patch.sh`
treats an empty patch as an explicit, successful no-op - checking emptiness itself rather than trusting
`patch`, whose behaviour on empty input differs between BSD `patch` on macOS and GNU `patch` on CI.
Keep the control runnable after the patch is non-empty: `diff -ru` between the pristine and patched
trees is the whole check.

**3. Run the library's own test suite against your patched classes, with your change switched off.**
That is the behaviour-preservation claim, and nothing else you can write is as strong. Kafka publishes
its compiled tests to Central, so the module takes them as a `test`-classifier dependency and points
surefire at them with `<dependenciesToScan>` on a *separate* execution (`kafka-upstream-tests`), so the
module's own tests are unaffected. Details that matter:

- include only the tests of classes you actually patch, plus their principal driver - scanning the
  whole jar runs thousands of unrelated tests and drowns the signal;
- set your feature flag off *explicitly* on that execution rather than inheriting whatever the default
  happens to be, and write that pin even before the flag has a reader, because the claim is about the
  patch and must never become a claim about a default;
- override inherited tag filters, which would otherwise silently drop every one of them;
- disable JUnit parallelism, because a suite written for a serial runner fails on shared fixtures and
  tells you nothing about your patch. Parallelism can arrive by accident - a `junit-platform.properties`
  inside a test-jar you depend on is enough;
- give the execution its own `reportsDirectory`. The count is a citable claim, and reading it out of
  the report files is what makes it re-derivable rather than remembered.

The count is only meaningful with zero exclusions and zero relaxed assertions: the moment you exclude a
test, the claim is void.

**4. Design around the regeneration foot-gun, and make it detectable.** The unpack step runs with
`overWriteReleases=true`, so *any* build invocation between editing the generated sources and
re-deriving the patch silently restores the tree and discards the edits. It says nothing when it does
this. `bin/regen-patch.sh` counts the hunks in the tracked patch *before* overwriting it and warns when
the count drops, pointing at `git checkout` as the recovery path.

The hunk count is a proxy, not the invariant, and it is wrong in both directions - adding lines can
*merge* hunks and lower it legitimately. The real invariant is content: every line the old patch added
must still be added, and the removed lines must be identical. Comparing those two sets between old and
new patch is a few lines of script and gives an answer rather than a hint. It is also what makes it
safe to *reduce* a patch deliberately: drop a file from `patched.classes`, delete it from both
generated trees, regenerate, and then prove the surviving files' change sets are untouched.

**5. Always `clean` before an experiment, and verify the instrumentation reached the run.**
`maven-dependency-plugin:unpack` preserves the archive's file timestamps, so a re-unpacked source file
goes *backwards in time* relative to the already-compiled class, and `maven-compiler-plugin` decides
nothing needs recompiling. A control-arm run without `clean` therefore tests the previous build's
classes while the developer believes the source is pristine, and will happily confirm a regression that
does not exist. Confirm with `javap -p -classpath target/classes ...` before believing any result.

**6. Handle the licence at distribution time, not at commit time.** Not committing the source removes
the commit-time obligation, but publishing an artifact containing *compiled, modified* Apache 2.0
classes triggers section 4(b): the distribution must carry prominent notices stating that you changed
the files. This repo names the modified classes in `NOTICE`, attributes the ASF, states plainly that
they were changed, and points at the patch as the complete expression of the change.

**7. Name the split-package hazard as the limitation it is, and let it decide publication.** The
artifact would contain classes in the third party's package namespace. Any consumer holding both it and
the original gets behaviour decided by classpath order, which build tools do not guarantee; class
loading is per class, so the result is always a mixture of two versions; and JPMS forbids split
packages outright. The technique is sound inside your own module's build and is **not** a distribution
mechanism. In this repo that is why `parallel-consumer-streams` sets `maven.deploy.skip`, `gpg.skip`
and the publishing plugin's `skipPublishing` - merging a leaf module is cheap to reverse, publishing an
artifact is not.

## Why This Matters

**The failure mode is silence, not error.** Every other approach here fails loudly. A missing reflective
field throws. A fork that does not build does not build. Classpath shadowing that loses the race
produces a green suite that measures the unmodified library, and there is nothing in the output to tell
you. Practices 1, 2 and 5 exist solely because the technique gives you no natural signal when it is not
working.

**The patch is the measurement.** "How little did we have to change" becomes `wc -l` on a tracked file,
reviewable in a normal diff, and re-derivable against a new upstream version. A vendored tree cannot
answer that question at all: the change and the copy are the same artifact, and the reviewer has no way
to see one without the other.

**Drift fails at build time rather than at runtime.** When the upstream version moves,
`apply-patch.sh` dry-runs, fails, and tells you the patch needs re-deriving. The equivalent vendored
copy compiles fine and throws `NoSuchMethodError` in production. The maintenance obligation is
identical in size and vastly better in timing.

**It keeps the licensing surface small.** No third-party source in the repository means no
copyright-gate machinery, no provenance headers, no review burden on files nobody here wrote. The only
remaining obligation attaches to the published artifact, and one `NOTICE` paragraph discharges it.

## When to Apply

- **Apply when** the change is bounded to a handful of classes you can name up front, the library
  publishes a sources jar for the exact version you depend on, and the artifact's consumers are you -
  your own tests, your own module - rather than arbitrary downstream applications.
- **Apply when** you need package-private or otherwise unreachable access and want the size of the
  intrusion to stay visible and reviewable over time.
- **Do not apply when** the patched class set keeps growing. Past roughly a dozen classes you are
  maintaining a fork with extra steps, and the sprawl is itself the result worth reporting.
- **Do not apply when** third parties must get the patched behaviour on their own classpath. Classpath
  order is not a distribution contract. Publish a fork under a different coordinate, or upstream the
  change.
- **Do not apply when** the target is a moving trunk rather than a pinned release. These classes have
  already diverged materially on Kafka 4.x, so a green result on 3.9 does not transfer unexamined.

## Examples

**The developer loop, and the order that matters:**

```bash
# 1. unpack pristine + patched trees, and apply the tracked patch.
#    process-sources, NOT generate-sources - the latter only unpacks, so regenerating from that
#    tree deletes every hunk. The leading `.` is required: the leaf module alone fails at enforcer.
./mvnw -pl .,parallel-consumer-streams process-sources

# 2. edit parallel-consumer-streams/target/kafka-patched/... like normal Java
#    RUN NO MAVEN between here and step 3 - unpack silently reverts your edits

# 3. re-derive the tracked patch from your edits (warns if the hunk count DROPPED)
parallel-consumer-streams/bin/regen-patch.sh

# 4. commit the patch. The generated trees are gitignored and never committed.
```

**The empty-patch control arm, encoded in the tooling rather than left to discipline:**

```bash
if ! grep -qE '^(---|\+\+\+|@@|diff )' "$patch_file"; then
    echo "apply-patch: $patch_file contains no hunks - control arm, generated sources left as released"
    exit 0
fi
```

An empty patch is a *successful* build that produces byte-for-byte the released classes, so the harness
can be falsified independently of anything you intend to change.

## Related

- `parallel-consumer-streams/README.md` - the user-facing statement of the same mechanism, its version
  pinning obligation, and the classpath hazard that keeps the module unpublished
- astubbs/parallel-consumer#255 - the tracking issue for Kafka Streams on Parallel Consumer
- astubbs/parallel-consumer#271 - the feasibility study the machinery was built inside, where the
  execution seam that motivates it still lives
