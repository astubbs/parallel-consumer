# The assertion generator sweeps every core enum, so a package-private one cannot exist

<!-- inflight-type: task -->
<!-- inflight-impact: ci -->

**Adding a package-private enum anywhere in `parallel-consumer-core` breaks the build, and the error
never says so.** The only shape an enum can take in core today is public, with every enclosing class
public too - so a build-time tool is deciding what the library's public API surface is. Recorded
here because the constraint has now been asserted, denied, and re-asserted on three separate
occasions, each time from the error text rather than from a measurement.

[`docs/building.md`](../building.md) **owns the generation phase** - what runs, when, and why the
generated sources are not committed. This note owns only the enum constraint, which that doc does
not cover. Its sibling
[`ci-180-generated-truth-build-traps.md`](ci-180-generated-truth-build-traps.md) owns the
`cleanTargetDir` and partial-`target/` traps; do not restate either here.

## The mechanism

`truth-generator-maven-plugin` is configured in `parallel-consumer-core/pom.xml` with an explicit
`<classes>` list and an `entryPointClassPackage` of `bz.stub.parallelconsumer`. Reading that
configuration suggests the generated set is the listed classes plus whatever is reachable from them.
**It is not.** Every enum on the module's compilation classpath gets a `Subject`, reachability
notwithstanding, and the sweep reaches the test trees as well as `src/main`.

The generated `Subject` for an enum lands in the enum's **own** package, which is harmless. The
problem is the two entry-point classes, `ManagedTruth` and `ManagedSubjectBuilder`, which the
`entryPointClassPackage` setting puts in `bz.stub.parallelconsumer`. Each writes one `that(...)` /
`assertThat(...)` overload **per swept enum**, importing the enum by qualified name. An enum that
`bz.stub.parallelconsumer` cannot see is therefore named by generated code that cannot compile.

To see the overload set and what it swept, build and read the imports of the generated
`ManagedSubjectBuilder`:

```
./mvnw -pl :parallel-consumer-core -am test-compile -DskipTests
grep ' that(' parallel-consumer-core/target/generated-test-sources/truth-assertions-managed/bz/stub/parallelconsumer/ManagedSubjectBuilder.java
```
<!-- file-refs: N/A - the generated sources are build output under target/, git-ignored by design and absent until the command above is run; docs/building.md owns why they are not committed -->

Enums nested in test classes under `integrationTests` appear in that list, none of which any
configured class references. That is the evidence the sweep is not reachability-based, and it is
cheaper to re-derive than to argue about.

## The error text is the reason this keeps being mis-stated

**No shape reports an access error.** Measured on `feat/504-engine-park-and-handback`, by adding one
throwaway enum to `bz.stub.parallelconsumer.internal`, building, and reverting:

| Enum shape | What the build says |
|---|---|
| public, top level | builds; `that(...)` overload appears as expected |
| package-private, top level | `name clash: class ManagedSubjectBuilder has two methods with the same erasure, yet neither overrides the other` |
| public, nested in a package-private class | the same `name clash`, plus `package <outer> does not exist` |
| package-private, nested in a package-private class | `package <outer> does not exist`, then `reference to assertThat is ambiguous` and `incompatible types: <X>Subject cannot be converted to <Y>Subject`, repeated across the entry-point class |

The third row is the one worth reading twice: making the **enum** public is not enough, because the
import is by qualified name through the enclosing class. The enum and its whole enclosing chain must
be public.

Every one of those messages points at a generated file and describes a symptom of an unresolvable
type - an erasure collision, an ambiguous call, a converted-to mismatch. A reader lands inside
generated code they did not write, sees a complaint about overload resolution, and reasonably
concludes the generator emitted something wrong. Nothing in the output mentions visibility, and the
offending enum is not named as the cause.

The failure also degrades in a way that hides the cause further: once a type is unresolvable, javac
reports the consequences at every call site, so the real trigger is a small minority of the output
and not at the top of it.

## What it costs

`bz.stub.parallelconsumer.fluent.OutcomeTag` is public **only** to satisfy this. It is an internal
tag vocabulary whose author wanted it package-private, and a build-time assertion generator is what
made it part of the published surface. The type's own javadoc records the measurement, which is the
right place for it - but the general rule has no home, so the next person reaching for an enum in
core rediscovers it from the same misleading error.

The second cost is the one already paid: a code comment stating this constraint was deleted as false
by `8954824e6`, on the strength of a read-only investigation that cited
`internal.ConsumerOwnership.Phase` having a generated `Subject` as proof that discovery is
reachability-based. It is not proof - `Phase` is swept because everything is swept - and `ed68074d6`
re-established the constraint by measurement shortly afterwards. Both commits are on
`docs/ux-modernisation`.

## What was tried

- **Package-private, in both nestings.** Fails as above. This is the shape the work actually wanted.
- **Reading the plugin configuration to predict the swept set.** Misleading - see above.
- **Narrowing the generator's recursion.** The plugin exposes a boolean `recursive` parameter, so
  turning it off is available. Rejected on the branch that measured it: it collapses the generated
  set rather than trimming it, and the chained assertions the suite is written against stop
  existing. Not re-proposed here, but not proven either - nobody has measured how much of the suite
  actually breaks.

## What has NOT been tried, and the options left open

**There is no exclusion parameter.** The plugin's own descriptor
(`META-INF/maven/plugin.xml` inside `truth-generator-maven-plugin`) declares exactly these knobs:
`classes`, `packages`, `subjectPackages`, `legacyClasses`, `entryPointClassPackage`,
`generateAssertionsInPackage`, `outputDirectory`, `cleanTargetDir`, `recursive`, `releaseTarget`,
`skip`. Nothing excludes, ignores or allowlists a type. Verified by reading the descriptor, not by
inference from documentation.
<!-- file-refs: N/A - this path is inside the truth-generator-maven-plugin jar in the local Maven repository, not a file in this tree -->

That leaves four directions, none of them chosen. **This note does not decide between them; the
owner does.**

1. **Add an exclusion parameter to the generator.** Truth Generator is this project's own
   (`astubbs/truth-generator`), so this is available rather than blocked on a third party - but it
   is a change in that repo, then a version bump here. The same two-repo shape as the
   `cleanTargetDir` fix already decided in the sibling note, and it could ride along with it.
2. **Scope the generator by package instead of by class.** The `packages` parameter exists and is
   unused here. Whether a package set can express "core's public API but not its internals" without
   losing the chained assertions is unmeasured.
3. **Keep such enums out of core.** Moves the problem rather than solving it, and only works for
   types that have somewhere else to live.
4. **Accept it and write the rule down** - in `docs/building.md`, beside the rest of the generation
   phase, so the next reader meets it before the error rather than after. Cheapest, and does not
   preclude any of the above.

Option 1 is the only one that gives the enum back its package-private form. Options 2 and 3 trade
one constraint for another; option 4 concedes the public surface permanently.

## What is verified here, and what is taken on report

**Verified on this branch, by building:** that the sweep is not reachability-based; that the
entry-point classes carry one overload per enum and live in `bz.stub.parallelconsumer`; the four-row
error table above, each row from its own build; that a public top-level enum builds clean; and that
the plugin descriptor has no exclusion parameter.

**Taken on report** from `ed68074d6`'s commit body, not re-measured here: that the
`fluent` package's own attempt failed the same way, that `OutcomeTag`'s public form was adopted for
this reason, and that turning `recursive` off breaks the suite's chained assertions.

**A caution for whoever re-runs the experiment.** `cleanTargetDir` is false, so a throwaway enum
leaves its generated `Subject` behind under `truth-assertions-templates` after the source file is
deleted, and the *next* build fails on the orphan rather than on anything real. Delete the generated
files for the probe type as well as the probe, or the control arm is contaminated - it was, once,
during the measurement above, and briefly read as "a public enum fails too".
