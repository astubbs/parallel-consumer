# `parallel-consumer-streams` - patching Kafka Streams without vendoring or forking it

> ## ALPHA. EXPERIMENTAL. NOT PUBLISHED.
> This module is in the reactor so the machinery below can be built and reviewed. It is **not**
> published to Maven Central, deliberately - see
> [The classpath hazard this module does not solve](#the-classpath-hazard-this-module-does-not-solve).
> Tracking issue: [astubbs#255](https://github.com/astubbs/parallel-consumer/issues/255).

**What is here today is the build machinery and its oracle, and nothing else.** Apache Kafka Streams'
`processor.internals` package is package-private, explicitly not an API, and has no seam at the point
Parallel Consumer needs one. This module establishes a repeatable way to change those internals - one
that keeps no Apache source in this repository, states the size of the change as a reviewable number,
and fails at build time rather than at runtime when the change stops applying.

**What is not here: the Parallel Consumer execution seam.** No record goes through Parallel Consumer
in this module today. `StreamTask` is not patched, there is no dispatcher, and there is no switch. That
work arrives in the PR stacked on this one, and it is what
[astubbs#271](https://github.com/astubbs/parallel-consumer/pull/271) was cut from.

---

## The mechanism

Four steps, all with plugins this build already had. The module's [`pom.xml`](pom.xml) carries the
reasoning for each in place; this is the shape.

1. **Unpack** the named classes from the published `kafka-streams` **sources** jar, twice - a
   *pristine* copy that is the regeneration baseline, and a working copy. `maven-dependency-plugin`,
   at `generate-sources`.
2. **Apply** the tracked patch to the working copy, dry-running first so a rejected hunk fails the
   build rather than leaving a half-applied tree that compiles.
   [`bin/apply-patch.sh`](bin/apply-patch.sh), at `process-sources`.
3. **Compile** the working copy into this module's own `target/classes`, by adding it as a source
   root. `build-helper-maven-plugin`.
4. **Let classpath order do the rest.** `target/classes` precedes the `kafka-streams` jar, so the
   patched classes win while every one of their thousand siblings still loads from the jar. Same
   package name and same classloader, so package-private access still works - without a fork.

**No Apache Kafka source is committed to this repository.** Only
[`src/main/patch/pc-streams.patch`](src/main/patch/pc-streams.patch) is tracked; the generated trees
are gitignored. Kafka's own test *fixture* `InternalMockProcessorContext` gets the same treatment
through a second, smaller patch, for the reason given below.

### What the patch changes, and why exactly these classes

| Class | Change | Why it is in the set |
|---|---|---|
| `AbstractProcessorContext` | the current record context and current processor node move from two `protected` fields to thread-confined ones | stock Streams holds one of these per task and gets away with it because exactly one thread is ever inside the task |
| `ProcessorContextImpl` | reads and writes them through the accessors instead of the inherited fields | it is a subclass doing `getfield`/`putfield` on those fields, so leaving it alone defeats the confinement *with no compile error to say so* |
| `RecordCollectorImpl` | two `HashMap`s become `ConcurrentHashMap`s | written from the producer's I/O thread for every in-flight send; the compiler would never have demanded this class, so it is named rather than discovered |

That is the whole patch. The set is **named in the pom, not discovered**
(`patched.classes`), and the stop-threshold is stated there: if it has to grow much past a dozen, the
sprawl is itself the answer to "how little had to change", and this is a fork by instalments.

Kafka's `InternalMockProcessorContext` needs the second patch because it *also* reads those fields
directly. Un-patched, every `RecordCollectorTest` case dies at construction with a
`NoSuchFieldError` - so without the fixture patch the oracle below cannot run at all.

---

## Why we believe the machinery works

The technique's failure mode is **silence**, not error. If the jar's copy of a class wins the
classpath race, every test still passes - it is simply testing unmodified Kafka Streams against
itself, beautifully. So each claim here has a control.

### The generated classes really do win

[`ShadowedClassLoadingTest`](src/test/java/bz/stub/parallelconsumer/streams/ShadowedClassLoadingTest.java)
asserts it directly rather than inferring it from behaviour: each generated class loads from a code
source under `/classes/` and not a `.jar`; `StreamTask` and `StreamThread`, which are deliberately
*not* generated, still load from the jar - which is what makes this shadowing rather than a fork; and
every generated class shares both a package name and a **classloader** with them, because different
classloaders split the package and break package-private access while the names still match.

`StreamTask` is the sharp one. It is the immediate neighbour of all three patched classes and the
class the seam will patch next, so "it still comes from the jar" is the tightest available statement
that the generated set is exactly what the pom declares.

### An empty patch is a no-op, and that is checked by the tooling

`apply-patch.sh` treats a patch with no hunks as an explicit, successful no-op - checking emptiness
itself rather than trusting `patch`, whose behaviour on empty input differs between BSD `patch` on
macOS and GNU `patch` on CI. Run the build that way and the generated tree is byte-for-byte the
released sources. Without that baseline, a later behavioural difference could not be attributed
between the technique and the change.

### Apache Kafka's own test suite, run against the patched classes

Kafka publishes its **compiled** tests to Maven Central. This module takes them as a `test`-classifier
dependency and points a dedicated surefire execution at them, so Kafka's own
`StreamTaskTest`, `RecordCollectorTest` and `ProcessorContextImplTest` exercise **our**
`AbstractProcessorContext`, **our** `ProcessorContextImpl` and **our** `RecordCollectorImpl`. Nothing
is excluded, rewritten, recompiled or relaxed. It runs in the module's normal `test` phase, on every
build, with no profile and no flag.

That is the behaviour-preservation claim, and it is the strongest one available: **anything the patch
broke that Kafka tested, fails here.**

> **Re-derive the counts; never copy them.** They move with the patch, with `kafka.version`, and they
> will move again when the seam arrives. Run the module's whole `test` phase and read the per-class
> numbers out of `target/surefire-reports-kafka-upstream/`.
>
> **Do not scope the run with `-Dtest=`.** It silently overrides that execution's `<includes>`, so
> Kafka's suite does not run at all - and the build still goes green, with the number you were
> checking never computed. It has cost several people a whole run.

---

## The classpath hazard this module does not solve

**This module is not published, and that is the reason.**

It compiles a handful of classes into Apache Kafka's *own* package namespace and depends on
`kafka-streams` for the rest. Inside this module's build that is controlled and asserted. As a
published dependency it is not defensible, in three distinct ways:

1. **Classpath order is a convention, not a guarantee.** Maven, Gradle, IDEs, shaded uber-jars and
   Spring Boot's loader may order entries differently. When ours lose, you silently get stock Kafka
   Streams with no error - the worst shape a failure can take.
2. **Class loading is per class, so the result is always a mixture.** That works only while both
   halves are the same version, and nothing checks that they are. A routine `kafka-streams` bump
   would run our patched internals against their newer ones.
3. **It is illegal on the module path.** JPMS forbids split packages outright.

Merging an isolated leaf module is cheap to reverse; publishing an artifact is not - **merge freely,
publish deliberately**. The pom carries the deploy, signing and publishing skips, and says so at the
point of the skip.

`NOTICE` already carries the Apache 2.0 section 4(b) statement naming the modified classes, so the
obligation is discharged for whatever is eventually distributed rather than deferred to the day it is.

---

## Working on the patch

**Never hand-edit `pc-streams.patch`.** Its `@@` headers encode line counts; edit the generated Java
and re-derive.

```bash
# 1. unpack the pristine tree AND produce the patched one. process-sources, NOT generate-sources -
#    generate-sources only unpacks, and regenerating from that tree deletes every hunk.
./mvnw -pl .,parallel-consumer-streams process-sources

# 2. edit parallel-consumer-streams/target/kafka-patched/... like normal Java.
#    RUN NO MAVEN between here and step 3 - unpack silently reverts your edits and says nothing.

# 3. re-derive the tracked patch
parallel-consumer-streams/bin/regen-patch.sh

# 4. commit the patch. The generated trees are gitignored and never committed.
```

The `.` in `-pl .,parallel-consumer-streams` is required: selecting the leaf module alone fails at
`enforcer:enforce`, because the parent is not in the reactor.

[`bin/regen-patch.sh`](bin/regen-patch.sh) warns when the hunk count drops, which is the tripwire for
edits lost to a stray maven run. Its header owns the rest, including why the hunk count is a proxy
rather than an invariant, and how to reconcile two branches that both regenerate the patch (merge the
generated Java, never the patch).

The fixture patch is re-derived the same way, with all three arguments:

```bash
parallel-consumer-streams/bin/regen-patch.sh kafka-test-pristine kafka-test-patched src/main/patch/pc-streams-testfixtures.patch
```

### On a Kafka version bump

The patch is derived against exactly the sources of the reactor's `${kafka.version}`, and
`org.apache.kafka.streams.processor.internals` is package-private, unsupported, and free to change
shape in any patch release. When it stops applying, the build fails **loudly** at `apply-patch.sh`,
which is a real improvement on a vendored copy: that would compile fine and throw `NoSuchMethodError`
in production. It remains a recurring maintenance obligation, with the upstream suite above as the
regression run behind each one.

On Kafka trunk and 4.x these classes have already diverged materially - `ProcessorContextImpl` is
`final` there, and the record context is mutated in place. A green result on 3.9 does **not** transfer
unexamined.

---

## Further reading

- [Patch a dependency's internals at build time instead of vendoring or forking it](../docs/solutions/architecture-patterns/patch-a-dependency-at-build-time-without-vendoring-it.md) -
  the technique in general, its practices, and when not to apply it
- [astubbs#255](https://github.com/astubbs/parallel-consumer/issues/255) - the tracking issue for
  Kafka Streams on Parallel Consumer
- [astubbs#271](https://github.com/astubbs/parallel-consumer/pull/271) - the feasibility study this
  machinery was cut from, where the execution seam and its measurements still live
