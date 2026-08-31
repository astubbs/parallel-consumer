# Static analysis for the proxy clients

Bug and bad-pattern detection for the language-proxy fan-out (astubbs/parallel-consumer#242): a
client module per language, one protocol schema, and one policy applied to each. **This is not about
formatting.** A formatter argues about where a brace goes; the tools below argue about whether the
code is wrong.

`.github/workflows/clients.yml` owns the per-language CI rows, `docs/ci.md` owns the schema gate's
CI job, and this document owns **which tool, why that one, what it catches, and what fails the
build**. Where a row and this document disagree, the row is what actually runs - fix whichever is
wrong, and say which.

**Only Java is filled in.** That is the state of the tree rather than a gap in this document: Java
is the only language whose module holds a client rather than a build fixture. Each remaining
language's row is written by the wave that makes it a client, answering the same five questions
below inside its own module.

## The shape, decided once

Every language answers the same five questions.

1. **Which tool, or none.** Maturity is the filter: widely adopted, actively maintained, and it
   finds real defects. A formatter is not a bug-finder, and neither is a style linter.
   **"Nothing mature - none added" is a result**, and it is recorded with its reason rather than
   left as an apparent gap for the next person to re-derive.
2. **What class of defect it catches**, in terms specific enough to tell whether it overlaps with
   something else already running.
3. **The exact local command.** This is a hard requirement, not a nicety: a check that only exists
   in CI is a check you meet after review rather than before it, and this repo's rule is to verify
   locally and never defer to CI. Every command in the table has been run on a developer box unless
   the row says otherwise, in which case it says so explicitly.
4. **How it is wired in CI**, so the local command and the gating command are the same thing.
5. **Severity policy** - see below, because it is the same everywhere.

### Severity policy: everything fails, nothing warns

There is no advisory tier anywhere in this table. A finding that nobody has to act on teaches
everybody to scroll past findings, and the next real one scrolls past with it.

Dismissing an individual finding is allowed and expected - but it is done **at the finding**, with
the reason written down: a narrowly scoped `spotbugs-exclude.xml` entry naming one class and one
pattern, an `except:` in `buf.yaml`, whatever the tool's equivalent is. What is refused is the wide
exclusion: a whole rule turned off, or a whole pattern muted across a module, because that silences
the finding for code nobody has written yet.

This is deliberately stricter than the repo-wide SpotBugs lane in `maven.yml`
(`static: spotbugs`), which is core-scoped, baseline-excluded and non-blocking. That lane suits a
codebase carrying long-standing findings
([`docs/inflight/static-spotbugs-latent-findings.md`](inflight/static-spotbugs-latent-findings.md)).
These modules carry none, and **starting a new module at zero is the one moment a clean-slate gate
is free**.

### Who owns each module's configuration

The rule set, the ignore file and the tool version for a client module are authored by **that
language's own wave**, inside that module. This document and `clients.yml` name the tool and the
invocation; the module owns its config file.

**So when a CI row fails on a diff that looks unrelated to it, check for a row/module mismatch
first** - a module that renamed its lint script, moved its config file, or changed the tool's
version. That is the likeliest cause by some distance, and it is invisible from either side alone.

## The table

| Target | Tool | Local command | Verified here |
|---|---|---|---|
| **Protocol (`.proto`)** | `buf lint` | `bin/check-proto-lint.sh` | yes - [`docs/ci.md`](ci.md) owns the gate and its CI wiring |
| **Java** (every module under its aggregator) | SpotBugs (gating) + ArchUnit | see [Java](#java---spotbugs-and-archunit) - the modules by name, not the aggregator | yes |
| Go, Python, Kotlin, TypeScript, Rust, C#, Ruby, C++, Swift, Scala | not yet chosen | - | n/a - each arrives with its client |

The last column distinguishes two things, because conflating them is how a check comes to be
believed rather than known:

- **yes** - the command was run here against real code **and proven able to fail**: a defect it
  should catch was introduced, the tool went red, the defect was reverted.
- **n/a** - nothing runs, because there is nothing yet to run it on.

---

## Java - SpotBugs and ArchUnit

Two tools, because they answer different questions, and the repo already runs both elsewhere.

### SpotBugs

**Catches:** bytecode-level defects the compiler accepts - null dereference on a path, resources
not closed, `equals`/`hashCode` mismatches, non-atomic read-modify-write on shared state, useless
comparisons, format-string mismatches - plus whatever the repo's detector plugins add, which on this
tree meant fb-contrib's feature-envy, exception-softening and could-be-static families.

**Wiring:** bound in `parallel-consumer-proxy-client-java/pom.xml`, inherited by every module beneath
it - including ones added later, which is why it is declared there rather than in each child - at the
**`process-test-classes`** phase with the **`check`** goal. All three choices are load-bearing:

- **Before `test`, not at `verify`** (the plugin's own default). The gating PR lane runs
  `bin/ci-unit-test.sh`, which stops at `test`. A `verify`-bound check would run only in the
  push-to-master full build - after review, on the wrong side of the merge - while reading as
  covered the entire time.
- **`process-test-classes`, not `process-classes`.** The root pom sets `includeTests=true`, so
  SpotBugs also reads `target/test-classes` - and at `process-classes` that directory has not been
  written yet. A clean build analysed main code only and passed; the next *incremental* build, with
  test classes left on disk, failed the same tree. A gate whose verdict depends on what a previous
  run left behind is not a gate. `process-test-classes` is the first phase after `test-compile` and
  is still before `test`.
- **`check`, not `spotbugs`.** The `spotbugs` goal writes a report and exits 0. `check` reads that
  report and fails. It runs the analysis itself, so there is no second execution to keep in step.

Effort `Max`, threshold `Medium` and the fb-contrib / findsecbugs / slf4j detector plugins all come
from the root pom's `pluginManagement` - nothing is restated.

**Local:**

```bash
JAVA_HOME=<a JDK 17> ./mvnw -Pci test -am -Dexcluded.groups=performance,chaos,quarantined \
  -pl :parallel-consumer-proxy-client-java-api,:parallel-consumer-proxy-client-java-direct,\
:parallel-consumer-proxy-client-java-grpc,:parallel-consumer-proxy-client-java-harness
```

Three traps are baked into that line, all of them silent:

- **Name the leaf artifacts, not the aggregator** - one `-pl` entry per module in that aggregator's
  `<modules>` block, which grows, so read it rather than copying this line forever.
  `-pl :parallel-consumer-proxy-client-java` selects the packaging-`pom` parent *only* - Maven does
  not walk into its modules - so it builds successfully, runs SpotBugs against a project with no
  class files, and reports `BUILD SUCCESS` having analysed nothing.
- **Keep `-Dexcluded.groups=...quarantined`.** Without it the reactor runs core's quarantined tests
  on the way through, and you diagnose an unrelated known flake
  ([`docs/inflight/test-untracked-ci-flakes.md`](inflight/test-untracked-ci-flakes.md)) instead of
  your own change. `bin/ci-unit-test.sh` hardcodes the same list.
- **`-am` is required**, not optional: the enforcer's `ReactorModuleConvergence` rule fails when a
  selected module's parents are absent from the reactor, and it fails at `validate`, before anything
  useful has happened.

`JAVA_HOME` must be JDK 17 - core's delombok step fails on a newer JDK.

**Where the dismissals live and what each says.** Every module carries its own
`spotbugs-exclude.xml`, and every entry in every one of them names a class and a pattern, most of
them a method or a field as well, with the argument for declining it written beside it. The two that
generalise beyond one line of code:

- **A translation method reads the source object and nothing else, and that is what makes it a
  translation.** fb-contrib's `CE_CLASS_ENVY` on `DirectParallelConsumerClient.toInboundRecord`
  suggests moving the method onto `RecordContext` - a `parallel-consumer-core` type, which must not
  know a client API exists. The mirror of that rule is enforced from the other side by
  `ClientSurfaceArchTest`, so the finding's own remedy is architecturally forbidden here.
- **Widening a parameter to its `*OrBuilder` supertype would weaken a frozen contract.** A generated
  message is something somebody could have sent; a builder is a mutable, mid-construction object that
  was never on any wire. Same call, same reasoning, as astubbs/parallel-consumer#383 recorded for the
  protocol module's own bridges.

Dismissals live in exclude files rather than `@SuppressFBWarnings` at the site - which would
otherwise be the better shape, because it keeps the reason next to the code - because that annotation
needs `spotbugs-annotations` on the classpath, and the api module is dependency-free by design (its
pom says so, and the direct sibling's `bannedDependencies` enforcer rule exists to keep it that way).
Once one module needs a file, the rest matching it is worth more than each choosing differently.

**Proven able to fail:** repeatedly and unprompted, before any exclusion or fix existed. On the
extraction rung alone it reddened the build on the api module's zero-copy accessors, on a
default-encoding `getBytes()` in a test, on three fb-contrib findings in the direct transport - two
of which were real and were fixed rather than excluded - and on an unencrypted server socket in the
harness lane. It also caught the ordering defect described above, by disagreeing with itself between
a clean build and an incremental one.

### ArchUnit

The repo's existing convention is a tiny per-module `TestConventionsArchTest` pointing ArchUnit at
that module's packages, with the rule logic living once in `TestConventionRules` (core's test-jar).
**That pattern is extended here rather than duplicated.**

- **The transport modules and the harness lane** get the standard `TestConventionsArchTest`, and so
  should any module added under the aggregator later. The rule that earns its place is the
  surefire-naming one: the shared conformance suite arrives in those modules by *subclassing*, and a
  subclass named outside surefire's default includes is never collected - the module would report
  green having run nothing.
- **`api`** gets `ClientSurfaceArchTest` instead, which is a different job: two rules over
  *production* code asserting that the shared surface names **no transport type**
  (`io.grpc..`, `com.google.protobuf..`, `bz.stub.parallelconsumer.proxy..`) and **no engine or
  Kafka type** (`org.apache.kafka..`, the engine internals). This is the rule the direct module's
  pom asks for by name - *"The Java reference work adds an ArchUnit rule covering the API SURFACE;
  this ban covers the CLASSPATH"* - and it complements the `ban-transport-dependencies` enforcer
  rule rather than repeating it: the enforcer reads the dependency **tree** and fires when a jar
  arrives; ArchUnit reads the **bytecode** and fires when a type is referenced, which catches the
  leak that arrives through a dependency already legitimately present.

  Why it matters more here than in an ordinary module: a `ByteString` or a `ConsumerRecord` on this
  surface is not a Java problem, it is a specification problem - the mirroring languages have no such
  type, so the shape stops being expressible and the fan-out diverges silently.

**One recorded gap, and it is guarded rather than merely recorded.** The api module does **not** run
the shared `TestConventionRules`, unlike every other module in the repo, because that rule library
ships in core's test-jar and the api pom forbids a dependency on core in any scope.

`EveryModuleWiresUpArchUnitTest` - which postdates the branch this module was extracted from, so the
collision is new rather than a rule anybody ignored - names this one module as its exemption, with
the reason at the site. What keeps that from being a hole is a second assertion beside it: an exempt
module must still run *some* `@AnalyzeClasses` wrapper of its own, so deleting `ClientSurfaceArchTest`
makes the module fail that test again instead of passing quietly. Proven by deleting it and watching
the exemption go red.

The cheapest fix that removes the exemption altogether is to extract `TestConventionRules` into a
small standalone test-support artifact that neither core nor the clients own - worth doing when a
second module hits the same wall, not before.

**Local:** the same Maven command as SpotBugs; ArchUnit runs as an ordinary surefire test.

## Adding a language, or a tool

1. Ask whether a **mature** bug-finder exists. Widely adopted, actively maintained, finds real
   defects. If the honest answer is "only a formatter", write that down here with the reason and add
   nothing. That is a complete answer.
2. Wire it into **the module's own recipe** - a script, a make target, an npm script - and point the
   CI row at that recipe rather than spelling the invocation out twice.
3. Pin the version where the module already pins things, with an upper bound.
4. **Prove it can fail.** Introduce a defect it should catch, watch it go red, revert. A tool that
   has never failed is decoration, and this repo has a whole write-up of checks that reported
   success without having run:
   [`docs/solutions/workflow-issues/a-check-that-reports-success-without-having-run.md`](solutions/workflow-issues/a-check-that-reports-success-without-having-run.md).
5. Replace the language's row in the table above, and say in it whether you ran the command or only
   recorded it.
