# Error Prone rule registry: the pin, what is off, and what turns each back on

<!-- inflight-type: register -->
<!-- inflight-impact: ci -->

**Consult this before suppressing an Error Prone finding, before adding a `-Xep:` flag, and before
asking why a check is not firing.** Every check this repo switches off is listed here with a reason
and a re-enable trigger. A check that is off and not in this file is a bug in the setup, not a
decision. Same contract as
[`static-spotbugs-rule-registry.md`](static-spotbugs-rule-registry.md), deliberately - two engines,
one discipline.

## The version pin, and the one thing that lifts it

`error_prone_core` is held at **2.42.0** and `nullaway` at **0.12.7**. Both are pinned below current,
and the pin is temporary rather than a preference.

**2.43.0 is the first Error Prone release compiled to class file version 65 (Java 21).** 2.42.0 is
61. This build requires JDK 17, which reads to 61, so from 2.43.0 the plugin class cannot be loaded
at all. Raising the toolchain is not the escape: Jabel's bundled Byte Buddy refuses class file 65, so
**no JDK satisfies both**. NullAway is a third constraint in the same knot - 0.12.7 dies against
2.43.0 and later on a class that release removed, and because NullAway takes Error Prone as
`compileOnly` no dependency tool can see the coupling. Move the two versions together.

**The trigger is Jabel's removal**, which is tracked with the Java baseline work it belongs to:
[`pr-53-java-baseline-kafka4.md`](pr-53-java-baseline-kafka4.md) (astubbs#53, deferred to 0.7.x).
That note owns the Jabel story; it is not restated here.

**What to do when Jabel goes:** raise `error-prone.version` and `nullaway.version` together to
current, delete this section, and re-run the off-set verification below - a newer Error Prone renames
and retires checks, so a `-Xep:` flag naming a check that no longer exists is silently inert rather
than an error. The full diagnosis behind the pin, with its control arms, is settled and lives in
[`error-prone-jabel-and-the-jdk-that-satisfies-neither-2026-08-25.md`](../solutions/build-errors/error-prone-jabel-and-the-jdk-that-satisfies-neither-2026-08-25.md).

## The measurement this is built from

Taken 2026-08-25 on the full reactor, `test-compile`, main and test sources, with every check on and
javac's warning cap lifted: **951 findings across 51 checks**. After the off-set and the
generated-source exclusion below: **251 findings across 33 checks - 63 in main code, 188 in test.**

That is a dated snapshot, not a live figure. It is also only readable because of two things that are
easy to get wrong, and both were:

- **javac caps output at 100 warnings per compilation and says nothing when it truncates.** With
  `-Xlint:all` and Error Prone feeding the same stream, the visible count was roughly a tenth of the
  real haul and read exactly like a small one. `-Xmaxwarns` is set high in the pom for that reason.
- **Generated sources were 54 of the findings** - the Truth generator's output, rewritten on every
  build, so nobody can act on one and the count moves whenever a clean run regenerates them.
  Excluded by path with `-XepExcludedPaths:`, which takes a **colon**; the equals form is rejected as
  `invalid flag` through a Maven error path that hides the message.

## Tier 1 - ON, and the three that are ERROR

Everything not listed in Tier 2 is on. Three checks are ERROR severity and fail the build.

**`DoNotCall` - newly enforceable, and that is the point.** `parallel-consumer-streams` annotates the
Kafka Streams DSL overloads it refuses with `@DoNotCall`, and until now **no build in this repo could
enforce it** - stated as a known hole in
[`a-deprecation-without-an-explanation-misattributes-itself-to-the-wrong-party.md`](../solutions/architecture-patterns/a-deprecation-without-an-explanation-misattributes-itself-to-the-wrong-party.md).
It is enforceable now. It arrived red on `ReactorTest.publishOn` and `.subscribeOn`, which called
`new Thread(...).run()`; **fixed rather than demoted**, because calling `run()` on a Thread is never
what anyone intends. The threads are started and joined, so the subscription really does happen off
the test thread and still completes before the method returns. Those tests still assert nothing; that
is a separate call and `docs/test-hardening/inactive-tests-audit-2026-08-08.md` owns it.

**`ReturnValueIgnored` - ON, one site suppressed.** `AbstractParallelEoSStreamProcessor` forces its
memoized worker-pool supplier at construction and discards the pool on purpose. Extracted into
`forceWorkerPoolConstruction()` so `@SuppressWarnings` covers exactly that call. A named local was
tried first and rejected: it satisfies Error Prone and then trips SpotBugs' `DLS_DEAD_LOCAL_STORE`,
which trades one finding for another rather than saying what is meant. Caught by counting SpotBugs
findings before and after, not by the build going green.

**`BanJNDI` - ON globally, suppressed at three named methods.**
`AbstractParallelEoSStreamProcessor#setupWorkerPool`, `#supervisorLoop` and `BrokerPollSystem#start`
look up a container-managed thread factory or executor by a name the embedding application supplies
through this library's own options, each falling back to Java SE when the lookup fails. That is the
feature, not a lint hit. **Suppressed at the site rather than demoted globally, and the narrowness is
proven**: a JNDI lookup added to an unannotated method in the same class fails the build at ERROR.

Also ON, and these are where the value is - almost all one finding each, and concurrency-shaped:
`WaitNotInLoop`, `UnsynchronizedOverridesSynchronized`, `LockNotBeforeTry`, `PrimitiveAtomicReference`,
`StaticAssignmentInConstructor`, `ModifyCollectionInEnhancedForLoop`, `StreamResourceLeak`,
`EmptyCatch`, `CatchAndPrintStackTrace`, `ModifiedButNotUsed`, `UnnecessaryAsync`, `ReferenceEquality`,
`EqualsGetClass`, `ByteBufferBackingArray`, `NarrowCalculation`, `LongDoubleConversion`, `IntLongMath`,
`MixedMutabilityReturnType`, `UndefinedEquals`, `StringSplitter`, `JavaTimeDefaultTimeZone`,
`ImmutableEnumChecker`, `MissingCasesInEnumSwitch`, `JdkObsolete`, `DefaultCharset`,
`StringCaseLocaleUsage`, `NonApiType`, `MissingOverride`, `BooleanLiteral`, `ClassCanBeStatic`,
`EffectivelyPrivate`, `InlineMeSuggester`, `FutureReturnValueIgnored`.

**`NullAway` is ON and is the deliberate exception to the bulk rule below**, at 148 findings (38 in
main code, 110 in test). Everything else that arrives in bulk is off; this one is not, because
nullness is the class several of this repo's tracked defects actually live in, and paying its arrival
cost is the point of adding it. The largest group is "initializer method does not guarantee @NonNull
fields are initialized along all control-flow paths", which is the shape of the
fields-assigned-outside-the-constructor problem the state classes already have. Main code first.

There is no source-root scoping available to hide the test half behind: NullAway's knobs are package-
or class-keyed (`AnnotatedPackages`, `UnannotatedSubPackages`, `ExcludedClasses`,
`ExcludedClassAnnotations`) and main and test share packages here - the same limitation the SpotBugs
registry ran into from the other direction.

## Top 3 to turn back on whole-tree, ranked

Same criterion as the other registries: **value over cost**, where value is how close the check sits
to a defect class this repo has already paid for, and cost is sites plus judgement. All three are
`profile: new` - the check is right and only the existing tree blocks it - so clearing the sites is
the entire job.

Three rather than five, deliberately. Below these the list is javadoc and formatting, and padding a
ranking with things nobody should do next makes the ranking worthless.

| # | Check | Sites | Why this one | Effort |
|---|---|--:|---|---|
| 1 | `InvalidParam` | 18 | **The only javadoc check that is really a correctness check** - a `@param` naming a parameter the method does not have means the signature changed and the doc did not. All main code, and this project publishes its javadoc. | Read 18, mostly mechanical |
| 2 | `UnusedMethod` | 58 | Dead code in a library is either genuinely dead or accidentally-public API surface, and the two need opposite fixes. High value precisely because it cannot be swept - every site is a question worth answering once. | Judgement per site |
| 3 | `CanonicalDuration` + `JavaDurationGetSecondsToToSeconds` | 38 | One `java.time` sweep clears two checks. Mostly tests, no judgement, and `Duration` handling is load-bearing in a library whose timeouts are its contract. | Mechanical |

**Deliberately not ranked:** the javadoc family minus `InvalidParam` (203 findings that are
documentation debt, not defect risk - and the README is generated from tagged source, so a sweep
there has a second-order effect worth doing on purpose rather than incidentally);
`UnnecessaryParentheses` at 240, the largest and least valuable on the list; `UnusedVariable` and
`BadImport`, both cheap but neither near a defect class.

**NullAway is not in this list because it is already ON**, at 148 findings, which is the deliberate
exception to the bulk-off rule stated above.

## OFF because it CRASHES THE BUILD, which Tier 2 is not about

`UnnecessaryStringBuilder` is off for a different reason from everything below, and the distinction
matters: Tier 2 is "arrives in bulk"; this one **cannot be left on at all**.

**Every `record` in code Error Prone analyses fails the compile.** 2.42.0 throws
`IllegalArgumentException: invalid replacement: [0, -1)` building its suggested fix over a record's
compiler-generated `toString()`, which has no source positions - so the check reports an internal
error rather than a finding, and javac fails.

**Measured, not inferred.** Found when `parallel-consumer-proxy-protocol`'s `WireDurationsTest` -
one `@Desugar record Row` - broke `test-compile`, and confirmed by a control arm: pasting a
three-component record into an unrelated core test file (`CapacitySchedules`) reproduced the same
crash on the same check, then reverted. Nothing about the proxy module is involved.

**It turns back on when the Error Prone pin lifts** (see the version-pin section above) - the fix is
theirs, not ours, and re-enabling should be paired with re-running the off-set verification. Until
then this repo would otherwise be one `record` away from a red build with no finding to read.

## Tier 2 - OFF, with the trigger that turns each back on

All off for one reason: **they arrive in bulk, and bulk on arrival is what makes a new engine get
ignored.** None is off because it is wrong. Counts are from the everything-on measurement.

| Check | Count | Why off | Turns back on when |
|---|--:|---|---|
| `UnnecessaryParentheses` | 240 | formatting opinion; this codebase parenthesises for readability, and 240 of anything drowns the hundred findings worth reading | a mechanical sweep is wanted, or a precedence bug is ever traced to parenthesisation |
| `InvalidInlineTag` | 106 | javadoc | with the javadoc pass - and the README is generated from tagged source, so that pass has a deadline of its own |
| `MissingSummary` | 71 | javadoc | as above |
| `EmptyBlockTag` | 21 | javadoc | as above |
| `InvalidParam` | 18 | javadoc, all main code. Closest of the family to a correctness check: a `@param` naming a parameter that does not exist is documentation that lies | as above, and read this one first |
| `InvalidBlockTag` | 4 | javadoc | as above |
| `UnrecognisedJavadocTag` | 3 | javadoc | as above |
| `InvalidLink` | 2 | javadoc | as above |
| `AlmostJavadoc` | 1 | javadoc | as above |
| `NotJavadoc` | 1 | javadoc | as above |
| `UnusedMethod` | 58 | needs judgement per site rather than a sweep: some are genuinely dead, some are public surface a library may not delete | with the God-class decomposition, or the next API review - release-gated either way |
| `UnusedVariable` | 54 | mostly test locals | mechanical sweep |
| `BadImport` | 34 | importing nested types; style | mechanical sweep |
| `CanonicalDuration` | 22 | `Duration` literal style, mostly tests | mechanical sweep |
| `JavaDurationGetSecondsToToSeconds` | 16 | `java.time` API preference | mechanical sweep, with `CanonicalDuration` |

## Verify by count, never by "the build went green"


**The rule and its evidence are owned by**
[`an-inert-analysis-config-reads-as-a-clean-codebase.md`](../solutions/workflow-issues/an-inert-analysis-config-reads-as-a-clean-codebase.md).
What is kept here is only the part that binds this engine.
A `-Xep:` flag naming a check that does not exist is **silently inert**, and a suppression that
matches nothing looks exactly like a suppression that works. After changing the off-set, run the
reactor and assert the check you disabled reports **zero**, and that the ones you did not disable
still report what they did.

That is how both mistakes in this file's own construction were caught: the generated-source haul the
warning cap was hiding, and the `DLS_DEAD_LOCAL_STORE` that the first `ReturnValueIgnored` fix
introduced while removing an Error Prone finding. Both builds were green.

**An ERROR-severity check reporting zero needs a positive control**, because "clean code" and
"accidentally switched off" look identical. Both were proven by mutation on the full reactor, one term
changed: restoring one `Thread.run()` fails the build on `DoNotCall`; adding a JNDI lookup to an
unannotated method fails it on `BanJNDI`. Run the **whole** reactor for this - a single-module run can
fail on the enforcer's `ReactorModuleConvergence` rule instead and read as a working control, which
has already happened once on this branch.

## The cost of running at compile, measured and accepted

The Error Prone configuration is **unconditional** - not in a profile, not in `pluginManagement` -
so it applies to every `javac` invocation in every module, main and test, with `<fork>true</fork>`
and around ten `--add-exports`/`--add-opens` flags. Several CI jobs each do a from-scratch full
reactor compile of the same commit, so the identical analysis runs several times per PR for the same
result.

**A simplification pass proposed gating it to one job, and it was declined.** The saving is real, but
the fix would remove the property the engine was chosen for. The build-hardening register's entry
says it plainly: Error Prone's advantage over SpotBugs is that its "findings block at compile rather
than accreting into a baseline", and this registry records `BanJNDI` proven by mutation to "fail the
build at ERROR". Gate it to one CI job and a developer's own build stops blocking - the finding still
appears, but remotely, later, and after the code has been written on top of. That is the SpotBugs
shape this whole change moved away from.

So the redundancy is carried deliberately. **The trigger for revisiting it is CI wall-clock becoming
a real constraint**, not tidiness - and if that happens, the lever is a profile plus one job flag,
and the price is compile-time blocking. Whoever pulls it should say so here.

## Rules for changing this file

The three that bind every rule registry - off requires an entry, an entry needs a trigger rather than
a date, and the off set only shrinks - are **owned by**
[`static-analysis-rule-profiles.md`](static-analysis-rule-profiles.md), "Rules every rule registry
follows". They are not restated here; a second copy is how two statements of one contract drift.

Specific to this file:

- **The entry point is a `-Xep:<Check>:OFF` flag in the parent pom**, so "off" here always means a
  compiler argument somebody can see in a diff. Tier 1 is empty of rows by construction.
- **Suppress at the narrowest scope that works, and prove the narrowness.** A site suppression plus a
  mutation showing the check still fires elsewhere beats a global demotion, every time. `BanJNDI` is
  the worked example: the suppression sat on three methods, one of them fifty lines long, until it
  was moved onto a private lookup helper whose entire body is the call being excused. Removing that
  one annotation fails the build; that is what "prove the narrowness" costs, and it is two minutes.
