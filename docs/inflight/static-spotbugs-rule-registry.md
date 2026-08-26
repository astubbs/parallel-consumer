# SpotBugs rule registry: what is off, why, and what turns it back on

<!-- inflight-type: register -->
<!-- inflight-impact: ci -->

**Consult this before suppressing a SpotBugs finding, and before asking why a rule is not firing.**
Every rule this repo switches off is listed here with a reason and a re-enable trigger. A rule that
is off and not in this file is a bug in the setup, not a decision.

## Why a rule registry, and not the baseline

**The baseline job is gone - this branch deleted it, and the registry is what replaced it.** Written
in the present tense below while it still existed; kept because it is the justification for the
design, not a description of the build. If you are reading a `.spotbugs-baseline.xml` somewhere, it
is a leftover.

The baseline job regenerated on every push to master and turned the whole report into an exclude
filter keyed on class plus method plus bug type. It was invisible, automatic, and unbounded: nothing
recorded what it swallowed or why, and it re-swallowed on every push. That is the same shape this
repo rejects everywhere else - `FreezingArchRule` is refused in
[`docs/inflight/static-archunit-main-code-rules.md`](static-archunit-main-code-rules.md) because
"a frozen violation is a documented invariant nobody enforces", and the quarantine registry is
enforced rather than advisory for the same reason.

So: **suppress by RULE, in a checked-in filter, with an entry here.** Coarser than the baseline on
purpose. A rule that is on catches every new instance including ones in new code; a rule that is off
is visible in a diff, has a named reason, and has something that turns it back on. The count of
off-rules only goes down.

The baseline's other defect was mechanical, and is worth carrying forward because the next cached
artefact will have the same shape: its cache key was `hashFiles('**/src/main/**/*.java')`, so a
pom-only change - adding a detector plugin, for one - did not invalidate it, and `restore-keys`
served the stale one.

## The lane REPORTS, it does not BLOCK - and nothing else says so

The job runs `./mvnw -Pci test-compile spotbugs:spotbugs spotbugs:check -Dspotbugs.failOnError=false`.
Both goals, and `failOnError=false` on the second: **a SpotBugs finding cannot fail a build today.**
That is deliberate staging, not an oversight - the rule filter and this registry had to exist before
findings could be made blocking, or the first red build would have been answered with a blanket
suppression - but it was nowhere written down, so a reader who saw a green tick and a configured
analyser would reasonably conclude the code was clean.

**`spotbugs:check` is there for REPORTING, not for gating.** The lane originally ran
`spotbugs:spotbugs` alone, which writes `spotbugsXml.xml` and prints not one finding: the log says
"Skipping ... report goal" and stops. Every bit of reporting was therefore GitHub-side - annotations,
the sticky comment, the check run - and anyone running the build locally got silence over hundreds of
findings. Review caught it by asking the question nobody had: *do they at least get reported, and in
the mvn build output?* They did not.

What `check` adds is the thing neither the annotations nor the sticky comment carry: the **rule name**
on every line, which is exactly what you need to tier a finding here.

```
[ERROR] Medium: JUnit test method ...testHandleStaleWorkNoSplit() passes constant to second
        (actual) assertion parameter ... UTAO_JUNIT_ASSERTION_ODDITIES_ACTUAL_CONSTANT
```

It is not bound to a lifecycle phase: SpotBugs at `effort=Max` over the whole reactor costs minutes,
and charging that to every local `mvn test` is how a plugin gets switched off. Run the two goals by
hand when you want the list.

**Trigger to make findings BLOCK** (drop `failOnError=false`): when Tier 2 is empty, i.e. every rule
that is off is off permanently and for a stated reason. At that point a finding means a genuinely new
instance of a rule the project has decided it wants, which is exactly the thing worth blocking on.
Flipping before then makes the next unrelated PR the one that has to triage the backlog.

## The measurement this is built from

Taken 2026-08-25 against a tree at master, running the full reactor with fb-contrib 7.7.4 and
find-sec-bugs 1.14.0 added to the existing `<effort>Max</effort>` / `<threshold>Medium</threshold>`
configuration, with **no baseline applied**. Main code only - stock detectors alone: 70 findings;
with both extensions: 225 across 58 rules. Adding `includeTests` takes it to 601. It is a dated
snapshot, not a live figure. (A later `test-compile` run counted 228 main-code findings rather than
225; the drift is generated sources the `compile`-only run had not produced, and is not chased.)

**Three rules were checked in the source. Everything else is classified by its message**, which is
the same standard [`static-spotbugs-latent-findings.md`](static-spotbugs-latent-findings.md) sets,
and the reason its tier is marked *proposed* rather than settled below.

## Tier 1 - ON, and the reason the extensions are worth having

**`MUI_CONTAINSKEY_BEFORE_GET`, `ShardManager.removeWorkFromShardFor` - CHECKED, and it is
astubbs#345.** The source reads `if (processingShards.containsKey(shardKey))`, then
`processingShards.get(shardKey)`, then dereferences the result. That is the check-then-get seam of
that bug exactly, and fb-contrib names it statically in seconds with no harness, no annotation and
no interleaving search.

**This finding has a shelf life, and the note should not outlive it.** astubbs#345 is **open, not
merged** - the seam is still on master, which is why the detector sees it. When that PR lands the
`containsKey`/`get` pair goes away and `MUI_CONTAINSKEY_BEFORE_GET` reports **zero**. That does not
weaken the case for fb-contrib; it means this paragraph stops describing a live finding and starts
describing history, and somebody should reword it rather than leave a reader hunting for a race that
is fixed. This PR does not depend on astubbs#345, so the two can land in either order.

<!-- post-merge: checked-begin -->
This **narrows a standing claim** that both concurrency PoCs rest on. The evaluation note says "no
static analysis the repo runs can see the class", which remains true - stock SpotBugs at max effort
does not. But the register's stronger gloss, "nothing static does", is now measurably wrong for one
member of the family. The honest rate is **one of the four known instances**: the seam astubbs#346
fixed was a stale-check followed by a lookup, not `containsKey` before `get`, so this detector never
fired on it while it was there, and the two torn-read value-divergence instances are outside what any
check-then-act detector looks for.
<!-- post-merge: checked-end -->

Also ON, each a single finding unless noted, all in code that matters:

| Rule | Where | Note |
|---|---|---|
| `IS2_INCONSISTENT_SYNC` (2) | `DynamicLoadFactor` | CHECKED: `currentFactor` locked 71% of the time, `lastStepTime` 50%. Partial synchronisation is the reportable thing. |
| `SUI_RETURNING_MUTABLE_STATIC_MAP` | `RemovedPartitionState` | CHECKED: `getIncompleteOffsetsBelowHighestSucceeded()` returns a mutable static set. This is the shared-collections defect already on file. |
| `MS_EXPOSE_REP`, `NP_NONNULL_RETURN_VIOLATION`, `NP_NULL_PARAM_DEREF_NONVIRTUAL` | `RemovedPartitionState` | same class, same neighbourhood |
| `MUI_CALLING_SIZE_ON_SUBCONTAINER` | `ShardManager` | |
| `ICAST_INTEGER_MULTIPLY_CAST_TO_LONG` | `ProcessingShard` | int multiply widened to long after the fact - a real arithmetic class |
| `NP_NULL_ON_SOME_PATH`, `NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE`, `NP_NONNULL_FIELD_NOT_INITIALIZED_IN_CONSTRUCTOR` | `OffsetMapCodecManager`, `AbstractParallelEoSStreamProcessor`, `PartitionState` | nullness |
| `EQ_COMPARETO_USE_OBJECT_EQUALS` | `EncodedOffsetPair` | |
| `DM_DEFAULT_ENCODING`, `MDM_STRING_BYTES_ENCODING` (3) | `CoreApp` | JVM-default-dependent - the class `forbidden-apis` targets, arriving from another direction |
| `PREDICTABLE_RANDOM`, `OBJECT_DESERIALIZATION` | `JavaUtils`, `OffsetSimpleSerialisation` | the only two find-sec-bugs findings with any weight |
| `DMC_DUBIOUS_MAP_COLLECTION`, `LUI_USE_GET0`, `OI_OPTIONAL_ISSUES_*` (3), `RCN_REDUNDANT_NULLCHECK_OF_NONNULL_VALUE`, `UEC_USE_ENUM_COLLECTIONS`, `MOM_MISLEADING_OVERLOAD_MODEL`, `NM_FIELD_NAMING_CONVENTION`, `BED_BOGUS_EXCEPTION_DECLARATION`, `SEO_SUBOPTIMAL_EXPRESSION_ORDER`, `PSC_PRESIZE_COLLECTIONS`, `SPP_FIELD_COULD_BE_STATIC`, `PRMC_POSSIBLY_REDUNDANT_METHOD_CALLS`, `UCPM_USE_CHARACTER_PARAMETERIZED_METHOD`, `OCP_OVERLY_CONCRETE_COLLECTION_PARAMETER` | various | one finding each; cheap to fix, so no reason to switch off |

The stock rules already inventoried in `static-spotbugs-latent-findings.md` - `AT_*` (11),
`JLM_JSR166_UTILCONCURRENT_MONITORENTER` (5), `UR_UNINIT_READ`, `SS_SHOULD_BE_STATIC`,
`SF_SWITCH_NO_DEFAULT` (3), `DCN_NULLPOINTER_EXCEPTION` - stay ON. That note owns their triage and
says which are real; nothing here re-decides it.

## The SLF4J detectors, probed and kept

`jp.skypencil.findbugs.slf4j:bug-pattern` has not been released since 2019 and still loads cleanly on
SpotBugs 4.10.3, taking the reactor total from 601 to 643. Two of its findings are real defects in
error paths, both **CHECKED** in the source:

- `AbstractParallelEoSStreamProcessor.doClose`:
  `log.error("PC closed due to error: {}", getFailureCause(), null)` - the cause is bound to the
  placeholder and the trailing argument is `null` rather than the throwable, so SLF4J prints the
  exception's `toString()` and **discards the stack trace**. That is the log line PC emits when it
  closes because something went wrong, in a library whose hardest open bug is a silent stall.
- `ThreadUtils.sleepLog`: `log.error("Sleep of {} interrupted", e, ms)` - arguments reversed. It
  prints "Sleep of java.lang.InterruptedException interrupted", drops the duration, and again
  attaches no stack trace.

| Rule | Count | Profile | Tier |
|---|--:|---|---|
| `SLF4J_PLACE_HOLDER_MISMATCH` | 2 | - | **ON** - both are the defects above |
| `SLF4J_ILLEGAL_PASSED_CLASS` | 3 | - | ON - all in test code, cheap |
| `SLF4J_SIGN_ONLY_FORMAT` | 3 | - | ON |
| `SLF4J_FORMAT_SHOULD_BE_CONST` | 28 | `old` | **OFF.** A non-constant format string is how this codebase composes conditional log messages - a deliberate idiom, not a backlog. Turns back on only if a placeholder bug is ever traced to a computed format string. |

## Test code, measured separately

`includeTests` is on and CI runs `test-compile`, so the test tree is analysed for the first time.
Measured the same day, same configuration: **601 findings total - 228 in main code, 373 in test
code**, zero unclassified (split by whether the class file lands in `target/classes` or
`target/test-classes`, not by class name, because a name-based guess mis-sorted a third of them).
Test code uses 75 distinct rules against main code's 59.

**Three checked in the source. The rest are classified by their message.**

### Test-side Tier 1 - ON

- **`UTAO_JUNIT_ASSERTION_ODDITIES_ACTUAL_CONSTANT` (4) - CHECKED, and worth fixing, but not what it
  first looked like.** All four are in
  `AbstractParallelEoSStreamProcessorConfigurationTest.testHandleStaleWorkSplit` and
  `.testHandleStaleWorkNoSplit`, and they are **argument-swapped**, not tautological:
  `Assertions.assertEquals(testInstance.getMailBoxSuccessCnt(), 1)` puts the constant in JUnit's
  *actual* slot. The assertion still tests the right thing; what breaks is the **failure message**,
  which reports expected and actual the wrong way round. That is a signal that lies while looking
  healthy - this directory's highest-priority impact class - and it costs most exactly when someone
  is reading a failure under time pressure. Cheap to fix.
- **`SWL_SLEEP_WITH_LOCK_HELD` (1)**, `MockConsumerCommitTimeoutTest$1.commitSync`. Sleeping while
  holding a lock, inside the mock that models commit timeouts, in a repo that tracks commit-timeout
  flakes. Not diagnosed here; worth someone's attention on that ground alone.
- **`AT_NONATOMIC_64BIT_PRIMITIVE` (1)**, `RebalanceEoSDeadlockTest.noDeadlockOnRevoke`. A
  non-atomic 64-bit primitive - the torn-read family's own shape - inside a deadlock test.
- `NOS_NON_OWNED_SYNCHRONIZATION` (1) `LargeVolumeInMemoryTests`,
  `LSYC_LOCAL_SYNCHRONIZED_COLLECTION` (1) `PartitionStateCommittedOffsetIT`,
  `OBL_UNSATISFIED_OBLIGATION` (2) `DbTest`, `IMC_IMMATURE_CLASS_PRINTSTACKTRACE` (2).
- find-sec-bugs, test-only, one each: `COMMAND_INJECTION`, `OVERLY_PERMISSIVE_FILE_PERMISSION`,
  `URLCONNECTION_SSRF_FD`. Low stakes in test code, but three findings is not a reason to switch a
  rule off.

### Test-side, read before deciding

**`ST_WRITE_TO_STATIC_FROM_INSTANCE_METHOD` (7).** Cross-test static state is not hypothetical here -
astubbs#101 was "uncollected tests, cross-test static state, and a timing flake". Seven instances,
none inspected. This is the test-side equivalent of `EXS_` above: possibly style, possibly a repeat
of a bug this repo has already paid for once.

### Test-side Tier 3 - OFF, expected to stay off

| Rule | Count | Profile | Reason |
|---|--:|---|---|
| `PREDICTABLE_RANDOM` | 10 in test code | `old` | A seeded, reproducible generator is what a test **should** use. The rule stays ON for main code, where its single finding is real. |
| `DLS_DEAD_LOCAL_STORE` | 19 | `old` | A named local holding an intermediate value is how readable test setup is written. |
| `ENMI_EQUALS_ON_ENUM` (12), `LSC_LITERAL_STRING_COMPARISON` (7), `CE_CLASS_ENVY` (5) | 24 | `new` | Style, and only the existing tests trip it. New test code has no reason to. |

## Top 5 to turn back on whole-tree, ranked

**Ranked by value divided by cost, not by count.** Value is how close the rule sits to a defect class
this repo has actually paid for; cost is how many sites and how much judgement each needs. Every one
is `profile: new` - the rule is right, only the existing tree blocks it - so clearing the sites is the
whole job, and each one that clears deletes a row from this file.

| # | Rule | Sites | Why this one | Effort |
|---|---|--:|---|---|
| 1 | `LEST_LOST_EXCEPTION_STACK_TRACE` | 5 | **Closest to a defect already found here.** This PR turned up two SLF4J calls that discard a stack trace, one of them on the line PC emits when it closes because something went wrong. Same class, different engine. | Read 5 sites |
| 2 | `MS_SHOULD_BE_FINAL` | 5 | Mutable statics in a concurrency library. `RemovedPartitionState`'s shared mutable set - already on file as a bug - is this family. | Read 5 sites |
| 3 | `LO_APPENDED_STRING_IN_FORMAT_STRING` | 2 | Cheapest on the list, and logs are the debugging surface for the stall family. | Mechanical |
| 4 | `EXS_EXCEPTION_SOFTENING_NO_CONSTRAINTS` | 12 | **Read this one before deciding**, and see [`static-sneaky-throws-blind-the-analysers.md`](static-sneaky-throws-blind-the-analysers.md), which owns the code-change half. `docs/solutions/best-practices/sneaky-thrown-checked-exceptions-defeat-spotbugs-dataflow.md` records that sneaky-throw already blinds SpotBugs here, so these twelve may mark a real analysis blind spot rather than style. Highest potential value, least certain. | Investigate first |
| 5 | `ENMI_EQUALS_ON_ENUM` | 7 | No judgement required at any site - `equals` to `==`. Clears a whole rule for near-zero risk. | Mechanical |

**Deliberately not in the top 5**, so the omissions are arguable rather than accidental:
`USBR` (28) and `UMTP` (26) are the two largest and the two least valuable - pure style. `CT_CONSTRUCTOR_THROW` and
`FCCD` are worth having but are gated on other work (the God-class decomposition and the ArchUnit
dependency-direction rule respectively), so effort spent now is effort spent twice.

## Tier 2, grouped by what clearing it actually takes

The trigger column says *when*; this says *what kind of work*. It is the difference between an
afternoon and a design decision.

- **Mechanical, no judgement** - `ENMI_EQUALS_ON_ENUM`, `NAB_NEEDLESS_BOOLEAN_CONSTANT_CONVERSION`,
  `LO_APPENDED_STRING_IN_FORMAT_STRING`, `USBR_UNNECESSARY_STORE_BEFORE_RETURN`.
- **Read the sites, then decide** - `LEST_LOST_EXCEPTION_STACK_TRACE`, `MS_SHOULD_BE_FINAL`,
  `EXS_EXCEPTION_SOFTENING_NO_CONSTRAINTS`, and both `old?` rows.
- **Blocked on other work** - `CT_CONSTRUCTOR_THROW` (God-class decomposition), `FCCD` (the ArchUnit
  dependency rule), `MRC_METHOD_RETURNS_CONSTANT` and `UMTP` (the options rework), `IPU_*`,
  `UP_UNUSED_PARAMETER`, `STT_TOSTRING_STORED_IN_FIELD` (next time the examples are touched).
- **Permanent** - everything in Tier 3, and the test-side `old` rows. Not work.

## Tier 2 - OFF now, with the trigger that turns each back on

Proposed, not settled: none of these was checked in the source. Each is off because it arrives in
bulk, and bulk on arrival is what makes a new engine get ignored.

**`profile:` marks which of these are off for a REASON versus off for a BACKLOG** - see
[`static-analysis-rule-profiles.md`](static-analysis-rule-profiles.md). `new` means the rule is right
and only the legacy tree blocks it, so it goes on for new code the day the diff gate exists; `old`
means it stays off everywhere. Almost all of Tier 2 is `new`, which is the point: these are rules
nobody disagreed with, currently unguarding every line being written.

| Rule | Count | Profile | Why off | Turns back on when |
|---|--:|---|---|---|
| `USBR_UNNECESSARY_STORE_BEFORE_RETURN` | 28 | `new` | pure style; a named local before `return` is often the more readable form | someone wants the sweep; it is mechanical |
| `UMTP_UNBOUND_METHOD_TEMPLATE_PARAMETER` | 26 | `new` | generics-signature style, concentrated in `ParallelConsumerOptions` and the vertx result type | the public API surface is next revised - a major, so release-gated |
| `NAB_NEEDLESS_BOOLEAN_CONSTANT_CONVERSION` | 14 | `new` | style | mechanical sweep |
| `EXS_EXCEPTION_SOFTENING_NO_CONSTRAINTS` | 12 | `new` | flags wrapping a checked exception in an unchecked one, which this codebase does deliberately | **read this one first.** `docs/solutions/best-practices/sneaky-thrown-checked-exceptions-defeat-spotbugs-dataflow.md` says sneaky-throw already defeats SpotBugs dataflow here, so these 12 may be pointing at a real analysis blind spot rather than at style |
| `CT_CONSTRUCTOR_THROW` | 9 | `new` | constructors that can throw; partially inherent to the builder-heavy design | after the God-class decomposition |
| `FCCD_FIND_CLASS_CIRCULAR_DEPENDENCY` | 8 | `new` | circular class dependencies | pairs with the ArchUnit dependency-direction rule that note already proposes - same finding, two engines |
| `ENMI_EQUALS_ON_ENUM` | 7 | `new` | `equals` on an enum instead of `==` | mechanical sweep |
| `MRC_METHOD_RETURNS_CONSTANT` | 7 | `new` | all in `ParallelConsumerOptions` | with the options rework |
| `RFI_SET_ACCESSIBLE` | 7 | `old?` | `setAccessible` in `AbstractParallelEoSStreamProcessor` (4 calls) and `ProducerWrapper` (3); the producer reflection is load-bearing | never, most likely - but the 7 findings are 7 CALLS in 2 classes, plus 5 more in 3 test classes, so size any attempt against 12 |
| `MS_SHOULD_BE_FINAL` | 5 | `new` | mutable statics in the codec classes | with the codec work |
| `LEST_LOST_EXCEPTION_STACK_TRACE` | 5 | `new` | same family as `EXS_`, read together | as above |
| `IPU_IMPROPER_PROPERTIES_USE` + `_SETPROPERTY` | 6 | `new` | examples only | when the examples are next touched |
| `UP_UNUSED_PARAMETER`, `STT_TOSTRING_STORED_IN_FIELD` | 8 | `new` | examples only (`CoreApp`) | as above |
| `ACEM_ABSTRACT_CLASS_EMPTY_METHODS` | 3 | `old?` | deliberate no-op hooks | probably permanent; needs one look |
| `LO_APPENDED_STRING_IN_FORMAT_STRING` | 2 | `new` | logging style | mechanical sweep |

**Two rows are marked `old?` rather than `old`, and the question mark is the honest part.**
`RFI_SET_ACCESSIBLE` covers the producer reflection, which is load-bearing (7 calls across 2 main
classes, and 5 more in tests - the count reads as "two sites" only if you count classes), and
`ACEM_ABSTRACT_CLASS_EMPTY_METHODS` covers deliberate no-op hooks - both look permanent, and neither
was checked in the source. Resolve them by reading the call sites, not by ageing.

## Tier 3 - OFF, expected to stay off

Every row here is `profile: old`: wrong for this codebase, so off for new code too. This is the
registry's real content, and after the diff gate lands it is close to all that should remain in it.

| Rule | Count | Reason |
|---|--:|---|
| `EI_EXPOSE_REP` / `EI_EXPOSE_REP2` | 21 | Storing and returning collaborators passed in **is** how this library composes - `PCModule` holds the options, `BrokerPollSystem` holds the `ConsumerManager`. Defensive copying would break the DI wiring rather than fix anything. This is not a new judgement: `static-spotbugs-latent-findings.md` reached it already, on the same rules, with the same reasoning. |
| `IMC_IMMATURE_CLASS_IDE_GENERATED_PARAMETER_NAMES` | 1 | opinion about parameter naming |

## The one CLASS-scoped suppression, and why it is not a rule-scoped one

`MUI_CONTAINSKEY_BEFORE_GET` is switched off for
`bz.stub.parallelconsumer.state.LincheckToolchainProbeTest` and for nothing else.

That class is astubbs#347's **red control**: a deliberately torn
`containsKey` / `get` / dereference on a `ConcurrentHashMap`, kept broken on purpose because it is
the arm proving Lincheck can see the defect class at all. Fixing the finding would turn that
calibration into what its own commit message calls "a green lamp wired to nothing".

**The suppression names the class, never the rule**, and the distinction is the whole point:
`MUI_CONTAINSKEY_BEFORE_GET` is the finding that justified adopting fb-contrib - it names
`ShardManager.removeWorkFromShardFor`, a real defect - so switching it off globally to quieten a
deliberate fixture would trade the headline result for silence. Asserted after the change rather
than assumed: the rule reports **0** on the probe and still reports on `ShardManager`.

**It also narrowed a claim in that probe's own javadoc**, which said SpotBugs reported nothing on
this shape. True of the stock detectors, and measured; with fb-contrib the shape is named in
seconds. The javadoc is corrected in the same change rather than left to contradict the tool now
analysing it - the probe is still the right red control for *Lincheck*, since a static detector
cannot exhibit an interleaving, but "no static analysis can see this" is now "no stock detector
could".

This is the model for any future site-scoped exclusion: name the class, state why the code is
deliberately that way, and assert the rule still fires elsewhere.

## A scoping limitation you will hit, and how it was found

**A SpotBugs filter cannot express "test source root".** The XML records `sourcepath` as the
PACKAGE-relative path (`bz/stub/parallelconsumer/mutiny/MutinyPCTest.java`), so `src/test` never
appears in it and a `<Source name="~.*src/test.*"/>` matcher **matches nothing at all**. The first
version of `spotbugs-exclude.xml` used exactly that, and it was completely inert. It was caught only
by asserting after the run that `PREDICTABLE_RANDOM` had actually gone to zero - the build was green
and the file was valid XML either way, which is this repo's standard silent-green shape.

The test-scoped entries therefore match on `<Class name="~.*(Test|Tests|IT|ITCase)(\$.*)?"/>`, the
weaker discriminator. **It leaks**: a helper named `Fixture`, `Harness` or `MockSomething` is test
code the pattern does not catch, which is why `PREDICTABLE_RANDOM` still reports ten and
`DLS_DEAD_LOCAL_STORE` one after filtering. Excluding those rules globally instead would drop the one
REAL main-code `PREDICTABLE_RANDOM` finding, so the leak is the better trade - but it is a leak, not
a design.

**Verify by count, never by "the build went green".** The rule and the five sightings behind it are
owned by
[`docs/solutions/workflow-issues/an-inert-analysis-config-reads-as-a-clean-codebase.md`](../solutions/workflow-issues/an-inert-analysis-config-reads-as-a-clean-codebase.md);
what follows is only its application to this file. After changing this file, run the analysis and
assert the rule you excluded reports zero. Filter typos do not fail anything.

## javac's own analysis, and the inert first attempt

`-Xlint:all -Xlint:-processing` is on, **without** `-Werror`, and `rawtypes` is additionally off for
TEST compilation only. There was no `<compilerArgs>` block in this build at all before, so this is
analysis that shipped with the compiler and was never switched on.

**An earlier version of this section said "172 warnings, all in test code or against deprecated
third-party API". Both halves were wrong, and the way it was caught is worth more than the
correction: a human scrolled the Files Changed tab of the PR that turned the flag on.** The
annotations there were mostly on `AbstractParallelEoSStreamProcessor` - which is main code - naming
`isUsingTransactionalProducer()` and `WorkContainer.setFuture`, which are this project's own API, not
a third party's.

Re-measured on core plus parent, `test-compile`, deduplicated by file, line and category:

| Where | Before the `rawtypes` narrowing | After |
|---|--:|--:|
| **main code** | 58 | **58** |
| `src/test/` + `src/test-integration/` | 226 | 184 |
| **generated sources** (Truth assertion generator) | **559** | 24 |
| total | 843 | **266** |

Two things that table settles. **The channel was two-thirds code nobody wrote** - 535 of the 585
`rawtypes` were generated `List` parameters in
`target/generated-test-sources/truth-assertions-managed`, which is why `rawtypes` is now off for test
compilation only and untouched for main. And **the headline number is meaningless without saying
whether generated sources are counted**, which the old text did not: the same tree is 843 or 284
depending on that one decision, so any target derived from it was arbitrary.

After the narrowing, the 266 break down as unchecked (138), deprecation (106), serial (13),
rawtypes (8, all main), cast (1).

`-Werror` is a second step, deliberately. Promote it when the inventory reaches zero, not before; a
build that goes red on a deprecation in a TestContainers class helps nobody. The 58 main-code
warnings are the ones worth working down first - they are our own deprecated API and our own
generics, and unlike the test ones nobody can argue they are somebody else's problem.

**The first attempt at this was inert, and the way it was caught is the reusable part.** The
`<compilerArgs>` block was inserted into the *first* `maven-compiler-plugin` declaration in the
parent pom - which lives inside the `intellij-idea-only` profile, and CI never activates it. The
build was green and the run reported **6** warnings, all of them from Lombok and the Truth generator
rather than from lint, which reads exactly like "the codebase is clean".

The check that caught it was `./mvnw help:effective-pom` plus `grep -c 'Xlint:all'`, asserting the
count is greater than zero. Green told me nothing; the effective pom told me the flag was not there.
**Do that for any build-config change whose only evidence is an absence of output** - AGENTS.md's
"verify your instrumentation actually reached the run", arriving in a pom rather than a log config.
The difference between the two runs was 6 warnings and 172 - both figures superseded by the table above, and kept here because they are the evidence for the detection method rather than a current count.

## Rules for changing this file

The three that bind every rule registry - off requires an entry, an entry needs a trigger rather than
a date, and the off set only shrinks - are **owned by**
[`static-analysis-rule-profiles.md`](static-analysis-rule-profiles.md), "Rules every rule registry
follows". They are not restated here; a second copy is how two statements of one contract drift.

Specific to this file:

- **The entry point is `config/spotbugs-exclude.xml`.** A rule reaches the off set by being added to that
  filter, so "off" here always means a `<Match>` element somebody can see in a diff.
- **Moving a rule from Tier 2 to Tier 1 deletes its row.** This file is deleted outright when Tier 2
  and Tier 3 are both empty - which is also the trigger for making the lane blocking, above.
- **Tier assignments marked proposed become settled by reading the source**, not by ageing. A
  proposal that has sat unexamined for months is still a proposal.
