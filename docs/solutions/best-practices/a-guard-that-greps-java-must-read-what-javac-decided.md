---
title: "A guard that greps Java source must read what javac decided: every text heuristic is a parser fragment with a silent failure mode"
module: tooling
date: 2026-09-05
category: best-practices
problem_type: best_practice
component: development_workflow
severity: high
applies_when:
  - "Writing or reviewing a guard that infers Java class shape (a class's name, whether it is abstract, its supertype, which methods carry an annotation) by grepping the .java source"
  - "The guard needs to reason about something the source text does not state directly: inherited annotations, meta-annotated tag groups, multiple top-level or nested classes in one file, multi-hop inheritance"
  - "A review round on the guard keeps finding a new false exemption or false demand of the same shape rather than a genuinely new class of bug"
  - "Deciding whether to add another grep/awk pattern to a shell guard or switch it to reading the compiler's own output"
  - "A bare keyword grep (extends, @Tag, @Quarantined) could match the word inside a comment, javadoc, or string literal"
tags:
  - javap
  - bytecode-introspection
  - grep-is-not-a-parser
  - guard-design
  - annotation-inheritance
  - silent-false-negative
  - ci-gate
  - test-completeness
related_components:
  - bin/lib/compiled-classes.mjs
  - bin/check-integration-shard-coverage.mjs
  - bin/test-check-integration-shard-coverage.mjs
---

# A guard that greps Java source must read what javac decided: every text heuristic is a parser fragment with a silent failure mode

## Context

A CI guard has to answer a question that sounds like text-matching but is not: "did every integration
test class that exists produce a failsafe report somewhere?" The lane it protects (`bin/ci-integration-test.sh`)
splits into a named heavy shard plus a catch-all defined by subtraction, so a class that lands in
neither shard fails nothing - it just silently stops running. The first implementation answered the
question by scanning `.java` source with shell tools: grep for `@Tag`/`@Quarantined`, derive the class
name from the filename, grep for `extends`. Across three review rounds it accumulated six defects, and
every one was the same shape, not six unrelated bugs:

1. a file-wide annotation grep exempted a whole class the moment *one method* carried the tag,
   un-guarding `LoadTest`'s ordinary tests;
2. the class name came from the filename, blind to three other package-private top-level classes
   sharing `Rebalance857CommitSyncDeadlockProbeIT.java` - one of them, `Probe4IT`, went uncovered;
3. a class that inherits `@RepeatedTest` from an abstract base declares no annotation in its own file
   and reads as a plain helper;
4. a bare `extends` grep matched the word inside a javadoc sentence in `ChaosSeed.java` and would have
   demanded a report from a helper that never runs - a permanent false RED, the opposite failure
   direction from the one the guard exists to prevent;
5. inheritance was followed one hop only;
6. picking a same-named class by `find | head -1` picked by filesystem order across modules.

Each fix was another fragment of a Java parser rewritten in grep. A dry run of the shell version also
gave a confidently wrong answer for an unrelated reason - zsh does not word-split an unquoted parameter
where bash does - which is the same class of trap wearing a different hat: text that looks structured
is not the structure. The operator's ruling after round three was direct: "Don't write it in bash." The
fix, on the current tree (astubbs/parallel-consumer#442, open and unmerged as of this writing), replaces
every one of those text heuristics with a read of what `javac` already decided.

## Guidance

**When a guard has to reason about compiled artefacts - class identity, inheritance, modifiers,
annotations, "does this class produce a test result" - read the compiler's own output, not the source
text a human wrote for a different audience.** The source was never structured for the guard's
question; the `.class` file is exactly that structure, already resolved.

`bin/lib/compiled-classes.mjs` does this by shelling to `javap -v -p` in batches (`inspect`, batch size
comment in the same file) and parsing the verbose disassembly into one record per class (`parseJavap`,
split on `^Classfile `). The facts it reads, and why each one only exists in bytecode:

| Fact | Where in `javap -v -p` output | Anchor in `compiled-classes.mjs` |
|---|---|---|
| Is the class abstract | `flags:` line, an access flag (`ACC_ABSTRACT`), and it must be the **first** `flags:` in the block - every later one belongs to a field or method | `classFlags` regex plus the comment "The first draft read the first one AFTER `super_class:`" |
| What it extends | `super_class:` field, followed to closure through a visited set | `buildIndex`'s `frontier`/`next` loop |
| Whether it (or an ancestor) has a test | JUnit annotation descriptors on methods, e.g. `Lorg/junit/jupiter/api/Test;` | `TEST_DESCRIPTORS` array, `inheritsTest` |
| Class-level vs method-level `@Tag` | the `RuntimeVisibleAnnotations:` block printed at **column 0** for the class; a member's block is indented | `classLevelAnnotations`, comment "That distinction is the whole of the first review round's finding" |
| A meta-annotated group (`@Quarantined` is itself `@Tag("quarantined")`) | the tag lives on the *annotation type's own* class-level block, one hop away | `resolveMetaTags` |
| Inner and `@Nested` classes | compile to `Outer$Inner`; filtered by name, and Jupiter reports `@Nested` results under the *enclosing* class's XML | `producesReport`, `NESTED_ANNOTATION` |
| Generic bounds (`<T extends Base>`) and javadoc prose | erased / never emitted - `super_class` is the only supertype that exists at this layer | comment block at the top of the file listing failure shapes that "stop existing rather than being handled" |

Two disciplines make this trustworthy rather than merely different:

- **Pin the contract with the compiler, not a hand-written transcript of it.** `bin/test-check-integration-shard-coverage.mjs`
  compiles real fixture `.java` sources with the JDK's own `javac`, under the *real* JUnit package
  names via stub annotation types, then reads them back with the real `javap`. What gets pinned is
  "javac and javap agree on X", not "this test's guess at javap's format agrees with itself." That
  discipline caught two more bugs before they reached CI: `@ParameterizedTest` lives in
  `org.junit.jupiter.params`, not `.api` - the draft (and the source-based predecessor) silently
  un-guarded `MultiTopicTest`, whose only tests are parameterised (`STUBS` comment naming this); and an
  outer class whose only tests are `@Nested` carries no test descriptor of its own and needs the
  `producesReport` inner-class walk to be demanded at all (fixture `OuterIT`).
- **A tool that did not run must never look like a tool that found nothing.** `inspect` in
  `compiled-classes.mjs` distinguishes "javap ran and rejected one name" (keep the partial output,
  the other classes still resolved) from "javap did not run at all" (`e.status == null` - missing
  binary, bad `JAVA_HOME`) and rethrows the second case so the gate exits 2. The comment names the
  failure directly: an empty index would otherwise print "every class produced a report" over zero
  classes, which is "the green-while-red this whole module exists to prevent." The self-test pins this
  by pointing `JAVA_HOME`/`PATH` at a directory with no JDK and asserting exit 2, "never a pass"
  (`check('javap unavailable is could-not-run (exit 2), never a pass', ...)`).

## Why This Matters

A grep-based structural parser's failure mode is silent in both directions, and silence is exactly what
a guard must never produce. Under-matching (defect 1: file-wide `@Tag` grep) produces a false GREEN -
the guard reports coverage that isn't there, which is worse than no guard because it is trusted. But
over-matching (defect 4: the javadoc `extends`) produces a false RED that runs the opposite direction
from the guard's purpose - a permanently red gate demanding a report from a class that structurally
cannot produce one teaches the team to stop reading it, which reopens the door to the true failure the
guard exists to catch. Both directions came from the same root: text pattern-matching cannot distinguish
"this token appears" from "this token means what I think it means," and every fix was necessarily
another special case bolted onto the same fragile parser - six rounds, six new special cases, no reason
to expect a seventh review pass would not find a seventh. Reading bytecode does not make the guard
smarter; it removes an entire category of question by asking a party (the compiler) that already
answered it correctly and exhaustively. The `compiled-classes.mjs` header states the trade explicitly:
this can only run after compilation, so it is useless as a pre-push tree gate - the guard is intentionally
scoped to the point in the pipeline where compiled classes exist, rather than trying to make source
regex do a job it cannot do reliably at any point in the pipeline. Testability follows from the same
move: bytecode facts can be pinned against the real compiler in a self-test (`test-check-integration-shard-coverage.mjs`),
where a regex's correctness can only ever be argued about, never demonstrated against ground truth.

## When to Apply

Apply this whenever a guard, gate, or script needs to reason about: class identity across files, class
modifiers (abstract, interface), inheritance chains, annotation presence at class vs. method scope, or
"which tests actually exist / will actually run." If the answer would otherwise require re-deriving
what a compiler already decided, read the compiler's output (`javap`, or the equivalent for the target
language) instead of the source.

**When NOT to apply it: when the job has no compiled artefacts to read.** `bin/check-integration-shard-balance.mjs`
(the shard-drift checker, on the same tree) still enumerates classes with a source-level regex
(`classesInTree`, matching `/^([a-z]+ )*class (\w+)/gm` over `git ls-files`-selected `.java` files) and
says so on the merits, not from oversight: it runs opt-in via `SHARD_BALANCE_NETWORK` in the Repo
Hygiene job, which does not run a Maven build, so there is no `target/test-classes` build output for `javap` to read - that directory only exists after a Maven build and is never tracked.
Its own comment names the same fragility this learning is about ("several package-private top-level
classes may share one file... the partition operates on compiled classes") and accepts it because the
checker is advisory-only (exits 0 on drift unless `--fail-over` is given) - the cost of a wrong class
name here is an imprecise wall-clock estimate, not a silently-skipped test. That is the dividing line:
reach for bytecode when a wrong answer is a correctness failure (a test that never runs and nothing
turns red); source scanning stays acceptable when the job structurally cannot see bytecode and the
consequence of imprecision is advisory.

## Examples

**Before (source regex, the failure the fix removes):** the predecessor's `extends` scan was a bare
`grep -oE 'extends[[:space:]]+[A-Za-z0-9_]+'` over `.java` text. It matched the word "extends" inside
`ChaosSeed.java`'s javadoc sentence "that class extends {@code BrokerIntegrationTest}" and would have
demanded a failsafe report from a helper class that never runs - saved in production only by an
incidental line wrap, which is not a property a guard should depend on.

**After (bytecode field, same question):** `parseJavap` in `bin/lib/compiled-classes.mjs` reads the
supertype from the `super_class:` field of the `javap -v -p` block, matched by
`block.match(/^\s*super_class:.*\/\/\s*(\S+)\s*$/m)`. A javadoc sentence, a generic bound
(`<T extends Base>`), and a comment never reach bytecode at all, so the failure mode does not get fixed
- it stops being expressible. The self-test pins exactly this case with fixture `Seed.java` (javadoc
saying "that class extends ProbeBase") and `Holder.java` (`class Holder<T extends ProbeBase>`), both
asserted to have `superName === 'java.lang.Object'` and `inheritsTest(...) === false`.

**A javap block fact, concretely:** for the fixture `Probe4IT extends ProbeBase` (four top-level classes
declared in one file, mirroring the real `Rebalance857CommitSyncDeadlockProbeIT.java` shape), `javap`'s
`this_class:` line names `fx.integrationTests.Probe4IT` and its `super_class:` line names
`fx.integrationTests.ProbeBase` regardless of which file either class's source lives in - the class-per-file
assumption that broke the filename-derived predecessor never enters the picture, because `classFilesUnder`
walks compiled `.class` files, not `.java` files, and each of the four compiles to its own class file.

**A fixture that pins the class-vs-method distinction (defect 1):** `LoadTest.java` in the self-test
declares `@Test void quick() {}` alongside `@Tag("performance") @Test void slow() {}` - one plain test,
one tagged one, no class-level `@Tag` at all. `classLevelAnnotations` only reads the
`RuntimeVisibleAnnotations:` block printed at column 0, so `get('LoadTest').classTags` is asserted to be
`[]`, and the self-test's end-to-end run asserts `LoadTest IS demanded despite its method-level @Tag`.
That is the exact case that silently un-guarded `LoadTest` under the file-wide grep.

**Ground truth, not inspection:** the PR's validation was not "the code looks right" but a comparison
against what CI actually did - the bytecode-derived required set matched exactly the classes the green
CI run executed (zero false demands, since one false demand fails every build), and the first real CI
run under the new gate printed "45 must report, 38 reports found, ok" (`check-integration-shard-coverage.mjs`'s
summary line format, `${r.candidates} compiled test classes, ${r.required.length} in integration lineage
must report, ${r.reported} reports found`).

## Related

- `docs/solutions/best-practices/a-guard-that-lexes-shell-commands-must-lex-like-the-shell.md` - the same shape one layer over: a guard that builds its own simplified model of a language instead of using the language's authoritative parser, and accretes one bypass per review round
- `docs/solutions/architecture-patterns/a-guard-must-assert-what-it-means-not-what-is-easy-to-check.md` - the general rule this is an instance of: write the predicate from the real thing, not from a checkable stand-in; there the proxy was thread identity, here it is source text
- `docs/solutions/workflow-issues/a-check-that-reports-success-without-having-run.md` - the exit-2-never-0 rule: a javap that did not run must not read as a suite with nothing to demand
- `docs/solutions/workflow-issues/a-harness-that-cannot-tell-never-ran-from-ran-and-agreed-2026-09-02.md` - why the self-test compiles real fixtures with javac rather than pinning a hand-written javap transcript
- astubbs/parallel-consumer#442 - the PR that carries the guard, its predecessor's six defects, and the three review rounds that found them
