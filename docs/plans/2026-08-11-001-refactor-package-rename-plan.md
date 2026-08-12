# Package rename: `io.confluent.parallelconsumer.*` → `bz.stub.parallelconsumer.*`

**Status:** **decided go, and executed - the rename landed on master in astubbs#294**, carried out by
the re-runnable `bin/rename-packages.sh` rather than by hand, for the merge reason set out in §4.6.
Everything below is the *pre-execution* record: the go/no-go, the evidence behind it, and the survey of
what would have to move. Read it as what was known on 2026-08-11, not as an open task list - the
inventory in §5 has since been worked, and `bin/rename-packages.sh --verify-only` is now the authority
on what is left rather than any checkbox here. The one measurement that required actually performing
the rename was run in a throwaway clone and thrown away with it; the real run confirmed it.
**Written:** 2026-08-11
**Expires:** expired on execution. Kept because the licence analysis (§2), the wire-format finding
(§4.1) and the traps in §5 are the reasoning behind the tool, not a plan anyone still has to follow.
**Ledger entry:** [`docs/inflight/branch-package-rename.md`](../inflight/branch-package-rename.md)
**Release gate:** [`docs/inflight/release-0600-blockers.md`](../inflight/release-0600-blockers.md)
**Prior art:** none. `docs/plans/`, `docs/solutions/`, `docs/inflight/`,
[`docs/refactoring.md`](../refactoring.md) and the issue tracker (all states) were searched for a
package rename before this was written and returned nothing. The adjacent work that exists is the
**Maven coordinate** rebrand, which already shipped into the unreleased `0.6.0.0` section of
`CHANGELOG.adoc` under `=== Breaking` (PR astubbs#55) - that changed the `groupId` only and
explicitly left the Java packages alone.

---

## 1. Why now, and why there is no second chance

Nothing has ever been published under `bz.stub.parallelconsumer`. No git tag, nothing on Maven
Central, so no downstream code imports our packages. Renaming today costs a user exactly nothing,
because there is no user of the fork's artifacts to cost.

The moment `0.6.0.0` is on Central that stops being true permanently. Everyone who adopts the fork
writes `import io.confluent.parallelconsumer.ParallelStreamProcessor;` and a later rename asks all of
them to migrate a **second** time, for a reason that will read as cosmetic from where they sit. The
first migration (the `groupId`) we can justify to them; the second one we cannot.

So this is not "should we rename" versus "should we not". It is "rename now, at zero cost" versus
"rename later at a real cost, or never". **There is no third moment**, which is why this is gated on
the release rather than left in the backlog.

## 2. What the licence actually permits, and the trap inside it

Upstream is Apache 2.0. The analysis, and the part people get wrong:

- **§4 permits the rename outright.** We may modify the Work and distribute the modified Work,
  including changing package names. Nothing in the grant is conditioned on preserving identifiers.
- **§6 grants no trademark rights.** `io.confluent` is Confluent's mark used as an *identifier in
  artifacts we publish*, on a fork Confluent does not maintain. Moving off it **reduces** our
  exposure; it does not create any. This is the affirmative reason to do it, not merely a tidy-up.
- **Confluent has not objected**, and links to this fork from theirs. That is not permission and we
  do not need permission, but it means the change is not adversarial and does not need to be framed
  as one.

**The trap: renaming the package does NOT let us drop anything.** The code stays a derivative work
whatever it is called, so §4(a)/(b)/(d) still bind:

| Obligation | Effect here |
|---|---|
| §4(a) give recipients a copy of the licence | `LICENSE` stays |
| §4(b) retain copyright, patent, trademark and attribution notices in the source | **Every Confluent copyright header stays, in every renamed file** |
| §4(d) retain the attribution notices in `NOTICE` | `NOTICE` stays, unchanged |
| §4(c) mark modified files | The `Modifications Copyright` line - and §5 of this plan is largely about it |

Anyone who reads "we moved off `io.confluent`" as "so we can drop the Confluent headers" has
inverted the licence. Renaming *increases* the header bookkeeping (§5, R1); it never decreases it.

**This is a different question from the Apache trademark work.** That one concerns the ASF's `Kafka`
mark in our product *branding*. This one concerns Confluent's mark in our *namespace*. They share a
shape and nothing else. Do not let a future session merge them into a single "rebrand" work item.

## 3. The work is the copyright provenance model, not the `sed`

The mechanical rename is a directory move and a find-and-replace. It is not where the effort is, and
treating it as the task is how this gets under-estimated.

`bin/check-copyright-headers.sh` decides a file's provenance **by exact path match** against the
fork-point file listing:

```sh
upstream_files=$(git ls-tree -r --name-only "$FORK_POINT")   # bin/check-copyright-headers.sh ~:86
...
elif grep -qxF "$f" <<< "$upstream_files"; then              # ~:137
```

Move `src/main/java/io/confluent/parallelconsumer/…` to `…/bz/stub/parallelconsumer/…` and **every
upstream-derived file misses that lookup**. Each one falls through to the fork-original branch, where
its retained Confluent header is a violation rather than a requirement. The scanner does not degrade
gracefully; it inverts.

**Measured, not predicted** (2026-08-11, by performing the rename in a throwaway clone and running
the scanner - the clone was discarded, so re-measure rather than trusting this figure if the tree has
moved):

| | |
|---|---|
| Baseline today | 233 java files checked, **0 violations** (`bash bin/check-copyright-headers.sh`) |
| After a naive rename | **197 violations** |
| After the provenance model is repaired | **121 files** still need a new `Modifications Copyright (C) <year> Antony Stubbs and contributors` line - 197 carry Confluent headers, 76 already have the line |
| Plus | **7** hard-coded paths in `RENAMED_FROM_UPSTREAM` (`bin/check-copyright-headers.sh:54-59`) and `EXTRACTED_FROM_UPSTREAM` (`:68`) |

The 121 modification lines are bookkeeping - correct, required by §4(c), and dull. **Redesigning the
provenance model is the engineering**, and it has to be designed rather than patched, because the
naive fix (add 200 entries to `RENAMED_FROM_UPSTREAM`) turns a rule into a manifest that nobody can
review and that every subsequent file move corrupts. Options to weigh in R1 below.

## 4. Settled, so nobody re-investigates

### 4.1 No wire-format exposure - the main risk, closed

Offset metadata is a magic byte plus a bitset or run-length payload plus base64. `OffsetEncoding`
dispatches on `byte magicByte`. **No class name reaches the wire**, so the rename cannot break offset
compatibility with data written by an earlier version.

Everything checked, and what it returned:

- The only Java serialisation in the tree is `OffsetSimpleSerialisation`'s
  `encodeAsJavaObjectStream` / `deserialiseJavaWriteObject`, which handle a `Set<Long>` and are
  called only from `WorkManagerOffsetMapCodecManagerTest`. No production path.
- No `Serializable`, no `serialVersionUID` in main source.
- No `Class.forName`, `ServiceLoader`, `loadClass` or `Proxy` in main source, so no reflective
  name-to-class lookup can go stale.
- Metrics prefix is the hand-written literal `"pc."`
  (`parallel-consumer-core/src/main/java/io/confluent/parallelconsumer/metrics/PCMetricsDef.java:75`),
  not derived from the package.
- MDC keys are hand-written literals `"pcId"` and `"offset"`
  (`…/internal/AbstractParallelEoSStreamProcessor.java:60,66`); thread names are `pc-*`. **No
  operator-visible string changes**, so dashboards and log filters are unaffected.

### 4.2 Downstream migration is small

At most ~25 public types. All five example apps **combined** import 8 distinct names; a typical
consumer imports 4-6. The user-facing instruction is a one-line `sed` (§8), not a migration guide.

### 4.3 Scale of the change

Re-measure rather than quoting these; the commands are the point:

| Set | Command | 2026-08-11 |
|---|---|---|
| Java files | `bash bin/check-copyright-headers.sh` | 233, all containing `io.confluent` |
| Java lines | `grep -rn 'io\.confluent' --include='*.java' .` | 969, of which 937 are `package`/`import` (fully automatic) |
| Java lines needing a human | the remainder | 32: ~23 javadoc `{@link}`/`@see`, 1 fully-qualified call, and **8 string literals** (6 `@AnalyzeClasses` + 1 `beAssignableTo` + 1 quarantine fixture) |
| Non-java | `grep -rnE 'io[\\.]*confluent'` | 31 files, 185 lines, plus ~110 more in `io/confluent` **path** form |

The 8 string literals are the dangerous ones and each is enumerated in §5.

### 4.5 Commit shape: two commits, and one commit is impossible - not merely worse

**Decided. The move goes in its own commit; the content edits follow in a second.** `git mv` only,
never `mv` plus `git add`.

Measured on a throwaway clone of master, git 2.51.2, no ambient `diff.*`/`merge.*` config:

| shape | renames detected | mis-paired | lowest similarity |
|---|---|---|---|
| **two commits** - move, then edits | **233 / 233** | 0 | `R100` |
| squashed into one | 232 / 233 | **4 invented** | `R061` |

Squashing does not merely lose a rename. It **invents** four, pairing the five per-module
`TestConventionsArchTest.java` files into a cross-module cycle (`streams→metrics`, `vertx→mutiny`,
`mutiny→reactor`, `reactor→vertx`), reporting one deleted and one as new. A bare count of `R` entries
sees nothing wrong, which is why the verification re-derives every rename's expected destination and
asserts the pairing, printing `mis-paired 0`.

**No setting can rescue it, and there is a measured reason.** Those files score `R071`-`R073`
against their own true former selves - the same band as against their siblings - so the correct pair
and the wrong pair are *indistinguishable by similarity*. `-M` is therefore a pure trade: `<=73%`
detects 232 with 3-4 invented, `>=74%` invents none but loses 11-13 real renames. `-B` /
`--break-rewrites` in every form was byte-identical to omitting it. `-C` /
`--find-copies-harder` made it worse (4 mis-pairings to 5, and 6 combined with `-B`).

**Even a working setting would be unusable.** Git's rename detection cannot be configured from the
repository: there is no `.gitattributes` mechanism for `-M`, `-B`, `merge.renameLimit` or
`diff.renames`. Any fix of the form "set X" holds on the clone that set it and silently does not on
CI, on a contributor's machine, or in the next agent's worktree. **This project requires the default
behaviour to be correct**, which rules the single-commit shape out independently of the measurement
above.

Keep `--single-commit` only as the self-test's experiment arm - it is the negative control proving
the mis-pairing detector fires. It is not an option to choose.

### 4.6 The merge hazard, and the only defence that exists

**Commit shape is irrelevant to merges.** `git merge` resolves renames over the base-to-tip *tree
delta*, which is identical however many commits span it; merging the two-commit and the
single-commit master into the same branch gave byte-identical results.

**The real hazard is a branch that has not been renamed at all**, and it is silent:

- Merging renamed master into an un-renamed branch exits **0 with zero conflicts and no warning**.
- A marker edit in the *streams* module's `TestConventionsArchTest.java` came out in the **mutiny**
  module's file. An edit to `BrokerPollSystem.java` in the same merge landed correctly, so the merge
  looks entirely healthy.

Nothing catches this. Measured and refuted as mitigations: `merge.directoryRenames` in all three
values (no effect), `merge.renameLimit`, `diff.renames=copies`, `-s recursive`, and - the only
repo-committable lever that could plausibly have applied - a `.gitattributes` entry marking the
files `-diff`, committed on both sides (**no effect**). `-X find-renames=75%` does surface it, but
drops 13 legitimate renames and is a per-invocation flag, not repo state.

**Therefore: running `bin/rename-packages.sh` on every open branch before it merges is mandatory,
and it is the only defence.** With the script run on both sides, the same case surfaces loudly as
`CONFLICT (rename/delete)` plus `CONFLICT (add/add)` on the correct path with the edit intact. This
is not best practice; it is the difference between a conflict and silent cross-module corruption.

## 5. The work, risk-ordered

Ordered by *how quietly it fails*, not by size. Everything above R5 fails silently or fails green.

### R1 - Copyright provenance (the actual engineering)

- [ ] **Redesign how `bin/check-copyright-headers.sh` resolves provenance across a directory move.**
      Path equality against the fork-point listing (`:86`, `:137`) is the thing that breaks. Weigh at
      least: (a) a **path-normalisation rule** applied before the lookup, mapping
      `**/bz/stub/parallelconsumer/**` back to `**/io/confluent/parallelconsumer/**`, which keeps the
      check a *rule*; (b) `git log --follow` / rename detection against the fork point, which is
      correct but slow and non-deterministic across shallow clones - note the workflow already needs
      `fetch-depth: 0`; (c) bulk `RENAMED_FROM_UPSTREAM` entries, which is a manifest of ~200 lines
      nobody can review and which the next file move silently invalidates. **(a) is the
      recommendation**; whichever is chosen, write the reasoning into the script's header.
- [ ] **Extend `bin/test-check-copyright-headers.sh` first, with a case that fails before the fix.**
      This is the negative control: a self-test that has never been seen to fail is decoration.
- [ ] **Add the `Modifications Copyright (C) 2026 Antony Stubbs and contributors` line to the 121
      files that lack it.** Mechanical, but it is a §4(c) obligation, not a lint.
- [ ] **Update the 7 hard-coded paths** in `RENAMED_FROM_UPSTREAM` (`:54-59`) and
      `EXTRACTED_FROM_UPSTREAM` (`:68`). These are literal paths under `io/confluent/…` and a
      normalisation rule may or may not cover them - check both directions of each pair.
- [ ] **Green means green:** `bash bin/check-copyright-headers.sh` back to 0 violations, and the
      count of checked files unchanged from 233. A drop in the *checked* count means files stopped
      being found, which passes.

### R2 - The escaped-regex blind spot (HIGHEST RISK, and invisible twice over)

Three files encode the package as an **escaped regex**, `io\.confluent\.parallelconsumer\.`. That
form is invisible to a find-and-replace on `io.confluent.parallelconsumer` **and** invisible to the
obvious verification sweep `grep -rn "io\.confluent"` - which returns 31 non-java files and omits
`bin/lib/quarantine-common.sh` entirely. A sweep that cannot see the thing it is verifying is worse
than no sweep, because it reports success.

- [ ] `bin/ci-mutation-test.sh:85` - `| { grep -E '^io\.confluent\.parallelconsumer\.' || true; })`
- [ ] `bin/ci-mutation-test.sh:104` - `DECIDABLE="${PIT_DECIDABLE_PACKAGES:-^io\.confluent\.parallelconsumer\.offsets\.}"`
- [ ] `bin/lib/quarantine-common.sh:13` - `QUARANTINE_ANNOTATION_ERE`, containing
      `(io\.confluent\.parallelconsumer\.)?Quarantined\(`

**What stale ones do.** The mutation ones **fail open, permanently**: `CHANGED_ALL` comes back empty,
the script prints `PIT: no core main-source classes changed - nothing to mutate, skipping` and
**exits 0**. The lane is green forever while scoring zero mutants, and nothing in the job summary
distinguishes that from a real pass. The quarantine one degrades only partially - the fully-qualified
annotation form stops being detected while the short form still is - and it **is** caught, by
`QuarantineRegistryScriptTest.fullyQualifiedAnnotationIsDetected`
(`parallel-consumer-core/src/test/java/io/confluent/parallelconsumer/QuarantineRegistryScriptTest.java:91`).
One of the three has a test. Two do not.

### R3 - Rules that go vacuous rather than red

ArchUnit pins fully-qualified class and package names as **strings**. A stale string does not fail;
it selects nothing, and selecting nothing is a pass.

- [ ] **`parallel-consumer-core/src/test/java/io/confluent/parallelconsumer/TestConventionRules.java:46`**
      - `.should().beAssignableTo("io.confluent.parallelconsumer.integrationTests.BrokerIntegrationTest")`.
      Stale, the condition never matches, so the rule passes while no longer guarding anything. The
      `that()` set stays large, so `failOnEmptyShould` never trips either. The guard it silently stops
      applying is the one that keeps Docker-dependent tests out of surefire.
- [ ] **The 6 `@AnalyzeClasses(packages = "io.confluent.parallelconsumer…")` declarations.** Stale,
      **zero classes are imported** and every rule in the module evaluates against an empty set:
      - `parallel-consumer-core/src/test/java/io/confluent/parallelconsumer/TestConventionsArchTest.java:15`
      - `parallel-consumer-vertx/src/test/java/io/confluent/parallelconsumer/vertx/TestConventionsArchTest.java:16`
      - `parallel-consumer-mutiny/src/test/java/io/confluent/parallelconsumer/mutiny/TestConventionsArchTest.java:16`
      - `parallel-consumer-reactor/src/test/java/io/confluent/parallelconsumer/reactor/TestConventionsArchTest.java:16`
      - `parallel-consumer-examples/parallel-consumer-example-metrics/src/test/java/io/confluent/parallelconsumer/examples/metrics/TestConventionsArchTest.java:16`
      - `parallel-consumer-examples/parallel-consumer-example-streams/src/test/java/io/confluent/parallelconsumer/examples/streams/TestConventionsArchTest.java:16`

      `test_classes_must_be_named_so_surefire_collects_them` sets `.allowEmptyShould(true)`
      (`TestConventionRules.java:139`) for a legitimate reason (the example modules really do match
      nothing), so it passes vacuously and the emptiness is indistinguishable from the intended case.
- [ ] **Prove they are still live after the rename** by breaking one deliberately and watching it go
      red. A rule that has only ever been observed passing is not evidence.

### R4 - The hand-written SPI file

- [ ] **`parallel-consumer-core/src/test/resources/META-INF/services/org.junit.platform.launcher.TestExecutionListener`**
      contains `io.confluent.csid.utils.MyRunListener`. It is a resource, not a `.java` file, so no
      compiler and no IDE refactor touches it. Stale, the JUnit launcher throws
      `ServiceConfigurationError` - which at least fails **loudly**, unlike everything above it.
      Note the `io.confluent.csid` prefix: a find-and-replace scoped to `io.confluent.parallelconsumer`
      misses the whole `io.confluent.csid.*` tree.

### R5 - Mechanical, but no tool does it for you

Each of these needs a human edit; most fail loudly, which is why they rank here.

- [ ] **Logback**: `<logger name="io.confluent…">` across 8 `logback*-test.xml` files (core, vertx,
      reactor, mutiny, and the vertx/streams/reactor/core examples). ~150 lines, of which ~10 are
      live rather than commented out. Stale loggers are silently inert - no error, just missing logs
      exactly when someone is debugging.
- [ ] **Truth-generator codegen**: `parallel-consumer-core/pom.xml:133-141` (the `<class>` list) and
      `:153` (`<entryPointClassPackage>`). **Fails loudly** - the build stops - and 14 test files
      import the generated `ManagedTruth`, so this cannot be missed.
- [ ] **`.github/workflows/mutation-full-sweep.yml:36,64,68`** - the dispatch input defaults and the
      usage comment.
- [ ] **`bin/ci-mutation-test.sh:66,70,151`** - unescaped, so an ordinary sweep does catch these
      three. Do not confuse them with R2's escaped ones in the same file.
- [ ] **`docs/todo-index.md`** - 48 path references. **Regenerate, do not edit**: `bin/todo-index.sh`.
      Gated by `bin/todo-index.sh --check` at `.github/workflows/pr-checklist.yml:52`, so a stale
      index fails the PR.
- [ ] **IntelliJ run configurations**: `.idea/runConfigurations/_Tag__transactions__.xml:4-5` and
      `.idea/runConfigurations/All_examples.xml:6,10`. **Note the typo**:
      `All_examples.xml:6` reads `io.confluent.parall**a**lconsumer.examples.core.*`, which a
      find-and-replace on the correct spelling will skip. It is already broken; fix it while there.
- [ ] **`parallel-consumer-core/src/test/resources/junit-platform.properties:6`** - commented out,
      referencing `io.confluent.csid.utils.ReplaceCamelCase`. Another `io.confluent.csid` case.
- [ ] **Prose in `docs/`** - `grep -rn 'io\.confluent\|io/confluent' docs/`. **Dated plan and
      investigation documents are historical records and must not be rewritten** to say something
      they did not say; only live reference prose gets updated. Decide per file, and expect the
      answer to be "leave it" for everything under `docs/plans/`.
- [ ] **`src/docs/README_TEMPLATE.adoc`**, then regenerate `README.adoc` with `./mvnw process-sources`.
      Never hand-edit `README.adoc`. See §8 for the wording, and §9 for what the README may say
      *before* the decision is taken.

### R6 - Checked and clean, so nobody checks again

No work needed, recorded so the next session does not re-derive it: **no `module-info.java`**, no
OSGi manifests, no `maven-shade` relocations, no SpotBugs/PMD/Checkstyle filter files keyed on
package, no JaCoCo includes, no `spring.factories`, no JSON or YAML golden files containing class
names. Surefire and failsafe select on the `integrationTest*` **path segment**
(`pom.xml:743-744,770` - the surefire `<exclude>`s and the failsafe `<include>`), not on
`io/confluent`, so the split survives the move untouched.

## 6. Two non-negotiable verification conditions

These are not "nice to check". Both defects in question **report success** when they are broken, so
neither a green build nor a clean grep is admissible evidence without them.

**(a) Verify the three escaped-regex files with a search that can actually see them.**

```sh
grep -rnE 'io[\\.]*confluent'
```

The habitual `grep -rn "io\.confluent"` **cannot** match `io\.confluent` as it appears on disk and
silently omits `bin/lib/quarantine-common.sh`. Any claim that the rename is complete which rests on
the habitual form is unsupported. Say which pattern was run.

**(b) Assert the mutation lane SCORES MUTANTS on the first post-rename PR. Do not accept the tick.**

`bin/ci-mutation-test.sh` exits **0** when it matches nothing, printing
`PIT: no core main-source classes changed - nothing to mutate, skipping`. That is indistinguishable
from a genuine "no relevant changes" pass, and it is what a stale regex produces on *every* PR
forever. So on the first PR after the rename: change a class under the decidable packages, and read
the job summary for a **mutation score and a survivor list**. A green check with the skip message is
the failure mode, not the pass.

While there, apply the same standard to R3: break one ArchUnit rule on purpose and confirm it goes
red. Three separate mechanisms here fail green; that is the defining property of this refactor and
the reason it is written down rather than just done.

## 7. The real completeness check: vet every remaining `confluent`, one at a time

After the rename, `grep -rni confluent` will still return a great deal, and it **should**. Go through
it one occurrence at a time and confirm each is legitimate attribution rather than something the
sweep missed. Given §6(a), this pass - not the grep - is the completeness check.

Legitimate, expected to remain:

- [ ] **`NOTICE`** - required by §4(d). Unchanged.
- [ ] **Copyright headers** on every upstream-derived file - required by §4(b). Unchanged, and now
      each carries the §4(c) modifications line.
- [ ] **Links to `confluentinc/parallel-consumer`** in `README`, `CHANGELOG`, `AGENTS.md`,
      `docs/upstream.md`, `docs/issue-references.md`, `docs/refactoring.md`, mirrors and
      `upstream-map.yaml`. Naming upstream is the point of those. Do not sweep by `AGENTS.md` alone:
      astubbs#272 split it into topic docs, so `grep -rl confluentinc/parallel-consumer docs/ AGENTS.md`
      is the list, not a fixed set of filenames.
- [ ] **The `master-confluent` mirror branch** and anything pinned to it.
- [ ] **`confluentinc/cp-kafka`** - the TestContainers image. Nothing to do with our namespace.
- [ ] **`.semaphore/`** - legacy Confluent CI, retained but inactive.
- [ ] **`io.confluent.csid.*`** - decide explicitly whether this moves too. It is a *different*
      Confluent-owned prefix (`csid`, not `parallelconsumer`) and every automated approach scoped to
      `io.confluent.parallelconsumer` misses it. Leaving it behind would defeat the §6 rationale in
      §2, so the default answer is that it moves; record the decision either way.

Anything that is none of the above is a miss. Fix it and note which sweep should have caught it.

## 8. User-facing wording, pre-drafted

Ready to paste **when the rename lands**, not before (see §9).

**For `src/docs/README_TEMPLATE.adoc`, under `== Upgrading`, extending the 0.5-to-0.6 subsection:**

> The Java packages move from `io.confluent.parallelconsumer` to `bz.stub.parallelconsumer`.
> The API itself is unchanged, so the migration is an import rewrite:
>
> ```sh
> grep -rl io.confluent.parallelconsumer src/ | xargs sed -i '' 's/io\.confluent\.parallelconsumer/bz.stub.parallelconsumer/g'
> ```
>
> A typical application imports four to six types from the library, so this is usually a
> single-commit change. Offset metadata is unaffected: it is encoded as a magic byte and a bitset,
> carries no class names, and remains readable across the change. Metric names (`pc.*`), MDC keys
> (`pcId`, `offset`) and thread names (`pc-*`) are unchanged, so dashboards and log filters need no edit.

**For the `0.6.0.0` release notes, under `=== Breaking`.** The `== 0.6.0.0` section is **generated at
release time from the commit log** and is not appendable by a PR (`AGENTS.md` → *Changelog*), so this
is a note to whoever runs the generation pass, not text to add now:

> Java packages renamed from `io.confluent.parallelconsumer.*` to `bz.stub.parallelconsumer.*`.
> Rewrite your imports; the API is otherwise unchanged, and committed offsets stay compatible.

**This must not be missed at generation time.** Two passages in the existing `== 0.6.0.0` section
state the opposite, so if the rename lands they become false in a published artifact and have to go
in the same pass:

- the section's opening paragraph - *"Drop-in compatible with upstream 0.5.x — the only required
  change is the Maven groupId (see Breaking below)"*;
- the `=== Breaking` bullet - *"the library API is otherwise unchanged from upstream"*, which reads
  as covering identifiers as well as signatures.

A third, blunter one - *"Java package names (`io.confluent.parallelconsumer.*`) are unchanged, so
imports are unaffected"* - used to sit under `=== Improvements` and was removed by astubbs#276, the
branding rename, for an unrelated reason. Do not go looking for it. Add these to the release
checklist rather than trusting the generator to notice, since the generator reads commits and this
is a *contradiction* rather than an omission.

**If the answer is no-go**, the existing wording is already correct and nothing needs writing. That
asymmetry is worth noticing: "no" is free to execute *today* and expensive to execute later, which is
the whole argument of §1.

## 9. What the README may say before the decision is taken

**Nothing that presents the rename as decided or done.** The README is a published artifact and this
is an open go/no-go, so publishing unshipped behaviour as fact would be wrong in the one document
whose readers cannot check.

What went in alongside this plan, because it is true today and shipping regardless:

- A `=== From 0.5 to 0.6` subsection under `== Upgrading`, documenting the Maven coordinate change
  from `io.confluent.parallelconsumer` to `bz.stub.parallelconsumer`. This filled a genuine gap:
  `0.6.0.0` already changes the coordinates and `== Upgrading` had only `=== From 0.4 to 0.5`.
- A tightened drop-in claim near the top: the API **and** the Java packages are unchanged from
  upstream `0.5.x`, and the Maven coordinate is the single edit a user makes, cross-referenced to
  `== Upgrading`.

If this plan is executed, both passages change together, using §8's wording; the drop-in claim in
particular stops being true as written and must not simply be qualified.

## 10. Recommendation

**Go**, on the §1 argument alone: the cost is zero now and non-zero forever afterwards, and §4.1
closes the only risk that could have made it expensive. The work is real but bounded, and it is
concentrated in one place (§3) that is worth improving anyway - a provenance model that cannot
survive a directory move is fragile independently of this change.

The condition on "go" is §6. This refactor's defining property is that three separate mechanisms
**fail green**, so the rename is not finished when the build passes. It is finished when the mutation
lane has been observed scoring mutants, an ArchUnit rule has been observed going red on purpose, and
§7's occurrence-by-occurrence pass is done.
