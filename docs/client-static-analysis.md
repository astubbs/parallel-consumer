# Static analysis for the proxy clients

Bug and bad-pattern detection for the language-proxy fan-out (astubbs#242): eleven client modules,
one protocol schema, and one policy applied to each. **This is not about formatting.** A formatter
argues about where a brace goes; the tools below argue about whether the code is wrong.

`.github/workflows/clients.yml` owns the per-language CI rows and
`bin/check-proto-lint.sh` owns the schema's gate; this document owns **which tool, why that one,
what it catches, and what fails the build**. Where a row and this document disagree, the row is what
actually runs — fix whichever is wrong, and say which.

## The shape, decided once

Every language answers the same five questions, and the table below is that answer eleven times.

1. **Which tool, or none.** Maturity is the filter: widely adopted, actively maintained, and it
   finds real defects. A formatter is not a bug-finder, and neither is a style linter.
   **"Nothing mature — none added" is a result**, and it is recorded with its reason rather than
   left as an apparent gap for the next person to re-derive.
2. **What class of defect it catches**, in terms specific enough to tell whether it overlaps with
   something else already running.
3. **The exact local command.** This is a hard requirement, not a nicety: a check that only exists
   in CI is a check you meet after review rather than before it, and this repo's rule is to verify
   locally and never defer to CI. Every command in the table has been run on a developer box unless
   the row says otherwise, in which case it says so explicitly.
4. **How it is wired in CI**, so the local command and the gating command are the same thing.
5. **Severity policy** — see below, because it is the same everywhere.

### Severity policy: everything fails, nothing warns

There is no advisory tier anywhere in this table. A finding that nobody has to act on teaches
everybody to scroll past findings, and the next real one scrolls past with it.

Dismissing an individual finding is allowed and expected — but it is done **at the finding**, with
the reason written down: a `//lint:ignore`, a `# type: ignore`, a narrowly scoped
`spotbugs-exclude.xml` entry naming one class and one pattern, an `except:` in `buf.yaml`. What is
refused is the wide exclusion: a whole rule turned off, or a whole pattern muted across a module,
because that silences the finding for code nobody has written yet.

This is deliberately stricter than the repo-wide SpotBugs lane in `maven.yml`
(`static: spotbugs`), which is core-scoped, baseline-excluded and non-blocking. That lane suits a
codebase carrying about 30 long-standing findings
([`docs/inflight/static-spotbugs-latent-findings.md`](inflight/static-spotbugs-latent-findings.md)).
These modules carry none, and **starting a new module at zero is the one moment a clean-slate gate
is free**.

### Who owns each module's configuration

The rule set, the ignore file and the tool version for a client module are authored by **that
language's own wave**, inside that module. This document and `clients.yml` name the tool and the
invocation; `.rubocop.yml`, `eslint.config.mjs`, `tsconfig.json`, `Cargo.toml`,
`Directory.Build.props`, `pyproject.toml`, `go.mod`'s `tool` block and `buf.yaml` belong to the
module. Kotlin deliberately has **no** config file at all, which is itself such a decision — see
its section.

**So when a CI row fails on a diff that looks unrelated to it, check for a row/module mismatch
first** — a module that renamed its lint script, moved its config file, or changed the tool's
version. That is the likeliest cause by some distance, and it is invisible from either side alone.

## The table

| Target | Tool | Local command | Verified here |
|---|---|---|---|
| **Protocol (`.proto`)** | `buf lint` | `bin/check-proto-lint.sh` | yes |
| **Java** (every module under its aggregator) | SpotBugs (gating) + ArchUnit | see [Java](#java--spotbugs-and-archunit) — the modules by name, not the aggregator | yes |
| **Go** | `go vet` + `staticcheck` | `parallel-consumer-proxy-client-go/scripts/analyse.sh` | yes |
| **Python** | `ruff` (incl. bandit rules) + `mypy` | `make lint`, in the module | yes |
| **Kotlin** | detekt | `parallel-consumer-proxy-client-kotlin/detekt.sh` | ran clean |
| **TypeScript** | `tsc` + typed `@typescript-eslint` | `npm run check`, in the module | ran clean |
| **Rust** | `cargo clippy -D warnings` | `cargo clippy --all-targets -- -D warnings` | ran clean |
| **C#** | Roslyn analyzers, warnings as errors | `dotnet build --configuration Release --no-incremental -warnaserror` | ran clean |
| **Ruby** | RuboCop (`Lint/`, `Security/`) | `bundle exec rubocop`, in the module | ran clean |
| **C++** | cppcheck | `cppcheck --enable=warning,style,performance --error-exitcode=1 --inline-suppr src` | no — no local toolchain |
| **Swift** | **nothing mature — none added** | (formatter only; see below) | n/a |
| **Scala** | **compiler flags only — no row yet** | (see below) | n/a |

The last column distinguishes two different things, because conflating them is how a check comes to
be believed rather than known:

- **yes** — the command was run here against real code **and proven able to fail**: a defect it
  should catch was introduced, the tool went red, the defect was reverted.
- **ran clean** — the command was executed here and passed, but the red-then-green proof was done by
  that language's own wave inside its module, and is recorded there rather than repeated here. For
  Kotlin the detekt jar and flags were separately shown to detect (six findings on a throwaway file
  outside the module, exit 2), so its quiet `exit 0` is a real pass and not a tool that never ran.
- **no** — nothing was executed; the row is a recorded decision only, and says why.

---

## Protocol — `buf lint`

**The schema is a static-analysis target in its own right, and the most important one in this
table**, because it is the single artifact all eleven clients are generated *from*. A defect there
is not one language's bug; it arrives in every language at once, as generated code nobody reads.

**Catches:** enum zero values that are not the `_UNSPECIFIED` default (which makes "unset"
indistinguishable from the first real value on the wire), package/directory disagreement (which
makes generated import paths differ per language), RPC request and response types shared between
methods, field-presence and naming conventions.

**Local:** `bin/check-proto-lint.sh` — needs `buf`, which is a mise-managed toolchain
(`mise use -g buf@latest`; 1.72.0 here). The module has no BSR dependencies, so the lint is entirely
local and downloads nothing.

**CI:** a step in `maven.yml`'s existing `proto: breaking` job, sharing its checkout and its `buf`
install. The lint runs *before* the breaking-change check. The job's name is deliberately unchanged:
it is a required check, and renaming a required check silently un-gates every open PR.

**Rules:** `STANDARD`, less two exceptions that were already in
`parallel-consumer-proxy-protocol/buf.yaml` with their justification beside them —
`RPC_REQUEST_STANDARD_NAME` and `RPC_RESPONSE_STANDARD_NAME`, because the single bidirectional
stream reuses `ClientMessage`/`ProxyMessage` rather than wrapping them in types ten languages would
each have to name for no information. **Those two were left exactly as the freeze wave wrote them**;
this change adds a gate around the existing rule set, it does not re-litigate it.

**Why now:** the schema already linted clean. Nothing *held* it clean, which is the whole gap —
a clean state that nothing checks is a clean state with a date on it.

**Proven able to fail:** renaming `COMMIT_MODE_UNSPECIFIED` to `COMMIT_MODE_DEFAULT` produced
`Enum zero value name "COMMIT_MODE_DEFAULT" should be suffixed with "_UNSPECIFIED"` and exit 1;
reverted, exit 0.

## Java — SpotBugs and ArchUnit

Two tools, because they answer different questions, and the repo already runs both elsewhere.

### SpotBugs

**Catches:** bytecode-level defects the compiler accepts — null dereference on a path, resources
not closed, `equals`/`hashCode` mismatches, non-atomic read-modify-write on shared state, useless
comparisons, format-string mismatches.

**Wiring:** bound in `parallel-consumer-proxy-client-java/pom.xml`, inherited by every module beneath
it - including ones added later, which is why it is declared there rather than in each child -
at the **`process-classes`** phase with the **`check`** goal. Both choices are load-bearing:

- **`process-classes`, not `verify`** (the plugin's own default). The gating PR lane runs
  `bin/ci-unit-test.sh`, which stops at `test`. A `verify`-bound check would run only in the
  push-to-master full build — after review, on the wrong side of the merge — while reading as
  covered the entire time.
- **`check`, not `spotbugs`.** The `spotbugs` goal writes a report and exits 0. `check` reads that
  report and fails. It runs the analysis itself, so there is no second execution to keep in step.

Effort `Max` and threshold `Medium` come from the root pom's `pluginManagement` — nothing is
restated.

**Local:**

```bash
./mvnw -Pci test -am -Dexcluded.groups=performance,chaos,quarantined \
  -pl :parallel-consumer-proxy-client-java-api,:parallel-consumer-proxy-client-java-direct,\
:parallel-consumer-proxy-client-java-grpc
```

Three traps are baked into that line, all of them silent:

- **Name the leaf artifacts, not the aggregator** — one `-pl` entry per module in that aggregator's
  `<modules>` block, which grows, so read it rather than copying this line forever.
  `-pl :parallel-consumer-proxy-client-java`
  selects the packaging-`pom` parent *only* — Maven does not walk into its modules — so it builds
  successfully, runs SpotBugs against a project with no class files, and reports `BUILD SUCCESS`
  having analysed nothing.
- **Keep `-Dexcluded.groups=...quarantined`.** Without it the reactor runs core's quarantined tests
  on the way through, and you diagnose an unrelated known flake
  ([`docs/inflight/test-untracked-ci-flakes.md`](inflight/test-untracked-ci-flakes.md)) instead of
  your own change. `bin/ci-unit-test.sh` hardcodes the same list.
- **`-am` is required**, not optional: the enforcer's `ReactorModuleConvergence` rule fails when a
  selected module's parents are absent from the reactor.

`JAVA_HOME` must be JDK 17 — core's delombok step fails on a newer JDK, and the mise-managed JDKs
are deliberately off `PATH`
(`JAVA_HOME=~/.local/share/mise/installs/java/temurin-17`).

**Findings on the current code, and how each was resolved** — the api module reported 7 on its first
run, and the reasoning is in `parallel-consumer-proxy-client-java-api/spotbugs-exclude.xml`
class by class:

- **6 × `EI_EXPOSE_REP`/`EI_EXPOSE_REP2` on `InboundRecord` and `OutboundRecord`** (`byte[]` key and
  value stored and returned without copying). **Not a defect — the documented contract.**
  `InboundRecord`'s javadoc says so three times, and copying every key and value would double the
  hot-path allocation of a library whose purpose is throughput. Excluded per class and per pattern,
  so a *future* mutable field on those classes is still reported.
- **1 × `EI_EXPOSE_REP` on `Outcome.produce()`.** **A false positive, checked in the source rather
  than judged by its label:** the field is built as
  `Collections.unmodifiableList(new ArrayList<>(produce))` in `Outcome.success()`, so there is no
  internal representation left to expose. SpotBugs does not track the wrapper through the
  constructor. Excluded for that one method only.

Dismissals live in an exclude file rather than `@SuppressFBWarnings` at the site — which would
otherwise be the better shape — because that annotation needs `spotbugs-annotations` on the
classpath, and the api module is dependency-free by design (its pom says so, and the direct
sibling's `bannedDependencies` enforcer rule exists to keep it that way). Every entry names one
class, and mostly one method, so the gate survives.

**A real concurrency bug it found in the gRPC client, unprompted.**
`GrpcParallelConsumerClient.channel` was reported `IS2_INCONSISTENT_SYNC` — *"locked 66% of time"*.
`poll()` assigns it inside `synchronized (this)`; `close()` reads it with no synchronization at all,
from a different thread. The `synchronized` block gives at-most-once **mutual exclusion**, which is a
different guarantee from **visibility**: with no happens-before edge, `close()` could read `null`
after `poll()` had built the channel, never shut it down, and leak the connection — and with it the
sidecar's Kafka group membership. Fixed by making the field `volatile`, which every other
cross-thread mutable field on that class already was; this one was missed, not decided.

Looking for **other instances of the same defect** (the repo's merge-prep rule) found one the
analyser did not flag: `requests`, assigned on the same unsynchronized line and read by the executor
threads under `transmitLock`. Made `volatile` for the same reason. Nothing else on the three
modules has this shape.

**Proven able to fail:** it already did, unprompted and twice — the 7 api findings and the gRPC
concurrency finding both reddened the build before any exclusion or fix existed. `direct` passes
with no filter at all.

### ArchUnit

The repo's existing convention is a tiny per-module `TestConventionsArchTest` pointing ArchUnit at
that module's packages, with the rule logic living once in `TestConventionRules` (core's test-jar).
**That pattern is extended here rather than duplicated.**

- **The transport modules** get the standard `TestConventionsArchTest`, and so should any module
  added under the aggregator later. The rule that earns its place
  is the surefire-naming one: both suites arrive by *subclassing* the api test-jar's conformance
  suite, and a subclass named outside surefire's default includes is never collected — the transport
  would report green having run nothing.
- **`api`** gets `ClientSurfaceArchTest` instead, which is a different job: two rules over
  *production* code asserting that the shared surface names **no transport type**
  (`io.grpc..`, `com.google.protobuf..`, `bz.stub.parallelconsumer.proxy..`) and **no engine or
  Kafka type** (`org.apache.kafka..`, the engine internals). This is the rule the direct module's
  pom asks for by name — *"The Java reference work adds an ArchUnit rule covering the API SURFACE;
  this ban covers the CLASSPATH"* — and it complements the `ban-transport-dependencies` enforcer
  rule rather than repeating it: the enforcer reads the dependency **tree** and fires when a jar
  arrives; ArchUnit reads the **bytecode** and fires when a type is referenced, which catches the
  leak that arrives through a dependency already legitimately present.

  Why it matters more here than in an ordinary module: a `ByteString` or a `ConsumerRecord` on this
  surface is not a Java problem, it is a specification problem — nine mirroring languages have no
  such type, so the shape stops being expressible and the fan-out diverges silently.

**One recorded gap.** The api module does **not** run the shared `TestConventionRules`, unlike every
other module in the repo, because that rule library ships in core's test-jar and the api pom forbids
a dependency on core in any scope. The gap is recorded rather than closed by weakening the pom. The
cheapest future fix is to extract `TestConventionRules` into a small standalone test-support artifact
that neither core nor the clients own — worth doing when a second module hits the same wall, not
before.

**Local:** the same Maven command as SpotBugs; ArchUnit runs as an ordinary surefire test.

**Proven able to fail**, both halves:

- Adding `kafka-clients` to the api pom and a `ConsumerRecord`-returning method to `InboundRecord` —
  the exact leak — failed `the_shared_surface_names_no_engine_or_kafka_type`, naming the method and
  the constructor call. Reverted; green.
- Adding a `@Test`-bearing class named `ProbeBadlyNamedSuite` to the direct module failed the shared
  `test_classes_must_be_named_so_surefire_collects_them` rule, which is the one that would otherwise
  let a whole transport's conformance suite go dark. Reverted; green.

## Go — `go vet` and `staticcheck`

**`go vet`** ships with the toolchain: no install, no pin to rot. Catches misuse the compiler
accepts — printf/format-arg mismatches, copied locks, lost struct tags, unreachable code.

**`staticcheck`** (honnef.co/go/tools) is the mature third-party analyser, and it is here because it
catches classes `go vet` does not look for at all: dead stores (`SA4006`), unused unexported code
(`U1000`), nil-dereference and error-handling mistakes (`SA5xxx`), impossible conditions, misused
stdlib APIs. That is a measured claim, not a brochure one — see the proof below.

**Pinned as a `tool` directive in `go.mod`**, exactly as the module already pins its protobuf
generators, so `go tool staticcheck` builds *that* version. Nothing is installed globally, and the
version a developer runs cannot differ from the version that gates. Adding it moved the module's
`go` directive to 1.25.0 — staticcheck v0.7.0's floor, and the toolchain `clients.yml` already pins
for this row. (`staticcheck` is also in the mise registry if you want it on `PATH` for editor use;
the build does not use that copy.)

**Local:** `parallel-consumer-proxy-clients/parallel-consumer-proxy-client-go/scripts/analyse.sh` —
runnable from anywhere, it finds its own module directory.

**CI:** the Go row's `scanner-cmd` is that same script. One recipe.

**Proven able to fail:** a dead store injected into `Client.Session()` left `go vet` at **exit 0**
and made staticcheck **exit 1** with `SA4006: this value of s is never used`. Reverted; both green.
A `%d`-with-a-string probe reddened `go vet` separately. The vet-clean/staticcheck-red pair is the
evidence that the second tool earns its place.

## Python — `ruff` and `mypy`

**`ruff`** was already here; what changed is that it now selects **`S`** (flake8-bandit) alongside
`E, F, W, B, UP, SIM, RUF`. `F` is pyflakes (undefined names, unused imports, shadowing), `B` is
bugbear (mutable default arguments, loop-variable capture, `except` without `raise from`), and `S`
is **the same rule set the `bandit` tool runs, in-process** — which is why bandit is not a separate
dependency. Three narrow per-file exemptions are recorded with their reasons in `pyproject.toml`:
`S101` where pytest's `assert` is the suite's only verb and where `sidecar.py` narrows an `Optional`
one line before using it, `S603`/`S607` where spawning the sidecar or driving `protoc` is the file's
declared job.

**`mypy`** is added, in two passes with different strictness:

- `mypy --strict src` — the shipped surface, fully annotated, no implicit `Any`.
- `mypy --check-untyped-defs tests tools` — bodies are checked without demanding a signature on
  every test function. The value there is catching an attribute that cannot exist, not decorating
  forty test declarations.

Both analysers are pinned **with upper bounds** in `pyproject.toml`'s `dev` extra
(`ruff>=0.9,<0.17`, `mypy>=1.15,<3`). An unbounded linter reddens the lane on a release day for
reasons unrelated to the diff, which is precisely how a real finding learns to look like noise.
`types-grpcio` is a dev dependency too: without typeshed's grpc stubs, mypy sees the whole channel
API as `Any` and the strict pass has nothing to check.

**A real defect this found, before any probe was injected.** `ClientOptions.topics` was annotated
`tuple[str, ...]` while `__post_init__` promises — in a comment, and in behaviour — to *"accept any
iterable of topics but hold a tuple"*. Every caller passing the obvious `["my-topic"]` was a type
error against the contract the class documents. Three sites in the suite did exactly that. Fixed by
annotating the field `Sequence[str]`, which a tuple satisfies, so the post-init normalisation is
unaffected. The finding only became visible once the package shipped its **PEP 561 `py.typed`
marker**, which was also missing — without it a type checker ignores the package's annotations
entirely, including in this repo's own tests, so the strict pass would have been checking work no
downstream user could benefit from.

**Local:** `make lint` in the module (`make ruff` and `make typecheck` run each half). Both come
out of the module's own venv.

**CI:** the Python row's `scanner-cmd` is `make lint`. It previously ran
`pip install ruff==0.9.2 && ruff check .` — a second pin, held in a file the module's owner does not
edit, which had already drifted several minor versions behind `pyproject.toml`. Removing it makes
the module the single authority.

**Proven able to fail:** a probe carrying a mutable default argument, an unused import, an unused
local, an `eval` and a `str` returned from an `int` function produced `B006`, `F401`, `F841`,
`S307` from ruff and `[return-value]` from mypy. Reverted; `make lint` green.

## Kotlin — detekt

*Configured by the Kotlin wave, inside its module. Run here, clean.*

**Catches:** the potential-bug and performance rule sets — swallowed and over-generic caught
exceptions, unused private members, unsafe `!!`, iterator misuse, `equals`/`hashCode` mismatches.

**No config file, deliberately** — and this is the wave's decision, recorded here so nobody
"fixes" it. detekt does **not** auto-discover a config file, so a `detekt.yml` present locally and
absent from the CI invocation (or the reverse) produces the one skew that matters: green on a laptop,
red in CI. The module therefore keeps none, satisfies the **default** ruleset, and suppresses an
individual rule with an `@Suppress` in the code, where the reason is reviewable — rather than in a
config file that disables it everywhere and silently.

**CI:** the sha256-verified detekt CLI jar, fetched from Maven Central rather than the GitHub
release page (Central artifacts are immutable; a release asset can be replaced in place under the
same URL).

**Local:** `parallel-consumer-proxy-client-kotlin/detekt.sh`, which fetches the same jar, checks the
same hash and passes the same flags. **The version and hash are two copies on purpose** — a workflow
cannot source a shell variable from a module script — so a version bump must move both in one
commit. `kotlinc` 2.4.10 is available locally via mise; the detekt CLI is a jar, not a mise tool.

**Proven able to fail:** by that wave, inside the module (an unused private function turned it red).
Independently confirmed here that the jar and flags detect at all — six findings on a throwaway file
outside the module, exit 2 — because a silent `exit 0` from a tool that analysed nothing looks
identical to a pass.

## TypeScript — `tsc` and typed `@typescript-eslint`

*Configured by the TypeScript wave, inside its module. Run here, clean.*

**The type check is the half that matters.** ESLint without type information cannot see a floating
Promise, an unhandled rejection, an unnecessary condition, or a narrowing that can never be true —
which is most of what a TypeScript bug looks like. So this row is `tsc --build` under strict
compiler options **plus** `@typescript-eslint`'s `recommended-type-checked` configuration, not the
untyped `recommended` set.

**Local and CI:** `npm run check`, which the module defines as `tsc --build && eslint .`, from its
pinned devDependencies — never an ambient `npx` fetch. Running the module's script rather than the
two tools keeps one recipe; if the script is renamed, that is the row/module mismatch to check
first. Node 24 is available locally via mise.

## Rust — `cargo clippy -D warnings`

*Configured by the Rust wave, inside its module. Run here, clean (clippy 0.1.97).*

**Catches:** clippy's `correctness` and `suspicious` groups are genuine defect detection —
`unwrap`/`expect` on values that can be `None`, misused iterators, incorrect `PartialEq`
implementations, comparisons that are always true, likely-wrong bit operations — plus `perf` and
`complexity`. `-D warnings` turns every lint into an error, which is the whole severity policy in
one flag.

**Local and CI:** `cargo clippy --all-targets -- -D warnings`. `--all-targets` matters: without it
tests and benches are not linted. clippy 0.1.97 is installed locally with the mise-managed Rust
toolchain.

## C# — Roslyn analyzers with warnings as errors

*Configured by the .NET wave, in its `Directory.Build.props`. Run here, clean.*

**Catches:** the .NET SDK's built-in analyzers run *inside* the compiler, so nothing external is
installed — CA rules for disposal, `async void`, culture-sensitive string comparison, and the
nullable-reference-type flow analysis that `<Nullable>enable</Nullable>` already switches on in the
seeded csproj.

**Wiring:** `AnalysisLevel=latest-recommended` and `TreatWarningsAsErrors` in the csproj, which makes
**the build itself the gate**. The CI row is therefore `dotnet build --configuration Release
--no-incremental -warnaserror`.

This row previously ran `dotnet format analyzers --verify-no-changes`, which is **not** an
equivalent: that command verifies only the analyzer diagnostics that happen to ship a code *fix*. A
genuine null-dereference warning ships none, so it passed clean through the check that was meant to
catch it. `--no-incremental` is there because analyzer diagnostics are not replayed for a project
the build decides is up to date.

**One discrepancy to hand back to that wave.** `Directory.Build.props` says *"Formatting is checked
separately by `dotnet format`, which the CI row already runs"* — written when the row ran the
`analyzers` subcommand, which checks analyzers and **not** whitespace or style, so that check was
never actually present. It is not added here either: this row is the bug-finding row, and adding a
formatting gate to another wave's module is that wave's call. If formatting is wanted, it is a
separate step running plain `dotnet format --verify-no-changes`.

**Local:** the same `dotnet build` command, run here against the module: clean, 0 warnings. Note the
local SDK is 10.0.400 while the row pins 8.0.404, and `AnalysisLevel=latest` resolves per-SDK — so
the local build applies a **superset** of the row's rules. That is the safe direction of the skew
(local can only fail early), and the props file says so.

## Ruby — RuboCop, weighted to `Lint/` and `Security/`

*Configured by the Ruby wave, inside its module. Run here, clean - 14 files, no offences.*

**Catches:** RuboCop is usually met as a style arbiter, and that half is deliberately turned down in
the module's `.rubocop.yml`. The half that earns its place is `Lint/` — shadowed variables, unreachable
code, `rescue` clauses that can never match, duplicate method definitions, void comparisons — and
`Security/` — `eval`, `Marshal.load`, `open` with interpolation.

**Local and CI:** `bundle exec rubocop`, with the version pinned in the module's `Gemfile` (1.89.0).
`bundle exec` rather than `gem install`: a version pinned in two places is a version that drifts in
one of them, and the row used to carry its own. RuboCop discovers `.rubocop.yml` itself, so unlike
detekt no config flag is needed. Ruby 4.0.6 and bundler are available locally via mise.

## C++ — cppcheck

*Not executed here: no C++ toolchain on this box, and the module is a skeleton.*

cppcheck is mature, widely deployed and finds real defects — buffer overruns, uninitialised
variables, null-pointer dereference, resource leaks, and misused STL. It is kept as seeded, with
`--error-exitcode=1` so findings fail rather than print.

**The obvious addition when the C++ wave starts is `clang-tidy`**, which is equally mature and
overlaps only partly: its `bugprone-*` and `clang-analyzer-*` checks are path-sensitive where
cppcheck is largely pattern-based. It is not wired now because a second analyser configured against
a module with no source is a configuration nobody has tested — that is the wave's call, not this
one's.

## Swift — nothing mature, none added

**This is the recorded result, not an omission.** Swift has no mature standalone analyser in the
SpotBugs/staticcheck class:

- **swift-format** — which the Swift row already runs — is a **formatter**. Its `lint` mode enforces
  layout and naming. It finds no defects, and the row is labelled so nobody reads it as though it
  does.
- **SwiftLint** is mature and widely adopted, but it is a *style* linter. Its handful of
  bug-adjacent rules do not add up to a defect finder, and wiring it would add a tool that mostly
  cries wolf.

**Swift's real static analysis is the compiler**: Swift 6 language mode's strict concurrency
checking catches data races at compile time, which is exactly the defect class that matters for a
concurrent consumer client. The lever is `-warnings-as-errors` plus the Swift 6 language mode in
`Package.swift` — **which this workflow row cannot write and the Swift wave owns**. That is the
recommendation to that wave, and it is the whole answer for Swift; do not go looking for a tool.

(There is no Swift toolchain on this machine either — Swift has no Debian 13 toolchain, recorded in
[`docs/inflight/parked-containerised-toolchains-and-runtime.md`](inflight/parked-containerised-toolchains-and-runtime.md)
— so nothing Swift-side can be verified locally today regardless.)

## Scala — compiler flags, and no CI row yet

The Scala module was seeded after the first fan-out wave and, unlike its siblings, **has no row in
`clients.yml`**; its own pom says the Scala wave adds one. So this is a recommendation held for that
wave rather than something wired now.

**The mature option is the compiler**: `-Xlint` with `-Wunused` and `-Werror` catches unused values,
inferred `Any`, non-exhaustive matches, discarded non-`Unit` values and adapted argument lists — the
Scala defect classes that actually bite. The third-party alternatives (`scapegoat`, `wartremover`)
are neither as widely adopted nor as reliably maintained across Scala versions, so **the compiler
flags are the recommendation and no third-party tool is proposed.**

Since the module builds with `scala-maven-plugin` in the ordinary Maven reactor, those flags go in
its pom and gate the normal build — no CI row is strictly required for the analysis, only for the
module's build.

## Adding a language, or a tool

1. Ask whether a **mature** bug-finder exists. Widely adopted, actively maintained, finds real
   defects. If the honest answer is "only a formatter", write that down here with the reason and add
   nothing. That is a complete answer.
2. Wire it into **the module's own recipe** — a script, a make target, an npm script — and point the
   CI row at that recipe rather than spelling the invocation out twice.
3. Pin the version where the module already pins things, with an upper bound.
4. **Prove it can fail.** Introduce a defect it should catch, watch it go red, revert. A tool that
   has never failed is decoration, and this repo has a whole write-up of checks that reported
   success without having run:
   [`docs/solutions/workflow-issues/a-check-that-reports-success-without-having-run.md`](solutions/workflow-issues/a-check-that-reports-success-without-having-run.md).
5. Add the row above, and say in it whether you ran the command or only recorded it.
