# Static analysis for the ten non-Java proxy clients, and where RacerD fits

<!-- inflight-type: register -->
<!-- inflight-impact: blind-spot -->

A survey, so it is read rather than done. It answers three questions for the polyglot proxy/sidecar
work: what already analyses each client, what a race detector could add, and which of these
languages CodeQL would cover for nothing.

**Read the premise correction first.** This note was commissioned to find gaps. There are almost
none: every one of the ten languages already has a bug-finder wired into a CI row and a local
recipe, and a document already owns the per-language policy. What is left is narrow, and this note
is mostly about that narrow part.

**Scope**: Python, Ruby, TypeScript, Swift, Go, C++, Rust, C#, Scala, Kotlin. **Java is not this
note's.** The Java core, and RacerD against it, are owned by
[`docs/inflight/ci-build-hardening-register.md`](ci-build-hardening-register.md) and
[`docs/inflight/static-spotbugs-rule-registry.md`](static-spotbugs-rule-registry.md), which carry the
attempt and its blocker. Cite those rather than restating them.

**Everything here that names a version was checked against the real registry on 2026-08-25**, not
recalled. Nothing was installed and no analyser was run.

## The premise correction: all ten are already wired

`docs/client-static-analysis.md` **owns the per-language policy** - which tool, why that one, what
it catches, what fails the build - and `.github/workflows/clients.yml` runs one row per language
with a `scanner` and a `scanner-cmd`. Both ship on `feats/polyglot-demos`,
`feats/proxy-requirements` and `feats/sidecar-entry-point`, not on master, which is why this note
does not link them as live paths.
<!-- file-refs: N/A - clients.yml, client-static-analysis.md and every module path in this note ship on the polyglot branches; this note is cut from master, where none of them exist yet -->

| Language | Already running | Recipe |
|---|---|---|
| Go | `go vet` + staticcheck, pinned by a `tool` directive in `go.mod` | `scripts/analyse.sh` |
| Python | ruff (with the `S`/bandit rules) + mypy `--strict` | `make lint` |
| Kotlin | detekt, default ruleset, no config file by decision | `detekt.sh` |
| TypeScript | `tsc --build` + `@typescript-eslint` `recommended-type-checked` | `npm run check` |
| Rust | `cargo clippy --all-targets -- -D warnings` | same |
| C# | Roslyn analyzers, `latest-recommended` + `TreatWarningsAsErrors` | `dotnet build` |
| Ruby | RuboCop weighted to `Lint/` and `Security/` | `bundle exec rubocop` |
| C++ | cppcheck, `--check-level=exhaustive --error-exitcode=1` | `scripts/analyse.sh` |
| Swift | Swift 6 language mode + `-warnings-as-errors`; swift-format for layout | package build |
| Scala | `scalac -Xlint -Wunused -Werror` | `scripts/analyse.sh` |
<!-- file-refs: N/A - every recipe named in this table lives in a client module that exists only on the polyglot branches -->

Two things that read as gaps and are not:

- **Swift is not unanalysed.** `docs/client-static-analysis.md` recorded "nothing mature, none
  added" and told the Swift wave the lever was the Swift 6 language mode plus
  `-warnings-as-errors`. That landed: `Package.swift` declares `swift-tools-version:6.0` with a
  comment calling it load-bearing for strict concurrency, and every target carries
  `.unsafeFlags(["-warnings-as-errors"])`. Swift's concurrency checking is a compile-time race
  analysis, which is more than any of the other nine get for free.
  <!-- file-refs: N/A - client-static-analysis.md and the Swift module ship on the polyglot branches, not on master -->

- **Scala's compiler lint is the mature option**, not a placeholder. Its rejection of third-party
  plugins needs one correction, below.

Every one of the ten module maturity fragments reads `maturity: alpha`, so no row is skipped: these
are started modules with real source, not skeletons.

## RacerD, and the three languages it could actually reach

**The support table, verified at fbinfer.com/docs/checker-racerd on 2026-08-25 rather than
recalled, quoted verbatim:**

```
C/C++/ObjC: Yes
C#/.Net: Yes
Erlang: No
Hack: No
Java: Yes
Python: No
Rust: No
Swift: No
```

Infer v1.3.0 published 2026-05-12, assets `infer-linux-x86_64-v1.3.0.tar.xz` and
`infer-osx-arm64-v1.3.0.tar.xz`. `facebook/infer` was pushed 2026-08-24, so the project itself is
unambiguously alive.

**Why it matters here more than any linter.** These clients are thin protocol clients for a
concurrency sidecar. Each one fans a stream of dispatches out to N executors and folds the outcomes
back onto one transport. That is the only interesting thing they do, and it is exactly the shape a
race detector reads and a style linter cannot see. A tool that polices application architecture is
worth close to nothing against four hundred lines of queue and session code.

**RacerD analyses a build.** It needs a compile command it can wrap, or a compilation database it
can read. For a thin client that is a real question and not a formality, so each arm below says
whether the build exists in this repo today.

### Java core - not this note's

Owned by [`docs/inflight/static-racerd-findings.md`](static-racerd-findings.md), which records that
the calibration was attempted and what it did and did not reach. Named here
only because it is the third arm of the one-tool-three-languages argument - an argument the C# and
C++ findings below substantially weaken.

### C++ client - buildable, and the blocker is CPU architecture, not language support

**The build exists.** The module has a `CMakeLists.txt` (`set(CMAKE_CXX_STANDARD 17)`,
`find_package(Threads REQUIRED)`), so either `infer run -- cmake --build` or a
`CMAKE_EXPORT_COMPILE_COMMANDS=ON` compilation database would give Infer what it needs.

**What it would be pointed at**: `src/dispatch_queue.cpp`, `src/session.cpp` and `src/client.cpp` -
the executor fan-out and the transport session. That is where a race in this client lives, and
cppcheck, which is largely pattern-based, does not model threads at all.

**The blocker, stated concretely because it decides the answer.** `bin/build-client.sh` puts C++ on
its *container* route: the host has no gRPC or protobuf dev packages, so the module builds inside a
`debian:trixie-slim` image and nowhere else. Infer publishes **linux-x86_64 and osx-arm64 only** -
there is no linux-arm64 binary. On an Apple Silicon dev box that container is arm64, so there is no
Infer to install in it; and the osx-arm64 build cannot help either, because the C++ client does not
build on the host at all. `clients.yml` runs `runs-on: ubuntu-latest`, which is x86_64, so **the CI
row could run Infer while a local run on the dev box could not.**
<!-- file-refs: N/A - build-client.sh and the C++ module ship on the polyglot branches, not on master -->


That inverts this repo's rule that a check must be runnable locally before it gates. It is a
decision to take deliberately, not a detail to discover after wiring it. The cheaper alternative if
that decision goes the other way is `clang-tidy`, which ships with LLVM (22.1.8, 2026-06-16) and is
already named in `docs/client-static-analysis.md` as the obvious second C++ tool; its
`concurrency-*` and `bugprone-*` checks are path-sensitive where cppcheck is not, and it runs on any
architecture the container already has a compiler for.
<!-- file-refs: N/A - client-static-analysis.md ships on the polyglot branches, not on master -->


### C# client - buildable, but the RacerD arm is dormant and I am rejecting it

**The build exists and is native**: the module's pom sets `pc.foreign.build.executable` to `dotnet`,
so `dotnet build` runs on the host with no container.

**The problem is that upstream Infer has no .NET frontend.** The `C#/.Net: Yes` in the table above
is delivered by **Infer#**, Microsoft's CIL frontend that translates built assemblies into Infer's
IR. Checked on 2026-08-25:

- `microsoft/infersharp` latest release **v1.5, 2023-05-31**
- last commit on `main` **2023-09-07**, repository `pushed_at` **2024-01-16**, 45 open issues
- the companion `microsoft/infersharpaction` last pushed **2023-07-19**

Three years with no release, against a module targeting `net8.0` built by SDK 8/10. **Do not wire
it.** This is the liveness trap the brief asked me to catch, and it is the more dangerous kind: the
parent project is thriving, so the arm looks alive from the Infer side.

**What covers that ground instead, and is alive**: `Microsoft.VisualStudio.Threading.Analyzers`,
**18.7.23, published 2026-06-22 on NuGet**. It is a Roslyn analyzer package, so it drops into the
`Directory.Build.props` that already sets `AnalysisLevel` and `TreatWarningsAsErrors` and needs no
new CI row, no new lane and no new local command. It catches the .NET concurrency defect classes
that actually occur - sync-over-async, `.Result`/`.Wait()` deadlock shapes, `async void`, thread
affinity violations. **It is a rules engine, not a race detector**: it will not find an unguarded
shared field the way RacerD would. Recommended as the pragmatic substitute, and labelled as a
substitute rather than an equivalent.

### The other seven: RacerD cannot help, and what holds that ground

`No` in the table is the whole answer for Python, Rust and Swift. Go, Ruby, TypeScript, Kotlin and
Scala are not listed at all. For none of these is the build the blocker.

| Language | What covers concurrency, if anything |
|---|---|
| Go | The **built-in race detector**. Free, mature, and the strongest tool in this whole survey - see the gap below. |
| Rust | The borrow checker plus `Send`/`Sync` at compile time. This is the one language where the problem is largely solved before any analyser runs. |
| Swift | Swift 6 strict concurrency, already on with `-warnings-as-errors`. Compile-time data-race checking. |
| TypeScript | Single-threaded event loop, so no shared-memory races. The real class is the floating promise, and typed `@typescript-eslint`'s `no-floating-promises` already covers it - which is why the type-checked config, not bare eslint, is the half that matters. |
| Kotlin | **Nothing static.** detekt's default ruleset is not a concurrency analyser. Lincheck (JetBrains, `lincheck-3.7`, 2026-07-29) is alive but is a *testing* framework you write against, not a tool you point at code. |
| Scala | **Nothing.** `-Xlint`/`-Wunused` does not model threads. |
| Python | **Nothing mature exists.** The GIL prevents corruption of interpreter internals; it does not prevent a check-then-act race in client code, and no analyser looks for one. |
| Ruby | **Nothing.** Same reasoning. |

### The one concrete concurrency gap the survey found

**The Go client's gating test command does not use the race detector.** The module's pom declares
`<pc.foreign.test.args>test ./...</pc.foreign.test.args>` - no `-race`. Yet
`docs/inflight/clients/go.md` says of a demo assertion "Covered by `go test -race`", which is a
developer-run claim the recipe that actually gates does not make.
<!-- file-refs: N/A - the Go module and its per-language note ship on the polyglot branches, not on master -->


Go's race detector is built into the toolchain, costs one flag, and this is the client with the
executor fan-out. **This is the cheapest real win in the survey** and it is worth more than adding
any new tool to any other language. It is not free at runtime (roughly 2-10x slower, higher memory),
so if the full suite is too slow under it, a `-race` run of the concurrency-bearing packages is
still strictly better than none.

## CodeQL: nine of the ten, and two are already paid for

**This is the most useful answer in the note.**

Default setup is confirmed live from the API rather than assumed:
`gh api repos/astubbs/parallel-consumer/code-scanning/default-setup` returns
`state: configured`, `languages: [actions, java-kotlin, python]`, `query_suite: default`,
`threat_model: remote`, `schedule: weekly`, updated 2026-07-24. Analyses are running per pull
request - the most recent are dated 2026-08-25 - and there are **0 open alerts**.

CodeQL's supported set (codeql.github.com, supported languages and frameworks): C/C++, C#, Go,
Java/Kotlin, JavaScript/TypeScript, Python, Ruby, Rust, Swift, GitHub Actions. **Scala is named
explicitly as unsupported**, so it is the one language here that CodeQL will never reach.

| Language | CodeQL status | Cost to add |
|---|---|---|
| Python | **already enabled** | none |
| Kotlin | **already enabled** (as `java-kotlin`) | none - but see the caveat |
| Go | supported | none; the docs say Go "does not currently require special configuration" |
| TypeScript | supported | none; same sentence |
| Ruby | supported | none; same sentence |
| C++ | supported | none; default setup uses **build mode `none`** for C/C++ |
| C# | supported | none; build mode `none` |
| Rust | supported | none; build mode `none` |
| Swift | supported | **the expensive one** - "Support for the analysis of Swift requires macOS", so it needs a macOS runner and `autobuild` |
| Scala | **not supported, ever** | n/a |

So: seven languages are addable by editing one list in repository settings, at the price of Actions
minutes per pull request. Only Swift costs anything structural. That is a far better return than
adding any per-language tool, and it is the thing to do first.

**Version ceilings to check before believing the coverage**, because a language outside the
supported version range parses badly rather than loudly: Ruby is supported "up to 3.3" while the
Ruby module's toolchain is 4.0.x - **check this before counting Ruby as covered**. TypeScript
2.6-7.0 (module pins 5.9.3, fine), Rust editions 2021 and 2024 (module is 2021, fine), Swift
5.4-6.3 (module is tools 6.0, fine).

**A caveat I could not discharge, stated rather than glossed.** Whether the *already enabled*
`java-kotlin` and `python` extractors actually see the client modules is **unverified**. Python
extraction is parse-based, so the Python client should be in scope on any branch carrying it.
Kotlin extraction needs a build, and the Kotlin client module only builds under
`-Dpc.foreignClients`, which a CodeQL autobuild has no reason to pass. With 0 alerts everywhere
there is no positive evidence either way, and 0 alerts is exactly what a language that was never
extracted also looks like. Verify before claiming Kotlin is covered.

## Cross-language tools, ranked by coverage

1. **Semgrep** - GA on all ten, Scala included (docs.semgrep.dev/supported-languages). Highest raw
   coverage of anything in this note. `semgrep` 1.174.0, published 2026-08-20 on PyPI; very much
   alive. **But coverage is not value here**: it is pattern matching, its rule library is weighted
   to security and taint, and cross-file dataflow is the paid tier. These clients accept no
   untrusted input beyond a sidecar they spawned themselves, so the taint rules have nothing to
   chew on. Ranked first on reach and last on expected yield.
2. **CodeQL** - nine of ten, two already running, seven of the rest free to enable. Genuine
   dataflow rather than pattern matching. The recommendation.
3. **Infer / RacerD** - three of ten on paper; **two in practice**, because the C# arm is a dormant
   third-party frontend. Narrowest reach, deepest analysis, and the only one that answers the
   question these clients actually raise.

## Liveness, checked 2026-08-25

Recommendations first, then the tools already wired, then the ones rejected. Source in brackets.

| Tool | Latest | Released | Verdict |
|---|---|---|---|
| Infer (RacerD) | v1.3.0 | 2026-05-12 | alive; repo pushed 2026-08-24 [GitHub releases] |
| Microsoft.VisualStudio.Threading.Analyzers | 18.7.23 | 2026-06-22 | alive [NuGet] |
| Semgrep | 1.174.0 | 2026-08-20 | alive [PyPI] |
| clang-tidy (LLVM) | 22.1.8 | 2026-06-16 | alive [GitHub releases] |
| staticcheck | v0.8.1 | 2026-08-21 | alive [proxy.golang.org] |
| ruff | 0.16.4 | 2026-08-20 | alive [PyPI] |
| mypy | 2.3.1 | 2026-08-15 | alive [PyPI] |
| RuboCop | 1.90.0 | 2026-08-24 | alive; module pins 1.89.0 [RubyGems] |
| eslint | 10.9.1 | 2026-08-24 | alive; module pins 10.8.1 [npm] |
| typescript-eslint | 8.68.0 | 2026-08-24 | alive; module pins 8.67.0 [npm] |
| TypeScript | 7.0.2 | 2026-07-08 | alive; module pins 5.9.3, a major behind [npm] |
| cppcheck | 2.21.0 | 2026-06-04 | alive [GitHub releases] |
| swift-format | 603.0.0 | 2026-06-30 | alive [GitHub releases] |
| SwiftLint | 0.65.1 | 2026-08-21 | alive, but a style linter - see below |
| detekt | 1.23.8 stable | 2025-02-21 | **stable line is 18 months old**, but 2.0 alphas are shipping (v2.0.0-alpha.6, 2026-08-04). Alive. Module pins 1.23.7. |
| scapegoat | v3.3.6 | 2026-06-13 | **alive** - see the correction below |
| Infer# / InferSharp | v1.5 | 2023-05-31 | **DORMANT - rejected** |
| WartRemover | v2.4.5 | 2020-02-25 | **DEAD** |
| loom (Rust) | 0.7.2 | 2024-04-23 | quiet for 2 years; not recommended anyway |
| golangci-lint | v2.13.1 | 2026-08-20 | alive; rejected on overlap, not liveness |
| pylint | 4.0.7 | 2026-08-09 | alive; rejected on overlap |
| Lincheck | lincheck-3.7 | 2026-07-29 | alive; a testing framework, not an analyser |

**Not checked, and named so nobody assumes it was**: SonarQube Cloud's free-tier eligibility for
public repositories, and the Miri release cadence (`rust-lang/miri` was pushed 2026-08-24, but it
ships with the nightly toolchain rather than as a release).

## A correction to a recorded rejection

`docs/client-static-analysis.md` rejects **scapegoat** partly on maintenance: that it and
WartRemover are "neither as widely adopted nor as reliably maintained across Scala versions". Half
of that is now wrong. WartRemover's last release was **v2.4.5 on 2020-02-25** - six years, dead, and
the rejection stands with a date on it. But `scapegoat-scala/scapegoat` released **v3.3.6 on
2026-06-13** and v3.3.5 ten days earlier, and was pushed 2026-08-22. It is actively maintained.
<!-- file-refs: N/A - client-static-analysis.md ships on the polyglot branches, not on master -->


The rest of that document's argument survives untouched and is the reason to keep: a compiler plugin
is published per Scala patch version, so gating on one makes a Scala upgrade wait on somebody else's
release. **Rejected on release coupling, not on maintenance.** Recorded here so the question is not
re-litigated on a premise that no longer holds.

## Not worth it, with reasons

Named so the same suggestions do not come back around.

- **Infer# / InferSharp for the C# client.** No release since 2023-05-31, no commit to `main` since
  2023-09-07. Reasoning in full above.
- **SwiftLint.** Alive (0.65.1, 2026-08-21) and widely adopted, but a *style* linter. Its
  bug-adjacent rules do not add up to a defect finder, and the Swift 6 language mode already does
  the analysis that matters. Rejected on shape, not on liveness.
- **WartRemover.** Dead since 2020.
- **golangci-lint.** A meta-runner over linters, and the Go row already runs the two that earn their
  place directly. Adopting it would add a config file and a version to pin in exchange for findings
  the row already gets.
- **pylint.** Overlaps ruff almost entirely, and is far slower. Nothing it finds that ruff's
  `F`/`B`/`SIM` selections do not.
- **loom (Rust).** Not a liveness rejection. It is an exhaustive-interleaving test harness you write
  code *against*, so adopting it means rewriting the Rust client's concurrency to use its
  primitives. That is a rewrite dressed as a tool, and the borrow checker has already done most of
  the job.
- **Lincheck (Kotlin).** Same shape as loom: a testing framework, not an analyser you point at
  existing code.
- **SonarQube Cloud.** A hosted dashboard, which is the opposite shape from this project's policy
  that every check fails rather than warns and runs from the module's own recipe. Scala is listed
  under the paid Team plan, so the one language CodeQL cannot reach is also the one Sonar would
  charge for. Its free-tier terms for public repositories were **not checked**.
- **A second analyser for a language that already has one**, absent a named defect class the current
  tool provably misses. The Go row's own header records the standard this repo holds tools to:
  staticcheck earned its place because a dead store left `go vet` at exit 0 and made staticcheck
  exit 1. That is the bar - a demonstrated miss - and nothing in this survey clears it for any
  language except C++ concurrency and the C# concurrency substitute.

## If someone picks this up, in order

1. **Add `-race` to the Go module's test args.** Free, and it is the only built-in race detector on
   the whole polyglot surface.
2. **Add Go, TypeScript, Ruby, C++, C#, and Rust to CodeQL default setup**, and verify Ruby's
   version ceiling before counting it. Six languages, one settings change.
3. **Verify whether the Kotlin client is actually extracted** by the `java-kotlin` analysis already
   running. Zero alerts is not evidence that it is.
4. **Add `Microsoft.VisualStudio.Threading.Analyzers`** to the .NET module's existing props. One
   package, no new lane.
5. **Decide the C++ question**: Infer on x86_64 CI only, breaking the run-it-locally rule, or
   `clang-tidy` in the existing container on any architecture. Do not start wiring before this is
   decided.
