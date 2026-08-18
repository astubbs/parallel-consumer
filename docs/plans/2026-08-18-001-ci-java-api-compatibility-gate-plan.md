# Java API-compatibility gate

<!-- artifact_contract: ce-unified-plan/v1
     artifact_readiness: requirements-only
     product_contract_source: ce-brainstorm -->

**Status:** **spike built and working, decisions deliberately still open.** The gate runs, the
self-test passes, and all four published modules check clean against the live baseline. What is
*not* settled is the policy: what should happen when it goes red, and whether it becomes a required
check. Those are recorded below as open decisions with recommended defaults, not as done.
**Written:** 2026-08-18
**Branch:** `ci/api-compatibility-gate`
**Prior art:** no Java-side prior art. `japicmp`, `revapi`, `clirr` and `semver` return nothing in
`docs/`, `bin/` or `.github/`; `docs/plans/`, `docs/solutions/` and `docs/inflight/` were searched
for API breakage and compatibility and returned nothing. **The significant find is a sibling, not a
predecessor:** `bin/check-proto-breaking.sh` (astubbs#242, unmerged) solves the identical problem for
the proxy protocol schema. Its design is adopted wholesale here - see §2.

## 1. The problem

A source- or binary-breaking change to the published Java API rides through CI unflagged. Nothing
compares the surface this branch produces against the surface already published.

Two facts make it worth fixing now rather than at 1.0:

- **`docs/refactoring.md` already states the policy and nothing enforces it.** Its release-gated
  queue says breaking changes "are **release-gated**: do not fold them into a minor/patch", and
  lists five waiting to be actioned in one pass. That is an invariant asserted in prose. The repo's
  own habit is to make such an invariant fail the build.
- **`0.6.0.0` is the last moment this is cheap.** Nothing has ever been published under
  `bz.stub.parallelconsumer` as a release, so there are no downstream callers to have broken yet.
  Once it is on Central, every future break has a cost that today's does not.

**What this is not for.** `docs/data/module-maturity.yaml` publishes the claim that "Stable-module
APIs remain pre-1.0 and can change before 1.0". The gate does not retract that. Its job is to make a
break **visible and deliberate**, not to forbid it.

## 2. The sibling this copies

`bin/check-proto-breaking.sh` had already answered the hard design questions. All four are adopted:

- **Self-arming grace branch.** Before a baseline exists the check says so and passes, arming itself
  the moment one is published. No second "arming" PR, and no gate blocking every PR until it can run.
- **A self-test that proves it can still say no.** A CI step mutates the guarded thing one way at a
  time and asserts each is caught.
- **Cannot-run is a third outcome (exit 2), not a pass.** "A gate that cannot locate what it guards
  has not run."
- **Job naming.** `api: breaking` sits alongside `proto: breaking`.

## 3. What was built

- `bin/check-api-breaking.sh` - resolves the published baseline per module, compares, exits
  0 / 1 / 2.
- `bin/test-check-api-breaking.sh` - five cases, all passing (§4).
- An opt-in `api-compat` Maven profile. Nothing runs in a normal or CI build without `-Papi-compat`.
- An `api: breaking` job in `maven.yml`, **not** a required status check.

Modules covered: core, vertx, reactor, mutiny. The examples modules are deploy-skipped, so they have
no published surface.

**Baseline:** the `0.6.0.0-SNAPSHOT` that `publish.yml` deploys to Maven Central on every green push
to master. Chosen because it exists *today*, and answers "does this branch change the published
surface?" - a question with an answer before any release is cut. Repointing it at a release later is
a one-property change; both are the same comparison with a different left-hand side.

## 4. The finding that shaped the design: three green checks that checked nothing

Getting this wrong is easy, silent, and looks exactly like success. Every one of these was hit while
building the spike, and **every one exited 0 printing `No changes.`**:

1. **`<oldVersion>` as a Maven coordinate.** The published baseline carries the *same version string*
   as the working tree (both `0.6.0.0-SNAPSHOT`), so Maven resolved both sides to the local artifact
   and japicmp compared the new jar against itself.
2. **A failed rebuild (missing `-am`).** The gate ran against the previous jar. The break was real,
   compiled, and invisible.
3. **A failed rebuild (test compile error).** Same outcome, different cause - and the more likely one
   in practice, since a breaking API change usually *does* break the tests that call it.

This is the repo's named failure class:
`docs/solutions/workflow-issues/a-check-that-reports-success-without-having-run.md`.

The script now refuses to run (exit 2) when the two sides are the same file, or when the built jar is
older than its own sources. Both cases are pinned by the self-test:

| Case | Expected | Result |
|---|---|---|
| Unchanged tree | exit 0 | PASS |
| Renamed public method | exit 1 | PASS |
| Removed public method | exit 1 | PASS |
| Artifact compared against itself | exit 2 | PASS |
| Stale jar / rebuild never happened | exit 2 | PASS |

Worked example of the gate firing, `PollContext.value()` renamed:

```
***! MODIFIED CLASS: PUBLIC bz.stub.parallelconsumer.PollContext
	---! REMOVED METHOD: PUBLIC(-) java.lang.Object value()
	+++  NEW METHOD: PUBLIC(+) java.lang.Object valueRenamed()
Semantic versioning suggestion: 1.0.0
```

That last line is the reason to prefer japicmp specifically: it names the version bump the change
demands, which is the input `CHANGELOG.adoc`'s `=== Breaking` section currently takes from memory.

## 5. Open decisions

Each carries a recommended default. None is implemented as policy yet.

- **D1 - what happens on red.** Options: hard block / advisory only / **block unless the break is
  recorded**. *Recommended: the third* - it is the only one consistent with a project that reserves
  the right to break pre-1.0 and has a written-but-unenforced release-gating policy. Currently the
  job fails on a break but is not a required check, so nothing is blocked.
- **D2 - become a required status check.** Cannot be done in the PR that creates the job: a required
  context no run has produced blocks every PR whose base predates it (`docs/ci.md`;
  `tooling: package rename` is un-armed for this reason). *Recommended: arm in a follow-up.*
- **D3 - what counts as public API.** Currently everything `public` outside `**.internal.**`, which
  is the only boundary the project has - a package name plus one sentence in `README.adoc`. This
  declares `.state`, `.offsets` and `.metrics` public. *Recommended: accept for now, revisit if the
  report proves noisy* - an `@InternalApi` annotation is the alternative.
- **D4 - binary vs source.** Both are reported; neither breaks the build (D1). Note the proto gate
  chose the *stricter* comparison deliberately, because the promise covers generated APIs and not
  only the bytes - the same argument applies here.
- **D5 - repoint the baseline at `0.6.0.0`** once released, or keep tracking master's snapshot, or
  run both. *Recommended: run both* - they answer different questions.

## 6. Assumptions recorded, not settled

Carried forward from the brainstorm without being examined, and each overturnable:

- Lombok-generated members count as public API. They appear in bytecode, so japicmp sees them. **Not
  yet observed to cause noise** - the four modules currently check clean - but a Lombok version bump
  could produce breaks that no source change caused.
- `ignoreMissingClasses` is on. Third-party types on the API surface (Kafka, SLF4J) are not on
  japicmp's classpath; without it, every one is a hard error. The cost is that changes reaching the
  API *through* those supertypes are not reflected.
- The self-test rebuilds `parallel-consumer-core` five times, so the CI job costs a few minutes. Not
  measured against the runner budget in `docs/inflight/ci-disabled-jobs-and-runner-load.md`.
