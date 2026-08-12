# Branch: language proxy (astubbs#242)

Requirements for a sidecar that runs Parallel Consumer and hands records to a non-Java
application's worker processes over loopback. Plan lives at
`docs/plans/2026-08-12-001-feat-language-proxy-plan.md`. No module exists yet — this branch is the
contract, not the build.

## Owed: the data records, and not before the module exists

A new user-visible module needs three YAML records, and all three must land in the PR that lands
the module, not in this one:

- A row in `docs/data/module-maturity.yaml` (fields per `docs/data/schema.yaml`, anchor
  `module_required:`), or a staged row in `docs/data/staging/module-maturity-rows.yaml` until then.
- A matching entry in `docs/data/testing-evidence.yaml` (anchor `module_evidence_required:`) whose
  id is what the maturity row's `evidence_id` points at.
- A feature record under `docs/features/` per `docs/features/README.md`, anchor `## Page contract`.

Writing them now would be wrong, and the corpus already has the scar: two feature records were
removed rather than shipped because their modules were not in `pom.xml`, so the Maven coordinates
they published could not resolve. `docs/inflight/next-experimental-module-records.md` holds that
rule. `bin/check-docs-data.sh` validates the schema but does **not** cross-check the module list
against `pom.xml`, so a missing row is silent — the gate will not catch this for us.

## Everywhere a new module has to be registered

Collected because missing one is the failure mode here, and two of them are easy to miss:

- Root `pom.xml`, anchor `<modules>` — before `parallel-consumer-examples`, which stays last.
- `.github/workflows/maven.yml`, **two** duplicate-code detector lists with **different
  separators** — anchor `duplicate-code-cross-check` is space-separated, anchor
  `duplicate-code-detection-tool` is comma-separated.
- The two YAML data files above.
- `.github/workflows/publish.yml` and `release.yml`, anchor `-pl '!:parallel-consumer-examples`,
  only if the module should not publish to Central.
- `src/docs/README_TEMPLATE.adoc` and `AGENTS.md` anchor `## Module Structure`. `README.adoc` is
  generated — editing it directly is wrong.

## Traps this branch has already paid for finding

- **`release.target` is 8.** The build compiles Java 17 source to Java 8 bytecode via Jabel, so
  modern networking APIs are invisible. A wire-protocol module almost certainly needs the override
  `parallel-consumer-mutiny/pom.xml` already models. Its comment records why this must be
  deliberate: at the wrong target the module compiled happily and failed at runtime, because the
  build cannot detect the mistake.
- **The duplicate-code cap is 5% absolute and the baseline is ~4.2%.** Copying the dashboard's
  `HostAllowlist` and port-walk into a sibling module is the shape of change that exceeds it. The
  cheaper order is to land this module depending on nothing from `feats/web-gui`, then extract a
  shared serving module once both are on trunk, where the extraction deletes duplication instead of
  creating it.
- **`gh` resolves to confluentinc unless `gh repo set-default astubbs/parallel-consumer` has run in
  the clone.** The config is local and uncommitted, so every fresh worktree and sandbox starts
  without it.

## What collides

`feats/web-gui` (astubbs/parallel-consumer#268) touches the same root `pom.xml` module line, the
same two workflow lists, `AGENTS.md`, and `NOTICE`. It also carries the `controlLoopHooks`
`CopyOnWriteArrayList` fix and the whole chaos scenario framework, both of which this work would
want. Whichever lands second resolves; nothing here depends on that branch by construction.

## Blocked on

Three values nobody has set, all recorded in the plan's `Resolve Before Planning`: the Go client's
effort budget, the latency multiple the first success criterion is judged against, and — the one
that changes the product rather than the plan — whether the users who asked for a Python client
need key-ordered concurrency or the parallel consumption Share Groups now supply.
