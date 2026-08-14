# Branch: language proxy (astubbs#242)

Requirements for a sidecar that runs Parallel Consumer and hands records to a non-Java
application's worker processes over loopback. Plan lives at
`docs/plans/2026-08-12-001-feat-language-proxy-plan.md`. No module exists yet — this branch is the
contract, not the build.

## U1 gate outcomes: gRPC cleared both, and one hint is not optional

Both gates KTD1 owed are cleared, measured on a throwaway probe (gRPC 1.73.0, protobuf-java 3.25.5,
GraalVM CE 25.0.2), not desk research. The probe is discarded; these outcomes are its only output.
U2 may author a schema against gRPC.

**R29 — declarable authority: CLEARED.** A `ServerInterceptor` reads the connection's declared
`:authority` from `ServerCall.getAuthority()` and rejects an unlisted one with `PERMISSION_DENIED`.
The rejection lands *before* application handling: closing the call in `interceptCall` and returning
a no-op listener means the service method never runs. Proven by counters rather than by inspection —
across a rejected connection both the service-invocation count and the application-message count
were unchanged. A connection declaring no authority is accepted, which is the default R29 asks for.
Behaves identically on the JVM and in the native image, so U6's allowlist has a real seam to enforce
on.

**R25 — native image: CLEARED.** The bidirectional-streaming hand-out loop builds with
`--no-fallback` and runs as a 45MB ELF binary under Substrate VM, completing the full
credit/record/outcome cycle. Build takes 33–52s.

### The protobuf hints, and the one that fails silently

Where the metadata actually comes from, since almost none of it is protobuf's:

- **`protobuf-java` 3.25.5 ships no native-image metadata at all.** Verified in the jar.
- **`grpc-netty-shaded` ships its own** — 19 `native-image.properties`, mostly Netty
  `--initialize-at-run-time`. Automatic, nothing to do. It warns that these sit under a
  non-recommended layout; harmless.
- **The GraalVM reachability metadata repository** (`native-maven-plugin`,
  `<metadataRepository><enabled>true</enabled>`) contributes exactly **one** entry:
  `java.time.Instant` with `allDeclaredMethods`, conditional on
  `io.grpc.internal.InstantTimeProvider`. It carries no entry for gRPC 1.73.0 and silently resolves
  to the `1.69.0` directory.

**Direct generated-API use needs no protobuf hints.** `setRecordId`/`getRecord` and the generated
parser are plain Java. That is the entire hand-out path, and it is why the first native build passed
without any hand-written config.

**Descriptor-driven reflection does need hints, and its absence is invisible until runtime.**
`getDescriptorForType`, `getField`, `TextFormat`, `JsonFormat` and `DynamicMessage` all go through
`GeneratedMessageV3.FieldAccessorTable`, which reflects on the generated accessors. Unregistered,
the build stays green, the binary runs, and the call fails only when that path is first exercised:

```
IllegalStateException: Generated message class "probe.gen.Record" missing method "getRecordId"
```

The fix, verified by rebuilding and re-running rather than assumed: a `reflect-config.json` under
`META-INF/native-image/` registering **each generated message class and its `$Builder`** with
`allDeclaredMethods` and `allDeclaredFields`. That moved the image from 3,209 to 3,214 reflective
types and from 2,279 to 2,821 methods, and the descriptor path then worked natively.

This is the same failure shape as the `release.target` trap below — compiles happily, fails at
runtime, build cannot detect it. So U2 should decide deliberately whether the schema's consumers may
touch the descriptor path at all, rather than discovering it in U7 when the sidecar is packaged.

## Interaction model: what is settled, and one dead end

The credit-based design in the plan is **superseded**. The sidecar registers as PC's user function and
returns a future completed when a worker reports — it is an `ExternalEngine`, the same seam Vert.x and
Reactor already use. The plan on disk has not been rewritten yet, so it still reads as if the credit
ledger were live; treat this note as current where the two disagree.

- **The wrapper is the layer, and Java is the degenerate case of it.** "PC in every language" means
  every language gets the same client wrapper; to the user it looks native. Java's wrapper is the same
  layer with one fewer hop, because it sits directly on the engine with no protobuf underneath. This is
  one client model with a missing layer in the Java case, not two architectures.
- **Workers never produce to Kafka directly.** A worker's output travels back through the engine over
  the protocol, and the sidecar produces. This keeps exactly-once entirely on the JVM side: one producer,
  one transactional id, behind one epoch check. Settled deliberately, for simplicity.
- **Fencing is Kafka's own EoS model, borrowed.** Each delivery carries an epoch; a dead client is
  fenced and the epoch increments when the record is handed to another worker. PC already implements
  the mechanism internally — `WorkContainer.deliveryCount` captured at dispatch, with
  `isReturnForSupersededDelivery()` discarding a return that names a superseded delivery. What the
  protocol adds is making that epoch explicit on the wire, echoed by the worker.
  Note the boundary honestly: this fences reports and Kafka-side effects. It cannot fence a worker's
  *external* side effects — a database write or an HTTP call — which is true of any at-least-once
  system and should not be implied otherwise.

**Dead end — do not re-propose: compiling PC to a native shared library.** The idea was to emit a
C-ABI library via GraalVM `native-image --shared` so non-JVM languages could run the engine in-process
with no protocol at all. It was an agent's extrapolation during ideation, not a proposal, and it is
rejected. Two things undercut it even on its own terms, recorded so the next session does not
rediscover them: the native-image gate this branch cleared produced an **executable**, not a `--shared`
export, which is materially different (entry-point surface, isolate and thread-attach semantics, GC
coexistence with a foreign runtime, callbacks re-entering from foreign threads — none tested); and the
Temporal precedent usually cited for it is narrower than it looks, since Temporal's Go and Java SDKs
are independent implementations rather than bindings over its shared core.

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
- **`native-image` needs a C toolchain, and its absence reads like a compile error.** It never links
  anything itself — it shells out to `gcc` — so a missing compiler surfaces at the link step and
  looks like a fault in the code being built. `build-essential` and `zlib1g-dev` are now in the
  Ansible workstation role. `gcc-14-base` alone is only runtime support files, so `command -v gcc`
  is precisely the check that misleads here. The image links `dl`, `pthread`, `rt` and `z`.

## What collides

`feats/web-gui` (astubbs/parallel-consumer#268) touches the same root `pom.xml` module line, the
same two workflow lists, `AGENTS.md`, and `NOTICE`. It also carries the `controlLoopHooks`
`CopyOnWriteArrayList` fix and the whole chaos scenario framework, both of which this work would
want. Whichever lands second resolves; nothing here depends on that branch by construction.

## Still open — none of it blocks a start

ASM3 — whether the users who asked for a Python client need key-ordered concurrency or the parallel
consumption Share Groups now supply — is **settled**: conversations with requesting users confirm
the narrower claim is what they need. The plan's Problem Frame records it. It is not an open risk
and should not be reopened as one.

Two values remain unset, and the plan carries them as explicit assumptions in its Planning Contract
rather than as blockers: ASM1, the Go client's effort budget, and ASM2, the latency multiple the
first success criterion is judged against. Each names what would falsify it.
