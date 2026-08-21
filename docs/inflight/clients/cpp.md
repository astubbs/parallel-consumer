# Client: C++ (astubbs#242)

Per-language working note for the C++ client of the language-proxy plan
(`docs/plans/2026-08-14-001-feat-language-proxy-plan.md`). Effort figures, divergence notes, and
anything the C++ wave learns that a later session needs go HERE - never appended to
`docs/inflight/branch-language-proxy.md` - one file per language, so concurrent waves never edit a
shared note.

**Status: wave one done.** Connect, `Configure`, a `Dispatch` wave, the user's function, the report
with the token echoed verbatim, and a clean client-initiated shutdown all work end to end over real
gRPC, and all four conformance scenarios the harness serves pass - each proven able to fail by
sabotage. The module's maturity and testing-evidence fragments no longer defer, so the clients
workflow's `cpp` row now gates for real.

## What exists

- `src/` - the library. `client.{h,cpp}` is the session; `dispatch_queue.{h,cpp}` is the queue AND
  the unresolved count in one object; `sidecar.{h,cpp}` is the child process and its lifecycle pipe;
  `options`, `session`, `record`, `outcome`, `error`, `logging` are the surface. `selftest.cpp` and
  `conformance_runner.cpp` are the two `main`s.
- `tests/` - thirteen tests under a sixty-line hand-rolled harness, run by `ctest` **inside the
  image**. Debian's googletest ships as sources every image would have to compile, and a client this
  size does not need a framework to say "this value was wrong".
- `scripts/analyse.sh` - the module's own cppcheck recipe, run by the image build **and** by the
  clients workflow's `cpp` row, which now names the script instead of spelling the invocation out.
- The build environment is unchanged in shape and was not redesigned; see below for what did change.

## Divergences this wave owes back to the authoring guide

- **The guide's logging table (§10.2) has no C++ row, and this wave picked TypeScript's answer**: an
  injectable `std::function` on the options, absent by default, so the library emits nothing until an
  application asks. C++ genuinely has no ecosystem facade - spdlog, glog, Boost.Log and `absl::log`
  all exist and none is *the* one - which is the same position §10.2 describes for TypeScript and
  settles the same way. The row to add reads: log through an injectable `Logger` on the options;
  silent because the field is empty by default; the application plugs in by setting it; no
  dependency added.
- **The token in the overflow error is now rendered, and this is the first client that does it.**
  The guide's §3.2 says to print the token's fields as they arrived and records that "no client does
  this yet: all five render the counts and omit the token". `DispatchQueue::overflow_message` prints
  `token.ShortDebugString()`, the generated message's own renderer, and
  `tests/dispatch_queue_test.cpp` asserts both fields appear. That sentence in the guide is now
  false and should be updated by the next resolver.
- **`poll` returns immediately and `session_end()` is a `std::shared_future<void>`** - the C++
  spelling of the JVM's `CompletionStage<Void> sessionEnd()` and of C#'s `Task`. Per the settlement
  of 2026-08-15 the shape is each language's own, so this is a data point rather than a divergence;
  the binding property - the caller learns the session ended and why, without ending the client -
  holds, because `get()` rethrows the cause.

## What changed outside this module, and why

- **`bin/build-client.sh`'s `--test` no longer hard-codes `pc-<lang>-toolchain-smoke`.** It now
  treats **the pairing** as the contract: every extracted executable `X` with a sibling `X-dynamic`
  is a portability claim with its own control - `X` must run on the host, `X-dynamic` must not - and
  an artifact with no `-dynamic` sibling (the conformance runner) is not a claim and is skipped
  rather than run with no arguments. At least one pair must exist, so an extraction that produced no
  claim cannot read as a pass. This was forced by deleting `toolchain-smoke/`, whose own header said
  to delete it once a real target proved the same three things; `pc-cpp-selftest` proves those three
  **and** that the client library links, which the smoke could not say.
- **`.github/workflows/clients.yml`'s `cpp` row now runs `scripts/analyse.sh`** instead of a
  spelled-out cppcheck line - which is what that file's own header asks for. The reason it had to
  change at all is worth knowing: `cppcheck --error-exitcode=1` fires on an **`information`**
  message as readily as on a finding, and the default check level emits
  `information: Limiting analysis of branches` on several of these files. The row as seeded would
  therefore have failed on a clean codebase. `--check-level=exhaustive` analyses the branches
  instead of announcing that it did not, which is the honest fix; it costs under a second here.
- **`LanguageRunners.java` gained a `cpp()` entry** whose build command is `bin/build-client.sh cpp`
  rather than a C++ tool, because there is no C++ toolchain on this machine to run. It needs Docker,
  and a missing daemon fails the build rather than skipping.

## What will bite the next wave here

- **The static link needs more than `pkg-config --static`, and that is still true.** Debian's
  `grpc++.pc` under-declares it: `libre2.a` calls absl's LOG machinery, whose archives appear in no
  `Requires` line, so the link fails on `absl::log_internal` symbols. `CMakeLists.txt` resolves it by
  putting every `libabsl_*.a` in a `-Wl,--start-group`, and needs `libssl-dev`, `zlib1g-dev` and
  `libzstd-dev` present for their `.a` files.
- **gRPC 1.51 / protobuf 3.21 are the versions you get.** Both predate the abseil-based protobuf
  API, so examples written against protobuf 4.x/5.x will not compile here. Building a newer gRPC from
  source inside the image is the escape hatch and costs tens of minutes per cold build.
- **cppcheck's `constParameterReference` is a false positive on the `execv` argv construction** -
  `execv` takes `char* const[]`, and only the NON-const `std::string::data()` overload returns a
  `char*`, so a const reference does not compile. It is suppressed inline, and the suppression must
  sit on the line **immediately above the reported line** - above the `std::transform(` call is one
  line too high and does nothing.
- **The gRPC sync API's threading rules shape the whole design**: one reader and one writer are safe
  concurrently, two of either are not. So the transport thread only reads, a dedicated writer thread
  drains an outbound queue and is the only caller of `Write`/`WritesDone`, and `Finish()` is called
  by the transport thread *after* joining the writer. A second writer would be a data race the
  library would not report.
- **The unresolved count must never be decremented on the executor's take.** It lives in
  `DispatchQueue`, and `Client::SettleGuard` is what frees the slot - a destructor, so an executor
  dying mid-record cannot skip it. Skip it once and the ceiling shrinks permanently, one slot per
  crash, and the client eventually declares a protocol violation against a correct proxy.
- **Measured** (32-core box): cold image build ~1 minute, most of it apt; an edit-and-rebuild of one
  source file ~35s, because a changed `COPY src` invalidates the layer and the generated protobuf
  translation units recompile with it. Static artifacts ~22 MB each; the dynamic control 0.6 MB.
- **A missing Docker is exit 2, not a pass** (`bin/build-client.sh`). Through Maven that surfaces as
  `Exit value: 2` in the exec failure message, while Maven's own exit stays 1 - so read the message,
  not just the code, when a CI row goes red.

## The demo (plan unit U35)

`demo/` beside the library: `run.sh`, a `Dockerfile`, a `docker-compose.yml`, `src/` and a
`tests/`. Two arms, per the contract - `AK core` on librdkafka, and `cpp-grpc` through **this
module's client library**, which spawns the sidecar itself. No hand-written gRPC anywhere: the Java
seed did that first and had to be rewritten, because it proved the engine worked and said nothing
about the client library.

### What the C++ demo does differently, and why

- **Container-only, with no native mode**, which is the one real divergence. `run.sh --native`
  answers with the reason rather than failing as an unknown flag. It follows from the same fact that
  makes `bin/build-client.sh` build this language in an image: gRPC, protobuf and librdkafka arrive
  as system dev packages, so there is no host toolchain to run natively against.
- **The broker is always supplied from outside.** Java starts one with Testcontainers when
  `--bootstrap` is absent; C++ has no Testcontainers, and the demo container is never granted the
  host Docker socket, so a missing address is an error with an explanation rather than a silently
  different run.
- **The sidecar is a launcher script the image installs**, at `/app/sidecar/sidecar`, named by
  `PC_DEMO_SIDECAR`. A foreign client library spawns a binary by absolute path and the sidecar is a
  JVM program, so something has to bridge that; `exec` in the launcher is load-bearing, because a
  forking wrapper would hold the lifecycle pipe the proxy watches for parent death.
  **`PC_DEMO_SIDECAR` is deliberately not an eighth flag** - the contract fixes the list at seven,
  and where the binary lives is a property of the image.
- **The demo image is a RUNTIME image built from its own toolchain stage**, the opposite of
  `../Dockerfile`, which exports statically linked artifacts and runs nothing. Nothing is extracted
  here, so there is no portability claim to prove and no reason to pay for the static link's absl
  archive group; the binary is dynamically linked and runs where it was built.
- **`demo/CMakeLists.txt` is a project of its own**, not a target in the module's. The demo needs
  librdkafka and the library does not, and adding it to the module's project would put a Kafka
  client into the image whose whole job is a portability claim about gRPC.
- **The Maven local repository IS a BuildKit cache mount here**, which the Java demo's Dockerfile
  says it could not have: that image computes a classpath file pointing into `/root/.m2`, so a cache
  mount would name jars the running container does not have. This one copies the jars out
  (`dependency:copy-dependencies`) and uses a wildcard classpath, which has no such coupling.
  `sharing=locked`, because a Maven local repository is not safe for concurrent writers and ten
  sibling demos may build at once.

### What was actually run, and what was not

Both entry paths were exercised by hand on this branch: `demo/run.sh` at the volume
`bin/ci-demo-test.sh` uses for the Java demo (`--records 20 --delay-ms 1 --concurrency 4
--partitions 2`), once with `--replay-factor 1` and once with `--replay-factor 2` so that the big
replay's second seed, second table and footnote ran too. Both arms processed every record and the
run exited 0. The no-argument path was exercised three ways: `run.sh` under **bash 3.2**, which is
what macOS ships and where the reference demo's empty-array expansion once aborted; the binary with
no arguments and no broker, which explains itself and exits 2; and `--help`, which the binary
answers itself because `docker compose run demo --help` reaches it directly.

**No measurement was taken and none should be read into those runs.** At twenty records the figures
are start-up and rebalance, which is exactly why the CI volume asserts arms rather than numbers. A
default-scale run is deferred to an unloaded machine - this one was running ten demo waves at once.

### The reader-experience pass, and the one thing it could not honour

The demo contract's "The output a reader actually sees" section was rewritten after someone watched
a demo and found it unimpressive. Three of its four rules are applied here; the fourth was already
true.

- **The banner is the first thing printed**, before the usage text and before a refused flag, not
  only before the fingerprint. C++'s copy names the language `C++` rather than the module
  directory's `cpp`.
- **Both arms name their library**: `AK core (librdkafka)` and `cpp-grpc (this client)`. The demo
  README now also answers the contract's "where a language has more than one serious client, say so"
  clause, and the answer for C++ is that it does not have one - `cppkafka` and `modern-cpp-kafka`
  are wrappers over librdkafka, so a second arm would price a header-only binding.
- **`records` and `keys` are new columns**, counted per arm from that arm's own delivery path
  (`rd_kafka_message_t` for AK core, `InboundRecord` for the sidecar arm) rather than restated from
  the seeding loop. The sidecar arm's key set is guarded by the same mutex as its counter, because
  every executor thread writes it; an unsynchronised `std::set` there is a race that would present
  as a slightly wrong key count rather than as a crash.
- **Broker log levels were already `WARN`** in `demo/docker-compose.yml` and were not touched.
- **The SIDECAR was not quiet, and that turned out to be two contract violations rather than
  noise.** This image gives the sidecar an SLF4J binding it would otherwise lack (see
  `demo/Dockerfile` for why). At slf4j-simple's default level it emitted the overwhelming majority
  of the demo's own output on a twenty-record run - including a full `ConsumerConfig values` dump
  per arm, which prints `bootstrap.servers`. The sidecar's stderr is inherited by the demo's
  container, so that address lands in the demo's own lines: `bin/ci-demo-conformance.sh` greps for
  exactly that and would have failed C++ on the credential rule. `demo/sidecar-launcher.sh` now
  passes `-Dorg.slf4j.simpleLogger.defaultLogLevel=warn`, overridable with `PC_SIDECAR_LOG_LEVEL`.
  **Every language whose demo image gains that binding inherits both problems**, so this is worth
  carrying into the repo-wide defect note rather than leaving as a C++ detail.

#### `bin/ci-demo-conformance.sh` no longer recognises this table, and that is silent

**Owed back to whoever owns `bin/`** - out of this note's ownership, so it is recorded rather than
fixed. That script reduces each demo's stdout to a skeleton and requires the skeletons to match. Two
of its `awk` patterns are now stale against the contract they enforce:

- the header pattern is `arm ... elapsed ... msg/s ... vs AK core`, which the two new columns break;
- the row pattern accepts `[A-Za-z0-9 _-]*` for an arm name and then `<elapsed>s <rate> <ratio>`,
  which both the parentheses in `AK core (librdkafka)` and the two extra figures break.

**Neither failure is visible as a failure.** With both patterns missing, a skeleton degrades to its
`DIAL` and `TITLE` lines, every language degrades identically, the diff is clean and the script
reports agreement - having stopped checking the columns and the arm order it exists to check. The
absolute assertions (the dials echoed, the address absent, no latency) still hold, so the run is not
worthless; it is just weaker than it reads. Fixing it means widening both patterns and normalising
the new `(library)` suffix the way `normalise_arms` already normalises the sidecar arm's name.

#### What this pass was verified against

`demo/run.sh --records 20 --concurrency 4 --partitions 2 --replay-factor 2` with
`PC_DEMO_DELAY_MS=3` - the conformance harness's own input - in the container, which is C++'s only
mode. It exited 0. Observed: the banner first, then the fingerprint; both arms labelled with their
library; both tables carrying `records` and `keys`; `20/20` in the small replay and `40/40` in the
big one, which is what the seeding makes deterministic at this volume (the key space is larger than
the record count, so keys equals records here and would equal the key space at default scale). With
the sidecar quietened the whole run is a few dozen lines, and `bootstrap.servers` appears nowhere in
them.

**No throughput figure from that run means anything and none is recorded here.** Ten language demos
were building and running on the box at the time; the elapsed columns are contention and start-up.
Output shape is what was being proven.

#### Rendering moved out of `demo.cpp` so the contract could be tested

`demo/src/demo_report.{h,cpp}` now owns `ArmResult`, the banner and the table; `demo.cpp` prints
what it returns. The split exists for one reason: the banner's wording, the column set and their
order are contract, and the arms that produce the figures need a broker that the image build does
not have - but rendering does not. `demo/tests/demo_report_test.cpp` holds all of it, and runs at
image-build time beside the option tests. The renderers return strings rather than writing to
`std::cout`, because the thing worth asserting is the text.

### What is NOT wired up

- **Neither entry point is in `bin/ci-demo-test.sh`.** That script and `.github/` were outside this
  wave's ownership, so the C++ demo is not run on every pull request the way the Java one is. The
  contract is explicit that "a demo with one tested entry point has an untested entry point", so
  this is a real gap rather than a deferral of polish. What it would need: a `cpp` row that runs
  `demo/run.sh --records <small> --replay-factor 1`, and nothing else - there is no native path to
  test separately, which makes C++ the cheapest language to add there.
- The demo's own tests cover the option surface and the output shape. The ARMS still need a broker,
  which the image build has none of - so nothing at build time proves that the `records` and `keys`
  columns are counted correctly, only that the table reports them. Running the demo is what proves
  the figures.

## Not implemented, and therefore not declared

Heartbeats and the liveness lease, the manifest reconnect and `Drop`, `WorkerDied`, terminal
outcomes, and the proxy-initiated `Shutdown` drain. `implemented_capabilities()` declares exactly
`["dispatch"]`, so the proxy never sends any of them; the queue's shutdown path therefore
**discards** queued records rather than reporting them `Released`, which is what §3.5 requires on a
session without the `shutdown` token. The wave that implements the drain sends `Released` from
`Client::shutdown`, under a `session_.negotiated(capability::kShutdown)` test - the comment is
already there.
