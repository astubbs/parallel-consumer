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

## Not implemented, and therefore not declared

Heartbeats and the liveness lease, the manifest reconnect and `Drop`, `WorkerDied`, terminal
outcomes, and the proxy-initiated `Shutdown` drain. `implemented_capabilities()` declares exactly
`["dispatch"]`, so the proxy never sends any of them; the queue's shutdown path therefore
**discards** queued records rather than reporting them `Released`, which is what §3.5 requires on a
session without the `shutdown` token. The wave that implements the drain sends `Released` from
`Client::shutdown`, under a `session_.negotiated(capability::kShutdown)` test - the comment is
already there.
