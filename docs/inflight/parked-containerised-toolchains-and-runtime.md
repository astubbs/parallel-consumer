# Containers for building the clients, and for running them

Two ideas that arrived together (astubbs#242, 2026-08-14, user). **The build half is now decided and
mostly moot; the runtime half is the one still open.**

## Decided: mise for anything it supports, and agents may install it

Every toolchain the fan-out was missing turned out to be in mise's registry, and the user's call is
to use it - **including letting agents run `mise use -g` themselves**, since GitHub's runners will
have to install the same things anyway. Landed the same day: `protoc@35.1`, `buf@1.72.0`,
`rust@1.97.1`, `kotlin@2.4.10`, `dotnet@10.0.400`, with `ruby` and `swift` following (ruby compiles
from source, so it is the slow one).

That shrinks the container question to whatever mise cannot serve well - realistically C++ with gRPC
dev libraries, and Swift if its Linux toolchain proves awkward. **Do not build an image fleet for
problems mise has already solved.**

Two things the newly-present tools immediately settled:

- **`buf` proves the freeze gate's timing argument.** Run against the commit before the language
  file options landed, `buf breaking` exits **100** and flags every one of them - `ruby_package
  changed from "" to ...`, `go_package`, `swift_prefix`, all of it. Adding a language's placement
  option after the freeze reaches master would therefore be a gate fight; adding it before was a
  one-line commit. The reasoning in that commit is now measured rather than asserted.
- **`PROTO_BREAKING_AGAINST` does not take a bare git ref**, despite the script header saying "any
  buf input: a git ref spec". buf reads a bare ref as a *path* and fails with "had no .proto files".
  The working form is `.git#ref=<ref>,subdir=parallel-consumer-proxy-protocol`. The gate does exit
  non-zero on the bad input rather than passing silently, so this is a documentation defect, not a
  hole - but it is the exact override the parked review findings want a self-test to use, so fix the
  header comment when that self-test is written.

## 1. Delegating the awkward BUILDS to containers (now a narrow case)

**The mechanism, if we do it:** a multi-stage Dockerfile per language whose final stage is only the
artifacts, extracted with BuildKit's `docker buildx build --output type=local,dest=<dir>`. No
container is run and nothing is copied out by hand; cache mounts keep the package managers warm.
The foreign-clients Maven profile already shells out to an `exec`, so the seam is a
`bin/build-client.sh <lang>` that picks container or native - nothing else in the build changes.

**Prior art is specifically build-only, and mature:** PyPA's `manylinux`/`cibuildwheel` (wheels
built in a pinned container, extracted, shipped), `cross` for Rust, `dockcross` for C/C++.

**It pays off in exactly the four languages that are hardest to install, and for one reason:** they
are all compiled, so the extracted artifact stands alone. Rust and Swift with static linking, C++
statically linked, C# published `--self-contained`. The caveat is linking discipline - a dynamically
linked artifact built against the image's glibc will bite on a different host, so static by default.

**It does nothing for the interpreted ones.** Python, TypeScript and Ruby barely have a build step,
and running a single test still needs the interpreter on the host. Node and Python are already here;
**Ruby is the one language containers cannot rescue** and still wants a host install.

**And CI never had this problem** - the language matrix uses `setup-<lang>` actions, which are
someone else's containers. This is a local-and-agent reproducibility question, which lowers its
stakes.

### MEASURED 2026-08-22: extraction does not work on macOS at all, and the caveat above understates why

The first full local run of the conformance suite got all ten runners built and then failed **20 of
20 scenarios for exactly the two container-route languages**, cpp and swift, each with **exit 126**
- "found, but cannot execute":

```
runner   : swift
exit     : 126
file     : ELF 64-bit LSB pie executable, ARM aarch64, dynamically linked
host     : macOS, arm64
```

The caveat above frames this as **linking discipline** - "static by default" so a glibc mismatch does
not bite on a different host. That is the right rule for Linux-to-Linux, and **it does not help
here**: no amount of static linking makes a Linux ELF execute on Darwin. Build-and-extract is sound
only when the host OS matches the image OS. On macOS the artifact is unrunnable however it is linked.

**The consequence for the fallback idea is the sharp one.** If a developer without mise is served by
"build it in a container instead", that fallback is broken precisely on the machines that need it:
macOS developers are the ones who cannot get Swift and C++ toolchains from mise, and they are the
ones for whom an extracted Linux binary cannot run. A container **build** fallback therefore has to
be a container **run** fallback too - which is section 2's question, not section 1's, and is a much
larger change than pointing an exec at Docker.

**CI is unaffected and will stay green through this**, because its runners are Linux, so an extracted
Linux binary is native there. That is the trap: the gap is invisible to every check that exists and
appears only on a developer machine, which is the same shape as the toolchain drift recorded in
[`ci-toolchain-versions-declared-twice.md`](ci-toolchain-versions-declared-twice.md).

Nothing here argues against the container route for *building* - it built both languages cleanly on a
host with neither toolchain, which is what it is for. It argues that "built" and "runnable here" are
different claims, and only the first one is currently true off Linux.

**Cheaper middle path to weigh first:** this repo already manages Java, Go and Node through mise,
and mise also covers Rust, Ruby and .NET. Extending what exists costs far less than an image fleet;
containers earn their place for the genuinely awkward two (Swift on Linux, C++ with gRPC dev libs).

## 2. Containers as a RUNTIME option - which is the real deployment shape

Not a workaround. A sidecar deployment puts the application and the proxy in one place whatever the
packaging, so this is the shape users will actually ship. Two forms, both nearly reachable already:

### The option set, settled 2026-08-14

Three packagings, each available against either sidecar build (JVM jar plus a runtime, or the
GraalVM native binary the feasibility gate proved at ~45MB with no JVM):

| Packaging | Spawn + parent-death | Loopback posture |
|---|---|---|
| No Docker - sidecar as a child process | unchanged | unchanged |
| **Same container** - app and sidecar together | unchanged | unchanged |
| Independent sidecar container, with a **Compose template** shipped ready-wired | **client must attach, not spawn** | **needs a shared network namespace** |

**Kubernetes is shelved until post-v6** by decision. Nothing is wasted by deferring it: the pod case
is the independent-container option with a different supervisor, so whatever the Compose template
needs is what K8s will need.

**The independent-container option is the only one that changes the contract, and it changes two
things at once:**

- **Attach becomes a client configuration surface, in every language.** Nobody spawns a sidecar that
  is already running, so a client needs an "attach to an endpoint" mode beside today's spawn path,
  and must not assume it owns the sidecar's lifetime. The specification already has the vocabulary -
  it separates operator-stop from parent-death, and `Shutdown` is on the frozen wire - but no client
  implements attach today. **Decide this before the remaining language waves**, because retrofitting
  a second lifecycle mode into eight finished clients is the expensive order.
- **Loopback stops meaning "only us" across two containers**, so the sidecar would have to bind
  beyond loopback and trip R18's opt-in and its no-authentication warning. Compose avoids this for
  free with `network_mode: "service:<sidecar>"`, which shares a network namespace so `127.0.0.1`
  still holds. **The template must do that**, or the independent option silently becomes the
  unauthenticated-surface option.

A refinement for whoever builds the JVM images: `jlink` produces a custom runtime well below a full
JRE without native-image's constraints - the middle option if Graal proves awkward for some feature.

### The shapes in detail

**One container - application plus sidecar, client spawns it.** The contract is unchanged: spawn,
loopback, parent-death all as specified. The JRE objection is already answered - the feasibility gate
proved the sidecar builds as a **GraalVM native image, a 45MB binary with no JVM** - so the image is
a language runtime plus one binary. This is what the per-language demo containers should be.

**Kubernetes pod sidecar - two containers.** Idiomatic, and the **loopback posture survives
untouched**, because containers in a pod share a network namespace: `127.0.0.1` still means only us,
so no exposure opt-in is involved. What changes is lifecycle - no spawn and no parent-death - but the
specification already separates operator-stop from parent-death (the probe forced that distinction)
and `Shutdown` is already on the frozen wire, so this needs no schema change. Wants K8s 1.29+ native
sidecars (`restartPolicy: Always`) for start/stop ordering.

**The new work is small either way:** an entrypoint that resolves the sidecar binary by explicit
path (the authoring guide already forbids a PATH lookup), and the three-mode selection working
non-interactively, which the demo contract's non-TTY fallback already specifies.

## What is left to settle

The build half is settled by mise. What remains:

- **The runtime shapes above** - which the demo containers force a decision on anyway.
- **C++ and Swift are the two mise does not serve, and Swift is now proven rather than suspected.**
  `mise use -g swift@latest` fails 404 on this box: Swift.org publishes Linux toolchains for Ubuntu,
  Amazon Linux and RHEL, and Debian 13 (trixie) is not among them. Do not retry the install - the
  routes are an official Swift container image, or an Ubuntu build run under compatibility. C++ is
  the other: gRPC and protobuf dev libraries are system packages, and static linking is what makes an
  extracted artifact portable. **These two are the container case; everything else is mise.**
- **Whether CI should follow.** The language matrix uses `setup-<lang>` actions today. Now that the
  local side is mise, the cheap consistency win is having CI use mise too, from the same
  `~/.config/mise/config.toml` pins - one place where a toolchain version is stated. Weigh it when a
  version skew first bites; do not churn working CI for symmetry alone.
