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

**Cheaper middle path to weigh first:** this repo already manages Java, Go and Node through mise,
and mise also covers Rust, Ruby and .NET. Extending what exists costs far less than an image fleet;
containers earn their place for the genuinely awkward two (Swift on Linux, C++ with gRPC dev libs).

## 2. Containers as a RUNTIME option - which is the real deployment shape

Not a workaround. A sidecar deployment puts the application and the proxy in one place whatever the
packaging, so this is the shape users will actually ship. Two forms, both nearly reachable already:

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
