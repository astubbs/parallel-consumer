# The container languages' images: cache them in CI, then shrink them

Two follow-ups from measuring the Swift build, **in this order** — the owner's rule: first make it
work, then make it faster and smaller. Neither blocks the Swift client; both are worth doing once it
exists.

## 1. CI has no local image store, so it re-downloads on every run

Measured on the development box, 2026-08-15:

| Base image | Size |
|---|---|
| `swift:6.1` | **3.38 GB** |
| `debian:trixie-slim` (the C++ base) | 78.6 MB |

Locally that 3.4 GB is pulled once and never again, which is why a "cold" build here looks
alarming and every later build does not. **A CI runner starts empty every time**, so it pays the
full pull on every run of the Swift row — and the C++ row's 79 MB is not remotely comparable.

Fix it in the workflow rather than in the Dockerfile: cache the image layers between runs, or push a
prebuilt image to a registry and pull that. The existing rows already cache per-language toolchains
with rotating keys, so there is a convention to follow rather than invent. **Verify the effect, not
the report** — a cache step that silently misses looks exactly like one that works, and this repo has
a documented history of checks that pass without having run.

## 2. 3.4 GB is very large, and probably not all needed

The official Swift image carries a full toolchain: compiler, stdlib, debugger, package manager. A
client build needs the compiler and stdlib; the runtime artifact needs neither, since it is statically
linked and executed on the host. So a multi-stage build with a slim final stage is likely already
happening for the *artifact* - what is large is the **builder**.

Worth trying, once the client works, in rough order of payoff: a slim or runtime-only Swift base for
anything that does not compile; splitting the protoc plugins into a base layer that never invalidates
(they are compiled from source because no Linux release binaries were found — **re-check that**, since
it may have changed); and `--mount=type=cache` for SwiftPM's resolve and build directories.

**Do not start this before the Swift client exists.** An image optimised around a build that does not
yet work will be optimised for the wrong shape, and the measurement that justifies each change needs
a working build to measure.
