# `mvn clean` does not remove Go's compiled output

Found 2026-08-17 while making clean work for every client language (astubbs#242). **Deliberately not
fixed** — the impact is small and both routes to a fix are disproportionate. Recorded so the gap is
known rather than assumed away.

## What is not cleaned

`go build ./...` compiles this module's sources into Go's **shared, content-addressed build cache**
(`go env GOCACHE`, `~/.cache/go-build`, 725MB at the time of writing). Nothing lands in the module, so
there is nothing for a `maven-clean-plugin` fileset to remove — which is why the Go pom carries no
fileset, and why that reads as "nothing to clean" when the truth is "the output is elsewhere".

The practical consequence is narrow but real: **`mvn clean` does not put Go back to a from-scratch
state.** A rebuild reuses cached compilation of our own code. That differs from Java, where clean
deletes our classes and only third-party dependencies survive in `~/.m2`.

## Why it is not fixed

- **Go has no per-package cache eviction.** `go clean -cache` empties the whole cache, which is shared
  with every worktree, agent and unrelated Go project on the machine. Out of bounds for the same
  reason the container languages must not drop BuildKit layers.
- **A shell-out cannot be bound to the `clean` phase here.** Both `exec-maven-plugin:exec` and
  `maven-antrun-plugin:run` declare `requiresDependencyResolution=test`, so binding either makes
  cleaning demand a resolvable dependency tree — measured, and the reason every language's clean is
  `maven-clean-plugin` filesets instead. So `go clean` cannot simply be delegated to.

## What `go clean` would remove, if it could run

Not nothing — `go clean -n ./...` lists package-named binaries and test binaries in the **source**
directories: `cmd/conformance-runner/conformance-runner`, `parallelconsumer/parallelconsumer.test`,
and their `.exe` and `main` variants.

**None of those exist today**, because nothing in this build writes a binary into the source tree —
the conformance runner is built into `target/`, which the default clean already removes. They would
appear only if someone added a `go build -o` into the package directory.

## If it ever matters

Two routes, neither worth taking now:

- A mojo that shells out without requiring dependency resolution, bound to `clean`, running
  `go clean ./...`.
- `maven-clean-plugin` filesets for `**/*.test`, `**/*.exe` and the package-named binaries — the last
  of which cannot be pattern-matched reliably, which is what makes this the weaker option.

**The trigger to revisit** is a Go build step that writes into the source tree. The gate suggested in
[`next-nothing-guards-the-cold-build.md`](next-nothing-guards-the-cold-build.md) — assert
`git status --ignored` is empty after build-then-clean — would catch exactly that, and would cover the
same regression for every other language at once.
