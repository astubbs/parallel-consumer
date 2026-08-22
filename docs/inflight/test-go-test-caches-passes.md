# `go test` caches passes, and these tests depend on state it cannot see

Found 2026-08-17 while documenting the multi-language build (astubbs#242). Open; not fixed, because
the fix is one flag and the question of whether to pay for it is the owner's.

## What happens

`go test ./...` **caches successful results** and reprints them as `ok … (cached)` without running
anything. The cache key is the test binary plus the inputs Go can see: source files, build flags, the
environment variables and files the test reads through the `os` package.

Nothing in this repo disables it — `pc.foreign.test.args` is `test ./...`, and no lane passes
`-count=1` or sets `GOFLAGS`. So every Go test run, local and CI, is eligible.

## Why it is not simply the build-cache argument again

`docs/inflight/bug-mvn-clean-does-not-clean-go-output.md` argues that Go's **build** cache is safe to
leave alone, because it is content-addressed and cannot serve a stale artifact: change an input and
the key changes. **That argument does not carry over cleanly here**, and the difference is the point.

- It holds for a pure unit test, whose entire input is source Go can hash.
- It does **not** hold for these tests. `parallelconsumer/session_test.go` and its neighbours **spawn a
  sidecar** and talk to it. The sidecar jar, the JVM, the engine's behaviour and the machine's timing
  are all inputs Go cannot see and does not hash.

So a Go source file unchanged plus an engine that has changed underneath equals **`ok (cached)`, with
nothing executed** — a pass reported for a run that did not happen. That is the class
[`docs/solutions/workflow-issues/a-check-that-reports-success-without-having-run.md`](../solutions/workflow-issues/a-check-that-reports-success-without-having-run.md)
exists for, and this repo has now met it nine times.

## How exposed we actually are

**Unquantified, and that is the first thing to settle.** The exposure needs a real change to the
engine with the Go client untouched, then a `mvn test` on the Go module, to see whether the lane
prints `(cached)` or re-runs. The reactor edge from the `go-e2e-harness` profile may already force a
rebuild that invalidates the key; nobody has checked.

Note the shape while checking: a **failing** test is never cached, so this only ever hides a pass that
should have become a failure — which is exactly the direction that matters.

## The fix, if it is wanted

`-count=1` on the test invocation disables the cache entirely. The cost is that every Go test run is a
full run; the Go suite is seconds, so the cost is small here.

**The wider question is whether the other languages have the same property** and nobody has looked:
Rust's test harness, .NET's, and Python's `pytest` all have their own caching stories. This note names
Go because Go is where it was observed, not because Go is special.
