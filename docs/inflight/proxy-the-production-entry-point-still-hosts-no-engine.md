# The sidecar's production entry point still hosts no engine

<!-- inflight-type: task -->
<!-- inflight-impact: stranded-work -->
<!-- inflight-state: deferred - unit U10, which owns the drain that has to land with it -->

`bz.stub.parallelconsumer.proxy.Main` binds a loopback port, announces it, admits one connection
under the transport's rules, dies with its parent - and answers every session `UNIMPLEMENTED`,
because `Main#sessionServiceFactory` still returns `NoEngineSessionService`. The engine it would
host is in the same module: `ConfigureHandler` is a `ProxyServiceGrpc.ProxyServiceImplBase` and
`ProxyProcessor` is behind it, and `TestModeMain` in this module's test tree wires exactly that
pairing today. **The substitution is one call site.** It is not made here.

## Why not here

`Main` arrived from astubbs/parallel-consumer#384, the sidecar-shell rung, which cut it from
`feats/sidecar-entry-point` **minus the engine, the configuration handler and the drain**. That
PR's body names what it left behind and who owns it: unit U10 - `DrainCoordinator`, the
integration test that spawns a real sidecar against a real broker, and the shared spawn helper -
and says U10 still has no PR, so whoever opens it reconciles against that rung.

Wiring the engine without the drain would ship a sidecar that accepts records and has no defined
behaviour for the shutdown that must hand them back. `Main` already reserves the exit code for it
and does not use it: astubbs/parallel-consumer#384 records exit 3 as U10's "a drain that timed out
with records still held", which nothing can currently return.

It would also silently invalidate eight tests rather than one seam. Every foreign client's
`SidecarHandshakeTest` (Kotlin, Scala, Go, Python, TypeScript, Rust, Ruby, C#) spawns `Main` and
asserts the refusal arrives **as `UNIMPLEMENTED` specifically** - `PERMISSION_DENIED` and
`RESOURCE_EXHAUSTED` are what the two interceptors raise before the service method runs, so the
status code is the assertion rather than "it failed". Those tests are not scaffolding: each carries
a permanent control arm pointed at a dead port, and they are the only cross-language evidence that
the client-side path up to the engine works. An engine behind `Main` makes their subject vanish, so
they have to move to a no-engine entry point in the same commit - which is a decision about what
`NoEngineSessionService` is for, and belongs with U10's design rather than ahead of it.

## What has to happen when U10 lands

- Replace `Main#sessionServiceFactory` with the engine-backed session service. One call site,
  named rather than inlined for exactly this.
- Decide `NoEngineSessionService`'s fate: **demoted to a test fixture** is the expected answer, not
  deleted, because the eight handshake tests and the transport's own tests need a `BindableService`
  that hosts nothing. Give it a test-only entry point for them to spawn, and re-point each
  language's `MAIN_CLASS` constant at it.
- Bring `DrainCoordinator` and exit code 3 with it, and the real-broker integration test.

## What is NOT waiting on this

The engine is exercised end to end today, just not through `Main`: `TestModeMain` hosts
`ConformanceHarness`'s engine lane, every foreign client's end-to-end test spawns it, and the
conformance matrix drives it. So this is a packaging gap, not an engine gap.
