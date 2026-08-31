# Two conformance cells cannot run until the sidecar hosts an engine

<!-- inflight-type: task -->
<!-- inflight-impact: test-debt -->
<!-- inflight-state: deferred - the sidecar engine -->

The shared conformance suite is on `master`, with the core control arm and the `java-direct` cell
running. **Two cells that the suite is designed to have are absent**, and both wait on the same
thing: the sidecar in `parallel-consumer-proxy` hosts no Parallel Consumer engine and answers every
session `UNIMPLEMENTED`.

| Cell | Where it goes | What it needs |
|---|---|---|
| the `java-grpc` binding of the shared suite | `JvmClientBindings` in `parallel-consumer-proxy-conformance` | an engine to dispatch, reached over the wire on a loopback port |
| the gRPC subclass of the transport-parameterised suite | `parallel-consumer-proxy-client-java-harness` | the same, plus the harness's engine lane |

Both are **absent rather than disabled**, and the distinction is the reason this note exists: a
`@Disabled` test still has to compile, and the classes their fixtures name - the engine, the
configure handler, the harness's `startEngine` lane - are not on any classpath here. Stubbing an
engine to make them compile was considered and rejected: agreement between bindings is this suite's
entire product, and a binding that agrees with a stand-in says nothing about a client.

## What stops this rotting

Neither absence is left to a comment. A guard test in each module asserts the cell exists **exactly
when** the engine is reachable, so it goes red whether the engine lands without the cell or the cell
is written without an engine:

- `TheEngineArrivingMustBringTheGrpcBindingTest`, in the conformance module
- `TheEngineArrivingMustBringTheGrpcCellTest`, in the harness module

The selector has its own half: `SelectorMatchingNothingFailsTest` asserts that
`-Dpc.conformance.language=java-grpc` **fails**, so a CI row naming the deferred cell cannot report
green having run only the control arm.

## What the engine work has to do here

Wire the binding, write the subclass, and run the five wired scenarios over the wire. The suite
itself needs no change - the scenarios, the assertions and the driver are already binding-blind, and
the harness's engine lane is the only piece that has to be rebuilt (it was stripped from
`ConformanceHarness` when the class was cut, and its shape is on `feats/proxy-requirements` as
`ProxyHarness.startEngine`).

**Reconcile `ConformanceHarness` with that branch's `ProxyHarness` rather than keeping both.** They
are one class: this one is that one with the engine lane removed, renamed because it no longer had
anything to do with the proxy, and moved into the conformance module because that is its only
consumer here.

## Related

- The ten foreign-language runners are a separate extraction out of astubbs#293, not this one.
