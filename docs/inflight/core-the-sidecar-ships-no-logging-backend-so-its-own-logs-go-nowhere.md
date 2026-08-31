# The sidecar logs to nothing - `slf4j-api` is on its runtime classpath and no provider is

<!-- inflight-type: bug -->
<!-- inflight-impact: blind-spot -->

`parallel-consumer-proxy`'s runtime classpath carries `slf4j-api` and no SLF4J provider, so every
`@Slf4j` call in the sidecar reaches a no-op logger. Started from a plain classpath, the JVM sidecar
says so out loud - `SLF4J(W): No SLF4J providers were found` on stderr - and then runs perfectly,
which is why nobody has noticed: stdout carries the port line exactly as the spawning contract
requires, and the process behaves correctly in every respect that is asserted.

The lines that go nowhere are the ones an operator would want most: `Sidecar listening on loopback
port ...`, the non-loopback opt-in's warning about the surface it is exposing, and
`NoEngineSessionService`'s refusal. A client author debugging a sidecar that "does nothing" has the
gRPC status and no server-side record of anything.

Found while building the native executable, but it is **not** a native-image problem - the JVM
sidecar behaves identically, and the native build is if anything cleaner about it, because with no
backend on the classpath there is nothing for build-time initialisation to fight.

## Why it is not a one-line fix

A library must not bind a logging backend; an **executable** normally must. This module is both -
the jar is a library-shaped artifact in a reactor whose other modules are libraries, and `Main` is a
program. So the decision is which of these the module is being packaged as, and it wants making
rather than assuming:

- a provider at runtime scope, which every downstream consumer of this jar then inherits;
- a provider only in whatever assembles the distributable, once one exists;
- or deliberately none, in which case the sidecar should not be calling `log` at all and the
  operator-facing lines belong on stderr, where the usage text already goes.

Nothing is blocked on it today: there is no distributable and no supported way to run this against a
broker. It should be settled by the rung that first ships something an operator runs.
