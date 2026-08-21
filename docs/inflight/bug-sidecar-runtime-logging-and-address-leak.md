# The sidecar's runtime logging is broken three ways, and one of them leaks the broker address

Found by three independent language agents during the demo fan-out (astubbs#242), each seeing one
face of it. None could see the whole from where they stood, and the fix each proposed would have
made one of the others worse - which is the reason this is written down as one defect rather than
three tickets.

## The three faces

- **Test scope on the classpath** - what the Java seed ships today. `dependency:build-classpath` at
  its default scope pulls in `parallel-consumer-core`'s **test** jar. Its `logback-test.xml` then
  prints logback's status report to **stdout**, ahead of the `port: <n>` line the client library
  scans for when it spawns the sidecar. That only works because the scan tolerates preceding lines.
- **Runtime scope** - the obvious fix, and what two agents recommended. `logback-classic` is
  **test scope repo-wide** (root pom, both the dependency and its management entry), and the proxy
  declares no provider of its own. Verified: `dependency:list -pl :parallel-consumer-proxy
  -DincludeScope=runtime` lists no logging provider at all. **The shipped sidecar has `slf4j-api`
  with nothing behind it and logs nothing.**
- **A provider with no configuration** - logback falls back to root `DEBUG`. Measured by the Scala
  agent: 4000+ lines of Netty frames and docker-java headers, burying the demo's own output.

## The fourth face, found later and arguably the worst

**The sidecar's diagnostics are structurally unavailable to the audience the sidecar exists for.**
Its logging goes to **stdout**, and stdout is the lifecycle channel the client library drains to read
the `port: <n>` line - so a foreign client consumes those lines and discards them. Observed by the
Rust agent: dozens of start-up lines when the sidecar is run by hand, none in any demo run, and a
failing sidecar reporting "session failed" with no reason attached.

**The Java seed hides this rather than escaping it.** Its `SidecarProcess` pumps that stream into its
own logger, so a Java developer sees everything and would never notice. Every other language sees
nothing. Fixing the provider and the configuration above does not fix this one: correctly configured
logging still goes down a channel that is drained and thrown away. Either the sidecar logs to
**stderr**, or the client libraries surface what they drain.

## The face that matters most, and why it is not cosmetic

**The Kafka clients log their full effective configuration at INFO, `bootstrap.servers` included,
several times per run.** The demo contract says the bootstrap address is never printed, and the
fingerprint honours that - but the rule currently binds only the fingerprint block, while the client
prints the address anyway.

It does not leak today, and **the reason is an accident**: the test jar's `logback-test.xml` pins
`org.apache.kafka.common.config.AbstractConfig` to `ERROR`. Verified against a full containerised
run of the Java seed - zero occurrences of `bootstrap.servers = `, four hits on `logback-test.xml`,
one `AbstractConfig -> ERROR`.

So **the recommended fix removes the protection**. Moving to runtime scope drops the test jar, which
drops the only configuration suppressing the dump. Own-cluster mode puts a user's real broker address
there, and R48/KTD11 treat that as credential-grade.

## What the fix has to do, all at once

0. Move the sidecar's own logging off **stdout**, which is the lifecycle channel, or have the client
   libraries surface what they drain. Without this the other three fix a stream nobody reads.
1. Give the proxy a runtime logging **provider**, so a shipped sidecar can log at all.
2. Give it a **configuration** that pins `org.apache.kafka.common.config.AbstractConfig` above INFO,
   so the address is suppressed by intent rather than by a test artifact's leftovers.
3. Then move the demos to `-DincludeScope=runtime`, which is correct once 1 and 2 hold and is
   actively harmful before.
4. Widen the contract's credential rule to bind **the whole run**, not just the fingerprint block.

Doing any one of these alone makes something worse. That is the finding.

## Where the pieces were seen

`docs/inflight/clients/python.md` (stdout ahead of the port line),
`docs/inflight/clients/typescript.md` (no provider on a runtime classpath),
`docs/inflight/clients/scala.md` (root DEBUG, and the client's own config dump),
`docs/inflight/clients/rust.md` (stdout is the lifecycle channel, so the logs are discarded).

`docs/inflight/clients/cpp.md` (no binding at all, and the fix demonstrated).

**Five agents, and each one's proposed fix leaves at least one of the others standing.** That is the
whole argument for treating it as one defect.

## Step 0 is not a guess - one client is already built to receive it

The C++ agent gave its own image an SLF4J binding and watched the sidecar log its port, its
configuration and its drain. Its note says the missing binding was "silencing exactly the
diagnostics **the client library inherits stderr to preserve**".

So the two halves fit: the Rust client **discards stdout** because that is the lifecycle channel it
drains for the port line, and the C++ client **inherits stderr** precisely so the sidecar can be
diagnosed. Moving the sidecar's own logging to stderr is therefore not a proposal needing a design -
at least one client already expects it there, and one has demonstrated the chain end to end.


## Confirmed live: the fix's own trap, hit by the C++ demo

The C++ agent gave its demo image an SLF4J binding - step 1 of the fix ordering above - and got both
of the problems steps 2 and 4 exist to prevent, in one run:

- **the flood**: 568 lines of demo output, roughly 500 of them routine sidecar chatter burying the
  two tables;
- **the leak**: a full `ConsumerConfig values` dump per arm, printing `bootstrap.servers = [...]`
  into lines the DEMO owns - not the broker's. `bin/ci-demo-conformance.sh` greps for exactly that
  and would have failed the credential rule.

It fixed its own image with `-Dorg.slf4j.simpleLogger.defaultLogLevel=warn`, overridable through
`PC_SIDECAR_LOG_LEVEL`.

**This is not a C++ finding.** Every language whose demo image gains that binding inherits both
problems, and adding the binding is precisely what the first step of the fix asks for. It is the
clearest evidence yet that these four steps are one job: shipping step 1 alone would hand every
language a noisier demo and a credential leak at the same time.
