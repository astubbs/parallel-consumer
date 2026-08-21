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
`docs/inflight/clients/scala.md` (root DEBUG, and the client's own config dump).
