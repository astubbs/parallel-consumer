# The native sidecar builds with no reachability configuration - and only because it has no engine

<!-- inflight-type: task -->
<!-- inflight-impact: coordination -->

`bin/native-image-sidecar.sh` builds `parallel-consumer-proxy`'s `Main` into a GraalVM native
executable with **one flag** - `--no-fallback` - and no `META-INF/native-image` configuration of any
kind. That is a real result and it is also a trap for whoever lands the engine rung, because the
reason it is easy is not that native image is easy here.

## Why it is easy today

The module's runtime classpath is gRPC, protobuf, Guava and `slf4j-api`. Nothing on it builds an
object from a **configuration string**, which is the one thing closed-world analysis cannot follow,
and nothing on it is a logging backend, which is the other thing that fights build-time
initialisation.

## What changes when the engine arrives

Both of those stop being true in the same commit, and **neither failure appears at build time**:

- **The Kafka client instantiates serializers and partitioners by reflection from configuration
  strings** (`key.deserializer=...ByteArrayDeserializer`). Reading the code does not find them. The
  build stays green and the binary fails at *runtime*, inside connect-time configuration - which is
  precisely where it failed on the branch this work was re-cut from.
- **A logging backend on the runtime classpath brings the analysis failure back.** With logback
  present, the plain build fails on `Logger.name` not being available during analysis; the fix is
  `--initialize-at-build-time=ch.qos.logback,org.slf4j,org.xml.sax,com.sun.org.apache.xerces,javax.xml`,
  and the script's header carries that recipe waiting for the day it is needed.

The prior work is worth reading before rediscovering either:
`git show ca05c7185:docs/inflight/perf-native-image-sidecar-works.md` on
`feats/native-image-sidecar` records five build attempts, the fact that **adding** reachability
configuration once broke a build that had already passed, and the captured metadata itself at
`git show ca05c7185:parallel-consumer-proxy/src/main/resources/META-INF/native-image/reachability-metadata.json`.
That capture is a starting point and not an answer: it was traced over one unordered 20-record run
with no failures, retries, rebalance or transactional commit, so every path it did not walk is still
invisible.

## What that rung actually owes

Not "copy the metadata file across". A capture is only as good as the session that produced it, so
the trace has to cross the retry path, the failure path, each commit mode and a rebalance - or,
better, the module gets something that **asserts** reachability rather than trusting a one-off
capture, which is the only version of this that does not rot silently.

## Related

- `bin/native-image-sidecar.sh` - the build recipe, the absent-toolchain contract and the exit codes.
- `.github/workflows/native-image.yml` - the two rows that run it, one of them Linux, which the
  earlier work never tried.
