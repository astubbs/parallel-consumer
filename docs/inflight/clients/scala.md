# Scala client (astubbs#242)

Seeded, not started. Added after the first fan-out wave rather than with the original nine, at the
owner's request - "why not, it's so cheap" - on the same reasoning as Kotlin: a JVM language can
depend on `parallel-consumer-proxy-client-java-api` directly and wrap an existing transport, with no
protobuf codegen hop of its own.

## What its wave inherits, and what it must decide

- **Build with `scala-maven-plugin`, not sbt.** The skeleton pom says why: sbt would be a second
  build system inside a Maven reactor. Reach for it only if the Maven plugin genuinely cannot
  express what Scala needs - and record the argument if so.
- **Wrap `java-grpc`. The Kotlin wave settled this, and paid for it.** It first wrote its own gRPC
  session on three grounds; two were real and have since been fixed at source rather than worked
  around - the transport module no longer drags the engine into a wrapper's build, and the shared
  API now takes an `AsyncRecordProcessor` returning a `CompletionStage<Outcome>` that a `Future`,
  an `IO` or a `Task` completes without parking a thread. What is left for a JVM client is its own
  spelling and its own concurrency idiom, which for Kotlin is one file (`Bridge.kt`) plus the
  session shape. `docs/inflight/clients/kotlin.md` has the whole argument, the measurements, and
  the honest list of what wrapping cost. Deciding differently means arguing against that, not
  starting fresh.
- **What that leaves Scala to decide** is the shape of `poll` in Scala's own concurrency model,
  whether a `Future`, a cats-effect `IO`, or a ZIO effect is the surface, and how cancellation maps
  onto "no verdict for this record" - the one rule where wrapping is easy to get subtly wrong,
  because completing the stage exceptionally becomes a *failure report on the wire*. Kotlin's
  `NoVerdictIsInventedTest` is the shape of the test to copy.
- **Scala 2.13 or 3.x** is unsettled. 3.x is current; 2.13 is what most Kafka-adjacent Scala code
  still runs on. Decide on evidence about who this client is for, not on novelty.
- **Static analysis is expected of every client module** (`docs/client-static-analysis.md` carries
  the cross-language decision). Scala's mature options are Scalafix and WartRemover; apply the same
  "only if mature, and it must run locally" filter the other languages did.
- **It has no CI row yet.** Every other client module has one in `.github/workflows/clients.yml`;
  this module was seeded after that file's rows were written, so its wave adds one mirroring the
  others - including the fragment-derived deferral gate, which is what keeps a skeleton's row green
  without pretending it ran.

## Why it is not in the current wave

The owner's call: seed it now, implement it in a later wave. Nothing blocks it - the JVM toolchain is
already present - so it is available whenever a wave has room.
