# Scala client (astubbs#242)

Seeded, not started. Added after the first fan-out wave rather than with the original nine, at the
owner's request - "why not, it's so cheap" - on the same reasoning as Kotlin: a JVM language can
depend on `parallel-consumer-proxy-client-java-api` directly and wrap an existing transport, with no
protobuf codegen hop of its own.

## What its wave inherits, and what it must decide

- **Build with `scala-maven-plugin`, not sbt.** The skeleton pom says why: sbt would be a second
  build system inside a Maven reactor. Reach for it only if the Maven plugin genuinely cannot
  express what Scala needs - and record the argument if so.
- **Which transport to wrap** - `java-grpc`, `java-direct`, or both - is the same question the
  Kotlin wave answers first. Read its note before deciding differently; two JVM clients disagreeing
  on this without a reason is the kind of drift the fan-out exists to avoid.
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
