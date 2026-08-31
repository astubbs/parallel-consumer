# Proxy clients

Eleven client modules, one per language the proxy sidecar speaks to
(astubbs/parallel-consumer#242). **One of them is a client; the other ten are still build
scaffolding.**

- **Java is real** - a shared client surface with two transports behind it, its own aggregator with
  four modules under it. [`parallel-consumer-proxy-client-java/README.md`](parallel-consumer-proxy-client-java/README.md)
  owns what it can and cannot do.
- **The other ten** each detect their toolchain, compile or package one source file, run the
  resulting program, and assert that it printed one deterministic fixture line. There is no Kafka,
  no protobuf, no gRPC and no Parallel Consumer semantics in any of them.

The scaffolding was settled first on purpose: wiring eleven toolchains into one Maven reactor is a
mechanical question with a mechanical answer, and it is worth answering before anything interesting
depends on it. The remaining ten clients are later rungs of the stack being extracted from
astubbs/parallel-consumer#293, which is where that work currently lives.

## Building it

```bash
bin/build.sh                                  # the three JVM modules; no foreign toolchain runs
./mvnw package -pl :parallel-consumer-proxy-client-go -am -Dpc.foreignClients
./mvnw package -Dpc.foreignClients            # every language your machine can build
```

An ordinary build contains **only** the JVM modules - Java, Kotlin and Scala, which need nothing
installed that the reactor does not already have. The other eight are not in the reactor at all
unless `-Dpc.foreignClients` is passed, and the root pom prints a notice saying so, because a
reactor that silently omits eight modules looks exactly like one that never had them.

## What an absent toolchain does

Nothing here is installed on a typical machine, so **a missing toolchain is reported and skipped,
not treated as a build failure**:

```
========================================================================
  FOREIGN CLIENT STEP SKIPPED - set PC_FOREIGN_CLIENTS_STRICT=1 to make this red instead
  module/step : rust
  missing     : cargo (not on PATH)
  nothing was built, compiled or asserted for this module
========================================================================
```

Add `-Dpc.foreign.strict=1` to make that case red instead. CI passes it, and that pairing is what
keeps the lenient default honest: on a CI row the toolchain is provisioned by the row, so its
absence there is a provisioning bug rather than a fact about the machine.
`bin/foreign-client-step.sh` owns this contract and states the one cost it leaves behind - Maven's
reactor summary still prints SUCCESS for a module whose step was skipped.

## The layout

| Module | Toolchain | Build | Run |
|---|---|---|---|
| `...-client-java` | the JDK already running Maven | Maven | surefire - **a real client, not a fixture** |
| `...-client-kotlin` | kotlin-maven-plugin, from Central | Maven | surefire |
| `...-client-scala` | scala-maven-plugin, from Central | Maven | surefire |
| `...-client-go` | `go` | `go build` | the linked binary |
| `...-client-python` | `python3` | `compileall` | the interpreter |
| `...-client-typescript` | `npm` | `npm run build` (tsc) | `node` |
| `...-client-rust` | `cargo` | `cargo build` | the linked binary |
| `...-client-ruby` | `ruby` | `ruby -c` | the interpreter |
| `...-client-dotnet` | `dotnet` | `dotnet build` | `dotnet <assembly>` |
| `...-client-cpp` | `c++` | the compiler invocation | the linked binary |
| `...-client-swift` | `swift` | `swift build` | the built binary |

Each module's own pom declares its build and run commands and explains its own choices; nothing
restates them, here or in the shared wrapper. Kotlin and Scala assert the fixture in their own test
frameworks rather than through the wrapper, because they have no toolchain to detect - but the
string has to agree across every module that still prints it, so `bin/foreign-client-step.sh`
derives it once and a change there has to change all of them.

**Java no longer prints the fixture line**, and that is not a gap: a module that compiles a client
surface, two transports and their suites is a stronger statement about the JDK than a class that
prints a string, and the fixture was deleted rather than relocated when the real client landed.

## If you are adding a language

Add its module directory, its pom (copy the nearest of the eight), a `<module>` line in this
directory's pom, one line in the root pom's notice, and a matrix row in
`.github/workflows/clients.yml`. Everything else is inherited.
