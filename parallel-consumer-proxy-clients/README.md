# Proxy clients - build scaffolding

Eleven client modules, one per language the proxy sidecar will eventually speak to
(astubbs/parallel-consumer#242). **Today none of them is a client.** Each one detects its
toolchain, compiles or packages one source file, runs the resulting program, and asserts that it
printed one deterministic fixture line. There is no Kafka, no protobuf, no gRPC and no Parallel
Consumer semantics anywhere in this directory.

That is the point. Wiring eleven toolchains into one Maven reactor is a mechanical question with a
mechanical answer, and it is worth settling on its own before anything interesting depends on it.
The clients themselves are later rungs of the stack being extracted from
astubbs/parallel-consumer#293, which is where all of that work currently lives.

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
| `...-client-java` | the JDK already running Maven | Maven | surefire |
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
restates them, here or in the shared wrapper. The three JVM modules assert the fixture in their own
test frameworks rather than through the wrapper, because they have no toolchain to detect - but the
string has to agree across all eleven, so `bin/foreign-client-step.sh` derives it once and a change
there has to change every module.

## If you are adding a language

Add its module directory, its pom (copy the nearest of the eight), a `<module>` line in this
directory's pom, one line in the root pom's notice, and a matrix row in
`.github/workflows/clients.yml`. Everything else is inherited.
