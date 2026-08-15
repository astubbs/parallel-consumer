# Client logging contract: the audit, and what each client owes

The contract is written and lives in the authoring guide's **§10** (`parallel-consumer-proxy/docs/
client-authoring-guide.md`). **That is the owner; nothing here restates a rule.** This file holds the
2026-08-15 audit of the seven existing clients against it, and the per-client task list the
implementation wave works from. Delete it when the last client is done (astubbs#242,
confluentinc#154).

Audited by reading source only - no client was run - so anything marked *not verified by execution*
is a code reading, not a measurement.

## Finding 1: draining the sidecar's pipes (§10.1) - nobody is currently at risk of the stall

Every one of the seven reads the child's **stdout** for the process's whole life, not just until the
`port:` line, and five say in a comment that they do it to avoid the full-pipe block. This is the
one part of the contract the fan-out got right independently, which is worth recording: the guide's
existing "scan for the port line rather than reading one" rule seems to have carried the drain with
it.

| Client | stdout | stderr | Verdict |
|---|---|---|---|
| Go | drained for the child's life (`sidecar.go`, the `keep draining so the child never blocks` branch) | `cmd.Stderr` left nil unless `Options.SidecarStderr` is set → Go connects it to the null device, **not** a pipe | no block; diagnostics discarded by default |
| Python | dedicated `pc-sidecar-stdout` thread (`_drain_stdout`) | dedicated `pc-sidecar-stderr` thread into a 40-line ring | no block; **the only client that drains a stderr *pipe*** |
| TypeScript | `readline` interface kept consuming after the port line (`settle` calls `lines.resume()`) | `stdio` third slot is `"inherit"` or `"ignore"` - never a pipe | no block |
| Rust | `tokio::spawn`ed line loop, `continue // keep draining` | `Stdio::inherit()` (default) or `Stdio::null()` | no block; the default is the right one |
| Ruby | `@drain` thread over `@stdout.each_line` | `Process.spawn(err: :close)` **by default** | no block, but see finding 3 |
| .NET | `PumpStdoutAsync`, loops past the port line | `PumpStderrAsync`, drained and written to `ClientOptions.SidecarErrorLog` when set | no block |
| Kotlin | `drainToChannel` daemon thread | `drainToTail` daemon thread into a 30-line ring | no block |
| Java | n/a - **the Java client does not spawn a sidecar**; only `GrpcParallelConsumerClient` connecting to a port. The JVM spawn lives in the Kotlin client's `Sidecar.kt` | n/a | n/a - see finding 5 |

**Nothing owes a fix for the blocking defect itself.** What is owed is the *default* on stderr, below.

## Finding 2: credential hygiene (§6, §10.4) - one gap, and it is latent rather than live

Checked every client's options type, record types, error messages and log call sites for anything
that renders a whole `Configure`, an options object, or a property map.

**No client logs a `Configure` message anywhere in library code.** Go (`client.go`, `NOTE the error
text: never the configuration itself`) and Rust (`client.rs`, `NOTE what is NOT in the error text
below`) both carry an explicit comment guarding it. Six of the seven options types carry a
hand-written renderer that omits the property map and prints its size:

- Rust - `impl fmt::Debug for ClientOptions`, `<redacted: {} entries>`; the derive is deliberately
  not used. The strongest of the seven.
- Ruby - `ClientOptions#inspect`, `kafka_properties=(#{kafka_properties.size} entries, redacted)`,
  with `alias to_s inspect` so interpolation is covered too.
- Python - `ClientOptions.__repr__`, hand-written over the `@dataclass`.
- Java - `ClientOptions.toString()`, `deliberately omits kafkaProperties`.
- Kotlin - `ClientOptions.toString()`, same.
- TypeScript - plain object; `errors.ts` states that nothing in it embeds `kafkaProperties`, and
  nothing does.

**The gap: .NET.** `ClientOptions` is a `public sealed record` (`ClientOptions.cs`) with a
`KafkaProperties` dictionary and **no `ToString`/`PrintMembers` override** - it is the only one of
the seven relying on a compiler-generated renderer for a credential-bearing type.

Stated precisely, because overstating it would be worse than not finding it: **this does not leak
credential values today.** A C# record's generated `ToString` prints `Name = value.ToString()`, and
`Dictionary<string,string>.ToString()` returns the type name, so `KafkaProperties` currently renders
as a type name. It is a **latent** defect with three ways to become live, none of which would go
red: swapping in a collection type whose `ToString` enumerates, converting to a positional record,
or anyone reaching for `JsonSerializer.Serialize(options)` in a diagnostic. The XML doc on the
property already states the rule in prose ("THIS CARRIES CREDENTIALS... never logs the map") - the
type just does not enforce it. Cheap fix, and §10.4 makes it a construction rule rather than a
call-site rule.

**Payload (§10.5): clean everywhere.** Every record type that could render bytes has a hand-written
renderer omitting them - Java `InboundRecord.toString()` (`payloads are untrusted input and do not
belong in log lines`), Python `InboundRecord.__repr__`/`OutboundRecord.__repr__`, Kotlin
`Records.kt` (`InboundRecord($topic-$partition@$offset, attempt $attempt)`), Ruby `record.rb#to_s`.
Kotlin's two `log.debug(...record...)` calls in `ParallelConsumerClient.kt` interpolate the **Java**
`InboundRecord`, whose `toString` is the safe one - checked, not assumed. Kotlin's
`PreviousFailure.toString()` renders the failure reason as `<N chars>` rather than the text, which
is the §10.5 shape the other clients should copy.

## Finding 3: Ruby closes the child's stderr descriptor

`sidecar.rb` spawns with `err: stderr` where the keyword defaults to `:close`, so by default the
sidecar runs **with file descriptor 2 closed**. Not a blocking risk, and not a data risk, but it is
the one option §10.1 rules out: the child writes to a closed descriptor, and fd 2 is then free to be
handed to the next file the JVM opens. `:close` should become the null device or an inherit.

## Finding 4: Python is the only client that logs unasked

Python is the client that got the drains right and the credential renderer right, and it is the one
that violates §10.2. It uses stdlib `logging` correctly per module (`getLogger(__name__)` in
`sidecar.py`, `_session.py`, `client.py`, `_pool.py`) but **installs no `logging.NullHandler()` on
the `parallel_consumer` top-level logger** - `src/parallel_consumer/__init__.py` contains no
`getLogger` call at all. Python's `lastResort` handler therefore prints WARNING and above to
`sys.stderr`, unformatted, in any application that has not configured logging. Three call sites
reach it today: `sidecar.py`'s `log.warning("sidecar did not exit within...")`, and `_session.py`'s
`log.warning("drain timed out...")` and `log.error("protocol violation: %s", ...)`.

## Finding 5: five of the seven have no logging at all

Only the JVM pair and Python log. **Go, TypeScript, Rust, Ruby and .NET emit nothing, through no
mechanism** - the sweep for logging calls across their library sources returns nothing but the Rust
`build.rs` cargo directives and doc-comment examples. That is not a violation of anything (silence
is the safe end to be wrong on), but it means five clients have no session-lifecycle diagnostics,
and the §10.3 floor - a session death must not be silent - is unmet in all five.

Java and Kotlin both log through SLF4J: Java via Lombok `@Slf4j` on `GrpcParallelConsumerClient` and
`DirectParallelConsumerClient`, Kotlin via `LoggerFactory.getLogger(...)` in `Sidecar.kt` and
`ParallelConsumerClient.kt`. Neither module declares an `slf4j-api` dependency of its own - it is
inherited from the reactor's parent pom, which is fine for a module in this build but is the kind of
thing that breaks when a client is published standalone.

## What each client owes

Ordered so the implementation wave can work down the column. None of it is a fix to a live stall.

| Client | Owes |
|---|---|
| **.NET** | (1) `PrintMembers`/`ToString` override on `ClientOptions` redacting `KafkaProperties` to a count - the one real hygiene gap. (2) `Microsoft.Extensions.Logging.Abstractions`, `ILoggerFactory` on `ClientOptions` defaulting to `NullLoggerFactory.Instance`, `[LoggerMessage]` call sites. (3) `SidecarErrorLog` defaults to nothing - make the default reach the application. |
| **Ruby** | (1) `err: :close` → null device or inherit. (2) `logger:` option, duck-typed, defaulting to `Logger.new(IO::NULL)`. (3) a gemspec, which does not exist yet, declaring `logger` - it stops being a default gem in Ruby 3.5. |
| **Python** | (1) `logging.getLogger("parallel_consumer").addHandler(logging.NullHandler())` in `__init__.py` - the whole fix. (2) promote the retained stderr tail into the error raised when the sidecar dies *mid-session*, not only at startup: `_startup_lines` keeps filling but is only read by `_await_port_line`. |
| **Go** | (1) `*slog.Logger` on `Options`, default `slog.New(slog.DiscardHandler)` - stdlib, `go.mod` is already `go 1.25.0` so `DiscardHandler` is available. (2) `SidecarStderr` nil currently means the null device; make the default reach the application. (3) a retained stderr tail for the death diagnostic - Go keeps none today. |
| **TypeScript** | (1) injectable `Logger` interface on the options, absent by default - **no dependency**, the ecosystem has no facade and inventing one for a thin client would be the wrong call. (2) a retained stderr tail: with `"inherit"` the lines reach the terminal but the client cannot put them in a thrown error. |
| **Rust** | (1) `log` 0.4 (not `tracing`) - no required transitive deps, and the crate's own guidance is that libraries link only against it. (2) a retained stderr tail, as TypeScript. Everything else is already the reference implementation. |
| **Kotlin** | Closest to done. (1) keep `slf4j-api` and do **not** add `kotlin-logging`. (2) declare `slf4j-api` explicitly if the client is ever published standalone. (3) `log.info("Connected: {}", session)` - confirm `Session.toString()` cannot grow a property map as the type gains fields. |
| **Java** | (1) as Kotlin for the dependency. (2) the §10.3 floor is the parked P0: a stream error parks every executor with no listener, so until the surface can report the death, ERROR-log it with its cause. §1 already names this defect and tells other clients not to mirror it. |

## Two things this audit did not settle

- **Whether to forward the sidecar's stdout into the application's log.** §10.1 makes draining
  mandatory and forwarding optional, defaulting to not. Python states the reasoning in
  `_drain_stdout` (`re-emitting it here would put text this library did not compose into the
  application's logs`) and it is the best argument in the tree, but no wave has had a user ask for
  the opposite yet.
- **Nothing tests any of this.** There is no conformance scenario for the drain, and there could be
  one - a sidecar told to emit more than a pipe buffer before its port line would fail every client
  that reads only one line. Worth raising with U31's harness work rather than inventing here; the
  §7 scenario list is where it would land.
