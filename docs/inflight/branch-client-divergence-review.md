# Divergence review: the five non-JVM proxy clients (astubbs#242)

A read-only comparison of `parallel-consumer-proxy-client-{go,python,typescript,rust,ruby}` against
each other, on the seven dimensions where five implementations of one protocol can quietly disagree.
The JVM clients are out of scope by the owner's call.

**Duplication is not the finding here - divergence is.** Five languages cannot share code, so the
only thing the fan-out can converge on is design, naming and semantics. Each table below says what
each client does; the verdict list at the end ranks the *accidental* differences (someone chose
differently for no reason) and names the single right answer. Language-forced differences are marked
as such and left alone.

Nothing here is applied. Every finding is a report; the owner decides what lands and where.

## Where all five already agree

The most valuable line in this document, because it is the evidence the fan-out is converging rather
than fragmenting. Verified in all five, by reading the code and not the comments:

1. **The declared capability set is exactly `["dispatch"]`**, as a named constant with a comment
   explaining why an empty list would be worse than a subset - `implementedCapabilities` (Go),
   `CAPABILITIES` (Python `_session.py`, Ruby `options.rb`), `DECLARED_CAPABILITIES` (TypeScript),
   `IMPLEMENTED_CAPABILITIES` (Rust). Five independent authors reached the same answer, and it is the
   one the Java reference got wrong when this review was written - since fixed in `e955e3acd`,
   `DISPATCH_CAPABILITY` in `WireMapping.toConfigure`.
2. **The token is opaque and echoed verbatim.** No client parses `record_id`, compares epochs, or
   rebuilds the token from parsed parts; none holds a request map, dedupe cache or completion
   registry. Stateless per record, five for five.
3. **`Configured` without a usable `max_concurrency` or `executor_count` is a protocol violation**,
   never a default and never "unlimited" - checked before the session opens in all five.
4. **Sidecar lifecycle is identical**: launched directly (never a shell), stdin held open and never
   written as the parent-death signal, stdout *scanned* for `port: <n>` rather than asserting the
   first line, stdout drained for the child's whole life, reaped by closing stdin with a kill only as
   a backstop. Five files carry near-identical prose about why - which is convergence, not copying,
   since they were written independently.
5. **Configuration is code.** No client reads a file, an environment variable or a command line for
   proxy configuration; all five state that sidecar args are not a configuration channel.
6. **Credentials never leave the stream.** Every client suppresses `kafka_properties` in its
   `repr`/`Debug`/`inspect`/error text, each with a comment saying why.
7. **`poll` does not block, and is at most once per client** - in all five, each documenting the
   choice in its README or class docs.
8. **A thrown/panicking processor becomes a failure report, in exactly one place** - `invoke` (Go,
   Ruby), `resolve_outcome` (Python), `applyProcessor` (TypeScript), `invoke`/`Blocking` (Rust).
9. **Null and empty are preserved** for key and value, both directions, with a tombstone test or
   comment in each.
10. **Overflow is a protocol violation, answered by cancelling the call and raising a local error
    naming the count** - all five reached the guide's "a gRPC client cannot set a status" conclusion.
11. **The `InboundRecord` field set is the same in all five**: topic, partition, offset, key, value,
    attempt, last-failure time, last-failure reason - no transport detail on the surface.

## 1. Session death

The sharpest dimension. `client-authoring-guide.md` §1 makes it normative - *"The caller can learn
the session ended, and why, without ending the client to find out"*. It named the Java reference's
silence as the thing not to mirror when this review was written; the reference has since answered it
with `sessionEnd()` (`061324e20`), so §1 now records that answer as the JVM's rather than a warning.

| | Go | Python | TypeScript | Rust | Ruby |
|---|---|---|---|---|---|
| Surface | `Done()` chan + `Err()` | `wait(timeout)` -> bool | `done(): Promise<void>` | `closed().await` | `wait(timeout)` -> bool |
| Fires on a mid-session stream error? | **NO** | yes (`_read`'s `finally: self._finished.set()`) | yes (`call.on("error")` -> `fail`) | yes (`shared.ended.send_replace(true)`) | yes (receiver thread ends) |
| Reports *why*? | no | only via `close()` re-raising `session.failure` | **yes - `done()` rejects with the cause** | only via `shutdown()`'s `Result` | **yes - `wait` calls `raise_any_failure`** |
| Executors on stream death | **parked forever** in `select` on `stopHandout`/`queue` | keep spinning (daemon threads) until `close()` | released - `queue.close()` hands every taker `null` | released - `queue.close()` ends `recv()` | parked on the condition variable until `close()` |

**Go has the Java P0, one for one.** `c.closed` is closed in exactly one place, inside
`closeOnce.Do` in `Close()`; `receive()` on a stream error calls `c.fail(...)` and returns without
touching it. So `Done()` never fires, `Err()` - documented as *"meaningful once Done is closed"* -
never becomes meaningful, and the executors block in
`select { case <-c.stopHandout: ... case rec := <-c.queue: ... }` with neither case ever ready. The
only escape is an application that independently decides to call `Close()` (which does return
`c.failure`) or that passed a context it later cancels. The README's own idiom - *"`Done` is closed
when..."* - is the one that hangs.

This is **not** language-forced: closing `c.closed` and `c.stopHandout` at the top of `receive()`'s
error path is a two-line fix, and every other client already does the equivalent.

Rust and Python are the middle case: the *that* is reliable, the *why* costs you the client
(`shutdown()` consumes `self`; `close()` is the only thing that raises Python's `Session.failure`,
which `ParallelConsumerClient` does not otherwise expose). TypeScript and Ruby deliver both from the
one call.

## 2. Sidecar spawn and port discovery

| | Go | Python | TypeScript | Rust | Ruby |
|---|---|---|---|---|---|
| Where the path lives | `Options.SidecarPath` | `client(sidecar=)` kwarg | `ClientOptions.sidecar.executable` | `ClientOptions.sidecar_path` | `Client.open(sidecar:)` kwarg |
| Absolute enforced? | yes - `filepath.IsAbs` | yes - `is_absolute()` **and** `is_file()` | **no - only a non-empty check** | yes - `is_absolute()` | **guard is inert** |
| Port scan | `bufio.Scanner`, `strings.CutPrefix` | thread scanning `PORT_LINE_PREFIX` | `readline` + `PORT_LINE` regex | `BufReader.lines()`, `parse_port_line` | thread + `PORT_LINE` regex |
| Startup budget | **none by default** (caller's `ctx`) | `timeout=60.0` | `startupTimeoutMs`, 30s | `connect_timeout`, 30s | `connect_timeout`, 30s |
| Sidecar stderr default | **discarded** (`cmd.Stderr` nil) | **captured**, replayed in the startup error | inherited | inherited | **discarded** (`stderr: :close`) |
| No port line | error naming the prefix | `SidecarError` + last 40 lines of output | `SidecarError` | `ClientError::Sidecar` | `SidecarError`, worded as a timeout even on EOF |

Ruby's absolute-path guard cannot fail: `SidecarCommand#initialize` runs
`super(File.expand_path(path.to_s), ...)` *before* `raise ... unless self.path.start_with?("/")`, and
`File.expand_path("proxy")` returns `"/<cwd>/proxy"`. Verified: `ruby -e 'p
File.expand_path("proxy").start_with?("/")'` prints `true`. So a relative path is silently resolved
against the working directory - exactly what guide §2 forbids - and the check that was written to
catch it reports success. TypeScript does not check at all (`resolveOptions` only rejects an empty
string), so `spawn` will happily take `"proxy"` and let Node resolve it.

Python's stderr handling is the best of the five and the difference is not cosmetic: it is the only
client where *"the sidecar never printed a port line"* arrives with the sidecar's own explanation
attached. Rust's `SidecarStderr::Inherit` doc states the principle - *"silencing a child process's
diagnostics by default is how a misconfigured broker becomes an unexplained hang"* - and Go and Ruby
do precisely that.

## 3. The dispatch queue (KTD39)

Rule by rule, `implemented` means the code enforces it, not that a comment asserts it.

| Rule | Go | Python | TypeScript | Rust | Ruby |
|---|---|---|---|---|---|
| 1. Transport never blocks | yes - buffered chan, `default:` on full | yes - reader only appends | yes - `offer` is synchronous | yes - `try_send` | yes - `offer` raises, never blocks (and says why `SizedQueue` is wrong) |
| ...but reads from when? | **`Poll()`** - nothing reads between `Open` and `Poll` | `session.start()` inside `poll()` | handshake (constructor attaches `on("data")`) | **handshake** - `start()` spawns `transport` with a comment saying why | handshake (`@receiver` in `connect`) |
| 2. Depth = `max_concurrency` | chan cap = ceiling | counter vs ceiling | counter vs ceiling | chan cap = ceiling | array size vs ceiling |
| ...counting what? | **queued only** | **unresolved (queued + executing)** | **unresolved** | **queued only** | **queued only** |
| 3. FIFO | chan order | `queue.Queue` | array + taker list | MPMC channel | array + condvar |
| Overflow -> violation + cancel | yes | yes (`_violated` cancels) | yes (`fail` cancels) | yes (drop stream = cancel) | yes (`@operation.cancel`) |
| Tests for any of it | none | none | **6 unit tests** incl. the overflow negative control | 2 unit tests | none |

**The counting basis is the divergence that hides a bug.** The guide's own worked example is
explicit: *"A fourth record arriving while A, B, C are all unresolved would overflow the queue"* -
the ceiling bounds *unresolved* records, queued plus executing. Go, Rust and Ruby bound only the
queued ones, so records leaving the queue for an executor make room. Replay the guide's example
against them: A, B, C queued at a ceiling of 3; two executors take A and B; D arrives; the channel or
array has space, `try_send` succeeds, no violation is raised. Three of five clients cannot detect the
condition their comments describe at length. TypeScript and Python are right, and TypeScript's
`DispatchQueue.inFlight` comment already names the reason (*"the count is of UNRESOLVED records"*).

Go is also the only client that starts reading the stream at `poll` rather than at the handshake,
with a user-visible gap: `Open()` returns to the application, which may do anything before `Poll()`,
and during that window nothing drains the stream - the head-of-line block rule 1 exists to prevent.
Rust's `transport` comment names the hazard exactly (*"the proxy may dispatch the moment it is
configured"*). Python's equivalent window is internal to `poll()` and not reachable by a caller.

## 4. Capabilities

| | Go | Python | TypeScript | Rust | Ruby |
|---|---|---|---|---|---|
| Declares exactly `["dispatch"]` | yes | yes | yes | yes | yes |
| Overridable by the caller | yes (`Options.Capabilities`) | no | no | yes (`capabilities: Option<Vec>`) | no |
| Has a `negotiated(token)` accessor | `Session.Negotiated` | no (a `frozenset` field) | no (a `ReadonlySet` field) | `Session::negotiated` | `Session#negotiated?` |
| Uses it in wave-one code | no | yes (`"shutdown" in self.capabilities`) | yes (in the close warning text) | no | yes (`negotiated?("shutdown")`) |
| Checks `dispatch` came *back* | no (a test does) | no | no | no (a test does) | no |
| Un-negotiated message arrives | records it as the session's **fatal failure**, keeps running | **cancels the stream** | `onWarning`, keeps running | records it as the session's **fatal failure**, keeps running | records it as the session's **fatal failure**, keeps running |
| `SetExecutorCount` | folded into "ignored" | fatal | fatal | folded into "ignored" | folded into "ignored" |
| Acts on `Shutdown`? | no | **yes - `_on_shutdown()` drains, with no capability check** | no (refuses explicitly) | no | no |

Two things here are wrong rather than merely different.

The specification says a receiver *"ignores it or fails the stream"* - two legal answers. Go, Rust
and Ruby invented a third: keep consuming, but write the "ignored" note into the slot that
`Close()`/`shutdown()`/`wait` reports as the session's fatal error. Ruby's is the sharpest, because
`wait` calls `raise_any_failure` unconditionally after the join - so `client.wait(5)` raises a
`ProtocolViolation` on a session that is healthy and still consuming. Each of the three comments its
own line as *"recording it keeps the violation visible without failing an otherwise healthy stream"*,
which is what it does not do.

Python is the only client that **acts** on an un-negotiated message: `_read` dispatches
`kind == "shutdown"` straight to `_on_shutdown()`, which drains the queue and half-closes. No
capability test guards it. Harmless while the proxy never sends `Shutdown` to a `["dispatch"]`
session; a direct breach of the negotiation rule the moment it does.

None of the five verifies that `dispatch` survived the handshake before running executors. Go and
Rust assert it in a test; nothing asserts it at runtime.

## 5. Outcome and failure translation

| | Go | Python | TypeScript | Rust | Ruby |
|---|---|---|---|---|---|
| Shape | `(Outcome, error)` | `Outcome \| None` or raise | `void \| Outcome \| Promise<...>` or throw | `Result<Outcome, ProcessingError>` | any value, or raise |
| Success constructors | `Succeed()`, `Produce(...)` | `Outcome.success(produce=)` | `success(produce?)` | `Outcome::success()`, `Outcome::produce(...)` | `Outcome.success(produce:)` |
| Explicit failure | *none - return an `error`* | `Outcome.failure(reason)` | `failure(reason?)` | *none - return `Err`* | `Outcome.failure(reason)` |
| Returning nothing | n/a (zero `Outcome` + nil error) | success | success | n/a | success |
| Returning a **non-Outcome** | n/a (typed) | **failure** | **failure** | n/a (typed) | **success** |
| Raise/throw/panic | recovered -> failure, `err.Error()` | caught -> failure, `str(e) or type name` | caught -> failure, `e.message or e.name` | `JoinError` -> failure | rescued -> failure, **`"#{e.class}: #{e.message}"`** |
| `KeyboardInterrupt`-class escape | n/a | re-raised deliberately | none | n/a | `StandardError` only, so `Interrupt` escapes |

The absent `failure()` constructor in Go and Rust is **language-forced and right** - both say so at
length, and both are correct that a second spelling of failure would be a thing to keep in step with
the error path forever.

The non-Outcome return is a genuine three-way disagreement on identical semantics. Ruby's
`Outcome.coerce` treating anything else as success is language-forced (a Ruby block always returns
its last expression, so a non-Outcome return carries no intent). TypeScript's failure is belt and
braces behind a compile-time `void | Outcome`. Python's is the exposed one: nothing type-checks a
user's function at runtime, so a processor that accidentally returns `True` produces a failure
report, an incremented attempt and a redelivery that will do it again - an infinite retry from a
stray return. Python's own test names the behaviour
(`test_returning_something_that_is_not_an_outcome_fails_the_record_not_the_client`), so it is a
decision, not an accident - but it is a decision only one of the three untyped clients took.

Ruby is also the only client that prefixes the exception class onto the reason. That text is
user-visible: it lands in the next delivery's `last_failure_reason`, so the same failing code reads
differently depending on which library you are running.

## 6. Close and shutdown

| | Go | Python | TypeScript | Rust | Ruby |
|---|---|---|---|---|---|
| Name | `Close()` | `close()` | `close()` | `shutdown(self)` | `close` |
| Idempotent | `sync.Once` | `_closed` under a lock | memoised promise | consumes `self` | `@close_mutex` |
| Executing records | finish and report | finish and report (drain deadline) | finish and report | finish and report | finish and report |
| Queued records | **dropped silently - `Released` never mentioned** | `Released` **when negotiated**, else dropped | dropped + `onWarning`; `Released` documented, not built | dropped; `Released` documented, not built | `Released` **when negotiated**, else dropped with a debug log |
| Can it still run a queued record? | **yes - unbiased `select`** | no | no | no - `biased;` + a pre-check | no |
| Half-close | `CloseSend()` | `_HALF_CLOSE` sentinel ends the request iterator | `call.end()` | closing the outbound channel | closing `@outbound` |
| Reap order | half-close, wait, cancel, close conn, stop sidecar | session close, pool close, sidecar close | session close, then sidecar in a `finally` | half-close, await transport, stop sidecar | half-close, join receiver, close channel, stop sidecar |

All five honour the specification's rule that `Released` is sent **only** when `shutdown` is
negotiated - trivially, since none negotiates it. Only Python and Ruby wrote the gate, so only those
two will do the right thing on the day the lease unit grants `shutdown`; Go, TypeScript and Rust will
silently keep discarding. Go is the weakest: `shutdown()` does not mention queued records at all - no
gate, no comment, no diagnostic - whereas Rust and TypeScript at least record the reasoning where the
next author will find it.

Go's executor loop is a plain `select` over `<-c.stopHandout` and `<-c.queue`. Go chooses uniformly
at random when both are ready, so after `Close()` closes `stopHandout` an executor with a non-empty
queue has an even chance per iteration of running one more record it was told to stop handing out.
Rust hit the same shape and guarded it (`biased;` plus `if *stop_handout.borrow() { break; }`); the
other three take the queue's own stop flag, which cannot race.

## 7. Naming of the user-facing surface

| | Go | Python | TypeScript | Rust | Ruby |
|---|---|---|---|---|---|
| Client type | `Client` | `ParallelConsumerClient` | `ParallelConsumerClient` | `ParallelConsumerClient` | `Client` |
| Open | `Open(ctx, opts)` | constructor + `poll` | `.open(options)` | **`connect(options)`** | `.open(options, sidecar:)` |
| Options type | **`Options`** | `ClientOptions` | `ClientOptions` | `ClientOptions` | `ClientOptions` |
| Sidecar lives | in options | **beside** options | in options (`sidecar: {...}`) | in options | **beside** options |
| End of session | `Done()` + `Err()` | `wait()` | `done()` | `closed()` | `wait()` |
| Stop | `Close()` | `close()` | `close()` | **`shutdown()`** | `close` |
| Failure predicate | `HasFailedBefore()` | *(none)* | *(none)* | `has_failed_before()` | **`failed_before?`** |
| Durations | `time.Duration` | `timedelta` | **`commitIntervalMs: number`** | `Duration` | **bare seconds `Numeric`** |
| Error surface | **untyped `fmt.Errorf` + one sentinel** | 3 types under a base | 5 types under a base | 6-variant enum | 6 types under a base |
| `Configure` coverage | + `drain_timeout`, `terminal_topic` | thinnest: no instance tag, drain, terminal, commit mode | **only one with `commitMode`** | + `drain_timeout`, `terminal_topic` | no drain, terminal, commit mode |

Rust's `connect`/`shutdown` pair is the outlier on both halves, and `shutdown` is the worse of the
two: the protocol already has a `shutdown` capability and a `Shutdown` message, so
`client.shutdown()` on a session that has not negotiated `shutdown` is a sentence that needs a
footnote. `close` was available and consumes `self` just as well.

Go's `Options` (not `ClientOptions`) and untyped errors are both single-client divergences.
`ProtocolViolation` is a name in three clients, a `ClientError::Protocol` variant in Rust, and
nothing at all in Go - a Go application cannot distinguish a protocol violation from a transport
failure without matching on message text. Guide §3.2 requires each client to *"name that error in
your README"*; only Python's README does.

Ruby's bare-seconds durations against TypeScript's `Ms`-suffixed numbers is the one naming
difference that can silently misconfigure a cluster: `commit_interval: 5` is five seconds, and
`commitIntervalMs: 5` is five milliseconds, and neither name warns you which convention you are in.

## Verdict list

Ranked by how much each would confuse a user or hide a bug. Every one is accidental unless the row
says otherwise.

1. **Go cannot report session death, and is now the only client where that is still true** -
   `Done()` never fires and `Err()` never becomes meaningful on a mid-session stream error, and the
   executors park. The Java reference had the identical defect when this review was written; it was
   fixed in `061324e20`, which ends the session in one place on the stream's error path - hand-out
   stops, the executor pool is shut down without interrupting anything, and the caller's
   `sessionEnd()` stage completes with the cause. So the shape to copy now exists in the reference
   as well as in the other four clients. **Right answer: Go closes `stopHandout` and `closed` on
   `receive()`'s error path**, a two-line fix. Highest priority because it is silent, it is the
   documented idiom, and the guide makes it normative.
2. **The in-flight ceiling counts the wrong thing in Go, Rust and Ruby** - queued only, where the
   guide's worked example bounds queued plus executing. The overflow detector every client writes
   forty lines of comment about cannot fire in the case the guide gives. **Right answer:
   TypeScript's - count unresolved records, decrement on report.** A proxy exceeding its own ceiling
   goes undetected in three of five clients, which is a bug-hiding divergence, not a stylistic one.
3. **"Ignore an un-negotiated message" means five things.** Right answer: **TypeScript's** - warn on
   the client's own diagnostic channel and keep going. Go, Rust and Ruby must stop writing an ignored
   message into the fatal-failure slot (Ruby's `wait` raises on a healthy session because of it), and
   Python must stop acting on `Shutdown` without a capability check.
4. **The absolute-sidecar-path rule is enforced in three of five.** TypeScript does not check;
   Ruby's check cannot fail because `File.expand_path` runs first. **Right answer: Python's** -
   reject a non-absolute path *and* confirm the file exists, before anything is spawned. This is the
   guide's security rule, and a guard that always passes is worse than none because it reads as
   covered.
5. **Go can run a record after being told to stop handing out**, from an unbiased `select`. Right
   answer: **Rust's** - bias the select and re-check the flag, or move the stop into the queue as the
   other three did.
6. **The `Released` gate exists in Python and Ruby only.** All five behave correctly today; three
   will silently regress the day `shutdown` is negotiated. Right answer: **Python's `_release`** -
   write the capability test now, even though the branch is dead code, because the wave that grants
   the capability will not go looking in three languages.
7. **The session-end surface has five names and three shapes** - `Done()`/`Err()`, `done()`,
   `closed()`, `wait()`, `wait()`. §1 has since settled that the *shape* is each language's own, so
   this is *expected* divergence rather than accidental - but the *why* half is not: TypeScript and Ruby
   deliver the cause from the same call; Python and Rust make you end the client to learn it. Right
   answer: the end-of-session call carries the reason.
8. **Sidecar stderr defaults split three ways** - discarded (Go, Ruby), inherited (TypeScript, Rust),
   captured and replayed into the startup error (Python). Right answer: **Python's**, with inherit as
   an acceptable second. A discarded stderr turns "the sidecar printed no port line" into an
   afternoon.
9. **Go has no default startup budget.** Every other client bounds the port-line wait (30s, 30s, 30s,
   60s); Go inherits the caller's context, so `context.Background()` waits forever on a sidecar that
   hangs without closing stdout. Right answer: a default, as everywhere else.
10. **Go has no error taxonomy.** Four clients let an application catch a protocol violation as a
    type; Go offers one sentinel (`ErrAlreadyPolling`) and `fmt.Errorf` strings. Right answer: typed
    or sentinel errors for at least protocol-violation, sidecar and transport - and the guide's
    "name it in your README" honoured by more than Python.
11. **A non-Outcome return is a failure in Python and TypeScript, a success in Ruby.** Ruby's is
    language-forced. Python's turns a stray return value into an infinite redelivery loop with no
    type checker to catch it. Right answer: settle it explicitly one way in the guide; Ruby's cannot
    move, so the question is only whether Python should join TypeScript or stand alone.
12. **`Configure` coverage is uneven**: `commit_mode` only in TypeScript; `drain_timeout` and
    `terminal_topic` only in Go and Rust; Python missing four fields including the instance tag.
    Right answer: one list, in the guide, that every client fills - a user cannot port an application
    between two of these libraries today without discovering which knobs their new language forgot.
13. **Naming leftovers**, each cheap to settle and pure cost to a user reading two libraries:
    `Options` vs `ClientOptions` (Go alone); `connect` vs `open` and `shutdown` vs `close` (Rust
    alone, and `shutdown` collides with the protocol's own token); `Succeed()` vs `success()`;
    `failed_before?` vs `has_failed_before`; the sidecar inside options (Go, Rust, TypeScript) versus
    beside them (Python, Ruby); Ruby's bare-seconds durations against TypeScript's `Ms` suffix.
14. **Only Go's stream reading starts at `poll` rather than the handshake**, leaving an
    application-controlled window in which nothing drains the stream. Right answer: **Rust's** - the
    transport task starts at connect, with the comment saying why.
15. **Python's `OutboundRecord` requires a topic** (`__post_init__` raises on an empty one), where the
    wire's `ProduceRecord.topic` is optional and means "the proxy's configured default". Python is
    the only client that removes that option from its users.
16. **The failure reason's text format**: Ruby alone prefixes the exception class. It reaches the next
    delivery's `last_failure_reason`, so identical user code produces different redelivery text
    depending on the library.
17. **Only TypeScript tests the queue rules.** Six unit tests including the overflow negative control;
    Rust has two; Go, Python and Ruby have none, and each has exactly one end-to-end test. The guide
    names a conformance scenario for this section
    (`the-client-queue-hands-out-fifo-and-releases-on-shutdown`) that no client runs. This is why
    finding 2 could sit in three clients undetected.

## What could NOT be determined by reading

So nobody mistakes an unread path for an agreeing one.

- **Nothing was built or run.** No client's test suite was executed and no sidecar was spawned. Every
  behavioural claim above is from reading source, including the two most consequential (findings 1
  and 2).
- **Finding 2 is not demonstrated.** No client has a test that dispatches a record while the ceiling
  is fully unresolved, so the divergence is derived from the counting code and the guide's worked
  example, not observed. TypeScript's `overflow past max_concurrency is a protocol violation` test
  proves only its own client.
- **Finding 5's frequency is unknown.** Go's `select` race is a real nondeterminism; whether it fires
  often enough to matter needs a stress run, not a reading.
- **Underlying gRPC flow control was not examined.** Whether `grpc-go`, `grpc-js`, `tonic`, and the
  Python and Ruby gems buffer enough to mask the "not reading the stream yet" windows (finding 14) is
  a library question I did not settle.
- **No generated protobuf code was read** in any of the five. Field-presence semantics
  (`HasField`/`has_x?`/`Option`/`undefined`) are taken from the specification and from each client's
  own comments; a generator that renders presence differently from what a client assumes would not
  show up here.
- **Neither demo, nor packaging, nor CI wiring was compared** - the seven dimensions are the library
  surface only. The `pom.xml`, `Makefile`, `Rakefile`, `package.json` and `Cargo.toml` per-client
  build shims were not reviewed.
- **The JVM clients were not read**, so where a non-JVM client mirrors or diverges from the Java
  reference I relied on the then-parked review findings' description of it rather than the Java
  source. Three other agents were live in this worktree, and the Java client was under active edit.
  Two of those descriptions have since been overtaken by fixes to the reference (`e955e3acd`,
  `061324e20`) and are corrected in place above; the rest of the comparison is as it was read.
- **`docs/inflight/clients/<lang>.md` was searched for prior claims** on each divergence above:
  nothing in those five notes records the sidecar-path guards, the stderr defaults, the ceiling's
  counting basis, or the un-negotiated-message handling as a deliberate choice. Absence of a note is
  not proof it was accidental, but no note argues the other way.
