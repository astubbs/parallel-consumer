# Client: TypeScript (astubbs#242)

Per-language working note for the TypeScript client of the language-proxy plan
(`docs/plans/2026-08-14-001-feat-language-proxy-plan.md`). Effort figures, divergence notes, and
anything the TypeScript wave learns that a later session needs go HERE - never appended to
`docs/inflight/branch-language-proxy.md` - one file per language, so concurrent waves never edit a
shared note.

**Status: wave one landed.** Connect, `Configure`, one `Dispatch` wave, the user's function, the
report with the token echoed verbatim, and a clean client-initiated shutdown, proven by one
end-to-end test against the real test-mode sidecar. The module is at
`parallel-consumer-proxy-clients/parallel-consumer-proxy-client-typescript/`; its maturity and
testing-evidence deferrals are lifted, so its CI row now runs. Later waves: leases and heartbeats,
the manifest reconnect, worker death, terminal outcomes, the shutdown drain, the demo and its
container, npm publishing, and the rest of the conformance suite.

## Decisions this wave took that a sync should confirm or overturn

- **Concurrency is PROMISE CONCURRENCY on the one event loop, not `worker_threads`.** This is the
  question the plan singles out for Node, so the argument matters more than the answer.
  `Configured.executor_count` becomes that many concurrent async invocations, each an executor loop
  taking FIFO from the dispatch queue.
  - *Why not worker threads:* a `worker_thread` is a fresh isolate with no shared heap, so a
    function can only reach it as a module path or a source string. That would make TypeScript the
    one client in the fan-out whose processor cannot be a closure, contradicting the reference
    surface's "a closure/lambda/callable - never an importable name". Python pays for closures with
    `fork`; Node has no equivalent, so the price would be the surface itself.
  - *Why it is the right default anyway:* Node's concurrency is for overlapping I/O, which is what
    the overwhelming majority of Node processors do. `executor_count` in-flight `await`s give real
    concurrency there.
  - *The honest limit, which is real and is written into the README rather than buried:* a processor
    doing CPU work **synchronously** blocks the event loop and therefore the transport - reports
    stop flowing, and once heartbeats exist they stop too, which expires leases and redelivers
    records the worker still holds. Node's own answer is for the application to offload CPU work and
    `await` it, which composes with this model. A worker-thread executor can be added later as an
    **option** without touching the wire, because `executor_count` is all the protocol knows.
  - Nothing tests the blocking case yet. It is recorded as a limitation on the module's
    testing-evidence fragment, not as a passing property.
- **`poll(processor)` does NOT block** - see the defect list below; the specification does not say,
  and this is the answer TypeScript chose. It starts consumption and returns; `done()` is a promise
  that settles when the session ends, and `close()` shuts down. One mechanism for the session's end,
  not two: there is deliberately no event emitter beside the promise, because a library offering
  both leaves every user guessing which one is authoritative. The client is also an
  `AsyncDisposable`, so `await using` works.
- **Three spellings of an outcome, one meaning**, resolved in one place (`applyProcessor`):
  returning nothing is a bare success, throwing or rejecting is a failure carrying the error's
  message, and returning an `Outcome` is for a success with records to produce or a failure with a
  reason but no exception. Go deliberately refused two spellings of failure; TypeScript accepts them
  because exceptions *are* the language's error idiom and a processor that lets one escape has said
  "failure" as clearly as a language can.
- **Only implemented capabilities are declared** - `capabilities: ["dispatch"]`, as a named constant
  listing what is implemented, so the set cannot fall out of step by omission. Same reasoning as the
  Go and Python waves, and the opposite of the Java reference client's empty list.
- **The token is echoed as the received object**, never rebuilt from parsed fields, so "opaque" is
  structural rather than a rule someone has to remember.
- **`Options` is a plain object literal, not a builder**, and an omitted property means "take the
  proxy's default" - which is what an absent field means on the wire, so the two agree with no
  translation table.
- **64-bit values are `bigint`** on the public surface (`record.offset`) and in the generated code
  (`Token.epoch`). A `number` truncates past 2^53, which is exactly what the golden corpus's
  deliberately beyond-int32 epoch exists to catch. The ergonomic cost is real - users compare
  against `0n`, not `0` - and it is the right trade.

## Specification defects, and what this client did about them

The first three were handed to this wave rather than rediscovered (a doc-fix session is repairing
them concurrently); they are recorded here because this module's behaviour depends on how they are
resolved. **Report only - a language wave does not edit the guide.**

1. **A gRPC client cannot fail a stream with `FAILED_PRECONDITION`.** Only a server sets a status.
   On queue overflow this client cancels the call and raises `ProtocolViolationError` naming the
   count the specification wanted named. Independently reached by the Go and Python waves, so it is
   a specification defect and not a language quirk.
2. **On a session without `shutdown`, there is no legal way to return queued records.** The guide's
   §5 has a closing client report every queued record `Released`; the capability rule forbids sending
   an outcome outside the negotiated set, and the harness negotiates only `["dispatch"]`. This client
   sends `Released` only when `shutdown` is negotiated, and otherwise discards the queue and lets the
   proxy reclaim it - and says so through the `onWarning` hook rather than silently.
3. **Whether `poll(processor)` blocks is unstated.** Answered above: it does not. If the reference
   surface means it to block, this client changes - but a blocking `poll` in TypeScript would have to
   be `await`ed, which would stop the same `async` function ever calling `close()`, and nothing in
   JavaScript could interrupt it from elsewhere. **This is the sync's to settle**; the doc-fix
   session asked each language for its answer, and this is TypeScript's.

Found by this wave:

4. **The `.proto` carries a placement option for eight languages and none for TypeScript.**
   `go_package`, `csharp_namespace`, `ruby_package`, `php_namespace`, `objc_class_prefix` and
   `swift_prefix` were added so no client has to supply placement on the command line. There is no
   equivalent file option for TypeScript - every TS generator takes its output layout from
   command-line options instead - so this module's `scripts/generate-proto.mjs` is the only record of
   where TypeScript's generated code lands and what it is called. That is not a schema gap to fix
   (the option does not exist), but the guide's codegen section should say so, or the next TS-shaped
   language (and a reviewer comparing modules) will look for the option and conclude it was forgotten.

## Harness observations

- **`protoc` from a normal install solved the Go wave's second finding for free.** Go had to borrow
  the `protoc` that the protocol module's `protobuf-maven-plugin` downloads and unzip the well-known
  types out of `protobuf-java`, because the Maven artifact is a bare executable with no `include/`.
  A standalone `protoc` (35.1 here) ships its own `include/google/protobuf/*.proto`, so
  `duration.proto` and `timestamp.proto` resolve with no include path at all. Worth writing into the
  guide as the preferred route, with the Maven-artifact procedure as the fallback for machines that
  cannot install one.
- **The mock ignores the subscription** (already recorded by the Go wave, confirmed here): the
  end-to-end test therefore asserts on the record's own content and its delivery count, never on the
  mere fact that a record arrived.
- **The plan defect the Go wave recorded is solved, not worked around.** `-am` cannot order a build
  this module has no dependency on, so the harness dependency lives in a profile gated on
  `-Dpc.foreignClients` (`typescript-e2e-harness` in the module's pom), exactly as the Python wave
  did it, and `maven-dependency-plugin` writes `target/sidecar-classpath.txt` where the test reads
  it. The CI matrix row's command works unchanged.

## Local bug-finding, and the proof it can fail

`npm run check` is the local gate, and it is deliberately two halves that catch different things:

```bash
npm ci && npm run check     # tsc --build (strict) then eslint with type-aware rules
npm run lint                # eslint alone - the CI matrix row's exact command
```

Versions, for the CI row to match: **Node 20.11+** (developed on 24.19, CI pins 22.17.0),
**typescript 5.9.3**, **eslint 10.8.1**, **typescript-eslint 8.67.0** (config
`recommendedTypeChecked`, with `no-floating-promises`, `no-misused-promises` and `require-await`
raised to errors), **ts-proto 2.12.0**. TypeScript is pinned to the 5.x line on purpose:
typescript-eslint 8.x declares `typescript >=4.8.4 <6.1.0`, so the 7.x line would silently lose the
type-aware rules, which are the half that finds bugs.

**It fails when it should**, established three ways rather than asserted:

- A deliberately planted floating promise (dropping the `await` in the client's `asyncDispose`) was
  reported as `no-floating-promises` **and** `require-await`, exit 1; reverting returned it to 0.
- It found two real defects in this wave's own code before any defect was planted: an unnecessary
  type assertion in the sidecar spawn, and a genuinely floating `test(...)` promise in the
  end-to-end test.
- `tsc --strict` caught a wrong assumption about `assert.throws`'s return type in the queue tests.

The end-to-end test's own negative control is recorded on the module's testing-evidence fragment:
throwing from the user function instead of returning produced three deliveries in the same quiet
period with the attempt count incrementing and the reason verbatim, and the test went red.

## Effort

ASM1 wanted a budget recorded **before** the unit started; none was, so R16 cannot be falsified
against one here either - recorded honestly rather than backfilled, which is the same hole the Go
wave reported. Actual wave-one effort, for whatever an uncalibrated point is worth: one agent
session, roughly two hours wall clock, ~1,100 lines of hand-written TypeScript across nine source
files, two test files and a generation script, on top of ~3,900 lines of generated stubs. The
end-to-end test passed on its first run against the real wire.
