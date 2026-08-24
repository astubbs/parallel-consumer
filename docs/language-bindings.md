# How a language binding crosses the boundary

Every binding this project ships answers the same handful of questions, and until now each answer
lived in whichever document happened to need it first. This is the map: the axes, what was decided
on each, and where the decision actually lives.

**This document owns nothing.** It is an index plus the cross-cutting analysis that has no other
home. Where it names a decision, the owning document is cited and **wins** — if the two disagree,
this one is stale and should be corrected here rather than argued from. Do not restate a decision's
reasoning here; link to it. A second copy of a rationale is a rationale that will drift.

Read this before designing anything that touches more than one binding, and before assuming a
question is open. Several are already closed.

## The five axes

A boundary crossing is not one decision, it is five, and they are much more independent than they
look. Systems that solved this before mixed and matched freely — Beam, PyFlink and PySpark each
answer these differently, and PyFlink answers the data plane two ways at once.

### 1. Control plane — how the host says what it wants

| Option | Who does it | Consequence |
|---|---|---|
| Reflective object proxy | **Py4J** (PySpark, PyFlink) — Python holds proxies and calls arbitrary Java methods | Maximum generality; permanently tied to a JVM at the far end |
| Language-neutral IDL | **Beam** Runner API — a versioned protobuf model of a pipeline | Inspectable and portable across engines; a model both sides must implement and keep in step |
| Explicit typed messages | **Ours** — one protobuf message per operation | Narrow; transport-shaped rather than JVM-shaped |

**Decided: explicit typed messages.** The consequence that matters and is easy to miss: Py4J is
Java-reflection-shaped, so it **can never become a C ABI**. Our messages carry no assumption about
what is at the far end, which is the only reason the FFI path is possible at all. Generality was
traded for portability, and that trade is the foundation of the shared C transport.

Owned by [`plans/2026-08-22-001-feat-shared-c-transport-plan.md`](plans/2026-08-22-001-feat-shared-c-transport-plan.md)
(CT2: the frames are the same frames).

### 2. Function delivery — how the host's code gets reached

| Option | Who does it | Consequence |
|---|---|---|
| Serialize the function | **Beam** — a pickled DoFn travels inside the pipeline proto | Required if the harness may run on another machine; imposes serializability on user code |
| Token and local lookup | **Ours** — an integer crosses, the callable stays put | Only works co-located; **no serializability constraint whatsoever** |

**Decided: tokens.** The gain is larger than it sounds and is worth selling rather than merely
recording: because nothing is serialized, a user function can capture **anything** — an open
database handle, a socket, a loaded ML model. Beam users cannot, and "cannot pickle this object" is
a well-known tax on that model. For the machine-learning audience in
[`../STRATEGY.md`](../STRATEGY.md), a loaded model is precisely the object that will not pickle.

The cost, stated plainly: we cannot distribute the harness away from the engine. Beam can.

### 3. Data plane — how per-record work crosses, and how often

This is **the open one**, and the most consequential.

| Option | Who does it | Consequence |
|---|---|---|
| One crossing per record | **Ours today** | Simplest; per-record outcomes are natural; the crossing cost is paid every record |
| Batched / vectorized | **PySpark** (Arrow-batched UDFs), **Beam** (bundles) | Amortises the crossing over N records; coarsens failure granularity |
| In-process, no IPC | **PyFlink thread mode** ([FLIP-206](https://cwiki.apache.org/confluence/display/FLINK/FLIP-206:+Support+PyFlink+Runtime+Execution+in+Thread+Mode), via PEMJA), **Bytewax** (Rust core + FFI) | Removes the IPC entirely; couples lifetimes and failure domains |

**Both escape routes have been taken by someone larger, and neither of them picked one and
stopped.** Flink kept process mode alongside thread mode, for isolation. That is the strongest hint
available about the shape of the answer: **both, selectable**, rather than a choice.

Nothing is decided here yet. See the unresolved collision below, which is the reason.

### 4. Transport — what the frames travel over

Socket (gRPC) today; a C ABI via GraalVM `--shared` proven in Go, Python and C.

Fully owned by [`plans/2026-08-22-001-feat-shared-c-transport-plan.md`](plans/2026-08-22-001-feat-shared-c-transport-plan.md)
and [`inflight/perf-embedding-the-engine-over-ffi.md`](inflight/perf-embedding-the-engine-over-ffi.md),
including the per-language hazards, the kill criterion, and why Node stays on the sidecar. Do not
re-derive any of it.

### 5. API shape — what the user's function signature looks like

Owned by [`inflight/next-batching-modes-for-clients.md`](inflight/next-batching-modes-for-clients.md).
Core's API is already batch-shaped and the clients modelled the degenerate single-record case as
though it were the API; the fix is to widen the existing surface, not to add a parallel batch API.

Note this is a **different question** from axis 3 even though both say "batching". Axis 3 is how many
records cross per hop. Axis 5 is how many records the user's function is handed per call. They can
differ: a wave already carries several records over the wire while each is processed and reported
individually.

**And there are two batch APIs, not one - they must not inherit each other's answer.** The two
products this project puts across the boundary have different contracts, so "add batching" means
something different in each:

| | Parallel Consumer clients | The Kafka Streams wrapper |
|---|---|---|
| What core already does | **Core's API is batch-shaped already** - `poll` hands the user a context of records, and a batch size of one is the degenerate case | **Kafka Streams is record-at-a-time by contract.** An operator receives one record and returns one value |
| So batching means | Widen the existing client surface to mirror the shape core already chose | **Buffering at the operator**, which changes what the operator means |
| Difficulty | An API-shape correction, already understood | A semantic change, and not obviously legal for a stateful operator |
| Owned by | [`inflight/next-batching-modes-for-clients.md`](inflight/next-batching-modes-for-clients.md) | Nobody yet |

The PC side is the easier of the two and the one with a decision already written down. The Streams
side has no owner and should not be assumed to follow: a stateless `mapValues` can buffer safely,
while anything stateful cannot without redefining the operator. Whoever picks up batching must say
which of the two they mean in the first sentence.

## The unresolved collision, and it is the important part of this document

**Axis 3's cheapest win directly contradicts a guarantee this project has already committed to.**

- Batching the data plane is worth orders of magnitude, not percentage points. One crossing
  amortised over a hundred records turns a few hundred microseconds per record into a few.
- Beam bought exactly that, and paid for it explicitly: **any element failing discards and retries
  the whole bundle**, and per-item outcomes were considered and deliberately rejected as not worth
  the complexity.
- [`inflight/next-batching-modes-for-clients.md`](inflight/next-batching-modes-for-clients.md) says
  the opposite, as a requirement: *"Per-record outcomes must survive... N records in must be able to
  produce N outcomes out."*
- And per-record outcomes with no processing clock are a stated differentiator against Share Groups
  in [`../STRATEGY.md`](../STRATEGY.md).

So the throughput fix that the two nearest comparable systems both adopted would, taken as they took
it, dissolve one of this project's stated advantages. That does not make it wrong — it makes it a
decision to take deliberately and in the open, with the options being roughly:

1. **Batch the transport, keep outcomes per-record.** N records in one hop, N outcomes back. Beam's
   coupling of the two was a simplification, not a necessity. This looks correct and should be
   costed first.
2. **Batch only where semantics permit it** — a stateless mapper can buffer, a stateful operator
   cannot without changing what the operator means.
3. **Do not batch; remove the hop instead** (axis 4, the C ABI). Attacks the per-crossing cost rather
   than the count.

Option 1 and option 3 are not alternatives — they compose, and they attack different terms.

## What has actually been measured

From the Kafka Streams PoC, one stream thread, loopback gRPC, on one developer machine: a round trip
of a few hundred microseconds, of which **the Python function itself was around 0.05%**.

Read carefully, that single number frames axes 3 and 4 entirely: **optimising the foreign function is
pointless, and only the crossing is worth attacking.** It does not, on its own, say whether to attack
the crossing's cost or its frequency, because it was never decomposed into transport versus engine
overhead. **That attribution is unfinished and is the cheapest useful experiment available** — a
control arm with a Java-side identity mapper against the same topology, one term changed.

Do not cite the number as a transport cost. It is a total.

## Prior art, and what each is good for

| System | Worth reading it for |
|---|---|
| **Apache Beam** (Fn API / Runner API) | The closest analogue overall. Bundles, the deliberate rejection of per-item outcomes, and the IDL-versus-handles trade |
| **PyFlink** | The same shape as us. Py4J control plane, and process-versus-thread mode as an explicit, retained choice |
| **PySpark** | Arrow batching as the answer to exactly the cost we measured |
| **Temporal** | Shared core plus thin bindings — and the limit, since its Go and Java SDKs are independent implementations |
| **Bytewax** | In-process FFI with no IPC at all, and a cautionary maintenance record |

Fuller notes, including Ray, Envoy `ext_proc`, Dask and the competitive landscape, live in
[`inflight/next-architecture-landscape-comparison.md`](inflight/next-architecture-landscape-comparison.md).

## Rules for adding a binding

- **A binding is a transport swap, not a fork.** The frames are identical across languages; a new
  binding implements a transport and nothing else. This is CT4 in the transport plan and it is the
  single most important rule here.
- **Conformance decides equivalence** (CT5). A binding is not "done" because its demo runs.
- **Mirror core's shape, not the first client's.** The batching note's general lesson, learned the
  expensive way.
- **Answer all five axes explicitly.** A binding that silently inherits an answer is how the axes
  drift apart between languages.
