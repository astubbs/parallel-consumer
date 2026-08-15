# An HTTP dialect of the proxy protocol, and clients generated from its spec

Raised 2026-08-15. Two ideas that turn out to be one: expose the sidecar over plain HTTP as a second
*dialect* of the same semantics, and generate the per-language clients from its specification the way
the gRPC ones are generated from the `.proto`.

## This is the original ask, not a new one

astubbs#242 — the mirror of confluentinc#154, which this whole workstream implements — says
**"REST proxy being the example"**, and describes clients getting records to process over the wire
while Parallel Consumer keeps ownership of concurrency, ordering and committing. We built the gRPC
dialect of a request that named HTTP first. Adding it closes the loop rather than widening the scope.

One divergence to be deliberate about: the issue's phrasing is that clients *request* records, which
is a **pull** model — and the plan rejected pull (the credit-ledger design) in favour of the engine
pushing when it decides. An HTTP dialect must not quietly reintroduce pull. SSE keeps the push.

## Why it earns its place — the arguments, ranked by how well each survives scrutiny

This section was rewritten 2026-08-15 to be **fair rather than persuasive**: the original led with
corporate proxies, which is the weakest of the arguments below. Every argument it made is retained;
what changed is the ordering and the honesty about each one's size.

**Read the whole section before quoting any part of it.** The strongest case and the strongest
objection are both here, and either one alone misrepresents the position.

### 1. The categorical one: browsers cannot speak gRPC

Not "poorly", not "with effort" — **not at all**. `grpc-web` requires a translating proxy in front of
the server, so a browser client is not a gRPC client. If browser-side work dispatch is ever wanted
(one of the architecture shapes in
[`next-http-strategy-ideas.md`](next-http-strategy-ideas.md), §4b), HTTP with server-sent events is
not a fallback — it is the only door.

This argument does not shrink with time, does not depend on anyone's network, and is not a percentage.
It also reframes the dialect: **its real justification may be a different product rather than a
compatibility hedge**, and that is a different decision to weigh.

### 2. The real but shrinking one: networks that cannot carry HTTP/2

**A gRPC client is unusable where the client can only reach the sidecar over HTTP** — corporate
forward proxies that terminate, mangle or downgrade HTTP/2, restricted networks, ingress and load
balancers still on HTTP/1.1, and hosted platforms without inbound HTTP/2 or streaming. That is a
deployment constraint rather than a preference, and no amount of client-side polish works around it.

**Stated fairly, it is weaker than it was.** HTTP/2 is long-established and broadly supported; this
argued much better in 2019 than it does now, and every year it argues slightly worse.

### 3. The long tail, unchanged

Runtimes with poor or absent gRPC support — embedded targets, Lua, Perl, R, older runtimes — and
anyone who wants to try the thing with `curl` before taking a dependency. This is a better answer to
that tail than the C shim considered in
[`parked-a-c-client-and-the-ffi-question.md`](parked-a-c-client-and-the-ffi-question.md), because
there is nothing to compile; **that note is partly superseded by this one.** Note the tension: those
runtimes are also the least likely to be running Kafka work in the first place.

### The objections, stated as strongly as the arguments

- **Today the sidecar is loopback-only, so none of §2 applies at all.** Nothing traverses anything,
  and the reachability case is entirely conditional on the sidecar moving across a network — the
  node-local-daemon or shared-server shapes in
  [`next-work-server-pitch-and-buyer.md`](next-work-server-pitch-and-buyer.md), §3c.
- **Nobody has asked for it.** Under the demand-decides rule this project imposes on itself, that is
  the number that counts, and it currently reads zero.
- **gRPC is measurably better everywhere it can go** (table below), so §2 and §3 buy reach at a real
  cost, where §1 buys something otherwise unreachable.

### What the second dialect actually costs, transport by transport

| | gRPC | HTTP + server-sent events |
|---|---|---|
| Payload | binary protobuf | UTF-8 text, so **binary keys and values need base64 — about 33% overhead**, and binary payloads are the normal case here |
| Reports | a framed message on the existing stream | **one HTTP request each** |
| Flow control | per-stream, from HTTP/2 | none; the in-flight ceiling is already doing application-level backpressure, so this is survivable rather than free |
| Client ergonomics | generated typed stubs, streaming idioms, deadlines, cancellation, status codes, interceptors | **excellent in a browser** (`EventSource` does the work), often poor outside one — many languages have no good client, so line parsing, reconnection and `Last-Event-ID` are hand-rolled |
| Session lifecycle | one connection carries both directions | dispatch and reports are separate, so the two halves can disagree about liveness — see the transport-seam section below |

**The symmetry worth remembering: server-sent events are better exactly where gRPC cannot go, and
worse everywhere else.** That is an argument for building the dialect *for the browser* rather than as
a general-purpose alternative — and it is why the HTTP client would be thinner than the gRPC one only
in a browser.

## The shape: one semantics, two dialects

The protocol's *meaning* stays single-sourced; only the encoding differs. **SSE for dispatch, POST
for reports** is the fit: server-sent events are server-to-client push over ordinary HTTP, supported
everywhere, and a report is a POST carrying the echoed token. That preserves the push model, the
verbatim token echo and per-record outcomes.

Build it as a **gateway inside the sidecar**, translating to the same engine seam the gRPC transport
uses — so there is one implementation of the semantics and two encodings of it, never two engines.

**The conformance suite already has the right shape.** Its binding key is effectively
*(language, dialect)*: `core` is no wire at all, `java-direct` an in-process call, `java-grpc` a real
stream — three bindings of one language today. HTTP bindings are more rows, and the scenarios and
assertions do not change, because they assert semantics rather than encoding. That is what makes this
testable from the first day rather than trusted.

## Generated clients, and the honest caveat

Maintenance is cheaper than it looks: **the expensive part is already done.** Eleven foreign build
systems, their CI rows, their toolchains, their static analysis and their conformance runners all
exist. Adding a dialect reuses every bit of that.

And the clients need not be hand-written. The gRPC ones are generated from the `.proto`; the HTTP ones
can be generated from an **OpenAPI specification**, whose generators cover far more languages than
this project targets. Same discipline as the schema freeze: one spec, N generated surfaces, and the
spec becomes the thing under change control.

**The caveat, which decides how much is really generated:** OpenAPI describes request/response well
and **streaming poorly** — it can name `text/event-stream` as a content type but not the event
semantics. So the dispatch stream is the part a generator will not give you. Expect: request/response
generated, the SSE consumption and the client-side controller hand-written per language.

**Which is where a wrapper earns its place anyway** — and it is not the HTTP mechanics. It is the
dispatch queue, the ceiling counting *unresolved* records, the transport never blocking, and session
death observable with its cause. Raw HTTP calls give none of that, and **every client that went wrong
in this project went wrong exactly there.** A thin generated surface plus a small hand-written
controller is the right split; "just use `curl`" is only true for a demo.

## The clients should share a controller and swap the transport

Owner's observation, 2026-08-15, and it is the right one: **if the HTTP dialect also needs a
client-side work dispatcher, the two dialects share most of a client.** Sketching what a client
actually contains says how much:

| Part | Dialect-specific? | Size and difficulty |
|---|---|---|
| Transport — connect, receive dispatches, send reports, session lifecycle | **yes** | small |
| Codec — protobuf or JSON to language-native types | **yes** | small, mostly generated |
| **Controller** — dispatch queue, executor pool, the unresolved-record ceiling, verdict channel, never blocking the transport thread, session death observable with its cause | **no** | **the large and hard part** |
| Public API surface | **no** | small |

So the seam is a **transport interface** — *push me dispatches, take my reports, tell me the session
ended and why* — with the controller depending on the abstraction. Pleasingly, that is the client-side
mirror of `ExternalEngine` on the engine side: one seam each, and the same reason for both.

**Four things that decide whether the seam is right:**

- **It must not leak framing.** gRPC offers an ordered, flow-controlled bidirectional stream; SSE
  offers a one-way server→client stream plus separate POSTs. Model the seam as bidirectional and HTTP
  has to fake it. Model it as *push plus report* and both fit honestly.
- **Termination must normalise.** gRPC has status codes and half-close; HTTP has a dropped connection
  and a status on the POST. The clients already expose session death as an observable with a cause,
  so the shape exists — it needs one vocabulary behind it.
- **Report ordering must stay irrelevant.** Reports travel the same connection under gRPC and separate
  requests under HTTP, so they can be reordered or race a reconnect. Verdicts already carry the
  record's echoed token, which should make ordering immaterial — **confirm that rather than assume
  it**, because it is the assumption the whole seam rests on.
- **Reconnect differs**: `Last-Event-ID` versus stream restart. The reconnect *window* is server-side
  and unaffected; the client's resume logic is not.

**Sequencing, which matters more than the design.** Do **not** abstract a transport that has one
implementation — that reliably produces the wrong seam. Write the first HTTP client concretely in
**one** language, see what actually differs, then extract the seam there and propagate. That is the
ship-the-vertical rule applied to an abstraction.

**What makes it safe is already built**: the conformance suite's binding key is *(language, dialect)*,
so `java-http` is a row rather than a project, and the suite is what proves a shared controller
behaves identically under both transports rather than merely appearing to.

### The first transport the seam should pay for: a Unix domain socket

Owner's call, 2026-08-15, to track this. It is the cheapest transport the composition would enable and
the only one that *improves the security posture* rather than merely extending reach:

- **gRPC supports Unix domain sockets directly**, so this is a target address rather than a new
  protocol — the smallest possible proof that the seam works, since the codec and the controller do
  not change at all.
- **It skips the TCP stack entirely**, which is pure win for a sidecar that is loopback-only by design.
- **The good part: filesystem permissions become the authorisation model.** Socket ownership and mode
  decide who may connect, enforced by the kernel. That is a **better answer than
  `AuthorityAllowlistInterceptor` for the same-host case**, because it does not depend on the client
  telling the truth about who it is — and same-host is the case the sidecar was designed for.

Worth building for that last reason alone, independently of whether the HTTP dialect is ever written.
It also makes a good first extraction target for the transport seam precisely because so little else
varies: if a UDS transport cannot be swapped in cleanly, the seam is wrong and it is cheap to find out.

## Related

The sidecar is already parked to embed the web dashboard post-v6
([`parked-sidecar-embeds-web-gui.md`](parked-sidecar-embeds-web-gui.md)), so an HTTP listener is
likely arriving regardless — worth building one listener with two purposes rather than two. The
loopback-only posture and its opt-in apply unchanged, and an HTTP surface makes browser-origin
questions real in a way the gRPC one did not.
