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

## Why it earns its place: reachability, not novelty

**A gRPC client is unusable where the client can only reach the sidecar over HTTP** — corporate
proxies that mangle HTTP/2, restricted networks, some hosted platforms. That is a real deployment
constraint rather than a preference, and no amount of client-side polish works around it.

Secondary, and still real: the long tail of runtimes with poor or absent gRPC support — embedded
targets, Lua, Perl, R, older runtimes — and anyone who wants to try the thing with `curl` before
adopting a dependency. This is a better answer to that tail than the C shim considered in
[`parked-a-c-client-and-the-ffi-question.md`](parked-a-c-client-and-the-ffi-question.md), because
there is nothing to compile; **that note is partly superseded by this one.**

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

## Related

The sidecar is already parked to embed the web dashboard post-v6
([`parked-sidecar-embeds-web-gui.md`](parked-sidecar-embeds-web-gui.md)), so an HTTP listener is
likely arriving regardless — worth building one listener with two purposes rather than two. The
loopback-only posture and its opt-in apply unchanged, and an HTTP surface makes browser-origin
questions real in a way the gRPC one did not.
