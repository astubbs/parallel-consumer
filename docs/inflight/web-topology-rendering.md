# Next: render the topology, and where that view belongs

<!-- inflight-type: feature -->
<!-- inflight-impact: reach -->
<!-- inflight-state: deferred - after the embedded dashboard lands -->

The Kafka Streams PoC can already report a topology
([`next-kafka-streams-foreign-wrappers.md`](next-kafka-streams-foreign-wrappers.md), U9). This note
is about turning that report into something a person looks at, and about the one decision that
should be taken before anybody writes a renderer: **it belongs in the dashboard that already
exists, not in a new UI.**

## What the protocol already gives us

A `Describe` request is answered with the same graph in two forms, deliberately:

- **`text`** — exactly what `Topology.describe()` prints. A compatibility surface: every Kafka
  Streams visualiser in existence parses this format.
- **`subtopologies`** — the graph already parsed into nodes, edges, kinds, topics and stores.

The demo prints the text today. That alone is a real capability for a language whose ecosystem has
no Streams tooling and no way to grow any: paste it into any of the tools below and get a diagram of
a topology that was defined in Python.

## Using the existing tools is free. Embedding them is not.

This is the trap, and it is licensing rather than engineering. Checked directly against the
repositories:

| Tool | Licence | Embeddable? |
|---|---|---|
| [`zz85/kafka-streams-viz`](https://github.com/zz85/kafka-streams-viz) | **none declared** | No — no licence means all rights reserved, regardless of how public the repo is |
| [`gaetancollaud/kafka-streams-visualization`](https://github.com/gaetancollaud/kafka-streams-visualization) | **none declared** | No — same |
| [KSTD](https://github.com/thriving-dev/kafka-streams-topology-design) | **GPL-3.0** | No — copying it into this Apache-2.0 project would force the combined work to GPL-3 |
| [KCM Hub topology explorer](https://kcmhub.io/tools.html) | hosted tool | Not distributed as embeddable source |

**Pointing a user at them is fine and costs nothing** — that is using a website, not distributing
code, and the docs already do it. Vendoring any of them is not available to us.

A missing licence is the sharper trap of the two, because a popular repository with no `LICENSE`
file reads as permissive and is the opposite. Do not let "it is on GitHub with hundreds of stars"
substitute for checking.

## So we write the renderer — and we are better placed than any of them

Every tool above parses the ASCII, because the ASCII is all they can get. It is a human-readable
rendering with no stability guarantee, and each of them has had to reverse-engineer its shape and
re-do that work when it shifts.

**We own both ends, so we skip the parsing entirely.** The structured form is already on the wire.
Rendering becomes a graph-layout exercise against typed data, which is a much smaller and much more
durable problem than the one those projects took on. That is a direct, concrete dividend of wrapping
the real engine rather than reimplementing it, and it is worth citing as one.

## Where the view belongs: the dashboard that already exists

**Do not build a new web UI for this.** There is already an embedded web dashboard
(astubbs/parallel-consumer#268, tracking astubbs/parallel-consumer#215, on `feats/web-gui`), and a
standing intent for the sidecar to embed it —
[`parked-sidecar-embeds-web-gui.md`](parked-sidecar-embeds-web-gui.md). A topology view is a panel
in that, not a second front end.

That note's constraints govern this work too and are not restated here beyond naming them: the
proxy must not depend on `feats/web-gui` until both are on trunk, the serving code must arrive as a
dependency rather than a copy because of the duplicate-code cap, and the sidecar's listener posture
governs the dashboard's port. Read it before starting.

The argument for the pairing is the same one that note makes, only stronger here. A sidecar is the
deployment where "see inside a running engine" has no competing UI, because the application is in
another language and cannot host its own view. For a Streams engine that is more true, not less: the
host described the topology through handles and has never seen the assembled graph, so the dashboard
is showing it something it genuinely cannot obtain any other way.

## Open questions, none of which block a start

- **Static or live?** A rendered topology is static — it is fixed once the description closes. The
  interesting version is coloured by runtime state: which nodes are active, per-node throughput,
  which task owns which partition. **That needs the engine-state gap closed first**; the protocol
  currently carries no state or assignment signal at all, which is recorded as a gap in the Streams
  note. Static first, and do not design the live version until the state signal exists.
- **Is this Streams-only?** The Describe mechanism is, but a Parallel Consumer session has a shape
  worth drawing too — shards, in-flight records, the concurrency the engine settled on. Whether one
  panel serves both, or they are separate views, is a design question for whoever owns the
  dashboard rather than for this note.
- **Which layout engine, and its licence.** The obvious candidates are Graphviz-in-JS builds. Check
  the licence of whatever is chosen *before* building against it, for exactly the reason the table
  above exists.
- **Does the demo keep printing the text?** It should. It costs nothing, it works with no UI at all,
  and it is the form a reader can paste elsewhere.

## Sequencing

Not now. It is downstream of the dashboard and the sidecar embedding it, and the live version is
downstream of the engine-state signal. The cheap half is already delivered: the demo prints the
text, so nobody is blocked from seeing their topology today.

## Prior art

- [`next-kafka-streams-foreign-wrappers.md`](next-kafka-streams-foreign-wrappers.md) — the Describe
  message, what the PoC found, and the deferred capabilities including the engine-state gap.
- [`parked-sidecar-embeds-web-gui.md`](parked-sidecar-embeds-web-gui.md) — **read before starting**;
  owns the constraints on embedding the dashboard in the sidecar.
- [`next-polyglot-demo-app.md`](next-polyglot-demo-app.md) — owns the demo app's UI and live loop,
  and is the other place a topology view might reasonably surface.
- [`../language-bindings.md`](../language-bindings.md) — how the boundary is crossed, and why the
  structured form exists on the wire at all.
