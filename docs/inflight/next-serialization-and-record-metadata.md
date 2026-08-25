# Next stage: serialization — and two fields the wire is missing first

Owner's direction, 2026-08-15: serialization is the next stage after the current one. Recorded with
the prerequisite that came out of checking the schema for it.

## Do this first: the record on the wire has no headers and no timestamp

`Record` in the frozen schema carries `topic`, `partition`, `offset`, `key`, `value` — and nothing
else. That is a functional gap independent of serialization, and it blocks part of it:

- **Headers** carry the tracing context (`traceparent`), routing and dead-letter metadata, and in
  some deployments the schema reference itself. A client that cannot see them cannot join a
  distributed trace or implement any header-conditional logic — a hard limitation for exactly the
  service-shaped applications this product targets.
- **The record timestamp** (and its type — event time versus log-append time) is needed for latency
  measurement and any time-based decision. Its absence is currently invisible because no client has
  asked for it yet.

Both are **additive**, so `buf breaking` permits them and the freeze is not in the way. Add them
**before the remaining language waves**, for the same reason the per-language file options were added
early: retrofitting a field into ten written clients costs ten times what adding it now does. The
produce path needs the mirror image — an outbound record should be able to set headers, or a client
can consume a traced message and emit an untraced one, silently breaking the trace at exactly the hop
that matters.

## The serialization decision itself

Today every client receives raw bytes and deserializes in its own language, and the demo contract
already anticipates this with its marked serde extension point. The real question the next stage must
answer is **where decoding happens**, and the honest tension is:

- **Client-side (today's model)** keeps the sidecar ignorant of schemas and the clients thin, but
  pushes the whole problem onto each language — and for Schema Registry users that means a registry
  client, credentials and a caching strategy **per language**, which is precisely the dependency
  weight this design exists to remove.
- **Sidecar-side** is where the best tooling already is: the JVM has the most mature Avro, Protobuf
  and JSON-Schema support and the reference Schema Registry client, and the sidecar already holds the
  Kafka credentials. Doing it there would give nine languages schema support for free — the single
  largest thing this architecture could offer that a thin binding cannot.

But sidecar-side decoding raises the question it must answer: **decoded into what?** Handing a
language-neutral tree (JSON, or a protobuf `Struct`) is lossy and slow for large payloads; handing
back the raw bytes plus a resolved schema lets each client decode natively but still needs a
per-language decoder. There may be a middle path — the sidecar resolves and caches the *schema*
while the client decodes the *payload* — which would remove the registry dependency without adding a
transcoding hop. That is the design work, and it should be settled with a measurement rather than a
preference, because the payload sizes decide it.

## Constraints any answer must respect

- **Do not expand the horizontal surface.** Anything requiring a new generated artifact per language
  costs ten implementations; a design that needs no per-language code is worth a lot.
- **Credentials stay on the JVM side.** Registry credentials must not travel to clients, for the same
  reason Kafka credentials do not.
- **Tombstones must survive.** A null value is distinct from an empty one, and every client already
  preserves that; a decoding layer must not flatten it.
- **Lockstep versioning applies** — see [`next-client-sidecar-version-lockstep.md`](next-client-sidecar-version-lockstep.md).
