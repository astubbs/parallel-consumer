# Pre-v6: the clients must expose core's batching modes

Owner's call, 2026-08-15, and the reasoning is defensive as much as technical:

> "One of the first things people are gonna say is, well, it'll be slower because it's one record at
> a time over a wire protocol. At least having the batching as it stands usable will help
> counter-argue that."

The objection is correct, and it will be the first one asked. Core already has the answer — batching
is implemented and shipped there — so the multi-language clients not exposing it is the gap, not the
architecture.

## The framing that matters: the API *is* batch, and single-record is the degenerate case

Owner's correction, and it reframes the work: **core's API is already batch-shaped.** `poll` hands
the user function a poll *context*, which is a container of records; a batch size of one simply
yields a context holding one. There is no separate single-record API in core to add batching to.

The clients modelled the degenerate case as though it were the API — one record in, one outcome out.
That is why adding batching later changes the user-facing signature in every language: a cost that
would not exist had they mirrored core's shape from the beginning, where batching is not a feature to
add but a size to configure.

The consequence for whoever picks this up: **do not design a second, parallel batch API.** Widen the
existing one so the record-shaped call becomes the convenience over a batch-shaped one, exactly as
core has it. And the general lesson for the remaining language waves — mirror the shape core already
chose rather than the shape the first client happened to need.

## Where this stands today

The proxy pins the batch size to **1** and the engine actively rejects anything larger, a deliberate
decision taken when the interaction model was settled (KTD10 in the language-proxy plan). So this is
a decision to revisit with its own reasoning in hand, not an oversight to patch: read why it was
pinned before undoing it, and record what changed.

Note the distinction that matters, because the words collide: the wire already carries a **wave** — a
`Dispatch` holding several records — but a wave is several records dispatched, each processed and
reported *individually*. **Batching** in core's sense is different: the user's function receives N
records in one call. The wire form may already suffice; the API shape does not.

## What it costs, honestly

- **Every client's user-facing surface changes**, in all the languages. That is the horizontal cost
  this project pays for anything, so it should land while the clients are young rather than after
  they are published and their examples are copied.
- **Per-record outcomes must survive.** The engine completes each record's container independently,
  which is what makes partial failure expressible. A batch API that returns one outcome for N records
  would throw that away — one bad record would poison its whole batch, turning a per-record retry
  into a batch-wide one. Whatever the shape, N records in must be able to produce N outcomes out.
- **Ordering guarantees must not weaken.** A batch must not be allowed to span shards in a way that
  breaks the ordering the product exists to provide; the engine's existing distinct-shard rule for
  waves is the precedent to follow.

## Why it is worth the cost

It is the honest answer to the throughput question rather than a rhetorical one, and it is already
built on the engine side. Measuring it is also the point — the performance work
([`parked-testing-as-a-feature-for-the-clients.md`](parked-testing-as-a-feature-for-the-clients.md))
should report the sidecar hop's cost *with batching in use*, since that is the configuration a
throughput-sensitive user would actually run. A benchmark that only measures the unbatched path
answers the objection in the worst possible way: by confirming it.
