# PC builds the clients it uses, rather than being handed them - the consumer half

<!-- inflight-type: feature -->
<!-- inflight-impact: reliability -->

astubbs/parallel-consumer#420 makes PC build its **producer** from configuration through a factory it
enforces, instead of taking a finished instance. The same argument applies to the **consumer**, it was
drafted in 2022, and nothing tracks it.

## The consumer half already has a draft

`origin/client-factory` takes a supplier rather than an instance. It is catalogued in
`branch_accounting` (`src/docs/development/upstream-map.yaml`); `bin/inflight.mjs branch client-factory`
answers from any checkout.

**It is more relevant now than when it was written, not less.** Master already enforces exclusive
consumer ownership at *runtime* - `ThreadConfinedConsumer` refuses a call from a thread that does not
own it, and `ConsumerOwnership` makes that a lifecycle. A factory would make structural what is
currently a runtime guard: PC cannot be handed a consumer somebody else still holds, because nobody
hands it one.

## Why it is worth doing as a pair

The two halves answer the same question - *who owns the client PC uses* - and answering it for the
producer alone leaves the API asymmetric: configuration in for one, instance in for the other.
astubbs#420's own reasoning (a producer PC built belongs to PC, and is closed rather than leaked when
construction fails) transfers directly.

## What it costs

**Breaking**, so it is release-gated: `docs/refactoring.md`'s *Breaking changes queued for next major
version* is where it belongs once someone commits to it, not this note. Taking an instance would have
to go, or become the deprecated path. The 2022 branch is a design reference rather than a diff to
apply - it predates the ownership lifecycle it would now be built on.

## Not started

Unowned. Recorded 2026-09-03 while reading every pre-fork branch, because the idea had no tracker at
all: astubbs#420 reaches it only through a line in `docs/refactoring.md`'s idea bank.
