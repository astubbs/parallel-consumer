# PC builds the clients it uses - configuration is now a path on both sides, but not the only one

<!-- inflight-type: feature -->
<!-- inflight-impact: reliability -->
<!-- post-merge: checked - the vet is attributed to astubbs#506, not to the branch that carried it -->
<!-- inflight-vetted: 2026-09-11 - rewritten against astubbs#506: `ParallelConsumerOptions` now carries `consumerConfig` beside `producerConfig` and `PCModule.buildConsumer` beside `buildProducer`, so the consumer half this note asked for exists - additively. Both instance fields, `consumer` and `producer`, are still there and still take the path they always took, so the structural-ownership argument below is untouched and is what remains open -->

<!-- post-merge: checked-begin - written in the past tense about a PR, which stays correct once it lands -->
**The consumer half of this note has landed, and the argument it was making has not.**
astubbs#506 (for astubbs#504) gave `ParallelConsumerOptions` a `consumerConfig` mirroring
`producerConfig`, with `PCModule.buildConsumer(Map)` as the protected substitution seam beside
`buildProducer(Map)`. A caller holding nothing but connection properties no longer has to construct a
client purely to hand it straight back.
<!-- post-merge: checked-end -->

That change is **additive**, deliberately: a supplied `consumer` takes exactly the path it took
before. So PC can still be handed a client that somebody else built, holds, and may close - which is
the thing this note exists to argue against.

## What is still open

**The instance path.** `consumer` and `producer` are both still fields on
`ParallelConsumerOptions`, and while they are, exclusive ownership stays a *runtime* guard -
`ThreadConfinedConsumer` refusing a call from a thread that does not own the client, and
`ConsumerOwnership` making that a lifecycle. The note's original claim was that building the client
would make structural what those two enforce at runtime: PC cannot be handed a consumer somebody else
still holds, because nobody hands it one. Nothing about the configuration path achieves that on its
own. Removing the instance path, or deprecating it into a documented legacy route, is what would.

**Breaking, so it is release-gated.** `docs/refactoring.md`'s *Breaking changes queued for next major
version* is where this belongs once somebody commits to it, not this note - that has not changed, and
nobody has committed to it. What has changed is that the replacement now exists, so the queued entry
would be a removal rather than a design.

**The producer half's own PR.** astubbs#420 is the rung that makes the producer side a factory PC
enforces rather than a map it happens to accept; `gh pr view 420 -R astubbs/parallel-consumer`
answers its current state. Until it lands, the two sides are enforced differently even though both
now accept configuration.

## The 2022 draft was answered in a different shape - read this before reviving it

`origin/client-factory` takes a **supplier**. The landed consumer half takes a **configuration map**
plus a protected `buildConsumer` the tests override, which is a different design answering the same
question, and it is the one in the tree. The branch is catalogued in `branch_accounting`
(`src/docs/development/upstream-map.yaml`); `bin/inflight.mjs branch client-factory` answers from any
checkout.

**So the open question about that branch is no longer "should somebody do this" but "does the
supplier shape still buy anything the config path does not".** A supplier defers construction and can
be re-invoked; a map is resolved once, memoised in `PCModule.consumer()`. Whether that difference
matters is unmeasured and undecided. Nobody should apply the 2022 branch as a diff either way - it
predates the ownership lifecycle it would now be built on.

## What the configuration path settled, and does not need re-litigating

<!-- post-merge: checked-begin - cites a merged PR's commit and its landed contract, both permanent -->
Recorded here because it is the kind of decision a later reader re-opens from first principles. On
the configuration path only - a supplied instance is untouched by all of it - PC refuses an explicit
`enable.auto.commit=true` rather than quietly inverting it, forces an absent one to false, and
requires `key.deserializer` and `value.deserializer` because the engine is generic over `<K, V>` and
has no pair it could default to that would not be silently wrong. The reasoning is in astubbs#506's
commit for the feature; the contract is in `ParallelConsumerOptions.validate()`.
<!-- post-merge: checked-end -->

## Not owned

Unowned, as before. The consumer half was taken up as part of other work rather than because anybody
picked this note up, so the remaining breaking change still has nobody on it.
