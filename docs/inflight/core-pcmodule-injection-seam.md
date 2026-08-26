# `PCModule`'s processor registration is write-only in production

<!-- inflight-type: task -->
<!-- inflight-impact: refactor -->

`PCModule` is a hand-rolled DI container - "Minimum dependency injection system, modled on how Dagger
works", deliberately not Dagger because PC has a zero-dependency policy. Dependencies come from lazy
memoising provider methods (`workManager()`, `producerWrap()`, `consumerManager()`).

**Setter injection does not compose with that.** A provider caches on first call and collaborators
capture what they were handed, so a setter called *after* anything resolved the dependency swaps the
module's field and leaves those collaborators on the previous instance; called before, it is
<!-- post-merge: checked -->
redundant with the provider. astubbs#57 removed `@Setter` from `workManager` on exactly that
reasoning - it had no production callers, and its only two call sites were test lines that retrieved
the instance from the module and set the identical reference straight back.

## The decision this needs

One `@Setter` remains, on `parallelEoSStreamProcessor`. It is described as breaking a construction
cycle - the processor's constructor needs the module, the module needs the processor, so
`AbstractParallelEoSStreamProcessor` calls `module.setParallelEoSStreamProcessor(this)`. Setter
injection is the standard answer to a genuine cycle, so on its own terms it is legitimate.

**But the cycle is not between two peers, and in production the field is write-only.** Evidence:

- The only production consumer is `BrokerPollSystem`, and `brokerPoller` takes the processor **as a
  parameter** - `AbstractParallelEoSStreamProcessor` calls `module.brokerPoller(this)`, passing
  itself. The module never resolves it from the field.
- Nothing in `src/main` reads the field back. Its only reader is the `pc()` provider.
- `pc()`'s only caller is `ProducerManagerTest`, which overrides it to substitute a spy.

So the registration exists to keep a lazy singleton consistent for a provider that only a test calls.

**Prefer deleting `pc()` and the field over hardening the setter.** The processor already hands
itself to the collaborator that needs it, and the test seam can be a constructed-and-passed processor
rather than an overridden provider. That removes the cycle instead of expressing it more safely - no
`Supplier`/`Lazy` indirection (Dagger's usual answer) and no extracted role interface are needed,
because no production dependency remains to invert.

**What to preserve or drop deliberately:** today a `pc()` call after an externally-built processor
returns that instance rather than constructing a second one. That is the only thing the setter buys,
and it is the one behaviour a deletion has to make a decision about.

Both members are `protected` on a class in `internal`, so this is not published API.

## Related

`PCModuleTestEnv` shadows the parent's private `workManager` field with one of its own and overrides
the provider. Overriding the provider is the right way to substitute in a Dagger-shaped module; the
shadowed field is what made the two mechanisms hard to tell apart in the first place.

<!-- post-merge: checked-begin -->
## The setter stopped being dead on 2026-08-26, which raises the priority rather than closing this

astubbs#57 removed `@Setter` from `PCModule.workManager` because nothing could use it correctly, and
then had to **put it back** at merge prep: master's `d702e2ca2` (astubbs#346) added
`WorkManagerStaleCheckDoubleLookupTest`, which installs a racing `WorkManager` double with
`module.setWorkManager(wm)` before anything asks the module for one.

It works, but by a route nobody would design: `PCModuleTestEnv` **shadows** `workManager` with its own
private field and overrides `workManager()`, so the setter reaches the test only through
`super.workManager()`, and only while no `workManagerController` latch is set. Set a latch and the
same call silently stops installing the double - the test would keep passing against the real work
manager.

So the seam this note is about now has a live caller depending on its most fragile property. Fixing
it means giving `PCModuleTestEnv` a real injection point and moving that test onto it; until then the
setter stays.
<!-- post-merge: checked-end -->
