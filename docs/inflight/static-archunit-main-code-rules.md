# ArchUnit pins three invariants and no architecture

<!-- inflight-type: bug -->
<!-- inflight-impact: blind-spot -->
<!-- inflight-state: deferred - after v6, tooling investment -->


Nothing is switched off at the FRAMEWORK level. There is no `archunit.properties`, no
`FreezingArchRule`, no `@Disabled`, no `allowEmptyShould` suppression anywhere in the repo. Every
rule that exists runs on every PR at ArchUnit's defaults.

**They no longer all pass because the codebase complies.** `rebalanceCallbacksMustNotBlock` carries a
hand-rolled allowlist - see "The allowlist this file warned about, and it exists now" below. This
paragraph said "nothing is switched off" full stop until 2026-08-31, which was true when written and
quietly stopped being true; the framework-level claim is the part that still holds.

The reason no findings are ever seen is that the rules police **hygiene and a handful of named
invariants**, not structure:

| Rule | Where | Polices |
|---|---|---|
| Integration tests live in an `integrationTest` package | `TestConventionRules` | test hygiene |
| Tests must not use shaded libraries | `TestConventionRules` | test hygiene |
| Test classes named so surefire collects them | `TestConventionRules` | test hygiene |
| `WorkContainer#getFuture()` has no production readers | `WorkContainerFutureIsWriteOnlyArchTest` | one main-code invariant |
| `ShardManager`'s shard-map setter has no production callers | `ShardMapIsNeverReplacedArchTest` | one main-code invariant |
| `ShardManager.processingShards` is assigned only at construction | `ShardMapIsNeverReplacedArchTest` | one main-code invariant |

The harness is in good shape and the hard part is already done: `TestConventionRules` is a shared
rule library shipped in the core test-jar, and each module has a thin `TestConventionsArchTest` that
points `@AnalyzeClasses` at its own packages. Core, vertx, reactor and mutiny all wire it up. Adding
a rule is a few lines in one place and it applies everywhere.

**No main-code architecture is policed.** Not layering, not package boundaries, not the dependency
direction between `internal` and `state`, not the God-class boundaries `docs/refactoring.md` wants
to decompose. The three invariant rules above are each one named member; none of them constrains a
shape.

## Why this is worth picking up

`docs/refactoring.md` carries **"Decompose the God class - `AbstractParallelEoSStreamProcessor`
(1533 lines)"**, marked high risk, plus a thread-model rework and a list of SpotBugs findings for
non-atomic and stale-thread state. Every one of those is a structural constraint that is currently
enforced by review attention alone. A decomposition with no rule holding the new boundaries will
drift back, and the drift is invisible until someone reads the whole class again.

The precedent for the value is on this branch: `WorkContainerFutureIsWriteOnlyArchTest` was written
because a comment saying "nothing reads this" is not enforcement, and mutation-checking it proved it
fires. That is one invariant. The class it guards has several more that nothing checks.

## Candidate rules, cheapest first

Each needs its own judgement about whether it is true today; a rule that fails on arrival needs the
code fixed or the rule narrowed, not a suppression. Read the granularity limits below before
costing any of them - one of them is not expressible in ArchUnit at all.

<!-- post-merge: checked-begin -->
- **Pin `ThreadConfinedConsumer`'s Lombok `@Delegate` excludes interface to its overrides**, so a
  method the wrapper means to guard cannot silently become an unguarded passthrough. The wrapper
  guards by *overriding* each `Consumer` method it cares about and listing that method on an
  excludes interface, so `@Delegate` does not generate a competing passthrough. The two lists have
  to agree, and nothing checks that they do: drop a signature from the excludes interface and the
  generated delegate wins, reaching the consumer without the ownership check, with no compile error
  and no test failure. A new Kafka `Consumer` method arriving in a client upgrade is the same hole
  from the other direction - it is delegated by default, and defaults are how a guard acquires a
  gap nobody chose.

  Same shape as the two invariant rules already shipped, so it is the per-invariant exception below
  rather than the post-v6 programme - but only once the guard is actually wired, since a rule
  pinning an unenforced seam pins nothing. Suggested by the simplify pass on
  astubbs/parallel-consumer#393, which is where the wrapper arrives; the claim call and the existing
  `ArchitectureTest` live on astubbs/parallel-consumer#29.
- **Dependency direction** between `internal`, `state`, `offsets` and the public API package - state
  should not reach back into the controller.
- **No raw `Thread` / executor construction outside the places that own lifecycle**, so the
  thread-model rework has a boundary to hold.
- **Non-volatile shared-state guard** for fields written by one thread and read by another.
  **`docs/refactoring.md` owns the offender list** - its `AT_STALE_THREAD_WRITE_OF_PRIMITIVE` entry
  names them and carries the fix; do not copy the names here, because a fix removes one there and
  leaves the copy asserting it. `state` in
  `AbstractParallelEoSStreamProcessor` is the one that makes the astubbs#209 race window
  unpredictable. ArchUnit can see a field's modifiers, so "these named fields must be volatile" is
  writable; **which thread reads a field is not in the model**, so the general form is not, and
  SpotBugs `IS2_INCONSISTENT_SYNC` is the detector for that class.
- **Public API surface** - what may be `public` outside the documented API packages, which would have
  caught the `// todo make private` in `WorkManager` before it became a `todo`.
- **Only `ThrowableUtils` may call `Throwable.getMessage()`** - turns "do not log bare messages" into
  "go through the utility", which is the enforceable form. `e.getMessage()` alone drops the type, the
  cause chain and the stack, and prints `null` for a message-less throwable; that was the original
  complaint behind astubbs#267, and a fresh instance appeared on astubbs#29's revoke path while that
  PR was in draft - which is the argument for a rule rather than vigilance. **Costed, not estimated:**
  8 `getMessage()` occurrences in main, 4 of them comments, 3 inside `ThrowableUtils`, leaving exactly
  **one** genuine call site elsewhere (`ProducerManager`, grep `lastErrorSavedForRethrow`) to exempt
  or migrate. `StringUtils` calls it on an SLF4J `FormattingTuple` rather than a `Throwable`, so an
  owner-typed rule excludes it for free.
<!-- post-merge: checked-end -->

## What ArchUnit's granularity rules out, measured

**ArchUnit sees a field ACCESS - origin code unit, target field, get or set. It does not see what is
invoked on the value that access loaded.** So no rule can distinguish a keyed `map.get(k)` from a
bulk `map.values()`, and no rule can require that lookups of a field go through an accessor.

This kills the "mandated accessor" form of the shard-map rule, and the measurement is worth keeping
because the form reads as obviously implementable until you look at the accesses. Exactly one
main-code access to `ShardManager.processingShards` is `getShard`; nearly all the rest are bulk
operations over `values()`, `keySet()`, `size()` or the whole map, which `getShard(key)` cannot
express, and the keyed ones that remain include both writes (`computeIfAbsent`, `remove`). A rule
requiring the accessor is therefore red on arrival, and the only way to green it is an allowlist of
the methods already there - a frozen baseline under a different name, which the trap below rules
out.

<!-- post-merge: checked-begin -->
The check-then-get seam that motivated the idiom was caught instead by fb-contrib's
`MUI_CONTAINSKEY_BEFORE_GET`, which named the method statically until astubbs#345 removed the seam -
see [`static-spotbugs-rule-registry.md`](static-spotbugs-rule-registry.md). **When a candidate rule
is about what happens to a value rather than about who may touch a member, SpotBugs is the tool and
ArchUnit is not.**
<!-- post-merge: checked-end -->

What ArchUnit could still pin about the same field is who may replace it, and that is what
`ShardMapIsNeverReplacedArchTest` does.

<!-- post-merge: checked-begin -->
**Statement ordering inside a method is out of reach too.** ArchUnit's model is classes, methods,
calls and dependencies - it cannot see that a log call happens *before* the bookkeeping that must not
be skipped. That is the shape of the worst defect astubbs#267 fixed (`runUserFunction`, grep
`Exception caught in user function running stage`), and it is invisible to any rule expressible here.
Only a test catches it, which is why that PR added one. Worth stating because "add an ArchUnit rule"
is a tempting answer to a class of problem it provably cannot address, and the candidate list above
reads as though the tool is general.
<!-- post-merge: checked-end -->

## Pin the names the rules depend on

**ArchUnit does not check that a `String`-named target exists.** Rename the field or drop the Lombok
annotation that generates the accessor, and the rule matches nothing, passes, and has asserted
nothing. Measured: pointing both shard-map rules at names that do not exist leaves both **green**.

Every rule naming a member as a string therefore ships with a reflective `@Test` asserting the
member is still there. Lombok-generated targets need it most - one annotation deletes them and no
compile error anywhere says so.

## When

**After 0.6.0.0.** Adding architecture rules during a release cut turns the build red for reasons
that have nothing to do with the release, and the God-class decomposition these rules would guard is
itself post-v6 work.

**One exception, and it is already in use:** a rule that pins an invariant created by work landing
now. `WorkContainerFutureIsWriteOnlyArchTest` and `ShardMapIsNeverReplacedArchTest` were both added
mid-branch for exactly that reason. Keep doing that per-invariant; it is not the programme this file
describes, and it does not wait for v6.

## The trap to avoid

Do not add rules in bulk and freeze the failures. `FreezingArchRule` exists and would let a wide net
land green on day one, but a frozen violation is a documented invariant nobody enforces - the exact
pattern this repo already rejects for quarantined tests, where the registry is "enforced, not
advisory". Add one rule at a time, green on arrival, each with a mutation check proving it can fail.

**An allowlist of the methods that already violate a rule is a frozen baseline wearing a source-code
disguise**, and it is the form the trap actually takes when `FreezingArchRule` is off the table. The
shard-map accessor rule above is the worked example of turning one down.

## The allowlist this file warned about, and it exists now

`ArchitectureTest.rebalanceCallbacksMustNotBlock` carries `KNOWN_BLOCKING_VIOLATIONS`, which is
exactly the shape named above. It arrived with one entry - the confluentinc#857 transactional revoke
wait, owned by astubbs#44 - and grew on 2026-08-31 when the rule's deny list was widened during a
defect-class sweep and immediately found a second, pre-existing defect on master (the retry queue's
write lock, six entries).

**Update 2026-09-03: those six entries are gone, and deleting them is what proved the warning.** The
defect they carried is fixed - the rebalance callbacks decline the lock instead of waiting for it -
so the list is back to the one confluentinc#857 transactional-revoke entry. Deleting them first, as
the re-enable path below says, is also what surfaced the finding the entries' own comments did not
claim: with the six deleted, the rule reported violations on the revoke and lost callbacks and
nothing at all on `onPartitionsAssigned`, which reaches the same lock through a METHOD REFERENCE the
walk cannot see. Write-up:
[`../solutions/runtime-errors/retry-queue-write-lock-on-the-rebalance-path.md`](../solutions/runtime-errors/retry-queue-write-lock-on-the-rebalance-path.md).

**Update 2026-09-03, later the same day: the walk that missed it now follows method references.**
`notReachBlockingCalls()` follows `getMethodReferencesFromSelf()` beside `getMethodCallsFromSelf()`, so the
shape above is reported rather than skipped - re-measured by restoring `.map(retryQueue::remove)` to the
production tree, which takes the rule from green to a report naming all three callbacks.
`RebalanceCallbackRuleControlTest` holds a fixture reaching a deny-listed acquire through a method reference
and asserts the rule reports it, so the hop cannot be dropped again silently; it fails on the pre-2026-09-03
walk, which is how it was checked.

**Constructor calls were the obvious next widening and were measured and rejected**, which is worth recording
because it reads as free. Enqueuing `getConstructorCallsFromSelf()` turns every factory call into a reach into
whatever the constructed object wires up: `PCModule.workManager()` contains `new WorkManager(..)`, whose
constructor registers a metrics gauge as a method reference, and that gauge reads the retry queue under its
read lock - a red rule on a path no callback takes. The general limit under it is that ArchUnit's model has
the same shape for a reference invoked now (a stream stage) and one invoked later (a gauge, an executor task),
so a reference-walking rule is conservative by construction.

**The tension is real and is not resolved here.** The argument for the entries: each names a defect
that exists on master, has a tracking note, and was not introduced by the branch that had to decide
what to do about it - the alternative was leaving a rule permanently red on inherited work, which
teaches people to ignore it. The argument against is the paragraph above, and it does not stop being
true because the entries are honest ones.

**What was learned by growing it, and it sharpens the warning rather than softening it:** the
allowlist was keyed on the ROOT method, so exempting one violation silenced that callback for every
blocking call - and it was concealing a second, unrelated violation *inside the method it exempted*,
which only appeared when the key was narrowed to `root => target`. So the trap is worse than "a
documented invariant nobody enforces": a coarse allowlist hides findings nobody documented at all.
If an allowlist is kept, **key it as narrowly as the tool allows**.

Two things follow for whoever picks this file up:

- The re-enable path is per entry, not per rule. Delete one pair, run the rule, and what it reports
  is the debt that entry was carrying - which is not always what its comment claims.
- `KNOWN_BLOCKING_VIOLATIONS` has no counterpart in the SpotBugs or Error Prone registries, which
  are per-analyser and do not know about ArchUnit. That is the actual gap in suppression tracking -
  not a missing cross-tool register, which those two already cover for their own tools, but that
  ArchUnit's accepted violations are recorded only in the source file that holds them.

