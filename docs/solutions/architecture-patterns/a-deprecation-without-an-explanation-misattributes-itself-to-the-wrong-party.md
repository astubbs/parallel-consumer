---
title: "A deprecation without an explanation misattributes itself to the wrong party"
date: 2026-08-11
category: architecture-patterns
module: parallel-consumer-streams
problem_type: architecture_pattern
component: service_object
severity: medium
root_cause: inadequate_documentation
resolution_type: documentation_update
applies_when:
  - "An annotation you attach to someone else's symbol (a third-party interface, a patched dependency, a language-level marker like `@Deprecated`) carries no explanation visible at the call site"
  - "Two annotations enforce the same refusal for different audiences: one that fails a build under a specific toolchain (ErrorProne, a linter), and one an IDE renders directly (`@Deprecated` strikethrough)"
  - "The marker being applied is inherited from the language or a well-known third party (`java.lang.Deprecated`, `@Override`), so a reader's default assumption is that the third party is the one speaking"
  - "The cost of a missed explanation is a false claim about who did what, not a functional defect"
symptoms:
  - "An IDE renders `stream.join(...)` with strikethrough and no visible reason attached"
  - "The refusal reason lives only in `@DoNotCall`'s message, which only ErrorProne surfaces, and an ErrorProne build already fails hard on it"
  - "A reader reasonably concludes Apache Kafka itself deprecated `join`, which is false and alarming"
related_components:
  - "PcUnsupportedConstruct"
  - "KStream"
  - "KTable"
  - "KGroupedStream"
  - "CogroupedKStream"
tags:
  - "kafka-streams"
  - "deprecation"
  - "javadoc"
  - "annotations"
  - "do-not-call"
  - "ide-strikethrough"
  - "misattribution"
  - "api-design"
---

# A deprecation without an explanation misattributes itself to the wrong party

> Extracted from `origin/feats/ks-streams-refuse-unsupported-surface` @801c87424, `docs/solutions/architecture-patterns/a-deprecation-without-an-explanation-misattributes-itself-to-the-wrong-party.md`.

## Context

When you decorate somebody else's API with an annotation, the annotation does not come with a byline. `@Deprecated` on `KStream#join` reads, to every human and every IDE, as *Apache Kafka deprecated `join`*. There is no syntax in Java for "a repackaging layer three levels down your classpath objects to this method, and only under one configuration". The reader gets a strikethrough and fills in the attribution themselves, and the attribution they reach for is the symbol's owner.

The worked case is `parallel-consumer-streams`, the Kafka Streams integration developed under astubbs/parallel-consumer#255 and opened as astubbs/parallel-consumer#271 (unmerged as of this writing). The module dispatches a Kafka Streams topology through Parallel Consumer's work-shard manager, which breaks the single-threaded, stream-time-ordered assumptions that joins, windowed operators and suppression are built on. Those constructs do not fail loudly on that path. They read a stream-time counter that never advances and emit wrong numbers quietly, which the module's README calls out as the reason refusal is the only honest option (`parallel-consumer-streams/README.md:224-228`).

So the module refuses them at compile time, by patching Apache Kafka's own `KStream`, `KTable`, `KGroupedStream` and `CogroupedKStream` interfaces with two annotations per refused overload: ErrorProne's `@DoNotCall`, carrying the reason as its message, and `java.lang.Deprecated`.

The first cut shipped exactly that and nothing else. Every refused overload carried a full, accurate, specific explanation of why it was refused, written into the `@DoNotCall` message, and an IDE showed the user a struck-through `stream.join(...)` with no reason attached at all. The plan had explicitly reasoned its way there: "`@DoNotCall`'s optional `value` carries the reason, so no javadoc edit is needed" (`docs/plans/2026-08-10-001-feat-refuse-unsupported-streams-surface-plan.md:276-278`, now struck through in place rather than deleted, because it is the assumption that produced the defect).

## Guidance

**If you put a warning marker on an API you do not own, the marker must say who is speaking, and it must say it in the channel the warning actually travels through.**

Three parts, in order of how often they get missed.

**1. Work out which audience each marker reaches, and do not let the reasoning live only in the narrow one.** `@DoNotCall` and `@Deprecated` look like belt and braces. They are not: they reach disjoint populations.

- `@DoNotCall` is a hard compile error, but only in a build configured with ErrorProne, which most builds are not. Notably, nothing in this repository compiles under ErrorProne, so there is no build here in which a call site can be observed failing (stated plainly in the test rather than glossed, `parallel-consumer-streams/src/test/java/io/confluent/parallelconsumer/streams/RefusedDslAnnotationsTest.java:44-46`). It is not free either: because the annotation is applied in patched *main* sources while the transitive copy is test scope, the module has to declare `error_prone_annotations` explicitly (`parallel-consumer-streams/pom.xml:131-138`).
- `@Deprecated` reaches everyone. Its real force is not the compiler warning, which is easy to drown out; it is IDE strikethrough, which is the most visible signal Java has. It appears while the user is typing, needs no build configuration, and is seen by people who will never run your build at all.

Putting the explanation only in `@DoNotCall`'s message routes it to the audience that already gets a hard stop with the full text printed, and starves the audience whose only signal is a visual one with no text attached. Keeping both annotations was right. The gap was that the *visible* marker carried no message.

**2. Name yourself in the marker.** A javadoc `@deprecated` tag is the message channel `@Deprecated` actually has, and an IDE renders it next to the strikethrough as the reason. The tag has to do four things: deny the misattribution outright, name the party objecting, carry the reason across from the narrow channel so the two do not drift, and point at the switch that turns the objection off.

**3. Annotate and throw; do not delete.** The instinct with an unsupported method is to remove it from the surface. Here that would have been destructive, and the module's own justification says why: "Nothing was deleted. The signatures are all still there, because Kafka's own test suite calls them heavily and deleting them would forfeit the evidence" (`parallel-consumer-streams/README.md:246-247`). That evidence is Apache Kafka's own compiled test classes, run unmodified against the patched classes with the dispatch seam off: `StreamTaskTest` 101, `StreamThreadTest` 231 with 21 skipped, `RecordCollectorTest` 59, `ProcessorContextImplTest` 28, 419 total with zero failures (`parallel-consumer-streams/README.md:57-72`). Note *how* they run, because it sharpens the point: those are Kafka's own already-compiled test classes taken from the `kafka-streams` test jar, not rewritten and not recompiled (`parallel-consumer-streams/README.md:69-72`). A deleted signature would still fail this module's own build, because the patched impl classes are recompiled from source and carry `@Override` on the refused methods. But it would not fail *in the evidence*. Kafka's tests are never recompiled, so their calls would link-fail at run time against classes you do not own and cannot edit, and the whole behaviour-preservation argument rests on exactly those classes. The refusal had to be additive: annotations at compile time, and an `UnsupportedOperationException` at runtime from `parallel-consumer-streams/src/main/java/io/confluent/parallelconsumer/streams/PcUnsupportedConstruct.java:134`, thrown only while the seam is on. The same file states the converse in its own javadoc: an *unconditional* refusal would break Kafka's suite too (`PcUnsupportedConstruct.java:121-129`).

### The trap: you cannot always add your own tag

Javadoc permits exactly one `@deprecated` tag per block. Two is malformed. So where the upstream API had *already* deprecated a method itself, there is nowhere to put a new tag, and the refusal has to be appended to the existing sentence instead.

In this module that is four `KTable` foreign-key overloads that Apache Kafka deprecated in 3.1. Their javadoc keeps Kafka's own tag as a context line and gains a continuation, for instance at `parallel-consumer-streams/src/main/patch/pc-streams.patch:894-898`:

```
 * @deprecated since 3.1, removal planned for 4.0. Use {@link #join(KTable, Function, ValueJoiner, TableJoined)} instead.
 *             Separately, {@code parallel-consumer-streams} also refuses this on the Parallel Consumer dispatch
 *             path (astubbs#255), because the subscription store is updated and read across both halves without
 *             ordering concurrent dispatch can honour; turn that dispatch off ({@code PcDispatchSwitch}) and it
 *             behaves exactly as stock Kafka Streams.
```

Note "Separately" and "also": the wording is doing attribution work, keeping Kafka's deprecation and this module's refusal as two distinct claims inside one tag. The same four sites also keep Kafka's own `@Deprecated` as a context line and gain only `@DoNotCall` (`parallel-consumer-streams/src/main/patch/pc-streams.patch:899-901`).

That asymmetry is the thing to design your guard around, because it makes the obvious count wrong. Across the four interfaces the patch adds 59 `@DoNotCall` annotations but only 55 `@Deprecated` and only 55 new javadoc `@deprecated` tags. A check that counts added `@Deprecated` against added tags is blind to precisely the four hardest cases.

### The guard

`RefusedDslAnnotationsTest` closes that from both ends, and it is worth copying the shape rather than the specifics.

- **Reflection over every overload, per interface** (`RefusedDslAnnotationsTest.java:99-116`, `:278-300`): `java.lang.Deprecated` is `RUNTIME`-retained, so every `join`, `leftJoin`, `outerJoin`, `windowedBy` and `suppress` on the four types can be enumerated and checked individually. This half is exhaustive by construction, and it is what catches a Kafka upgrade adding a twenty-ninth `join` overload. It is name-based and deliberately coarse.
- **A count assertion before the loop** (`RefusedDslAnnotationsTest.java:289-292`): if upstream renames the method, the enumeration silently finds nothing and the loop becomes vacuous. The count makes that failure loud.
- **A constant-pool scan for `@DoNotCall`** (`RefusedDslAnnotationsTest.java:119-127`): `@DoNotCall` is `CLASS`-retained, so reflection cannot see it. The test reads each compiled interface as bytes and looks for `Lcom/google/errorprone/annotations/DoNotCall;`. This is weaker, and the test says so rather than skipping quietly (`RefusedDslAnnotationsTest.java:37-42`); one UTF8 entry covers the whole file, so it would pass with 1 of 59 annotated (`RefusedDslAnnotationsTest.java:129-135`).
- **A per-annotation count over the tracked patch text** (`RefusedDslAnnotationsTest.java:137-152`), which is what the constant-pool scan cannot give: 28 on `KStream`, 25 on `KTable`, 3 on `KGroupedStream`, 3 on `CogroupedKStream`.
- **A structural walk of the patch's post-image** (`RefusedDslAnnotationsTest.java:169-217`, helper at `:234-264`): every `@Deprecated` the patch adds must sit behind a javadoc block carrying a `@deprecated` tag. It reconstructs each hunk's post-image from added *and* context lines, resets at hunk boundaries, and skips intervening annotations when walking back to the closing `*/`. It says nothing about wording, so a reword passes and a forgotten overload fails.
- **A phrase count that reaches the four appended cases** (`RefusedDslAnnotationsTest.java:212-216`): every refused overload's javadoc names `{@code parallel-consumer-streams}` exactly once, so counting that phrase gives 59 whether the tag was new or appended. It is deliberately coupled to one phrase, so a reword has to come to the test and say so.
- **A control** (`RefusedDslAnnotationsTest.java:266-276`): `mapValues`, `filter`, `groupByKey`, `count`, `reduce` and `toStream` must *not* be deprecated. Without it, deprecating the entire interface would satisfy every assertion above while refusing the operators the module exists to support. The control has its own guard against becoming a no-op through a typo or an upstream rename (`RefusedDslAnnotationsTest.java:312-314`).

What the guard deliberately does not cover is worth knowing before you copy it. It cannot observe an actual ErrorProne compile error, because no build here runs ErrorProne, so it proves annotation presence rather than compiler behaviour. It cannot tell you *which* methods carry `@DoNotCall` inside a class file, only that something does. It says nothing about wording quality beyond one coupled phrase. And the reflective `KTable` assertion passes at the four already-deprecated sites whether or not this patch ever touched them, which is exactly the hole the phrase count exists to fill.

One placement detail worth stealing: the annotations go on the **interfaces**, not the impls, because a call site resolves against its receiver's static type. `stream.join(...)` on a `KStream` variable resolves to `KStream#join`, and an annotation on `KStreamImpl` would never be consulted (`RefusedDslAnnotationsTest.java:48-50`). The patch bears this out: the four impl classes are patched for other reasons (`pc-streams.patch:55`, `:99`, `:143`, `:187`) and carry zero added `@DoNotCall` between them.

## Why This Matters

The failure mode is not "the warning is unhelpful". It is "the warning is actively false, and it defames a third party to your users".

A reader who sees `stream.join(...)` struck through in their editor concludes that entirely normal, current, fully supported Apache Kafka Streams usage is obsolete. They may go looking for a replacement API that does not exist. They may file a bug against Kafka. They may conclude the Kafka Streams DSL is being wound down. Every one of those is a real cost, imposed on someone who did nothing wrong, by a module they may not even know is on their classpath.

Worse, the reason the misinformation survived review is that the explanation genuinely existed and was genuinely good. The `@DoNotCall` messages are specific and name the mechanism: window close driven by `observedStreamTime`, which does not advance on the dispatch path and is corrupted under concurrent read-modify-write; foreign-key joins whose subscription store is updated and read across both halves without ordering concurrent dispatch can honour. Reviewing the diff, you read those messages and feel informed. You are not the audience at risk. The audience at risk sees none of that text, and the diff does not show you what they see.

There is a second-order point. This was a UX defect inside a change whose entire purpose was UX: the refusal exists so users get told, at compile time, that a construct is unsafe, rather than getting silently wrong numbers. Shipping it with the reason routed to the wrong channel meant the feature half-worked in the direction it was specifically designed to work in.

The generalisation is worth stating plainly: **any annotation you place on a symbol you do not own is read as the owner speaking, unless it names you.** That covers `@Deprecated` on repackaged or shaded APIs, but also `@Deprecated` added by an internal platform team to a shared library, `@Nullable` corrections layered onto a vendor SDK, and any lint suppression or marker attached to generated or vendored code. If the marker has a message channel, use it to say who is objecting and why. If it does not have one, that is a strong argument against using that marker at all.

## When to Apply

- You are adding `@Deprecated` to a class, interface or method whose source you did not author: repackaged, shaded, vendored, patched, generated, or owned by another team.
- You are pairing a build-tool-specific marker (ErrorProne, NullAway, a custom lint rule, an annotation processor) with a language-level one, and only one of them is going to be read by the whole audience.
- You are narrowing a supported surface and reaching for deletion. Ask what compiles - or merely *links* - against those signatures today, particularly upstream test suites you are using as evidence.
- Upstream has already deprecated the thing you want to deprecate. Check for the one-tag-per-block constraint before assuming your usual annotation pattern applies.
- You are writing a test that counts annotations in a patch or a generated artefact, and some of the sites already carry the annotation from upstream. Your counts will not line up, and the discrepancy is exactly where the coverage hole is.

## Examples

### The pattern, at one refused site

`KStream#join(KStream, ValueJoiner, JoinWindows)` as it now appears, at `parallel-consumer-streams/src/main/patch/pc-streams.patch:296-305`:

```
+     * @deprecated Not deprecated by Apache Kafka: {@code parallel-consumer-streams} refuses this on the Parallel
+     *             Consumer dispatch path (astubbs#255), because join emission is stream-time gated and stream time does
+     *             not advance there. Turn that dispatch off ({@code PcDispatchSwitch}) and this behaves exactly as
+     *             stock Kafka Streams.
      */
+    @Deprecated
+    @DoNotCall("KStream-KStream join is not supported on the Parallel Consumer dispatch path (astubbs#255): join emission is stream-time gated and stream time does not advance there")
     <VO, VR> KStream<K, VR> join(final KStream<K, VO> otherStream,
                                  final ValueJoiner<? super V, ? super VO, ? extends VR> joiner,
                                  final JoinWindows windows);
```

Before the follow-up fix on astubbs/parallel-consumer#255, the same site had the two annotations and no tag. Everything the reader needed was in the string on the `@DoNotCall` line, which their IDE does not render.

Two sentences, four jobs:

1. **"Not deprecated by Apache Kafka"** kills the misattribution in the first five words, before anything else is read.
2. **"`{@code parallel-consumer-streams}` refuses this"** names the party speaking and links the issue.
3. **"because join emission is stream-time gated and stream time does not advance there"** is the same reason the `@DoNotCall` message gives, in prose. The reason is carried with the construct rather than written at the throw site, so the compile error, the runtime message and the javadoc stay consistent (`PcUnsupportedConstruct.java:114-119`).
4. **"Turn that dispatch off (`PcDispatchSwitch`) and this behaves exactly as stock Kafka Streams"** scopes the objection. It is conditional, and the condition is a switch the reader controls.

### Where the counts stop matching, and why that is the interesting part

| What the patch adds | Count | Spread |
|---|---|---|
| `@DoNotCall` | 59 | `KStream` 28, `KTable` 25, `KGroupedStream` 3, `CogroupedKStream` 3 |
| `@Deprecated` | 55 | `KStream` 28, `KTable` 21, `KGroupedStream` 3, `CogroupedKStream` 3 |
| New javadoc `@deprecated` tags | 55 | same spread as `@Deprecated` |
| Javadoc lines naming `{@code parallel-consumer-streams}` | 59 | 55 in new tags, 4 appended to Kafka's own |

The four-way gap on `KTable` is the whole story. Those are the foreign-key `join` and `leftJoin` overloads Kafka deprecated in 3.1 (`pc-streams.patch:894`, `:936`, `:978`, `:1020`), which is why the last row, not the second, is the one that proves full coverage. A guard built on `@Deprecated` counts alone would have been blind at four sites, and specifically at the four where getting the wording right is hardest.

### Five names, four interfaces, seven reasons

The refusal spans five method names (`join`, `leftJoin`, `outerJoin`, `suppress`, `windowedBy`) across four interfaces, and the `@DoNotCall` messages group by construct rather than sharing one generic string, so the compile error a user sees matches the runtime message the same call would produce. Seven distinct messages cover the 59 sites. On `KStream`: `KStream-KStream join` 12, `KStream-KTable join` 8, `KStream-GlobalKTable join` 8. On `KTable`: `KTable-KTable join` 12, `KTable-KTable foreign-key join` 12, `suppress` 1 (`pc-streams.patch:705` and its tag at `:699-702`). On both grouped-stream types, `windowedBy` 3 each, sharing one `observedStreamTime` message across both (`pc-streams.patch:16-23`).

### The verification that made the change safe to ship

Two checks are worth imitating whenever you edit javadoc inside a patch against third-party source.

- **A control arm on the doc tooling.** The tags were checked by running javadoc over patched and pristine sources under `-Xdoclint:all`, far stricter than the project's own `-Xdoclint:none`. Both arms produced an identical set of 36 errors and 15 warnings, all Kafka's own pre-existing complaints, so the tags introduce no new diagnostic. Recorded in the fix commit under astubbs/parallel-consumer#255.
- **Reading the tripwire instead of obeying it.** `parallel-consumer-streams/bin/regen-patch.sh` warns when the regenerated hunk count drops, on the theory that a drop means lost work (`regen-patch.sh:133-136`). This change tripped it, 112 to 110, as a false positive: inserting four lines of tag into each of three adjacent `KGroupedStream.windowedBy` javadocs brought the hunks inside diff's context window and merged them. The script's own header documents this exact failure mode and instructs the reader to investigate rather than assume (`regen-patch.sh:55-62`).

## Related

- astubbs/parallel-consumer#255 - the Kafka Streams on Parallel Consumer work, including the refusal layer and the javadoc fix.
- astubbs/parallel-consumer#271 - the PR carrying this branch, opened and unmerged as of this writing.
- [A type gate is a claim about a hierarchy you did not write](a-type-gate-is-a-claim-about-a-hierarchy-you-did-not-write.md) - the other failure mode of the same refusal feature. That one is about the runtime `instanceof` backstop missing a store type; this one is about the compile-time annotations misattributing themselves. Worth reading together.
- [Patch a dependency at build time without vendoring it](patch-a-dependency-at-build-time-without-vendoring-it.md) - the build mechanism through which these annotations are injected into Apache Kafka's own classes.
- [Kafka Streams task lifecycle callbacks do not mean what they are named](../integration-issues/kafka-streams-task-lifecycle-callbacks-do-not-mean-what-they-are-named.md) - the same family of lesson on a different axis: a name or marker is not its contract, so check who actually reads or calls it.
- [Status words belong in status artefacts](../conventions/status-words-belong-in-status-artefacts.md) - a sibling rather than an overlap: a symbol misleads its reader unless the surrounding artefact states what is actually true, applied to identifier naming instead of annotation authorship.
- `parallel-consumer-streams/README.md:230-251` - the user-facing statement of what you get and when, why nothing was deleted, and the evidence gate for taking a construct off the refused list.
- `docs/plans/2026-08-10-001-feat-refuse-unsupported-streams-surface-plan.md:276-283` - the original "no javadoc edit is needed" reasoning, struck through in place with the correction attached.
