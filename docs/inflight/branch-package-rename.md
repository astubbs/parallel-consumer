# Package rename `io.confluent.parallelconsumer.*` → `bz.stub.parallelconsumer.*`

<!-- inflight-type: task -->
<!-- inflight-impact: coordination -->


The package-rename project's entry. Branches working the rename keep their own account of it at this
same path, so when they converge git raises a conflict and whoever resolves it reads both and combines
them into the single entry for the project. That is deliberate, and more reliable than a
cross-reference someone has to remember to follow. This entry is one of those combinations: the
decision that opened the project, from the branch that proposed it, plus the findings recorded while
it was carried out.

Nothing below blocks anything; it exists so a finding already made is not re-derived, or worse,
mis-repaired.

**Status: decided, and landed on master in astubbs#294.** The reasoning is kept rather than deleted,
because "why was this done at that moment, and why could it not be deferred" outlives the change - and
because the traps recorded here are the reason the sweep was built the way it was.

Full task inventory, evidence and Apache 2.0 analysis:
[`docs/plans/2026-08-11-001-refactor-package-rename-plan.md`](../plans/2026-08-11-001-refactor-package-rename-plan.md).

## The decision, and the window it had to fit in

It went ahead in 0.6.0.0, and it had to land before v6 shipped. Nothing had ever been published under
the fork's `bz.stub.parallelconsumer` groupId, so no downstream code imported our packages yet.
Renaming then cost users nothing. Renaming after v6 asks everyone who adopted the fork to migrate a
second time, for a reason that will look cosmetic to them. There was no third moment.

**The README was written for the new namespace before the code was** - its `== Upgrading` section tells
users to find-and-replace their imports. That made the docs ahead of the code until the rename landed,
so had it slipped out of v6 the README would have had to be reverted in the same breath rather than
left describing imports that did not exist. It did not slip; the code caught up.

## Why it is worth doing at all

Apache 2.0 §6 grants no trademark rights. `io.confluent.*` is Confluent's mark, and it was our last
remaining use of it as an identifier in shipped artifacts on a fork Confluent does not maintain.
Moving off it reduces exposure; the licence permits the rename outright (§4), and the obligations it
does impose - retain the Confluent copyright headers, keep `NOTICE`, mark modified files - are
unaffected either way.

**This is a different question from the Apache trademark work.** That one is about the ASF's `Kafka`
mark in our product *branding*; this one is about Confluent's mark in our *namespace*. They share a
shape and nothing else - do not let a future session merge them into one "rebrand".

## The three things a sweep could not be trusted to catch

All three were real, and all three are handled on master. They are recorded because each one is a
class of failure that reports success, and the next namespace-wide change will meet the same shapes.

- **A clean `grep -rn "io\.confluent"` was never evidence the rename was complete.** Files encode the
  package as an escaped regex (`io\.confluent\.parallelconsumer\.`), invisible to both a
  find-and-replace and that verification sweep. Search allowing the backslash:
  `grep -rnE 'io[\\.]*confluent'`. `bin/rename-packages.sh` documents this as the first of the two
  findings that govern its design, and rewrites the escaped form explicitly.
- **The mutation gate failed open.** Stale, `bin/ci-mutation-test.sh` matched nothing, printed
  `PIT: no core main-source classes changed - nothing to mutate, skipping` and exited 0 - green
  forever while scoring zero mutants. Its patterns now name `bz.stub.parallelconsumer`; the standing
  instruction is unchanged - assert the lane actually scores mutants, do not accept the tick.
- **An ArchUnit rule could go vacuous silently.** `TestConventionRules.java` pins a fully-qualified
  class name as a *string*; stale, the condition never fires and the rule passes, so the guard keeping
  Docker-dependent tests out of surefire quietly stops guarding. `failOnEmptyShould` does not catch it.
  The rule now names the new package, and astubbs#294 also had to exempt the Testcontainers support
  package the rename brought into its scope.

## What the work actually was

Not the `sed`. `bin/check-copyright-headers.sh` decides provenance by exact path match against the
fork-point file listing, so moving the package directories makes every upstream-derived file look
fork-original and its retained Confluent header an error - 197 violations, measured by performing the
rename in a throwaway clone rather than predicting it. Redesigning that provenance model was the
engineering; 121 files then needed a `Modifications Copyright` line, which is bookkeeping on top.

## Settled, so nobody re-investigates

- **No wire-format exposure.** Offset metadata is magic-byte plus bitset/run-length plus base64; no
  class name reaches the wire. The rename cannot break offset compatibility. This was the main risk
  and it is closed. It is also the fact the published upgrade instructions lead with: an existing
  consumer group upgrades in place, with no offset reset or migration.
- **Downstream migration is small.** ~25 public types at most; all five example apps combined import
  8 distinct names, a typical consumer 4-6.

## The real completeness check

Vet every remaining `confluent` occurrence one at a time and confirm each is legitimate attribution
(`NOTICE`, copyright headers, upstream links, the pinned `master-confluent` mirror) rather than
something the sweep missed. Given the escaped-regex trap above, that pass - not the grep - is the
completeness check, and it is now mechanised: `bin/rename-packages.sh --verify-only` prints every
match it skipped inside its excluded set and every line held by a freeze region, so each exemption is
audited rather than assumed.

## The one legacy-token reference, and how it was repaired

Carried over from astubbs/parallel-consumer#289 - the change that cleared the legacy-token residue
ahead of the rename. It is recorded in full because the *reasoning* is what stops the next person
repairing it the wrong way.

`parallel-consumer-core/src/test-integration/.../integrationTests/KafkaSanityTests.java`, on the
javadoc of `pausedConsumerStillLongPollsForNothing`, read:

```java
/**
 * @link io.confluent.csid.asyncconsumer.BrokerPollSystem#pollBrokerForRecords
 */
```

`io.confluent.csid.asyncconsumer` is the project's pre-parallel-consumer package name and has no
tracked source file, so the rename script's completeness sweep flags it on the legacy token. It was
the **only** surviving occurrence: astubbs#289 deleted the rest, which were all inert logback logger
entries, and deliberately left this one alone rather than repair a reference into a package the rename
was about to move again.

What was established about it, and what the repair therefore had to do:

- **It is live, not dead.** The class survives as `bz.stub.parallelconsumer.internal.BrokerPollSystem`,
  and `pollBrokerForRecords` is still a real method on it. Only the package in the reference was
  stale. So the fix was to re-point it, not to delete it - the opposite of the treatment the logger
  entries got.
- **The method is `private`.** A javadoc `@see` or `{@link}` at *member* granularity will not resolve
  to it from another class, and doclint reports that as a reference-not-found error. Link the class
  and name the method in `{@code}`, or keep the pointer as prose.
- **`@link` there was not a javadoc tag.** It was used as a *block* tag; the inline form `{@link ...}`
  is the only valid one. Whatever the sweep rewrote it to, the surrounding form needed fixing too, or
  a doclint-enabled build would complain about an unknown block tag rather than about the package.
- Test sources are not javadoc'd today, which is why none of this ever surfaced as a build failure.

The javadoc now reads `{@code pollBrokerForRecords}` on
`{@link bz.stub.parallelconsumer.internal.BrokerPollSystem}` - class-granularity link, method named in
`{@code}`, inline form - which is exactly what the three points above prescribe.

## Do not assume the asciidoc tags in the example poms are dead

`exampleRepo` was unreferenced, which astubbs#289 removed - but that is **not** true of the tagging
apparatus generally, and the tempting generalisation is wrong. `src/docs/README_TEMPLATE.adoc`
carries `include::...[tag=exampleDep]` for the core, reactor and vertx example poms, and the
extracted `<dependency>` blocks are visibly present in the generated `README.adoc`. Deleting those
markers would break the published install instructions.

The asymmetry is in the history: both tags arrived in `7b4f5a5dd`, but the `exampleRepo` include was
deleted in `e7146adf0` (2020, "post release of 0.1 to repo1") while the `exampleDep` includes were
never removed.

The one region that genuinely was orphaned - `parallel-consumer-example-metrics/pom.xml`, whose
`exampleDep` markers no include named - has had its markers removed by astubbs#289. The template
documents the metrics module's `CoreApp` but never its dependency snippet; removal was chosen over
adding the missing include, so if a metrics snippet is ever wanted in the README the markers come
back with it.
