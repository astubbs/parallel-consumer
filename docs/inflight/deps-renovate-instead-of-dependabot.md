# Should the dependency bot be Renovate rather than Dependabot

<!-- inflight-type: task -->
<!-- inflight-impact: deps-debt -->

**Leaning: no, on current evidence.** This note began as a case *for* switching and the case did not
survive checking. It is kept rather than deleted because the reasoning is the useful part - the next
person to have this idea should meet the counter-evidence, not re-derive it.

## The original argument, and why it was wrong

The claim was that [`deps-deferred-majors.md`](deps-deferred-majors.md) is a register of held-back
upgrades that the bot cannot read, so it keeps proposing them and a human keeps re-deciding, and that
Renovate's `packageRules` would fix that by holding the decision and its reason together in config.

**Every part of that is already true of the Dependabot config we have.** `.github/dependabot.yml`
carries scoped `ignore` entries, each with its reason in a comment, and they line up with the
register's rows:

```yaml
- dependency-name: "org.apache.kafka:*"          # Kafka 4 (needs Java 11 baseline)
- dependency-name: "org.junit.jupiter:*"         # JUnit 6 (needs Java 17 + ArchUnit engine)
- dependency-name: "org.testcontainers:*"        # Testcontainers 2.x
- dependency-name: "io.vertx:*"                  # Vert.x 5
```

It also already batches by risk class - a `maven-non-major` group covering minor and patch updates -
which was the other half of the argument.

**And the duplication it complained about is deliberate, in the right direction.** The config's own
comment says declaring ignores there is better than `@dependabot ignore` PR comments, "whose ignore
conditions live invisibly in Dependabot's state and give future maintainers no clue why a dependency
silently stopped updating", and then points at the register: "Full rationale for each:
docs/inflight/deps-deferred-majors.md." A short reason at the rule plus a pointer to the long one is
the pattern this repository wants, not the drift the original note accused it of. That work landed in
astubbs#78 (the earlier draft of this note mis-cited astubbs#76, which is an unrelated closed bump).

## What would actually still have to be true to justify a switch

A real gap, not a general preference. Candidates, none of them established:

- **A release-age delay.** Not proposing a version until it has been public for some days is cheap
  insurance against a bad or withdrawn release. Renovate has `minimumReleaseAge`. **Whether
  Dependabot's own cooldown setting covers this is unchecked** - check before using it as an argument.
- **Grouping by Maven property rather than by artifact pattern.** This POM drives versions through
  properties (`<kafka.version>`, `<vertx.version>`), and upstream's Renovate branch names show it
  bumping those properties directly. Our `patterns`-based groups reach a similar result by a different
  route; nobody has shown a case where the difference bites.
- **A concrete instance of the bot proposing something the ignore list should have caught**, which
  would mean the current mechanism is failing rather than merely being unfashionable.

Against any of that: Renovate is a third-party app where Dependabot is native and already configured,
its config surface can produce more noise than it removes, and the migration work is real - every
ignore and group would have to be re-expressed and the register repointed.

## How this note got it wrong, which is the transferable part

The original was written from the branch names upstream left behind, plus the register, without
opening `.github/dependabot.yml`. Every claim it made about "what Dependabot cannot do" was a claim
about a config file that was sitting in the repository, unread. It was caught by a review pass on the
same branch, a few hours later.

That is precisely the failure this branch documents in
[`upstream-mirror-bodies-are-stale.md`](upstream-mirror-bodies-are-stale.md): a confident record
written from adjacent evidence rather than from the thing itself, which then reads as settled. Worth
keeping visible here, because a note arguing for a tooling change is exactly the kind that gets
believed later without anyone re-checking its premise.

## Delete when

A concrete gap above is established and acted on, or someone decides the question is closed and says
so in [`deps-deferred-majors.md`](deps-deferred-majors.md) so it is not re-proposed from scratch.
