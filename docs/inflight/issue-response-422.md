# Draft response to astubbs#422 - posted by the pre-release sweep, not by this PR

<!-- inflight-type: task -->
<!-- inflight-impact: coordination -->
<!-- post-merge: exempt-file - a drafted issue reply, held until the pre-release sweep posts it.
     It deliberately outlives the PR that wrote it, so it cannot be written in post-merge terms. -->

Written while the context is here, per `docs/inflight/AGENTS.md` - the agents who did the work hold
the best context at merge time, and by release time it has to be re-mined from commit logs.

**Not posted, and it survives this PR.** It is deleted when it is posted and not before; the sweep in
[`docs/releasing.md`](../releasing.md), "Post the drafted issue responses before you freeze the
section", is what consumes it, so all of these go out together with one view of what shipped.

---

Fixed in astubbs/parallel-consumer#422.

**What changed for you.** In `PERIODIC_TRANSACTIONAL_PRODUCER` mode, a commit interval you set
explicitly is now always kept - including
`commitInterval(ParallelConsumerOptions.DEFAULT_COMMIT_INTERVAL)`, the case this issue reported,
where the value was silently replaced with the 100ms transactional default. An interval you do *not*
set still resolves to 100ms under transactions and to Kafka's five seconds everywhere else. Nothing
about what a running processor commits changes, and no existing configuration that already worked
behaves differently.

One thing does read differently, and only before you hand the options over. The resolution now
happens in `getCommitInterval()` rather than in `validate()`, so an options object built for
transactional mode with no interval set reports 100ms straight away, where it used to report five
seconds until a processor was constructed. If you log or inspect options before that point, that is
the number you will see - and it is the one the processor was always going to use.

**Why `equals` was not the fix.** The retired note behind this issue suggested swapping `==` for
`equals`, and that would have made things worse. A user who writes `Duration.ofSeconds(5)` gets a
fresh object, so identity is false and their value is already kept today - the right outcome. Under
`equals` every explicit five seconds would have been reduced to 100ms, turning a narrow defect into a
broad one. The fix instead stops inferring "unset" from the value at all: the builder leaves the
field null, and `getCommitInterval()` resolves null from the commit mode. Because the resolution is
in the getter rather than in `validate()`, a null can never escape whatever order a caller reads the
options in.

**The regression test.**
`ParallelConsumerOptionsTest#explicitDefaultConstantInTransactionalModeIsKept` is the reproduction,
and was observed red before the fix (`expected: PT5S but was: PT0.1S`). It sits in a four-way truth
table alongside `#explicitFiveSecondsInTransactionalModeIsKept` - which guards specifically against
the rejected `equals` change - plus the two unset arms that keep the auto-reduction and its scoping
to transactional mode honest.
