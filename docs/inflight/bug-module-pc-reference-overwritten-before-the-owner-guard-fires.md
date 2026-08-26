# A second processor overwrites the module's `pc` reference before the owner guard can refuse it

<!-- inflight-type: bug -->
<!-- inflight-impact: misdirection -->

<!-- post-merge: checked-begin -->
**Open, pre-existing, and the mirror image of the owner guard added by
astubbs/parallel-consumer#322.** That guard binds `PCModule`'s memoised collaborators to one owner,
so a second `WorkManager` or a second processor is refused rather than handed the first's instance.
This is the gap it cannot close.
<!-- post-merge: checked-end -->

`AbstractParallelEoSStreamProcessor`'s constructor calls `module.setParallelEoSStreamProcessor(this)`
before it resolves `module.brokerPoller(this)`. The setter is an unguarded Lombok `@Setter` on a
plain field, so a second processor built against one module **overwrites the module's `pc` reference
first, and only then is rejected**. The exception leaves the module pointing at a half-built
newcomer that nobody owns - so the failure is worse after the guard than before it, on that one path.

<!-- post-merge: checked-begin -->
**Why it was not fixed alongside that guard:** it is a main-code behaviour change on a constructor
path, outside a change whose subject was a broker-level race reproduction
(astubbs/parallel-consumer#322). Recorded here so the next session finds it rather than
rediscovering it.
<!-- post-merge: checked-end -->

**What settling it involves:** either the setter refuses a second owner the way the getters now do,
or the ordering changes so nothing is written until every guard has passed. The second is the safer
shape - a guard that fires after a mutation is a guard that has already lost - but it moves
constructor ordering, which is why it wants its own change and its own test.
