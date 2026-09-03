# Concepts

Shared domain vocabulary for this project — entities, named processes, and status concepts with
project-specific meaning. Seeded with core domain vocabulary, then accretes as ce-compound and
ce-compound-refresh process learnings; direct edits are fine. Glossary only, not a spec or catch-all.

Seeded from the transactional-commit learning of 2026-08-07 (the concurrency and
transactional-commit area) and extended from the guard-lexing learning of 2026-09-01 (the agent
harness area). Other areas of the project are not yet described here.

## Relationships

The **produce lock** and the **commit lock** are the two sides of one readers-writer lock, and that
pairing is the load-bearing structure of the transactional commit path: many workers may hold the
produce side at once, but the controller's commit side excludes them all. Everything else in the
commit cluster — **dirty**, **eager processing during commit**, **commit lock timeout** — is a rule
about when one side may be taken or must wait.

## Parallel consumption

**Control loop**
The project's single controller thread: it drains completions, decides when to commit, and hands work
out to the worker pool. It is the only thread that commits, which is why commit decisions are
described from its point of view.

**Broker poller**
The thread that fetches records from the broker and keeps the consumer's group membership alive,
separately from the control loop. Keeping it distinct from the control loop matters: a stalled
controller and a stalled poller are different failures with different symptoms.

**Shard**
The unit of ordering. Records are distributed to shards by key or by partition depending on the
configured ordering, and a shard processes its records in order while different shards run in
parallel. This is how the project gets concurrency without adding partitions.

**Admission target**
The number of records the engine allows in flight at once — the control variable that governs
concurrency. Today it is derived statically from the user's configured concurrency limit; the self-scaling
work makes it adaptive. Distinct from thread count: threads are capacity, admission is the throttle, so the same
mechanism governs the thread-pool and async engines alike.

**In-flight work**
Records handed to the worker pool and not yet resolved as succeeded or failed. Distinct from records
merely fetched: in-flight work is what a commit must wait for, and what a shutdown must drain.

**Commit frontier**
The offset a partition would resume from if consumption restarted — the highest offset committed for
it. It is *exclusive*: it names the next record expected to be polled, not the last one completed.

The frontier advances only across a contiguous run of completed records, so a completed record sitting
above an in-flight one does not move it. A partition can therefore have most of its work done and still
be committing its starting offset. That property is also what makes the frontier the thing worth
asserting about a commit: which intermediate offsets a partition commits on the way there depends on
when the periodic commit happens to fire, but where it ends up does not.

**Assignment epoch**
A per-partition counter incremented each time the partition is assigned to this instance. Records
are stamped with the partition's current epoch as they are polled and carry it through processing;
work whose stamp no longer matches is stale — fetched under an assignment that has since been
revoked — and is discarded rather than completed or committed. This is what stops work still in
flight across a rebalance from acting on a partition the instance no longer owns.

**Revoke path**
The work an instance performs while the group is taking partitions away from it. It runs inside the
consumer's revocation callback, which the broker poller executes as part of its own fetch call — so
everything done there is charged against the interval the group allows between fetches, and an
instance that dwells too long is judged dead and evicted. That budget is why the revoke path commits
opportunistically rather than waiting: it prefers to let uncommitted work be redelivered to the new
owner over risking the member's membership. It is also where several of this project's hardest
defects have clustered, because it is the one place where rebalance handling, committing, and
in-flight work meet on a thread that must not block.

**Back-pressure pause**
The broker poller pausing its own subscription because the engine's internal buffers are full —
self-imposed, invisible to the user, and expected to release itself once processing catches up.
The authority for whether a partition is paused this way is the Kafka consumer's own pause state;
the engine asks it rather than keeping its own record, because rebalances alter that state in
protocol-dependent ways no local copy can track.

**User pause**
The user-facing paused state of the whole engine: work stops being handed to the worker pool, but
in-flight work completes, pending commits still happen, and polling may continue until buffers
fill. Deliberately distinct from a back-pressure pause — it is an engine state, not a broker-level
subscription pause.

## Transactional commit

**Produce lock**
The shared side of the transactional lock, taken by a worker for as long as it may still produce
records into the current transaction. Many workers hold it concurrently. Whether it is taken before
the user's function runs or only when the result is produced depends on eager processing during
commit.

It is released only once the completed work has been handed back to the controller — releasing it
earlier would let a commit collect offsets that do not yet account for work already produced, which is
the exactly-once invariant this lock exists to protect.

**Commit lock**
The exclusive side of the transactional lock, taken by the control loop before it collects offsets, so
that no further records can enter the transaction while the offsets for it are being gathered. Because
it is exclusive, acquiring it means waiting for every held produce lock to be released.

Acquisition is bounded: if the wait exceeds the configured limit, the attempt fails rather than
blocking forever, and the failure is fatal to the instance. That deliberate bound is what turns "a
user function is taking too long to allow a commit" into a visible error instead of a hang.

**Eager processing during commit**
Whether workers may begin processing new records while a commit is in progress. When disallowed, a
worker takes the produce lock before running the user's function, so a slow function blocks commits;
when allowed, the lock is taken only at produce time, and processing overlaps the commit. The choice
trades commit latency against throughput.

**Dirty**
A partition has at least one successfully completed record whose offset has not yet been committed.
Only a *success* makes a partition dirty — a failure does not — and the controller attempts a commit
only when something is dirty.

The asymmetry is load-bearing: a partition whose records are all failing is never dirty, so no commit
is attempted for it, and anything waiting on a commit-time behaviour will wait indefinitely.

## Test reliability

**Load-tightness flake**
A test that fails because a deadline or assertion margin is too tight to survive a contended machine,
rather than because the code under test is wrong. Named as a family here because its members look
identical to real product stalls and must be told apart before either is "fixed".

**Unforceable trigger**
A test that awaits a consequence whose precondition it cannot guarantee will occur. Distinct from a
load-tightness flake: the margin is not too small, the awaited event may simply never happen in some
interleavings, so no timeout is long enough to make the test reliable.

**Tick-path assertion**
A test assertion that pins the intermediate observations a system passed through on its way to a settled
state, rather than pinning the settled state itself. Because those intermediates are only captured when
a periodic action happens to fire, the assertion's truth is a property of the machine's speed rather
than of the code under test — green on an idle box, red under load, with both readings correct.

Distinct from a load-tightness flake and an unforceable trigger: no deadline is too tight, and no
awaited event is missing. The awaited condition is one of several legitimate outcomes, and once a
different one has occurred it can never become true, so the wait always expires in full.

**Timing proxy**
A bound on elapsed time asserted as though it established a correctness property it cannot actually
reach — "nothing has progressed for N seconds" standing in for "it is wedged". Because a busy system
and a stuck one both stop advancing the watched value, the bound cannot separate them, and every
crossing needs a second experiment before it means anything.

Distinct from a load-tightness flake, and the distinction decides the fix. A load-tightness flake has
the right property and too small a margin, so more margin repairs it. A timing proxy has the wrong
property, so widening only postpones the next false crossing and blinds the bound to the defect it was
sized against.

Two of its behaviours mislead readers rather than tests. Its recorded peak is fixed by the instrument
— bound plus detection latency — so repeated near-identical measurements read as a signature while
discriminating nothing. And because any one crossing fails the whole run, its false-positive rate
compounds with the number of independent scenarios, so a suite gets redder as it gets more thorough.

**Observation (as against a violation)**
A probe finding that is measured and reported but never fails the run, as distinct from a violation,
which gates. The split exists so a detector aimed at a timing property can keep producing its number
without asserting a verdict it cannot support.

Demotion suppresses the finding, never the measurement: the peak a demoted detector records is what a
future re-calibration reads, so a detector that stopped measuring would delete that evidence with
nothing going red to say so. An observation's only reader is a passing run's own output, which is why
where it is printed matters as much as that it is recorded.

**Ambient probe**
The always-on recorder attached to broker integration tests, which annotates a failure with
consumer-group progress evidence so the contention-versus-product-bug question is answered before
manual diagnosis starts. Its clean verdict is only informative when its detectors could have fired for
the test in question — a short, low-volume test cannot trip them, so it needs a positive control like
any other instrument.

**Red-proof**
The verification that a new or extended test fails against the code as it was before the fix it
guards — a regression test that has never failed proves nothing.

**Priced bound**
A scenario, iteration or repetition budget on a probabilistic probe that was derived from a measured
per-attempt hit rate, rather than chosen because the probe passed at it. The distinction is the whole
difference between a test and a flake: a probe that explores for a race has a hit *rate*, and only a
budget priced against that rate has a known miss probability.

A priced bound is a claim about the machine it was priced on, because the same probe finds the same
race at materially different rates on different hardware — so a rate quoted without its machine is
incomplete in the way a distance quoted without units is. Pricing a bound therefore also means
recording which machine it belongs to, and treating a bound carried onto other hardware as
unmeasured until it is measured there.

**Starved run**
A deliberate run of a probabilistic probe far below its intended budget, so that it misses often
enough for the miss fraction to estimate the per-attempt hit rate. It is the cheap way to price a
bound: runs at the intended budget almost never miss and so carry almost no information, while the
rate recovered from starved runs prices every candidate budget at once.

The proof requires a deliberately mismatched pair: old code, new tests. Any procedure that reverts
both together produces a matched pair and a vacuous pass, so a red-proof that does not go red is
first evidence against the method, not for the code. The same demand applied to an analyser rather
than a test is what catches inert configuration.

**Inert configuration**
Analysis or build settings that are present in the source, syntactically valid, and never reach the
run they were written for - so the tool executes correctly against a configuration that is not the
one you wrote. Distinct from a broken tool: nothing errors, nothing is skipped, and the report is
truthful about a scope nobody intended.

It is invisible to every signal except a count, because the absence of findings it produces is
indistinguishable from a clean codebase. Suppressions are the mirror case: one matching nothing looks
exactly like one that works. The verification is therefore to assert the number - that a disabled
rule reports zero, that an enabled one reports more than zero - never to observe that the build
passed.

**Filtered diagnostic**
A diagnostic the code emits correctly and no reader can see, because the logging profile in force
suppresses that level for the package it was written in. Distinct from a missing diagnostic: the call
executed, the evidence was produced, and only the transport discarded it. Neighbour of an inert
configuration - both make an absence unreadable, one by never reaching the tool, the other by never
reaching the reader.

A search for the line returns the same nothing whether the code path ran or not, so silence carries
no information in either direction. Two habits follow: prove the level reached the run before
believing any zero, and when both branches of a decision are worth observing, emit them at the same
level - logging only one makes "took the other branch" indistinguishable from "never reached the
fork". Evidence a failure message must carry belongs in the assertion, which no profile can filter.

**Positive control**
An arm of a measurement whose only job is to register a hit, proving the instrument could have detected
something on this run. Its own reading is never the result — it is what licenses reading every other
number, so a zero there makes the rest of the run uninterpretable rather than clean.

Required wherever a negative is the outcome being reported, because a tool that observed nothing and a
tool that could observe nothing produce the same output.

**Control arm**
An arm that declares the anomaly it is watching for to be *forbidden*, so the run fails if it appears.
Distinct from a positive control, which must fire: a control arm must not, and the distinction is what
separates a checked claim from an unchecked one. The same absence observed without that declaration is
only a bound at the sample size reached.

**Faithful arm**
An arm that keeps every real surrounding access in place, run alongside a reduced arm that strips them,
so the pair says whether the surrounding code was closing the hole by accident rather than by design.
The gap between the two rates is the result; collapsing the arms into one deletes it.

**Replica probe**
A probe that reproduces the code it models by hand instead of importing it — necessary when the probe
must control declarations the real code does not expose, and bound to its subject by nothing but
whoever copied it. Its distinguishing property is that it decays silently: when the modelled code
moves, the probe keeps passing, so it needs a correspondence check that fails on divergence or it is
only as current as its last manual review.

## Ratchet

A gate that can only turn one way: the recorded set of accepted findings may **shrink**, never
silently grow. Fix one, delete its entry; introduce one, the build fails.

The word is used here for an **identity set**, not a threshold - the distinction is the whole point.
A ceiling of "at most N findings" is satisfied just as well by fixing one defect and introducing
another, so it cannot tell a codebase that improved from one that swapped a problem for a different
problem. Keying on the identity of each finding closes that: an unfamiliar identity is a new defect,
and a missing one means somebody fixed something without recording it, which is how a set quietly
stops meaning anything. Both directions fail on purpose.

A ratchet is therefore a way to adopt a strict check against code that cannot pass it yet: accept
today's findings as known, and every new one is blocked from that moment. It is not a suppression
list - a suppression says "never report this", where a ratchet says "this one is already on the
books, and here is the list you are expected to shorten".

## Refactor window

A **refactor candidate** is a file the project has already decided to decompose, listed as such
rather than inferred from size or complexity. The decision is the entry condition: a large file
nobody has committed to breaking up is not a candidate.

Its **refactor window** is the period in which no large in-flight change stands on it, so a
decomposition can land without forcing an expensive merge onto somebody else's unfinished work. The
window is measured by the **largest single divergence** any live branch holds against the mainline
for that file - not by how many branches touch it. The two come apart routinely: a file every branch
adjusts by a line or two is open, while one branch rewriting half of it is enough to close the
window on its own. Counting branches answers a question nobody asked.

A window closes and reopens on its own as work lands, so the state is derived rather than declared,
and nothing about it is stored: a remembered verdict is a second thing that can be wrong.

## Agent harness

### Guard
A check that runs automatically against an agent's action and can refuse it. Guards are split by
failure direction, and the split decides their construction: a **refusing guard** must fail closed
(a guard that cannot run must still block, because its whole value is stopping the action), so it
inlines its logic rather than depending on anything it might fail to load; an **advisory reminder**
must fail open (a reminder that breaks must stay silent rather than jam the action it decorates),
so it may share helpers freely. The two look alike from outside - both observe the same actions -
which is why the distinction is recorded per guard rather than inferred from where one is installed.

### Advisory reminder
A guard that only informs: it surfaces context alongside an action - open work, drift, a caveat -
and never blocks. Its failure budget is the mirror of a refusing guard's: silence is acceptable,
blocking is not, and a reminder that fires too often trains its reader to skip it, which is the
same end state as one that never fires.

### Labelled fallback
The rule that an automated answer must name its own provenance, and that an answer derived from a
weaker source must say so. A guard that cannot read the authoritative fact (the thing the action
itself states) degrades tier by tier to weaker sources, and the message carries which tier
answered - so a wrong answer is checkable instead of confidently misleading. The discipline exists
because the failure mode it prevents is silent: a guess presented as a measurement reads exactly
like a measurement.

### Gate
A repository check that a build or merge must pass, run both locally and in CI from one shared set
so the two cannot drift. Gates differ from guards in what they inspect: a gate examines the state
of the tree or the pull request, while a guard examines an action about to happen. A gate that
cannot run must say so loudly - a skipped gate counted as a pass is the silent failure this
vocabulary keeps naming from different sides.

## Flagged ambiguities

- **"Stall", "load-tightness flake" and "unforceable trigger" had been used loosely for the same red
  test.** They are distinct: a stall is the product failing to make progress, a load-tightness flake is
  a real deadline missed under contention, and an unforceable trigger is an awaited event that never
  occurred. All three present as the same expired await, and the whole diagnostic difficulty of this
  area is telling them apart.
- **An un-priced bound is the fifth member, and the only one that is a test-authoring fault rather
  than a diagnosis problem.** A probabilistic probe whose budget was chosen because it passed, not
  priced against a measured hit rate, presents as an ordinary intermittent red. It is told apart by
  asking whether the probe's hit rate was ever measured at all: if it was not, the red is neither a
  product bug nor contention but an unfinished calibration, and the fix is to price the bound rather
  than to diagnose the run.
- **A tick-path assertion presents as that same expired await, and is the fourth member of the
  confusion.** It is told apart by asking whether what the test actually saw is *also correct*: the
  other three all mean the expected thing did not happen, while a tick-path assertion means something
  equally valid happened instead.
- **"Paused" names three distinct things.** A user pause is an engine state; a back-pressure pause
  is the poller pausing the broker subscription; and the Kafka consumer's own pause state is the
  authority the back-pressure pause manipulates. A user report of "paused consumption" typically
  describes none of them — a stall presenting as a pause — so the word alone attributes nothing.
