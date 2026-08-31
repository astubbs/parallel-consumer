# Concepts

Shared domain vocabulary for this project — entities, named processes, and status concepts with
project-specific meaning. Seeded with core domain vocabulary, then accretes as ce-compound and
ce-compound-refresh process learnings; direct edits are fine. Glossary only, not a spec or catch-all.

Seeded from the transactional-commit learning of 2026-08-07, so it covers the concurrency and
transactional-commit area. Other areas of the project are not yet described here.

## Relationships

The **produce lock** and the **commit lock** are the two sides of one readers-writer lock, and that
pairing is the load-bearing structure of the transactional commit path: many workers may hold the
produce side at once, but the controller's commit side excludes them all. Everything else in the
commit cluster — **dirty**, **eager processing during commit**, **commit lock timeout** — is a rule
about when one side may be taken or must wait.

## Naming

**AK core** (never bare "core")
The Apache Kafka client itself - `KafkaConsumer`, `KafkaProducer` - as opposed to anything in this
project. Say **AK core**, because bare "core" collides with **`parallel-consumer-core`**, the module,
and the two turn up a sentence apart whenever the engine is being compared against the client it
wraps. "For Java, core is the native client" and "build the core/sleep demo" were written in the same
document meaning opposite things.
<p>
"AK" is already the repo's abbreviation for Apache Kafka. The comparison demo names its serial lane
`AK_CORE` for the same reason.

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
Records handed to the worker pool and not yet resolved. Distinct from records merely fetched:
in-flight work is what a commit must wait for, and what a shutdown must drain.

**Verdict**
The outcome a delivered record reports back: succeeded, or failed. Distinct from the *return* itself,
because work can come back with no verdict at all — when the process holding it disappears before
reporting on it. A verdict-free return is not a failure: it consumes no retry attempt and earns no
retry delay. Returns are matched to a delivery, so a late one arriving after the record has already
been redelivered is recognised as superseded and ignored rather than acted on twice.

**Commit frontier**
The offset a partition would resume from if consumption restarted — the highest offset committed for
it. It is *exclusive*: it names the next record expected to be polled, not the last one completed.

The frontier advances only across a contiguous run of completed records, so a completed record sitting
above an in-flight one does not move it. A partition can therefore have most of its work done and still
be committing its starting offset. That property is also what makes the frontier the thing worth
asserting about a commit: which intermediate offsets a partition commits on the way there depends on
when the periodic commit happens to fire, but where it ends up does not.

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
