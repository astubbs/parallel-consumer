# Example: Kafka Streams driven by Parallel Consumer

Kafka Streams parallelises across *partitions*. Within one partition it hands records to your topology
strictly one at a time, so one slow record stalls every record queued behind it, even on unrelated keys.
This example runs against `parallel-consumer-streams`, which replaces that hand-off with Parallel
Consumer's worker pool so records on different keys in the **same** partition run concurrently. Your
topology stays ordinary Kafka Streams code with no Parallel Consumer API in it: taking the dependency is
the whole integration, and a static switch (`PcDispatchSwitch`) decides which path a task uses.

**The switch is OFF by default and this demo turns it on explicitly.** That default is deliberate, and the
reason is a missing refusal rather than caution: joins, windows, suppression, exactly-once and stream-time
punctuation are unsupported on the PC path and are *not yet refused*, so a topology using one would be
dispatched anyway and get wrong behaviour with nothing in the log to say so. The topology here is a
stateless `mapValues`, which is inside what the seam supports. `parallel-consumer-streams/README.md` owns
the full envelope.

The demo puts one 1500ms record at the head of a partition with 24 fast 25ms records behind it, then runs
that workload under stock dispatch and under PC dispatch and prints both sets of latencies.

Needs Docker; it starts its own Kafka. On a warm build the whole command below took 26s, of which the demo
itself reported 17s (3.6s of that starting the broker). A first run also builds the reactor from scratch
and pulls the Kafka image, so budget several minutes. About 8s of the demo is deliberate sleeping: four
arms, each with a 1500ms blocker and 24 records of 25ms. Expect some build noise first (`Jabel:
initialized`, `apply-patch: applied 33 hunk(s)`) - that is the build generating the patched Kafka classes,
which is normal.

## Run it

```bash
./mvnw -Pdemo -pl parallel-consumer-examples/parallel-consumer-example-streams-pc -am \
       -DskipTests process-classes
```

## What you should see

Scroll to the `SUMMARY` block at the bottom. It stands on its own. Abridged, with markers added here:

```
  Workload: 1 record costing 1,500ms then 24 costing 25ms, ONE partition, one stream thread, pool of 4.
  Latencies below share one t=0, the instant the first record entered the topology.

  Evidence this was really Parallel Consumer driving Kafka Streams:
    - the patched StreamTask loaded from parallel-consumer-streams, not the kafka-streams jar,
      and it carries the dispatch seam                                                          (1)
    - 25 of 25 records went through PC's worker pool in each PC arm, and 0 in each stock arm    (2)

  HEAD-OF-LINE BLOCKING (fast records on their own keys)
    fastest fast record      1,542ms stock  ->        30ms PC    51.40x                         (3)
    median  fast record      1,897ms stock  ->       143ms PC    13.27x                         (4)
    whole batch drained      2,274ms stock  ->     1,510ms PC     1.51x                         (5)
    through PC's pool             0 stock  ->        25 PC   (of 25 records)                    (2)

  NEGATIVE CONTROL (every record on ONE key, so PC has no concurrency to exploit)
    fastest fast record      1,537ms stock  ->     1,540ms PC     1.00x                         (6)
    median  fast record      1,892ms stock  ->     1,911ms PC     0.99x
    whole batch drained      2,277ms stock  ->     2,289ms PC     0.99x
    through PC's pool             0 stock  ->        25 PC   (of 25 records)

  NOISE FLOOR (the two STOCK arms, which do identical work - stock ignores keys)
    median  fast record      1,897ms       ->     1,892ms         1.00x                         (7)
```

Ratios are stock/PC: above 1.0 means PC won. Every latency is elapsed-since-start on a clock shared by all
24 fast records, so it measures queue position, which is what head-of-line blocking is.

1. **The demo is not lying to you.** These patched Kafka classes only take effect by winning classpath
   order. When they lose, nothing throws, and the demo prints plausible numbers that mean nothing. The run
   checks this first, prints where each class loaded from, and aborts loudly if the stock jar won. It also
   asks the loaded `StreamTask` for the `pcDispatcher` field the patch adds - and prints its declared type -
   because coming from the right jar and actually carrying the patch are two different questions with the
   same happy answer. `PartitionGroup` is checked in the other direction: it is *not* patched, so seeing it
   come from the stock jar proves the check can tell the two apart.
2. **Parallel Consumer really did the work.** That counter increments at exactly one place in the codebase
   (`PcDispatchCounters.onDispatchedToPool`, called only from `PcTaskDispatcher`), so it cannot read
   non-zero unless records went through PC's worker pool. Both numbers are read back from the run, not
   printed from constants: if any arm's counters disagree, the summary replaces this claim with a banner
   telling you not to quote the numbers. Each PC arm also prints `splitPollWaits` and `wakesOnWork`, which
   are the *second* mechanism - see (6) - reporting itself separately.
3. **The blocking is gone.** Under stock dispatch even the luckiest fast record waited ~1.5s for a record
   it shared nothing with but a partition. Under PC dispatch the quickest cost roughly its own 25ms.
4. **Quote this one, not row 3.** Row 3 approaches the workload's own 1500/25 cost ratio by construction,
   so it shows the blocking is gone without measuring how much is typically saved. The median does. Across
   five consecutive runs on one machine it read 13.3x, 14.5x, 14.8x, 17.9x and 19.0x.
5. **The batch really did finish sooner**, so the fast records were not sped up at the batch's expense.
6. **The honest part, and it has moved.** With every record on one key there is no concurrency to exploit,
   so PC should confer no advantage - and it does not: this arm lands on parity. On the branch this demo
   was written on it *lost*, at roughly 0.7x, because Kafka Streams polls and processes on one thread, so a
   blocked poll stalled dispatch that stock had nothing to lose by blocking. The seam here ships with a
   split poll wait that a worker completion can end, and closing that gap is what it bought. The run prints
   a section saying so, and `poll.ms` is deliberately left at its default rather than tuned down, which
   would narrow the same gap by mitigation instead.
7. **Read every small ratio against this.** The two stock arms process byte-identical work - stock Kafka
   Streams ignores keys, and the per-record cost here is chosen by value - so their difference is this
   run's own variance, measured inside the run. It matters because parity is not "1.00x": one run's control
   read 1.19x and that run's noise floor read 1.20x, so the control had measured nothing. Without the floor
   on screen that run looks like a finding.

Your numbers will differ; the shape is what matters. Note the "stock" arm is these patched classes with
the switch off, not the vanilla jar - `parallel-consumer-example-streams` is the separate,
provably-unpatched baseline. Per-key ordering and offset-commit correctness are properties of
`parallel-consumer-streams` covered by its own tests, not something this demo measures.

## What stops the demo flattering itself

Each of these was proven able to fire, by breaking the thing it guards and checking it went off.

| Guard | What it catches |
|---|---|
| `ClasspathGuard` | the patched classes lost the classpath race, or came from the right jar without the patch applied |
| per-arm counter warnings | an arm ran the other dispatch path from the one it says it ran |
| the `DO NOT QUOTE` banner | any arm warned, so the summary may not restate the evidence claim |
| the `!! UNEXPECTED` line | the run contradicts what the section predicted, so the fixed verdict prose below it is not to be trusted |
| the noise floor | a difference too small to be distinguishable from the machine |

The last of those was earned. With the dispatch switch sabotaged so the PC arms ran stock, the headline
measured 1.01x - and the check, which then asked only whether PC came out ahead at all, passed it and
printed "a fast record no longer waits for an unrelated slow one" over a run in which every fast record
had waited. It now requires a margin far outside any measured noise, and `LatencyScenarioTest` pins that
case, because a passing demo run never reaches this branch and the ordinary build does not run the demo.

## Try something else

Edit, then rerun the same command; it recompiles and reapplies the patch cleanly.

- **Pool size, costs, record count:** constants at the top of
  `src/main/java/bz/stub/parallelconsumer/examples/streams/pc/ArmRunner.java`.
- **Key layout:** the `allOneKey` flag passed to each `LatencyScenario` in `StreamsOnPcDemo.java`.
- **Turn the seam off entirely:** drop the `PcDispatchSwitch.enable(...)` call in `ArmRunner` and watch
  every guard above fire at once. That is the sabotage described in the previous section.
- **Add a demonstration:** implement `DemoSection` (title, run, optional summary) and add it to the list
  in `StreamsOnPcDemo.main`.
- **Skip broker startup on repeat runs:** put `testcontainers.reuse.enable=true` in
  `~/.testcontainers.properties`. The run says which mode it is in, and leaves the container running at
  the end only when reuse is genuinely on. Until you opt in, each run starts and discards its own broker.

## More

Caveats, limitations and how the patching works: `parallel-consumer-streams/README.md`. Tracking issue:
[astubbs#255](https://github.com/astubbs/parallel-consumer/issues/255). The module is alpha, and neither
it nor `parallel-consumer-streams` is published.
