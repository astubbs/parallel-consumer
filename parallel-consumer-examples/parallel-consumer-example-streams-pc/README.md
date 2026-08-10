# Example: Kafka Streams driven by Parallel Consumer

Kafka Streams parallelises across *partitions*. Within one partition it hands records to your topology
strictly one at a time, so one slow record stalls every record queued behind it, even on unrelated keys.
This example runs against `parallel-consumer-streams`, which replaces that hand-off with Parallel
Consumer's worker pool so records on different keys in the **same** partition run concurrently. Your
topology stays ordinary Kafka Streams code with no Parallel Consumer API in it: taking the dependency is
the whole integration, and a static switch (`PcDispatchSwitch`) decides which path a task uses.

The demo puts one 1500ms record at the head of a partition with 24 fast 25ms records behind it, then runs
that workload under stock dispatch and under PC dispatch and prints both sets of latencies.

Needs Docker; it starts its own Kafka. On a warm build the whole command below took 44s, of which the
demo itself reported 23s (6s of that starting the broker). A first run also builds the reactor from
scratch and pulls the Kafka image, so budget several minutes. About 8s of the demo is deliberate
sleeping: four arms, each with a 1500ms blocker and 24 records of 25ms. Expect some build noise first
(`Jabel: initialized`, `apply-patch: applied 30 hunk(s)`) - that is the build generating the patched
Kafka classes, which is normal.

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
    fastest fast record      1,555ms stock  ->        28ms PC    55.54x                         (3)
    median  fast record      1,904ms stock  ->       344ms PC     5.53x                         (4)
    whole batch drained      2,278ms stock  ->     1,611ms PC     1.41x                         (5)
    through PC's pool             0 stock  ->        25 PC   (of 25 records)                    (2)

  NEGATIVE CONTROL (every record on ONE key, so PC has no concurrency to exploit)
    fastest fast record      1,599ms stock  ->     1,662ms PC     0.96x                         (6)
    median  fast record      1,927ms stock  ->     2,794ms PC     0.69x
    whole batch drained      2,332ms stock  ->     4,012ms PC     0.58x
    through PC's pool             0 stock  ->        25 PC   (of 25 records)
```

Ratios are stock/PC: above 1.0 means PC won. Every latency is elapsed-since-start on a clock shared by all
24 fast records, so it measures queue position, which is what head-of-line blocking is.

1. **The demo is not lying to you.** These patched Kafka classes only take effect by winning classpath
   order. When they lose, nothing throws, and the demo prints plausible numbers that mean nothing. The run
   checks this first, prints where each class loaded from, and aborts loudly if the stock jar won. It also
   asks the loaded `StreamTask` for the `pcDispatcher` field the patch adds, because coming from the right
   jar and actually carrying the patch are two different questions with the same happy answer.
2. **Parallel Consumer really did the work.** That counter increments at exactly one place in the codebase
   (`PcDispatchCounters.onDispatchedToPool`, called only from `PcTaskDispatcher`), so it cannot read
   non-zero unless records went through PC's worker pool. Both numbers are read back from the run, not
   printed from constants: if any arm's counters disagree, the summary replaces this claim with a banner
   telling you not to quote the numbers.
3. **The blocking is gone.** Under stock dispatch even the luckiest fast record waited ~1.5s for a record
   it shared nothing with but a partition. Under PC dispatch the quickest cost roughly its own 25ms.
4. **Quote this one, not row 3.** Row 3 approaches the workload's own 1500/25 cost ratio by construction,
   so it shows the blocking is gone without measuring how much is typically saved. The median does.
5. **The batch really did finish sooner**, so the fast records were not sped up at the batch's expense.
6. **The honest part.** With every record on one key there is no concurrency to exploit and PC comes out
   *slower*, at 0.69x median and 0.58x on the whole batch. It is shown rather than hidden because it is
   what proves rows 3 to 5 are key concurrency and not just a generally faster path. The run prints a
   section explaining the cause (Kafka Streams couples polling and processing on one thread) and why it is
   not tuned away here. It is also what bounds the one term the runner does not control: the stock arm
   always runs first, so PC meets a warmer JVM, and a warm-up effect large enough to carry row 4 could not
   leave PC losing here.

Your numbers will differ; the shape is what matters. Note the "stock" arm is these patched classes with
the switch off, not the vanilla jar - `parallel-consumer-example-streams` is the separate,
provably-unpatched baseline. Per-key ordering and offset-commit correctness are properties of
`parallel-consumer-streams` covered by its own tests, not something this demo measures.

## Try something else

Edit, then rerun the same command; it recompiles and reapplies the patch cleanly.

- **Pool size, costs, record count:** constants at the top of
  `src/main/java/io/confluent/parallelconsumer/examples/streams/pc/ArmRunner.java`.
- **Key layout:** the `allOneKey` flag passed to each `LatencyScenario` in `StreamsOnPcDemo.java`.
- **Add a demonstration:** implement `DemoSection` (title, run, optional summary) and add it to the list
  in `StreamsOnPcDemo.main`.
- **Skip broker startup on repeat runs:** put `testcontainers.reuse.enable=true` in
  `~/.testcontainers.properties`. The run says which mode it is in, and leaves the container running at
  the end only when reuse is genuinely on. Until you opt in, each run starts and discards its own broker.

## More

Caveats, limitations and how the patching works: `parallel-consumer-streams/README.md`. Tracking issue:
[astubbs#255](https://github.com/astubbs/parallel-consumer/issues/255). The module is alpha.
