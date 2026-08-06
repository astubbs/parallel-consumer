# Next: qualify the remaining issue references tree-wide

The reference gate requires any `#NNN` below 1000 to name its repo (`astubbs#NNN` / `upstream #NNN`),
because the fork's numbers sit entirely inside upstream's range - **48 of the 51 numbers cited in one
PR's files existed in *both* repos, meaning different things**. It checks **added lines only**, so it
never fires on text nobody is editing.

The files touched by the reference-convention work are done. **The rest of the tree is not.**

**Do this as its own PR** - a single pass to finish the migration, not opportunistic drips. It is a
no-behaviour-change edit, so it reviews quickly alone and would be unreadable mixed into feature work.

## Where they are

Overwhelmingly `.md`, with a small `.java` tail and a handful in `.yml`/`.adoc`/`.sh`. Count them
fresh rather than trusting a number here - the backlog shrinks as the sweep runs:

```bash
node -e '
const fs=require("fs"), cp=require("child_process");
const gate=require("./.github/scripts/issue-ref-gate.js");
const byExt = {};
for (const f of cp.execSync("git ls-files", {encoding:"utf8"}).trim().split("\n")) {
  if (gate.isExempt(f) || !/\.(md|adoc|java|ya?ml|sh|js)$/.test(f) || !fs.existsSync(f)) continue;
  for (const l of fs.readFileSync(f, "utf8").split("\n"))
    for (const m of gate.stripQualified(l).matchAll(/(?<![\w\/#])#(\d+)\b/g))
      if (+m[1] < 1000) byExt[f.split(".").pop()] = (byExt[f.split(".").pop()] || 0) + 1;
}
console.log(byExt);'
```

## The Java set is already classified - do not re-derive it

Every Java reference below was resolved against **both** repos and read in context. All are genuine
issue references. The two classes that once made this look risky are already dealt with: the fixture numbers
in the quarantine script tests are now `#999999`/`#999998` (unmistakably fake, above the threshold),
and the anchor form `<a href=".../issues/329">Github issue #329</a>` is stripped by the gate.

Most are `confluentinc` - unsurprising, since these comments largely predate the fork.

| File:line | Ref | Becomes |
|---|---|---|
| `ParallelConsumerOptions.java:314` | `#24` | `confluentinc#24` |
| `ParallelConsumerOptions.java:321` | `#21` | `confluentinc#21` |
| `ParallelEoSStreamProcessor.java:121` | `#356` | `confluentinc#356` |
| `internal/AbstractParallelEoSStreamProcessor.java:655` | `#809` | `confluentinc#809` |
| `internal/ProducerManager.java:110` | `#356` | `confluentinc#356` |
| `offsets/OffsetRunLength.java:90` | `#546` | `confluentinc#546` |
| `state/ShardManager.java:279` | `#857` | `confluentinc#857` |
| `state/WorkManager.java:240` | `#857` | `confluentinc#857` |
| `integrationTests/CustomConsumersTest.java:22` | `#195` | `confluentinc#195` |
| `integrationTests/MultiInstanceRebalanceTest.java:90` | `#188` | `confluentinc#188` |
| `integrationTests/MultiInstanceRebalanceTest.java:90` | `#189` | `confluentinc#189` |
| `integrationTests/MultiTopicTest.java:32` | `#184` | `confluentinc#184` |
| `integrationTests/RebalanceEoSDeadlockTest.java:40` | `#541` | `confluentinc#541` |
| `integrationTests/TransactionAndCommitModeTest.java:53` | `#25` | `confluentinc#25` |
| `integrationTests/VeryLargeMessageVolumeTest.java:53` | `#35` | `confluentinc#35` |
| `integrationTests/VeryLargeMessageVolumeTest.java:68` | `#35` | `confluentinc#35` |
| `integrationTests/chaostests/AbstractRevokeUnderWorkScenario.java:91` | `#857` | `confluentinc#857` |
| `integrationTests/chaostests/ChaosRevokeUnderWorkCooperativeIT.java:26` | `#857` | `confluentinc#857` |
| `integrationTests/chaostests/ChaosRevokeUnderWorkCooperativeIT.java:52` | `#857` | `confluentinc#857` |
| `integrationTests/chaostests/ChaosRevokeUnderWorkIT.java:16` | `#857` | `confluentinc#857` |
| `integrationTests/chaostests/ChaosRevokeUnderWorkIT.java:54` | `#857` | `confluentinc#857` |
| `integrationTests/chaostests/ProgressProbe.java:81` | `#857` | `confluentinc#857` |
| `integrationTests/chaostests/ProgressProbe.java:92` | `#857` | `confluentinc#857` |
| `integrationTests/utils/KafkaClientUtils.java:72` | `#857` | `confluentinc#857` |
| `integrationTests/utils/ManagedPCInstance.java:92` | `#857` | `confluentinc#857` |
| `integrationTests/utils/ManagedPCInstance.java:146` | `#83` | `astubbs#83` |
| `MockConsumerCommitFailedTest.java:22` | `#100` | `astubbs#100` |
| `ParallelConsumerOptionsTest.java:23` | `#355` | `confluentinc#355` |
| `ParallelEoSStreamProcessorTest.java:1038` | `#433` | `confluentinc#433` |
| `Quarantined.java:20` | `#80` | `astubbs#80` |
| `TestConventionRules.java:108` | `#100` | `astubbs#100` |
| `internal/ProducerManagerTest.java:62` | `#355` | `confluentinc#355` |
| `offsets/OffsetEncodingTests.java:70` | `#37` | `confluentinc#37` |
| `offsets/OffsetEncodingTests.java:73` | `#35` | `confluentinc#35` |
| `state/PartitionStateCommittedOffsetTest.java:157` | `#409` | `confluentinc#409` |
| `state/WorkManagerTest.java:663` | `#219` | `confluentinc#219` |

Three worth knowing before you start:

- **`@Tag("#355")`** (x2) is a JUnit tag name rather than prose, so this is a rename of the tag string
  rather than a prefix on prose. It is still an upstream reference and should read like one:
  `@Tag("confluentinc#355")`. Verified safe - nothing selects on that tag (no pom, script or workflow
  filters it), so renaming changes no behaviour.
- **`WorkManagerTest:663`** sits inside commented-out code. Qualify it anyway, or the next reader
  re-opens the question.
- **`#100`** appears twice and exists in *both* repos. Context settles it - `"a regression test added in PR #100"` is this fork's
  rebalance-commit fix.

## The `.md` bulk

Mostly mechanical, but resolve each one rather than prefixing blind. `#29`, `#100` and `#114` all
exist in both repos meaning different things.

## How, and the trap

`git grep` for candidates, but **classify each before rewriting it**. A previous mechanical sweep
rewrote 77 refs and got three classes wrong: `CHANGELOG.adoc` entries below 0.6.0.0 (whose header
already declares them upstream), every number in `upstream-map.yaml` (upstream by construction), and
an `AGENTS.md` convention shown *by example*, where rewriting the example changed the documented rule.
Only two of those three are protected by a **file exemption**: `CHANGELOG.adoc` and
`upstream-map.yaml`. The `AGENTS.md` example is not - it survives because it is wrapped in backtick
code spans, which `stripQualified` removes in any file. So when you rewrite prose that shows a
reference *by example*, check it is in a code span; nothing else will save it.

`EXEMPT_PATHS` has four entries, and the other two are exempt for different reasons again:
`upstream-pr-analysis.adoc` (every number in it is upstream by construction) and the gate's own
`issue-ref-gate.test.js` (deliberately full of fake references). Respect all four, but do not assume
"exempt" and "one of the classes that went wrong" are the same set.

Reuse the gate's own definition of "unqualified", so the sweep and CI agree:

```js
const gate = require("./.github/scripts/issue-ref-gate.js");
gate.stripQualified(line).matchAll(/(?<![\w\/#])#(\d+)\b/g)   // hits < 1000 need qualifying
```

Resolve numbers in both repos with `gh issue view N -R astubbs/parallel-consumer` and
`-R confluentinc/parallel-consumer`. Where the fork mirrors the upstream issue, cite the mirror - it
is the number a reader of this repo can act on.
