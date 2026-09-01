# Next: what the Java demo seed left open

The seed landed in astubbs#328 (plan unit U35, first half). These are the things it deliberately did
not do, ranked by what they block. Each was surfaced by building or reviewing the seed, so none of
them is speculative.

## 1. The measurements are not reproducible across sessions, and nobody knows why

**This gates trusting any number the demo prints, so it outranks everything else here.**

The AK core arm measured 344-346 msg/s in one session and 299-303 in the next, on the same machine.
It is the denominator of every ratio in both tables, so a 15% shift moves every published figure.

The obvious suspect was a review fix that moved that arm's clock to start after its consumer is
built. **A control arm refuted it**: clock restored, nothing else changed, same host, 303 msg/s -
inside the corrected code's own spread. Host state is the remaining candidate and was **not**
isolated, so it is a hypothesis.

Until this is settled, treat the ratios as reproducible within a session and not across sessions.
The full record, including the numbers either side, is in the branch document - see the relocation
note at the bottom of this file.

## 2. The two transports disagree about `enable.auto.commit`

Owned by [`bug-direct-client-does-not-disable-auto-commit.md`](bug-direct-client-does-not-disable-auto-commit.md),
which has the evidence and the decision it needs. Listed here only because it is the one item on this
page that **ten client authors inherit** rather than the Java demo alone, and because the demo
currently works around it in a line that becomes redundant the moment it is closed.

## 3. Review findings surfaced and left un-acted

None of these breaks anything today. They are listed because each was verified during review and then
consciously not fixed, which is worth distinguishing from not noticed.

- **`DemoBroker#brokerImage` duplicates the CP-image derivation** in core's `BrokerIntegrationTest`.
  The reason is recorded at the method - referencing that class starts a singleton broker in a static
  initialiser - but that blocks calling the class, not extracting the pure sub-computation.
- **`run.sh` and the `Dockerfile` duplicate the build-classpath invocation and the `java -cp` launch**,
  and already differ in incidental ways. Nothing keeps them in step.
- **Nothing checks that the compose broker image tracks the derived one.** Both files say in prose
  that they must match; that is not a check. A unit test comparing the compose literal against
  `DemoBroker`'s derived value would close it.
- **`DemoBroker` has no tests**, though its supplied-broker branch, its properties map and its image
  derivation need no Docker at all.
- **The effective-configuration fingerprint omits what most moves the numbers** - JVM, core count,
  resolved broker image, whether a bootstrap was supplied. It is the demo's reproducibility promise,
  and it currently records the dials but not the machine. Keep the address itself out; a test guards
  that.

## 4. Not started, and correctly so

- **U35's second half - the reading demo.** Three modes (own-cluster / broker / mock), the TTY prompt
  and its documented non-TTY fallback, the `PLACE SERDE SETUP IN YOUR LANGUAGE HERE` marker, and the
  rate-limited sample of message content. `parallel-consumer-proxy/demo/README.md` states the
  boundary and why some of those are actively wrong for a comparison demo. **KTD40 in the plan still
  describes every demo as having the three modes**, so plan and seed disagree on paper until someone
  either amends KTD40 or records that it governs the reading demo only. That reconciliation is the
  real open item here, not the code.
- **The ten language demos.** Shape fixed, nothing blocking, start with Python because its worker
  processes are the hardest case for the non-occupying-wait rule.
- **The executor-count formula**, which gates the Python demo specifically - owned by
  [`blocker-executor-count-formula.md`](blocker-executor-count-formula.md).

## Relocation note for whoever merges astubbs#328

`branch-classic-comparison-demo.md` is a `branch-` file, so this directory's rules say it is deleted
when its work lands. **It currently holds the only copy of the fan-out's fixed shape, the seed's
decision history and every measured number** - including the AK core figures item 1 above depends on.
Deleting it wholesale would take all of that with it. Shrink it to what is still open and rename it,
rather than removing it.
