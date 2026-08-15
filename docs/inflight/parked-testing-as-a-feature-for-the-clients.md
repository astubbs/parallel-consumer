# Pre-v6: the shared test suite is a product feature, and the clients must be able to prove it

Owner's requirement, 2026-08-15. When ten client libraries ship at once, the first honest question a
reader will ask is whether they are generated slop. **The answer has to be visible, not asserted**:
every client passes the same conformance suite, exercising the same scenarios, with the same
assertions - and that fact belongs in the documentation as a feature of the product, not as a
footnote in a contributing guide.

This is pre-v6 because the credibility problem arrives with the release, not after it.

## What has to exist

- **A feature entry describing the shared conformance architecture** in the features corpus, written
  for a reader deciding whether to trust the libraries: one suite, one set of scenarios, run
  identically against every language, against a real broker as well as the mock.
- **Real testing-evidence entries per client module**, replacing the wave-one deferrals. The corpus
  and its gate already exist (`docs/data/testing-evidence.d/<artifact>.yaml`, merged and cross-checked
  by `bin/check-docs-data.sh` against the Maven reactor), so this is filling in a structure rather
  than inventing one - and the gate means a claim cannot quietly go stale.
- **The claim must be true when written.** Today each client runs one end-to-end scenario plus its own
  unit tests; the shared suite that would justify the strong version of this claim is a later unit.
  Write the entries when the suite exists, and until then say precisely what is covered. An
  overstated evidence entry is worse than a deferral, because the deferral is honest.

## Performance belongs in the same story

Run the shared performance test for each language client, and **compare each one against the
standard Java client** - the baseline the existing performance tests already measure
(`bin/performance-test.sh`).

- **Not against other languages' native Kafka clients.** That is a different product question, it
  invites an argument nobody needs, and it would drag librdkafka-based dependencies into modules
  built precisely to avoid them.
- **Not languages against each other.** A slow language looks bad for reasons that have nothing to do
  with this project.
- The number that matters is what the sidecar hop costs relative to in-process Java, per language.
  That is both the honest disclosure a user needs and, if the answer is good, the most persuasive
  thing the release can say.

## Why this is not just marketing

The suite is what makes ten independently-written clients defensible at all. It is also the thing
that catches the divergence a fan-out invites: a client that quietly reorders, drops or double-counts
records fails a shared scenario in a way its own hand-written test never would. Publishing the
architecture is therefore a claim the project must be willing to keep true - which is the right
pressure to be under.

Related: [`parked-chaos-suite-against-client-libraries.md`](parked-chaos-suite-against-client-libraries.md)
covers running the *chaos* suite against the clients, and its results-channel discussion is the
mechanism this note depends on.
