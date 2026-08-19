# Feature records for the experimental modules, held until the modules land

<!-- inflight-class: candidate -->


`docs/features/kafka-streams-experimental.yaml` and `kafka-connect-experimental.yaml` were written and
then removed from the corpus. They are held here until their modules exist.
<!-- file-refs: N/A - the sentence is about records that were written and then not kept -->

**Why.** Neither `parallel-consumer-streams-spike` nor `parallel-consumer-connect` is a module in
`pom.xml` or a directory in the tree. Both records carried `status: planned` with
`target_release: 0.6.0.0`, which is honest, but they also carried a Maven coordinate that reads as
copy-pasteable for an artifact that will not resolve. The corpus's own standard, set by the maturity
work, is not to assert what the tree contradicts.

**When they return.** Whichever PR lands each module restores its record in the same change, with
`status: published`, a real `since`, and setup that resolves. Do not rewrite the drafts - but do not
look for them in master's history either: the astubbs#273 squash-merge made them unreachable from
here. Where each draft actually is:

- **Streams**: its staged successor already lives at `docs/features/staging/kafka-streams-integration.yaml`.
- **Connect**: recovered verbatim from the side-line `origin/docs/v6-release-ideas-codex` and
  committed on the module-landing PR itself (astubbs#269, as `docs/features/staging/kafka-connect-experimental.yaml`).

Tracking: astubbs#255 for Streams, astubbs#240 for Connect.

## Delete when

Both modules are in the reactor and both records are back in `docs/features/`.
