# Feature records for the experimental modules, held until the modules land

`docs/features/kafka-streams-experimental.yaml` and `kafka-connect-experimental.yaml` were written and
then removed from the corpus. They are held here until their modules exist.

**Why.** Neither `parallel-consumer-streams-spike` nor `parallel-consumer-connect` is a module in
`pom.xml` or a directory in the tree. Both records carried `status: planned` with
`target_release: 0.6.0.0`, which is honest, but they also carried a Maven coordinate that reads as
copy-pasteable for an artifact that will not resolve. The corpus's own standard, set by the maturity
work, is not to assert what the tree contradicts.

**When they return.** Whichever PR lands each module restores its record in the same change, with
`status: published`, a real `since`, and setup that resolves. Recover the drafted content from git
history rather than rewriting it: they were removed on this branch, and both are good drafts.

Tracking: astubbs#255 for Streams, astubbs#240 for Connect.

## Delete when

Both modules are in the reactor and both records are back in `docs/features/`.
