# `feats/native-image-sidecar` is superseded by astubbs#385, and retained as evidence

<!-- inflight-type: task -->
<!-- inflight-impact: coordination -->

`feats/native-image-sidecar` (two commits on top of `feats/polyglot-demos`, no PR - never had one)
is where the native-image discovery originally happened: a GraalVM build of the **full-engine**
sidecar, proven by a Python demo against a real engine and broker, carrying the large
reachability-metadata file that build required.

**Its work is superseded**: astubbs/parallel-consumer#385 (`feats/proxy-native-image`) re-proved
the claim on the decomposed stack against the engine-less shell - where one flag suffices and no
reachability metadata is needed, precisely because the engine is absent. Almost nothing ported,
and that asymmetry is the finding, recorded on astubbs#385's branch in
`docs/inflight/core-the-native-sidecar-is-easy-only-while-it-has-no-engine.md` and in
`bin/native-image-sidecar.sh`'s header, which preserves the old branch's full-engine recipe by
reference.
<!-- file-refs: N/A - both paths live on feats/proxy-native-image (astubbs#385), not on master or this note's branch -->

**Its evidence is not superseded.** The reachability metadata and the recorded build-failure
recipe on `feats/native-image-sidecar` are the prior art for the engine-present native build - a
question that became live again when the Wagon A stack was merged back into
`feats/proxy-requirements` and the engine returned there. Do not merge into
`feats/native-image-sidecar`, do not rebase it, do not delete it yet.

## Delete when

The engine-present native-image build lands (using or knowingly superseding
`feats/native-image-sidecar`'s reachability recipe) - then that branch has no remaining
evidentiary value and may be deleted, and this note with it.
