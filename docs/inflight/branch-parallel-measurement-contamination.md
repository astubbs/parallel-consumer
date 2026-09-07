# Two branches' parallelism measurements may have a contaminated control arm

<!-- inflight-type: bug -->
<!-- inflight-impact: misdirection -->
<!-- inflight-state: deferred - after v6, the affected branches are not in the merge order -->


<!-- post-merge: checked -->
`ci/reenable-parallel-tests` and astubbs/parallel-consumer#105 both measure JUnit execution parallelism. Any
numbers they gathered before core stopped configuring the other modules' runners were taken against an "off"
arm that was not off everywhere: core's `src/test/resources/junit-platform.properties` was packaged at the root of the
core **tests** jar, so the eight modules depending on that jar (the four integrations plus every example) ran
at `factor=20` regardless of their own `${parallel-tests}` setting.
<!-- file-refs: N/A - describes where the file WAS when it caused the contamination -->

Re-take any measurement from before that, or confirm the arm it used was unaffected. This was written before
either had an open PR, which is why it is here rather than in a review comment.

<!-- post-merge: checked -->
**Settled for astubbs/parallel-consumer#105: its arm WAS affected.** The leak was closed by
astubbs/parallel-consumer#265, which deletes `parallel-consumer-core/src/test/resources/junit-platform.properties`;
that PR's wall-clock figures were all taken before it, so they describe a gate whose non-core modules were
thread-parallel despite `parallel-tests=false`. The fork-ORDERING statistics that PR ships are unaffected -
they were regenerated against the merged tree - but its headline seconds need re-taking before anyone cites
them. `ci/reenable-parallel-tests` is still unchecked.
<!-- file-refs: N/A - names the file the fix DELETED, so it is deliberately gone from the tree -->

Worth knowing before re-running either: `docs/solutions/test-flakiness/unit-tests-parallelise-by-forking-not-threading-2026-07-29.md`
already measured the choice - forking x12 gives a reliable 1:38, thread parallelism x20 gives an intermittent
~2:32 - which is why the `ci` profile sets `parallel-tests=false`. The leak was pushing eight modules into
exactly the configuration this repo had measured as the unreliable one.
