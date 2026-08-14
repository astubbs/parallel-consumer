# Two branches' parallelism measurements may have a contaminated control arm

`ci/reenable-parallel-tests` and `optimize/unit-gate` both measure JUnit execution parallelism. Any numbers
they gathered before core stopped configuring the other modules' runners were taken against an "off" arm that
was not off everywhere: core's `src/test/resources/junit-platform.properties` was packaged at the root of the
core **tests** jar, so the eight modules depending on that jar (the four integrations plus every example) ran
at `factor=20` regardless of their own `${parallel-tests}` setting.

Re-take any measurement from before that, or confirm the arm it used was unaffected. Neither branch had an
open PR when this was written, which is why the note is here rather than in a review comment.

Worth knowing before re-running either: `docs/solutions/test-flakiness/unit-tests-parallelise-by-forking-not-threading-2026-07-29.md`
already measured the choice - forking x12 gives a reliable 1:38, thread parallelism x20 gives an intermittent
~2:32 - which is why the `ci` profile sets `parallel-tests=false`. The leak was pushing eight modules into
exactly the configuration this repo had measured as the unreliable one.
