# The conformance corpus

One YAML file per case, read by `CaseLoader` and executed by `Oracle` under the two proofs
`CorpusGateTest` runs per case: the determinism control arm, and the positive control over the
case's author-chosen perturbed twin (R7, R8). `CorpusCoverageTest` holds the same directory against
the ten builder operations `BuilderSurface` lists, crediting an operation only when the topology the
oracle *built* contains a node for it.

**A case is data, not a Java fixture** - a foreign driver reads these files as easily as the oracle
does, which is the whole point of the format. Every case's base instant is `2025-01-01T02:00:00Z`,
two hours past the epoch, so a window-arithmetic bug cannot hide behind a clamp at zero; every
record's timestamp is an explicit offset from it, so nothing inherits wall-clock time.

## Why each twin cannot be absorbed

The positive control is the arm whose job is to fire. A twin the case's operations can absorb proves
nothing, and when the control does not fire **the twin is wrong, never the control**. So each
perturbation below is chosen against the operations it has to survive: a key change or an extra
record where the values are ignored, a change to the *last* value under `last-wins`, a change to an
*earlier* value under `concat`, a value change under `upper` or `count-bytes`.

| Case | Operations | Where the twin diverges | Why it cannot be absorbed |
|---|---|---|---|
| `map-values-upper-to-sink` | source, map-values (`upper`), sink | sink `out` | `upper` is injective on the ASCII piped here, and the changed key appears once, so the non-windowed sink's last-per-key fold cannot overwrite the change away |
| `reduce-last-wins-table-to-stream` | source, group-by-key, reduce (`last-wins`), to-stream, sink | store `latest-per-key`, sink `out` | the twin changes the **last** value under its key; `last-wins` keeps only the newest, so a change to any earlier record would be overwritten by the record after it |
| `reduce-concat-keeps-every-value` | source, group-by-key, reduce (`concat`) | store `concatenated` | the twin changes the **first** value, the deliberate opposite: `concat` accumulates every value in arrival order, so nothing later overwrites it. The case has no sink, so the store is the whole final state |
| `join-stream-with-table-concat-sides` | source (x2), group-by-key, reduce (`last-wins`), join (`concat-sides`), sink | sink `out` | the joined value carries the stream-side value verbatim; the table side is untouched, so the reduce store agrees and only the operation that reads *both* sides can show the change |
| `hopping-count-by-key` | source, group-by-key, windowed-by (hopping), count, to-stream, sink | store `counts`, sink `out` | `count` ignores values entirely, so the twin changes a **key** - which moves a record into a different key's windows in the store and a different key's emissions in the sink |
| `tumbling-aggregate-concat-at-final-state` | source, group-by-key, windowed-by (tumbling), aggregate (`concat`), to-stream, sink | store `aggregates`, sink `out` | `concat` accumulates every value in the window, so a value change lands in the accumulator and in every emission after it |
| `windowed-aggregate-emitted-on-window-close` | source, group-by-key, windowed-by, aggregate (`count-bytes`), to-stream, sink - **oracle-only** | store `byte-counts`, sink `out` | the twin lengthens a value, and `count-bytes` totals the bytes of the values in a window, so the closed window's total changes and reaches the suppressed sink |
| `aggregate-names-function-and-combine` | **refusal class** - no outcome, no twin, never executed | | |
| `windowed-by-retention-below-minimum` | **refusal class** - no outcome, no twin, never executed | | |

## The handle-kind transitions

R12 asks for a chain per transition in the grammar, because divergence in a wire-crossing binding
lives at the joins between operations rather than inside them:

- **stream to grouped stream to table** - `reduce-last-wins-table-to-stream` (`group-by-key` then
  `reduce`), and again in the join case's table side.
- **table to stream** - the same case's `to-stream`, and the two windowed cases' `to-stream` off a
  windowed table.
- **stream to time-windowed stream** - `hopping-count-by-key` and
  `tumbling-aggregate-concat-at-final-state`. Both carry a `to-stream`, and must: `windowed-by`
  mints no topology node of its own and is credited by the key-selecting node a windowed `to-stream`
  produces, so a windowed case with neither a `to-stream` nor an emit rule has nothing to witness it
  and the coverage gate reports it as named-but-not-built.

## The oracle-only case

`windowed-aggregate-emitted-on-window-close` is the corpus's one case naming `emit: on-window-close`
(KTD5). That attribute is outside the wrapper's builder grammar - the one named exception to the
otherwise one-to-one translation from a case to a builder call - so no binding can exercise it until
the wrapper exposes an emit control. It is executed, counted, and its translation is still checked,
but the coverage gate credits **nothing** for it: every operation it names is covered by another
case.

Under close-driven suppression Kafka emits a window only when a later record advances stream time
past its end plus grace, and `TopologyTestDriver` never advances stream time on `close()`. The
record at `at-ms: 7200000` is that later record; the loader refuses a pinned-emit case without one,
because such a case would pass every proof on its store alone while observing nothing about emit.
The effect is visible in the sink: with the emit attribute the sink holds one record, the first
hour's closed window, and never the trailing record's own window, which is still open when the run
ends; with the attribute removed the same case's sink holds every intermediate update *and* the
open window.

## The refusal class

Two cases declare a fault the **wire** must raise for an invalid specification rather than an
outcome (R15). Plain Kafka Streams never refuses what the wire invented, so no oracle row exists for
either: they are loaded, counted, given a skipped-by-design cell at the gate, and never executed.
Executing them is a driver-rung obligation.

Each fault is spelt in the *wire's* vocabulary, taken from the Streams proto and its assembler on
the `research/kafka-streams-foreign-wrappers` branch, so a driver's mapping from a case to a fault
is one-to-one:

- `aggregate-carries-both-function-token-and-combine` - the proto's `Aggregate` names
  `function_token` and `combine` as alternatives, exactly one of which must be set, and refuses a
  call carrying both rather than resolving them by precedence, which would silently discard half of
  what the host said.
- `windowed-by-names-retention-ms-below-the-minimum` - the proto's `TimeWindowSpec` requires
  `retention_ms` to be at least `size_ms + grace_ms`, and refuses anything below it with the minimum
  named, rather than letting a store that drops still-open windows surface later as Kafka's own
  exception.

The loader deliberately does **not** enforce that minimum: its window rules check that all four
fields are present and in range, and leave the minimum to the wire. That is what lets a
refusal-class case carrying an out-of-range retention load at all, which is what R15 needs.

## Adding a case

Add one file, run `CorpusGateTest` and `CorpusCoverageTest`, and read what they say before adding
the next. Two failures are worth recognising on sight:

- **The positive control did not fire.** The twin is absorbed by the case's own operations. Fix the
  twin.
- **`<operation>` is named but not built.** The oracle's topology holds no node crediting an
  operation the case names - either the shape gives that credit nothing to witness (the windowed
  case without a `to-stream`, above), or the oracle dropped the operation.
