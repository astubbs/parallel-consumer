# Equal-sized offset encodings are chosen by set iteration order, so the winner is not deterministic

<!-- inflight-type: bug -->
<!-- inflight-impact: blind-spot -->

`OffsetSimultaneousEncoder` holds its encoders in a `ConcurrentHashMap` key set and feeds the
finished candidates into a `TreeSet` ordered by encoded size alone. When two candidates tie on size,
which one survives depends on identity hash order, and that varies from call to call.

## Measured

The same three-offset `PartitionState` encoded as magic byte `110` in one call and `108` in the next,
both five bytes. The sighting and the reasoning are in `RiderSupplierGuardTest`'s javadoc, anchor
`Why this is not a byte-for-byte comparison against a separately built baseline` - that test wanted
to assert the rider-less payload byte for byte against a baseline built by encoding the same state a
second time, and could not, because the baseline itself is not stable.

## What it costs

- No wire-format test can assert a payload byte for byte against an independently built expectation;
  every such test has to assert the parts the competition does not decide (the envelope's magic
  byte, the rider slot) and treat the inner encoding as opaque.
- A reader diagnosing a commit cannot reproduce which encoding a given state will produce, so "the
  same state encodes the same way" is not a property an operator can rely on.

## Proposed remedy

Break the tie by magic byte - or any fixed total order over `OffsetEncoding` - after size. The
choice is then a pure function of the state, byte-identity assertions against a separately built
baseline become possible, and nothing about the wire format changes for a reader.

## Delete when

The tie-break is deterministic and a test asserts the same state encodes the same way twice.
