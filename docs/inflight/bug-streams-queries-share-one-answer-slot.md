# Two concurrent `StreamsSession.get()` calls receive each other's answers

<!-- inflight-type: bug -->
<!-- inflight-impact: misdirection -->

Found 2026-08-25 while running the re-entrancy experiment in
[`streams-coupling-dimensions.md`](streams-coupling-dimensions.md), and **independent of it**: no
invocation, no mapper, no re-entrancy. Two ordinary host threads calling the public query API at the
same time is enough.

```
asked for alpha, got: b'beta'
asked for beta,  got: b'beta'
```

No exception, no fault, no log line. The caller is handed a confident wrong value for a key it did
not ask about. **This is the silent member of the family** - the re-entrancy hang beside it is loud
and survivable.

## Mechanism

`Get` carries `store_name` and `key` and **no correlation** - unlike a builder call, which has
`call_id` and a per-call waiter. `StreamsSession` therefore holds exactly one `_got` event and one
`_get_result` slot for every query in the session, so the second answer to arrive overwrites the
first and both callers read whichever landed last.

`Describe`/`TopologyDescription` has the identical shape - one `_described`, one `_description` -
and the same defect.

The existing `test_a_second_query_waits_for_its_own_answer` does not cover this: two **sequential**
queries are handled correctly by the `_got.clear()` in `get`.

## The fix, and why it comes first

Give `Get`/`GetResult` and `Describe`/`TopologyDescription` a correlation and key the waiters by it,
exactly as `BuilderCall`/`HandleAssigned` already do with `call_id`. The wire is `v1alpha1` and free
to change, and the pattern to copy is already in the same file.

**Do this before moving user functions off the reader thread.** That is the other half of the
re-entrancy fix, and doing it first makes this worse rather than better: it turns re-entrant queries
from a hang into more concurrent callers contending for the one answer slot, which is precisely this
bug.

## Evidence in the tree

`parallel-consumer-proxy-clients/parallel-consumer-proxy-client-python/tests/test_streams_reentrancy.py`
holds `test_two_concurrent_queries_each_receive_their_own_answer`, marked
`xfail(strict=True)`. It asserts the CORRECT behaviour and records that the code does not do it yet
- so when the correlation lands, the test becomes an XPASS failure telling whoever fixed it to
delete the marker. The neighbouring re-entrancy tests in that file are ordinary characterisation
tests asserting what the code *does*, because they pin an accepted limitation rather than a defect;
the two markers are deliberately different and the file explains why.
