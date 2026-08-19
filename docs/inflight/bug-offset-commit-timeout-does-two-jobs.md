# `offsetCommitTimeout` does two jobs, and 10s is wrong for both

<!-- inflight-priority: medium -->

Open after astubbs#204. That PR made the option mean "the whole commit" rather than "one attempt",
which is the precondition for fixing this - but it does not fix it, and the default is untouched.

## What it currently bounds

Two things, from one option (`ParallelConsumerOptions`, `private final Duration offsetCommitTimeout`,
default `Duration.ofSeconds(10)`), plus Kafka's own bound underneath:

| Level | Thread | Bounded by | Default |
|---|---|---|---|
| `ConsumerOffsetCommitter.commitAndWait` waiting for a commit response | **control** | `offsetCommitTimeout` | 10s |
| `ConsumerManager.commitSync`'s retry loop | **poll** | `offsetCommitTimeout` | 10s |
| One `consumer.commitSync(offsetsToSend)` call, which retries internally | poll | `default.api.timeout.ms` | **60s** |

The 60s is verified, not recalled: read from `ConsumerConfig` in kafka-clients 3.9.2, the version this
repo builds against.

## Why the default is wrong, and why raising it is not the fix

**As the retry budget it is far too small.** A single Kafka call can run 60s, so a 10s total is spent
before the first attempt returns: exactly one try, no retry ever. Kafka refuses this shape in its own
config - a producer will not accept `delivery.timeout.ms` below `request.timeout.ms + linger.ms` -
precisely so a total budget is never smaller than one attempt. PC cannot make that check at
construction (`Consumer` exposes no configuration), so astubbs#204 reports it when it bites instead:
giving up after one attempt now says that no retry was reachable.

**As the waiter's deadline it wants to be small.** `commitAndWait` blocks the **control** thread, and
no work is distributed while it waits. Raise the option to 120s to make retries reachable and a
slow-but-alive commit stalls processing for two minutes.

So the two uses pull in opposite directions and one number cannot serve both. Raising the default
alone trades a processing stall for retry capability, silently.

## The options, none yet chosen

1. **Pass the budget down instead of layering over it.** `consumer.commitSync(Map, Duration)` exists
   for exactly this: Kafka bounds the operation and throws on expiry, so PC's hand-rolled timeout
   retry loop mostly disappears - one level instead of three, and the option means what it says. The
   loop cannot go entirely: the SASL retry-with-backoff and `WakeupException` handling still need it.
   **The cost, and it is the real question:** PC then stops distinguishing "how long one API call may
   take" from "how long we will keep trying to commit these offsets". Those may genuinely want to be
   different numbers - which is why Kafka has both - or the distinction may be a knob nobody wants.
2. **Split into two options**, one per job: a waiter deadline (short, protects the control loop) and a
   commit budget (above `default.api.timeout.ms`, makes retries reachable). Honest, and adds a knob to
   a library whose users already meet a lot of Kafka's.
3. **Raise the default above 60s and accept the stall.** Cheapest, and the one to be most suspicious
   of - it fixes the arithmetic and hides the conflict.

There is a real tension behind the choice: Kafka's knobs exist for good reasons, and every one PC adds
is another thing a user has to hold. Fewer knobs is a feature until the day one number has to be two.

## What would settle it

Whether anyone genuinely needs "one API call" and "total commit effort" to differ. If not, option 1
and delete a concept. If yes, option 2 and name both.

Related: astubbs/parallel-consumer#317 (a commit-failure seam) is the other half of the same area -
what happens *after* the budget is spent, rather than how long it is.
