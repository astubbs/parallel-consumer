# The clients ask for a return value; core Parallel Consumer does not

**The one API-shape divergence worth fixing, and it is not between the clients** (astubbs#242,
raised by the owner 2026-08-15 while checking the fan-out had not drifted).

## What is consistent, and what is not

Across all seven landed clients the *calling* shape is the same: `poll` plus a per-record function
the user writes inline — a closure, lambda, block or callable, never an importable name. That part
held under six independent implementations, which is the thing worth knowing.

What differs is the **user function's contract**, and the difference is against *core Parallel
Consumer itself*:

- Core takes a **void** consumer of a poll context. A Java user writes `poll(ctx -> doWork())` and
  throws to fail.
- Every proxy client requires the function to **return an outcome**. The same user writes
  `poll(r -> { doWork(); return Outcome.success(); })`.

The reason for the return value is sound and should not be reversed: languages without exceptions
must be able to mirror the surface exactly, so failure has to be expressible as a value. But that
argument justifies *offering* the outcome form, not *requiring* it in languages that also have
exceptions.

## The fix, which is additive and cheap

Offer the void form alongside the outcome form wherever the language has an exception idiom, so the
common case reads exactly as core does and the outcome form is reached for only when the user wants
to produce records or to fail without throwing.

- **Python and Ruby already do this** - a function that returns nothing succeeded, and raising is
  the failure - which is why neither of those waves reported friction here.
- **Java, Kotlin, C#, TypeScript, Rust and Go do not.** Java is the one that matters most, because a
  Java user is the person most likely to be moving *from* core Parallel Consumer and most likely to
  read the difference as the product changing shape underneath them.
- The Rust case is deliberately different and should stay so: `Result<Outcome, ProcessingError>`
  makes an illegal state unrepresentable, and `?` is already the idiomatic failure path.

Nothing about this is breaking - an overload or an optional form is purely additive - so it does not
need to precede anything. It should land before the clients are published, since after that the
awkward form is the one in every example anyone has copied.

## Deeper parity, deliberately not proposed here

Core hands the user a poll *context*; the clients hand an inbound *record*. Naming them alike would
be a larger change with real costs (the context carries JVM-shaped concepts), and no wave has
reported it as friction. Recorded only so the next reader knows it was considered rather than
missed.
