# Next: testing infrastructure for the multi-language demos

Deferred by the owner (2026-08-21) - captured as ambition, not scheduled. Ranked by how much drift
each item makes **impossible** rather than merely detectable, because eleven implementations of a
prose contract will drift and the only question is whether anything notices.

The two coverages this builds on, and the console/ledger split it assumes, are **owned by**
[`parallel-consumer-proxy-clients/AGENTS.md`](../../parallel-consumer-proxy-clients/AGENTS.md) -
read that first; this file is the candidate work, not the rules.

## 1. Generate the option surface instead of transcribing it

The seven flags, their defaults, their `PC_DEMO_*` names and the help text are **pure data**, and are
currently reproduced by eleven people reading prose carefully. Put them in one machine-readable file
beside the contract and generate each language's parser - or at minimum its help text and defaults
table.

**The precedent is already in this repo and it works**: `proxy.proto` is one schema producing eleven
languages' wire code, and nobody transcribes a protocol field by hand. The demo surface is the same
shape of problem. This is the biggest single win available, because it **retires** a class of
assertion rather than adding one.

## 2. Golden `Configure` messages, per client

Record the exact protocol message each client sends for identical `ClientOptions`, and diff the
eleven against each other.

**This defect class has already bitten twice, in the same file, weeks apart**: the Java demo's
hand-written arm left `ordering` unset - so it silently ran KEY-ordered against four unordered arms -
and then left `capabilities` empty, negotiating a different session. Both produced a working arm and
a plausible number. `ConfigureParityTest` in the Java demo module is the shape, at one-client scale;
generalising it is the work.

The protocol message is the **real interface** between eleven clients and one engine, it is invisible
to the output suite, and conformance does not compare clients to each other.

## 3. Output assertions that imply coverage underneath

Cheap to add, and each is a strong proxy for machinery the demo otherwise only exercises silently.

- **Peak in-flight against the configured ceiling.** Two assertions in one number: parallelism
  genuinely happened (peak > 1; the serial arm must peak at exactly 1) and the engine's ceiling held
  (peak <= configured). **This would have caught a real bug**: Kotlin's blocking sleep capped
  in-flight at 64 while the fingerprint printed 200, and it took arithmetic to expose. Owner approved
  putting this in the table tail.
- **Second pass consumes nothing.** Re-open the same consumer group after the run and poll once;
  print `second pass: 0 records`. If offsets committed correctly that is zero, and if the custom
  run-length/bitset encoding lost or mis-encoded anything it is not. **The most intricate code in the
  product, proved end to end by one line of output and about five lines of demo code.**
- **Distinct `(partition, offset)` equals records processed** - nothing was redelivered, which
  exercises epoch fencing and the retry path.
- **Per-partition spread** - proves assignment worked and no partition starved.
- **A failure-percentage knob**, reporting that every record eventually succeeded and the maximum
  attempts seen. That exercises retry scheduling and terminal resolution, an entire subsystem the
  demo currently never touches. Decision 9 in the comparison demo's own record listed this as an
  intended knob from the start.

## 4. The per-record ledger, and assertions on the file rather than the screen

The demo writes a machine-readable ledger - identifier, key, partition, offset, attempt, received and
completed timestamps - **off by default, behind a flag**, with assertions run against the file. The
rule that keeps this from spoiling the demo is in the AGENTS.md linked above.

What a ledger makes checkable that a console cannot:

- **overlap** - records whose processing intervals overlap prove concurrency *directly*, rather than
  being inferred from a rate;
- **order** - with a known publish order and key format, per-key sequence is checked outright. Pacing
  the producer deliberately makes the interleaving unambiguous in the record.

## 5. Assert the public API shape

The client-authoring guide describes the surface every client should expose and **nothing enforces
it**, so one language can end up with `open/poll/close` and another with `connect/consume/shutdown`.
A canonical list of names and argument order, checked per language. Cheap, and it is the thing a user
actually meets.

## 6. One shared seed dataset, defined as data

Each demo currently re-implements "records over a key space". Define it once - key format, value
format, order - and have every demo consume that definition, so `unique keys = 1000` becomes an
assertion about shared data rather than eleven implementations that happen to agree.

## 7. Make the contract executable

A checker that parses the flag table out of `parallel-consumer-proxy/demo/README.md` and asserts
every language's `--help` lists exactly those flags. Then the document cannot drift from the code in
either direction - today the doc is authoritative but unenforced.

## 8. Per-language performance baselines - wide bands only

Worth having to catch a change that makes something ten times slower. **Not** to police percentages.

**The trap is already documented**: measurements here have not been shown to reproduce across
sessions - the Java seed's serial arm moved from ~345 to ~300 msg/s on one machine, the obvious cause
was refuted by a control arm, and it remains unexplained
([`next-demo-seed-followups.md`](next-demo-seed-followups.md), item 1). Until that is understood a
tight band is a flake generator, and a band wide enough to be honest is still worth having.
