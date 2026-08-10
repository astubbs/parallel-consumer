# What the benchmark refutations say to test next

**Capture-now note.** Six predictions were refuted on 2026-08-11, and each refutation points at a
question worth answering. Ranked by what would change a decision rather than by curiosity.

## 1. Wake-on-work is load-bearing, and we did not know it

Predicted: backlog catch-up would be largely INDEPENDENT of wake-on-work, since work is always available
so the poll wait barely applies. **Backwards.** It fires on 94% of records there, and disabling it
collapses the advantage from **3.76x to 1.31x** - two thirds of the benefit.

Only discovered because the agent ran a control arm against *our own fix* rather than only against stock.

**Test next:** does the same hold in the steady-state and low-rate cells, or is wake-on-work specifically a
saturation mechanism? That determines whether it is an optimisation or load-bearing to the whole claim -
and therefore how hard it must be defended in review and how prominently it belongs in the README.

## 2. The advantage shrinks with depth, and nobody knows where it settles

4.11x / 3.78x / 3.45x at depths 200 / 1200 / 3000, one run each. The absolute saving still compounds
(6s / 32s / 75s), which is what an operator actually feels.

**Test next:** extend the sweep past 3000 and find the asymptote. If it plateaus, that number is the honest
long-run claim. If it keeps falling, the claim must be depth-qualified.

## 3. CPU-bound: two true numbers, and the comparison depends on what you hold equal

3.85x on an idle box, **1.19x at equal thread count.** Stock runs one record per thread whether it is
blocked *or computing*, which is why the idle-box figure is so high.

**Test next:** settle which comparison is the honest default to publish. Both are true; presenting only one
is a choice that a hostile reader will notice.

## 4. Skew halves the advantage, and real keyspaces are skewed

Zipf 1.5 gives 2.00x where uniform gives 4.05x.

**Test next:** where does it stop paying? Find the skew at which the advantage disappears, because that is
the honest adoption boundary and the first thing a knowledgeable reader will ask for.

## 5. Partitions and dispatch compose

Stock 4-partition/4-thread 3.90x, seam 1-partition 3.78x, **together 15.65x** - and multi-partition
dispatch is currently listed as untested.

**Test next:** verify multi-partition dispatch properly. If it holds, "use both" is a stronger story than
either alone, and it retires the "just add partitions" objection rather than conceding it.

## 6. A fixture that was a sleep in disguise

The first CPU-bound fixture was a deadline-bounded spin - a sleep wearing a costume - and the second was
defeated by the fair scheduler. Caught only because the negative control failed.

**Test next:** nothing. Recorded because it is the method working: a control arm that fails is how you learn
your fixture does not do what its name says.

## Delete when

Each question is answered in the results document, or explicitly dropped with a reason.
