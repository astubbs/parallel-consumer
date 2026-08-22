# Next: licensing strategy - Apache-2.0 today, and whether that stays true

<!-- inflight-type: task -->
<!-- inflight-impact: process -->

Opened 2026-08-21. **There was no licensing-strategy note before this one.** The nearest existing
entry, `next-fork-packaging-docs-and-licensing.md` (on `docs/ks-handover-rerank-defect-one`), is about
packaging and documenting the patched-Kafka modules - a narrower question. The fork's own licence
posture has been recorded only in scattered memory and commit messages.

**Today: Apache-2.0**, inherited from upstream, with Confluent's copyright headers retained on
pre-fork files and fork headers on new ones (`docs/copyright.md`). Nothing here proposes changing
that. This note exists so that if it is ever considered, the reasoning is written down first.

## Why it is being raised now

A competitor comparison ([`market-analysis-llingr.md`](market-analysis-llingr.md)) found that
**licensing is the single most decisive difference between the two products for an enterprise
evaluator** - more decisive than any feature - and that the comparison currently favours us
overwhelmingly. That is an asset, and an asset worth understanding before it is spent.

## The three models on the table

| | **Apache-2.0** (PC today) | **AGPL-3.0 + commercial** (llingr) | **BSL / Business Source Licence** (under consideration) |
|---|---|---|---|
| Nature | Permissive | Strong copyleft with a network clause | **Source-available, not open source** |
| Closed-source commercial use | Free | Needs the paid licence | **Prohibited for the restricted use, until the change date** |
| The restriction | None | §13: users interacting over a network must be offered the source of your modified version | An arbitrary "additional use grant" - typically "you may not offer this as a competing service" |
| Time limit | None | None | **Converts to an open licence (usually Apache-2.0) after a change date, typically 4 years** |
| OSI open source? | Yes | Yes | **No** |
| CNCF / foundation hostable? | Yes | Yes | **No** |
| Typical corporate policy | Approved by default | **Frequently banned outright** | Requires legal review; increasingly recognised but still a blocker for some |
| Who it stops | Nobody | Anyone who cannot publish their source | **Only competitors** - ordinary users are unaffected |
| Contributor effect | Contributions flow freely | Contributors may need a CLA for the dual licence | Usually needs a CLA, which deters casual contribution |

**The essential difference between the two non-permissive options.** AGPL restricts **everybody** and
sells an exemption; BSL restricts **only the competing-service use case** and lets everyone else
proceed unaffected. For a *user*, BSL is far less intrusive than AGPL - an ordinary company embedding
a BSL library in its own product is usually unrestricted, whereas AGPL's network clause reaches them
directly. For a *foundation, distribution or a policy-driven procurement team*, BSL is worse, because
it is not open source at all and cannot be treated as such.

So llingr is not points on one line. AGPL maximises leverage over every user; BSL maximises adoption
among users while excluding one competitor class. **BSL is the friendlier of the two to the people we
would want using this**, which is worth knowing given the instinct behind considering it is usually
"protect against a cloud vendor", not "monetise every user".

## What this project would actually be protecting against

Worth stating, because the answer shapes the choice:

- **A hyperscaler offering PC as a managed service** - the case BSL was invented for (MariaDB, then
  HashiCorp, Sentry, Redpanda). Plausible? PC is a *client library*, not a server. There is not much
  to run as a service, which is the strongest argument that BSL solves a problem this project does not
  have.
- **A competitor embedding it** - AGPL would address this; BSL typically would not, since embedding
  is not usually the restricted use.
- **Nothing** - and Apache-2.0's frictionlessness is itself the strategy. Adoption is the scarce
  resource for a library, and every licence above costs adoption.

## Things that must not be lost if this is ever revisited

- **Relicensing is not unilateral.** Upstream is Apache-2.0 with many contributors; the fork carries
  their copyright. Changing the licence for the *whole* codebase is not available. What is available
  is licensing **new, separately-copyrighted modules** differently - which is a real option and a much
  smaller decision.
- **A licence change is irreversible in practice**, because the community reaction is.
- **The fork already made one licensing decision worth honouring**: keeping Confluent's headers, on
  legal grounds. See memory and `docs/copyright.md`.
- **Upstream-PR compatibility is no longer a goal** (corrected 2026-08-05), which removes one former
  constraint.

## Open questions

1. **Is anything actually being protected?** If PC is a library rather than a service, BSL's core use
   case may not apply. This should be settled before any licence work.
2. **Would a differently-licensed new module be enough** - e.g. a future commercial add-on - leaving
   the core Apache-2.0? This is the low-cost option nobody has evaluated.
3. **Does the current Apache-2.0 advantage over an AGPL competitor change the answer?** Right now that
   advantage is free and material. Adopting a restrictive licence would spend it.

## The distinction the owner actually wants: extenders vs utility users

**Raised 2026-08-21.** The instinct is right and it is worth naming precisely, because it is not the
line AGPL draws.

> *Should the paid tier apply to someone shipping a product that extends PC - a rules-engine server,
> say - rather than someone using PC as a utility, like a telco processing its own records faster?*

That distinction is **redistribution**, not network interaction:

- **The utility user** runs PC inside their own systems, processes their own data, ships nothing.
- **The extender** builds PC into something they hand to someone else - a product, an appliance, a
  platform.

**AGPL does not draw that line.** Its trigger is §13: *users interacting with the software remotely
over a network* must be offered the corresponding source. A telco running PC behind a customer-facing
API is arguably captured by that, and their lawyers will assume they are - which is exactly why AGPL
is banned outright at many companies rather than negotiated. **AGPL would charge the utility user and
the extender alike**, which is the opposite of the intent above.

### Licence families that do draw it

| Family | Where the line falls | Fit for "extenders pay, utility users don't" |
|---|---|---|
| **LGPL-2.1/3.0** | Free to *use* as a library, including in closed products; must publish changes **to the library itself**, and permit relinking | **Closest existing fit.** But it charges nobody - the extender owes source for their modifications, not money |
| **MPL-2.0** | Per-file copyleft: modified PC files stay open, the surrounding product does not | Same shape as LGPL, weaker, and file-level scope is easier to comply with in Java |
| **Apache-2.0 + commercial add-on** | Core is free for everyone; a separately-copyrighted module is sold | **The option with no downside**, and question 2 above |
| **BSL with a tailored additional use grant** | Whatever the grant says - it can be written to permit internal use and restrict redistribution | Technically achievable; costs open-source status for everyone |
| **AGPL-3.0 + commercial** | Network interaction, not redistribution | **Does not draw this line at all** |

### The honest assessment

- **Nothing above charges the extender money except a dual-licence arrangement**, and a dual licence
  needs a CLA plus sole copyright - which this fork does not have and cannot obtain, since upstream's
  contributors hold copyright on the pre-fork code (see "Relicensing is not unilateral" above). **The
  core cannot be dual-licensed. That is a hard constraint, not a preference.**
- **What can be dual-licensed is new, separately-copyrighted work.** A rules-engine server, a control
  plane, a managed-scaling component, the language proxies as a packaged product - each is new code,
  each could carry a different licence, and none of that touches the Apache-2.0 core.
- **That is also the better commercial shape anyway.** The extender who builds a product on PC is
  already going to want the parts that are hard to build: adaptive scaling with real feedback, a
  control plane, a UI, support. Selling those is a product decision, not a licensing one - and it
  leaves the core's frictionless adoption, currently the project's strongest commercial asset,
  entirely intact.

**Recommendation, for the record: do not restrict the core.** The line the owner wants - extenders
pay, utility users do not - is reachable by *building something extenders want to buy*, and is not
reachable by relicensing what already exists. Revisit only if a specific extender appears and asks.

## Can we use Confluent's licence - the one on REST Proxy? No, and it would be the wrong shape anyway

**Asked 2026-08-21.** Confluent's Schema Registry, REST Proxy, ksqlDB and Community Connectors are
under the **Confluent Community License (CCL)** - source-available, with one restriction: you may not
*"make available any software-as-a-service, platform-as-a-service, infrastructure-as-a-service or
other similar online service that **competes with Confluent products**."*

Three reasons it does not work here, in increasing order of severity:

1. **It is not offered as a reusable template.** BSL 1.1 and Elastic License 2.0 are written with
   fill-in parameters and an explicit invitation for anyone to adopt them. The CCL is written with
   Confluent as the named licensor, and Confluent's own FAQ says nothing about third parties using the
   text. Adopting it would mean copying another company's bespoke licence without a grant to do so.
2. **The restriction points at the wrong target.** Its prohibited use is competing with *Confluent's*
   products. Applied to this project verbatim, it would restrict our users from competing with
   Confluent while placing no restriction at all on competing with us - which is not a licence, it is
   a favour to a third party.
3. **This project is a fork of a Confluent project.** Putting Confluent's own bespoke licence on
   derived work, under a different Maven coordinate, invites exactly the confusion about origin and
   endorsement that the fork's naming and copyright policy (`docs/copyright.md`) exists to avoid. It
   is a trademark-adjacent question, not merely a licensing one.

**What CCL actually is, generically, is Elastic License 2.0** - same idea, same "no offering it as a
competing managed service" restriction, but written to be adopted by anyone and with the licensor left
as a parameter. **If the SaaS-restriction shape is ever wanted, ELv2 or BSL are the instruments; the
CCL is not available to us.** And per "What this project would actually be protecting against" above,
that shape solves a problem a client library probably does not have.

## Scope: the decision is about the new roadmap modules, not the core

**Owner's direction, 2026-08-21, and it settles open question 2.** The licensing question that matters
is not whether to relicense what exists. It is **what licence every new module in the roadmap ships
under** - and that decision is still fully open, because none of them exist yet.

The candidates, from [`docs/data/roadmap.yaml`](../data/roadmap.yaml) and the inflight notes, are the
ones that are **new, separately-copyrighted work** rather than changes to the Apache-2.0 core:

| Module | Why it is a candidate |
|---|---|
| **A web view of a running instance** (`running-instance-visibility`) | A server and a UI - the one genuinely product-shaped thing on the roadmap, and the closest analogue to what a competitor charges for |
| **The language proxies** (astubbs#242) and their packaged distribution | New code in new languages; the packaging *is* the product for non-JVM users |
| **Adaptive concurrency** ([`next-auto-scaling.md`](next-auto-scaling.md)) | The strongest differentiator, and the one an extender would most want |
| **Kafka Streams and Connect integrations** | New modules, though these are ecosystem reach - restricting them works against adoption |
| **A control plane / external scaling signal** | Does not exist yet, and is the most obviously commercial of the set |

**The rule that makes this coherent:** the core stays Apache-2.0 permanently, and each new module is a
separate decision made *when it is created* - because a licence chosen at creation costs nothing,
while a licence changed later costs the community's trust. Any module that is a **library** the user
compiles against should follow the core and stay permissive, or it damages the adoption the core is
there to win. Any module that is a **service or a product** - the web view, a control plane - is where a
different licence is arguable, and where a dual licence is even *possible*, since the copyright would
be entirely ours.

**Nothing needs deciding today.** What needs recording is that the choice belongs at each module's
creation, and that the option is only preserved if new modules are kept structurally separate from the
core rather than accreting into it.

## Related

- [`market-analysis-llingr.md`](market-analysis-llingr.md) - the licence comparison that prompted this,
  and the evidence that it matters commercially.
- `next-fork-packaging-docs-and-licensing.md` (on `docs/ks-handover-rerank-defect-one`) - the narrower
  packaging question for the patched-Kafka modules.
