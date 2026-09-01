# The "is there a second serious client?" answer is missing for most languages

Open, 2026-08-22. The contract asks every demo to **run every serious client in its ecosystem as its
own arm**, or say why not. One language does this properly, three name libraries without answering
the question, and three say nothing at all.

## Where each language stands

Measured by whether `<module>/demo/README.md` carries a table naming the ecosystem's Kafka clients
*and* a reason for each one the demo does not run:

| state | languages |
|---|---|
| **Reasoned table** - names the alternatives and why each is or is not an arm | ruby |
| **Names libraries, no reasoned table** | python (`aiokafka`, `confluent-kafka`), rust (`rdkafka`), swift (`swift-kafka-client`) |
| **Has some prose, no library named** | java, kotlin, typescript, dotnet |
| **Nothing** | scala, go, cpp |

Go is a known case with the reasoning already written down elsewhere rather than in its README: its
agent named franz-go, confluent-kafka-go and sarama, ruled out confluent-kafka-go because cgo would
break the `CGO_ENABLED=0` build that lets the binary run in the JVM stage, and **declined sarama only
because the conformance harness then rejected extra arms**. That constraint has since been lifted -
the harness now permits and reports extra arms - so **sarama is unblocked and nothing has picked it
up.**

## The concrete miss: Karafka

Ruby's table is the good one, and it still has a hole. It names `rdkafka` (the serial arm) and
`ruby-kafka` (**not run** - archived by its authors in 2023, which is a sound reason). It does not
name **Karafka**, which appears exactly once in the whole module, as an aside in the `Gemfile`: "it
is what Karafka is built on".

That is the wrong one to leave out. Karafka is Ruby's *concurrent consumption framework* - the same
category as Parallel Consumer itself - so it is the most informative comparison available in that
ecosystem, not the least. `ruby-kafka` has a reason to be skipped; Karafka has none recorded.

## What to do, and what NOT to do

**Do the survey per language before adding any arm.** The value is the reasoning, not the row: a
reader asking "is this fast in my language" is asking about the client they already use, and an
omission with no reason reads as an oversight - which, in Karafka's case, it was.

**Do not assume the answer from outside the ecosystem.** Each of these needs someone to check what is
actually current and maintained: an archived project is a good reason to skip, a cgo dependency that
breaks a static build is a good reason to skip, and "I had not heard of it" is not.

**Beware the two traps already paid for here.** A client that needs cgo can break the container
build's static-linking assumption (Go's case), and an extra arm changes the demo's runtime and disk
cost in CI - the eleven-language matrix is already the slowest thing in the build.

## Related

- The arm-naming pass of 2026-08-22 prefixed every Parallel Consumer arm with `pc-`, so any new arm
  should follow: `pc-<lang>-<client>` for a PC arm, and the plain client name for an `AK core` arm.
- [`next-demo-testing-infrastructure.md`](next-demo-testing-infrastructure.md) - the ranked ambition
  for the demo harnesses; more arms is more surface for the drift check to compare.
