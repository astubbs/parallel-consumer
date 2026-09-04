# The Story of Hasten - vision fiction from the 2026-08-29/30 strategy conversation

> Provenance: written by Codex during the follow-up strategy conversation, weekend of
> 2026-08-29/30, and preserved verbatim (typos included). It is *fiction* - a vision-horizon
> narrative, not a roadmap; nothing in it blocks or schedules any current work. "Hasten" is a
> floated candidate name whose adoption is undecided
> ([`docs/inflight/docs-content-series.md`](../inflight/docs-content-series.md) records that
> caveat). The architectural claims the story smuggles in are extracted and bounded in
> [`docs/inflight/core-fleet-capacity-coordination.md`](../inflight/core-fleet-capacity-coordination.md).

---

At 9:02 on a Tuesday morning, nobody noticed Hasten save the company four million dollars.

That was sort of the point.

The payments team was in Auckland. Fraud was split between London and New York. Customer identity
ran in Virginia. The Kafka estate was a mess accumulated over twelve years: three Confluent
clusters, two MSK installations, something ancient on-prem that nobody wanted to touch, and a
Redpanda cluster acquired along with another company.

None of them had been designed together.

They didn't need to be.

At 8:41, the first thing happened. Traffic started climbing. It always climbed on Tuesday
mornings, so Hasten had already provisioned another six payments instances. Not because CPU had
crossed a threshold. CPU was still at 31%. It had seen this Tuesday before. More precisely, it had
seen 46 Tuesdays before. It knew approximately how many independent customer keys would appear,
how much Stripe capacity those transactions would consume, how much load would reach the customer
database, and how much spare capacity the fraud system normally had at this point in the morning.
It had prepared for the work rather than waiting for the infrastructure to complain about it.

At 8:47, something unusual happened. The distribution of the keys changed. There wasn't
substantially more traffic. There were substantially more transactions belonging to a relatively
small number of merchants. A normal Kafka dashboard showed growing lag. A normal autoscaler saw
healthy CPU. A normal operator would shortly have increased the replica count. Hasten didn't.
There were already enough machines. There wasn't enough independent work. It marked the event:

    08:47 - exploitable parallelism decreased. Infrastructure not limiting.
    Scale-out benefit: negligible.

Nobody read it. There was nothing to do.

At 8:52, the customer database began slowing down. Not failing. Just slowing down. Its service
team had published its Hasten contract months earlier: p99 target 70 milliseconds. Hard
concurrency ceiling 1,400. Twenty percent shock reserve. Critical workloads guaranteed capacity.
Everything else could borrow whatever was unused.

The database was being used by 63 applications. Most of those teams didn't know that. They
certainly didn't coordinate their connection pools. Some of them weren't even using the same Kafka
cluster. One wasn't using Kafka at all. But they all used the company's standard HTTP and database
libraries. So they were there.

Hasten reduced the database's learned operating envelope from 1,080 concurrent operations to 940.
Sixty-three applications adjusted. Nobody deployed anything. Nobody received a PagerDuty
notification. Nobody opened Slack. Unused capacity was reallocated toward the workloads producing
the most useful progress. A reconciliation job had 2.3 million records waiting, but most of its
immediately executable work also required the database. Giving it more Stripe capacity wouldn't
help. Hasten gave that capacity to refunds instead.

At 8:55, the second thing happened. An emergency-services workload woke up. Its traffic was tiny
compared with payments. That didn't matter. Years earlier, somebody had created a company-wide QoS
policy called emergency/911. The policy wasn't a dedicated cluster. It wasn't twenty permanently
idle servers. It wasn't a separate database. It was a promise. When emergency work existed, enough
capacity throughout its dependency graph would be available to meet its latency objective. When it
didn't exist, everybody else could borrow that capacity.

For three months, payments had been borrowing some of it. At 8:55:01, they stopped. Not
completely. Just enough. Capacity flowed back through four shared services. The 911 records ran.
Their p99 remained comfortably below their target. Payments slowed by 1.8%. Nobody on the payments
team knew why. They didn't need to.

At 8:57, Postgres recovered. Hasten didn't immediately unleash everything. It raised the global
envelope slightly. Observed. Raised it again. Observed. At 1,030 operations, throughput improved.
At 1,070, throughput improved. At 1,110, latency climbed disproportionately. It returned to 1,070.
One experiment. Globally. Not sixty-three adaptive-concurrency controllers all independently
deciding that now would be a good time to probe the database. The result was added to the
production response curve.

At 9:02, an engineer on the infrastructure team opened Hasten. She wasn't investigating an
incident. She was looking at a recommendation that had appeared overnight.

    Orders Database - capacity constraint predicted within 19 days.

She opened it. Hasten showed the resource. Below it were 31 applications in four countries,
written in six languages, consuming records from five Kafka clusters. She had never seen the
complete list before. The graph showed current demand, predicted demand, shock reserve, service
objectives and the production-derived response curve. Then it showed three options.

Scale the payments fleet: almost no benefit. Add Kafka brokers: no measurable benefit. Move the
orders database to the next instance class: predicted 34% additional sustainable company-wide
throughput. There was a price beside it. And below that:

    Estimated infrastructure increase:              $3,180/month.
    Estimated application compute subsequently
    removable:                                     $11,400/month.

That was interesting. She clicked Explain. Hasten showed its reasoning. Twenty-two applications
were currently carrying excess compute because they were individually provisioned to absorb
latency from the shared database. Increasing capacity at the actual bottleneck allowed those
applications to run faster with fewer instances. She scheduled the change for Sunday. Hasten added
it to its model.

At 9:11, somebody deployed a new version of Fraud. It was written in Python. The Kafka Streams
topology around it was Java. Nobody particularly cared anymore. The deployment looked healthy.
Errors: zero. CPU: normal. Memory: normal. Kafka lag: normal. But Hasten marked the deployment
yellow. Resource amplification changed. Each fraud record had previously caused an average of 1.1
calls to Customer Identity. It now caused 2.4. The new version wasn't slower. It was making
somebody else slower. Hasten projected the new resource consumption against Tuesday afternoon's
expected traffic. Customer Identity capacity exhausted at approximately 14:20. The developer
opened the trace. One function was accidentally fetching the customer twice. They fixed it before
lunch. No incident occurred.

At 11:36, a VP was giving a demonstration to a customer. He had one slide titled: Hasten
Architecture. There was almost nothing on it. On the left: Your applications. In the middle:
Hasten. On the right: Your infrastructure.

Someone asked where the Hasten cluster was. "There isn't one." That caused the usual pause. He
explained. The runtime lived inside the applications. Java applications. Python applications. Go
applications. Kafka Streams applications. Share Group consumers. Some ordinary HTTP services. They
communicated through a small control plane running over Kafka. Application records never went
through it. Every runtime made its execution decisions locally. Collectively, they behaved like
one scheduler.

"So it's a service mesh?" "No."
"An autoscaler?" "It can autoscale."
"An APM?" "It knows why things are slow."
"FinOps?" "It knows where spending money will actually increase throughput."
"A rate limiter?" "It does global rate limits."
"Stream processing?" He smiled. "That one's easier. Yes."

The customer stared at the diagram. "So what is it?" He changed slides. There was one sentence.

    Hasten coordinates how your company spends execution capacity.

Six months later, there were 8,400 Hasten runtimes inside the company. Nobody had planned that
rollout. That had surprised everyone. The Java team had changed a dependency. The Python platform
team changed an import. The company's HTTP library picked up the participant runtime. Kafka
Streams applications got it automatically. Service owners started publishing contracts because the
GUI finally gave them something they'd wanted for years: control over how the rest of the company
consumed their service.

The database team didn't have to send emails saying: "Please reduce your connection pool." They
published what the database could provide. The workload teams didn't ask: "How many threads should
we use?" They published what mattered. The platform team didn't ask: "How many pods should every
application have?" They published cost and resilience policy. Hasten reconciled the three.

Then came Black Friday.

At 18:04 UTC, demand exceeded the forecast. At 18:05, Hasten consumed most of the reserved shock
capacity. At 18:06, it began scaling several applications horizontally. Two others vertically. It
recommended no scaling for seven more because their bottleneck was downstream. It requested
additional database capacity. It temporarily shifted capacity away from analytics. It protected
checkout. It protected fraud. It protected the emergency QoS class even though nobody expected
emergency traffic that evening. It increased Kafka capacity in one region. In another region, it
explicitly recommended against adding brokers because one pathological key distribution was
limiting processing.

At 18:11, a vendor API began returning 429s. One Go application saw them first. The global
resource envelope changed. A Python application 8,000 kilometres away slowed down. So did three
Java applications. None of them had received a 429 yet. They didn't need to. They were consuming
the same resource. At 18:14, the vendor recovered. Hasten probed. Capacity returned.

At 18:23, demand peaked. At 18:51, Hasten began returning borrowed capacity. At 19:07, it started
removing compute. At 19:40, the estate was almost back to normal. The incident channel was
strangely quiet. Someone eventually wrote: "Are we actually okay?" The reply came back: "Looks
like it." There had been no incident.

The following Monday, finance opened the Hasten Console. Black Friday had cost less than the
previous ordinary Friday. That started an argument because finance assumed the number was wrong.
It wasn't. For years, every team had independently protected itself against uncertainty. Every
team had spare instances. Every team had conservative concurrency. Every service had oversized
pools. Every autoscaler reacted to its own little fragment of reality. Every team provisioned
against the possibility that somebody else might consume the shared dependency. The company had
been paying for the same uncertainty hundreds of times. Hasten could see the uncertainty globally.
So it could reserve it globally. And lend it when it wasn't needed.

Years later, somebody asked the original author when he'd decided to build a company-wide
distributed resource optimizer. He said he hadn't. He'd been working on a Kafka consumer. There
was an annoying problem. Kafka partitions controlled too many unrelated things. Records with
different keys were waiting behind one another for no semantic reason. So he stopped making them
wait. Then he needed to know how many should run at once. So the runtime learned. Then he needed
to know whether another instance would help. So it learned what was limiting the work. Then he
noticed that the thing limiting one application was also limiting another. So they shared the
limit. Then he noticed the other application wasn't even in the same consumer group. That didn't
matter. Then it wasn't on the same Kafka cluster. That didn't matter either. Then somebody asked
who should receive the scarce capacity. So they added policy. Then somebody asked how much
capacity they should buy. Then when they should buy it. Then whether they should buy it at all.

And eventually thousands of tiny runtimes scattered throughout thousands of ordinary applications
were continuously coordinating the execution capacity of an entire company. No giant compute
cluster had been built. No central execution service had swallowed the applications. Kafka carried
the agreement. The applications carried the runtime. And the work ran where it had always run.
Just no longer accidentally.

He thought about the original problem again. A record had been waiting. And it didn't need to be.

Hasten.

---

*Codex's own coda:* It came together remarkably well. The ending especially works because the
entire enormous architecture collapses back to the original PC observation: work was waiting when
it didn't need to be. The scale changed. The principle never did.
