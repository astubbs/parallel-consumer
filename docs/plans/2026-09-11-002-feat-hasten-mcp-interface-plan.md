---
title: Hasten MCP Interface - Plan
type: feat
date: 2026-09-11
topic: hasten-mcp-interface
artifact_contract: ce-unified-plan/v1
artifact_readiness: implementation-ready
product_contract_source: ce-brainstorm
execution: code
---

# Hasten MCP Interface - Plan

## Goal Capsule

- **Objective:** An operator's or developer's agent, connected over MCP (Model Context Protocol) to one running Hasten instance, can explain a situation that Hasten reports mechanically today, from evidence the instance already holds. Milestone one is that single instance, read-only: the hello world. The fleet view and the write path are later milestones whose direction this document records but does not specify.
- **Product authority:** This document, for the surface and its behaviours in milestone one, and for the recorded direction of the fleet and write milestones. It extends the embedded dashboard (astubbs#268), whose snapshot it projects, and it inherits that PR's read-only, loopback, unauthenticated posture unchanged. The control-plane note (`docs/inflight/web-control-plane.md`) owns the Observe/Explain/Act split this work sits inside; the admission controller (astubbs#333) and the navigator (astubbs#392, astubbs#456) own the explanations this surface returns. The fleet and write milestones are not active scope.
- **Open blockers:** None. The module shape is chosen (KD14) and the substrate it sits on is astubbs#514, whose API this plan is written against; the confirmation that was listed before planning is closed. What planning deferred is now settled in the Planning Contract, and what remains open is named there as a check rather than a blocker.
- **Stop conditions:** Stop and ask before widening the bind beyond loopback, before adding any tool that is not a read, before touching engine code, and before adding a dependency to the substrate or the core. Each of those reverses a decision this document records.
- **Execution profile:** Prove the staged fault against the reading before any agent is involved, so a failed acceptance run is attributable. The acceptance test is graded by an agent and needs its control arm, so it is not a boolean and does not belong in the ordinary lane.

---

## Product Contract

### Summary

Embed a read-only MCP server in the Hasten runtime, beside the existing dashboard server, exposing everything one instance knows: its state snapshot, a short history of recent samples, its configuration, and every explanation the engine already computes. An agent attached to that instance does the analysis; the tools return evidence and the engine's own verdicts, never a verdict computed for this surface. The acceptance test is an agent explaining, from the tools alone, why a staged instance has stopped committing on one partition - a fault the engine's existing state already accounts for, so milestone one changes no engine code.

### Problem Frame

Hasten already reports things a plain Kafka consumer cannot: a partition paused because its offset map reached three quarters of the metadata it may commit, a commit frontier held behind one incomplete record with thousands of later records already done, observed concurrency far below what was configured, a load factor that stepped up because workers were starving. On the branches that carry them, the admission controller names why the concurrency target is what it is, and the navigator names why a record is waiting on a shared resource and what rate is available. The dashboard (astubbs#268) draws all of this for a human on one instance, on one loopback port.

None of it reaches an agent. The person who meets these reports first is a developer on a laptop with a coding agent open, and the person who most needs them explained is an operator on Kubernetes asking why Hasten has paused, throttled or held something across a deployment. Both have an agent that could read the evidence and reason about it. Today the dashboard already publishes the machine-readable state document and event stream its page is drawn from, but an agent reaches them only if a human wires the URL and explains what each field means. The point of an MCP surface is that the machine-readable feed comes first and the human query interface second: the state exists, the analytics on top are what an agent does best, and Hasten never has to grow an analytics brain of its own.

### Key Decisions

- KD1. **Milestone one is read-only, and it is one instance.** The hello world proves the surface and the acceptance shape on a single running instance with no new infrastructure. (session-settled: user-directed - chosen over a fleet-first milestone and over including the write path: the simplest version first, and the fleet and writes each carry a design the hello world does not need.) Governs R1, R2, R12, R13.
- KD15. **Two primitives, split by job: a subscribable resource for the live state, tools for the questions an agent asks while reasoning.** The state is a document that changes continuously and is already published as an event stream, so it is a resource, and the server declares the subscribe capability so a client is told when it changes rather than polling for it. The questions an operator's agent asks mid-reasoning - why did this stop, what is holding the frontier - are tool calls, because a tool is what a model invokes during its own work and a subscription does nothing for a one-shot diagnostic. (session-settled: user-directed, 2026-09-12 - chosen over tools-only, which was the first draft's unexamined default and which can only notice change when the model calls again.) **Two things the plan states rather than assumes:** a resource-updated notification carries no data, so a client is told the resource changed and reads it again - this is not state pushed into a model's reasoning; and client support varies, some clients surfacing resources as context a person attaches and some not supporting subscriptions at all, so the tools must answer every question in AE1 on their own and the resource must be an improvement rather than a dependency. Governs R1, R2, R6, R26.
- KD2. **The MCP surface precedes any analytics query interface.** The machine-readable feed is built before a human query UI on top of the dashboard, because the agent does the querying. (session-settled: user-directed - chosen over building analytics queries into the dashboard first: an MCP server is the first consumer of the analytics that already exist.) Governs R1, R6.
- KD3. **Tools return everything the engine has, evidence and verdicts alike, and the agent analyses.** Nothing is computed for this surface. Where the engine already computes an explanation, that explanation is returned as a fact beside the evidence it rests on. (session-settled: user-directed - chosen over tools that compute explanations for the agent, and over evidence-only tools that hide engine verdicts: the value is agentic analysis of state and stats, and a computed explanation that exists is part of the state.) Governs R6, R7, R8, R9, R10, R11, R21.
- KD4. **Not net new: the surface projects the dashboard's snapshot.** The MCP tools read the same published snapshot the dashboard's state document and event stream read, extended where R7 and R8 need more, and no further: milestone one reads what the engine already publishes. (session-settled: user-directed - chosen over a separate state model for MCP: the analytics underneath are already exposed, and two models would drift.) Governs R4, R6, R7, R8.
- KD5. **The posture is inherited unchanged: read-only, loopback, no authentication, nothing starts without being asked.** The dashboard's security argument is that there are no write paths, and milestone one adds none. (session-settled: user-approved - chosen over opening the bind or adding a bearer token now: reachability comes from a port-forward, and authentication is a requirement of the first write path, not of this milestone.) Governs R3, R12, R13, R14, R19, R20, R22.
- KD6. **Acceptance is an agent explaining a staged, Hasten-specific situation from the tools alone.** The stage is head-of-line blocking: one partition's commit frontier held behind a single incomplete record while later records on it have finished. (Revised 2026-09-12, user-directed: "engine telemetry is not hello world". The first stage was a partition stopped at offset-map capacity, which the review showed needs per-partition state the engine does not retain - so milestone one stages the fault its existing state already explains, and the telemetry moves to a later milestone.) (session-settled: user-directed - chosen over plumbing-only acceptance and over a scaling verdict as the acceptance question: the question must be one only Hasten can raise, and the mechanical fact is Hasten's while the explanation is the agent's; the paused-partition stage was proposed with the hot-key stall as the alternative and accepted.) Governs R15, R16.
- KD7. **A fleet is every application running Hasten, across clusters; the fleet milestone scopes to one cluster to begin with.** An instance carries its application, its consumer group and its cluster as attributes, and none of the three bounds a fleet - the boundary is administrative, not topological. (session-settled: user-directed, 2026-09-12: "fleet boundary is actually multi cluster. Single cluster for now though" - chosen over fleet-as-one-consumer-group, and over cluster identity being the boundary.) So the first fleet view reads one cluster, and nothing in its design may assume that is all a fleet can be: membership is an attribute an instance carries, never something derived from which cluster it happens to be on. Direction for the fleet milestone; governs nothing in milestone one.
- KD8. **Fleet state fans in to the elected controller node only, never to every instance.** An instance never pulls from every peer and never consumes every peer's snapshot: both are quadratic in fleet size. Fleet questions go to the controller; instance questions are answered locally. (session-settled: user-directed - chosen over every instance holding the whole fleet from a shared topic: "that would explode".) Direction for the fleet milestone.
- KD9. **The controller node is the navigator's controller rung, not a second coordinator.** The node that will coordinate named shared resources across applications (the rung astubbs#228 keeps open) is the node that holds the fleet view and, later, receives writes. The controller role runs either inside one of the application instances or as a standalone process that hosts nobody's application; both are the same code, so the standalone form is expected to be trivial, and it is the option for a fleet whose owner does not want a controller running inside an application. (session-settled: user-directed - chosen over a separate fleet aggregator, and over embedded-only hosting: the MCP surface meshes with the named-resource controller system rather than duplicating its election and its view, and the standalone host was asked for as an option that must stay cheap.) Direction for the fleet milestone.
- KD10. **Redirect where the client can reach the target; proxy only where it cannot.** Fleet reads and all writes redirect to the controller when the fleet is reachable from the client, and are proxied by the instance the client reached when it is not, as through a port-forward to one pod. **The server never guesses what the client can reach.** It redirects; the client reports back when it could not follow, and the instance then proxies. Pod DNS, service addresses, NAT and port-forwards make server-side reachability a poor proxy for client-side, so there is no test the server could apply alone. (session-settled: user-directed, 2026-09-12 - chosen over proxying always, which loses the saving, and over the server inferring reachability, which has no implementable rule.) Direction for the fleet milestone.
- KD11. **The fleet store is deferred to the fleet milestone.** Two candidates fan in at one place and are recorded: the controller consuming a Kafka topic that every instance publishes to, and the controller or the MCP server querying the operator's Prometheus, which already scrapes the fleet and holds its history. Kubernetes label discovery and consumer-group membership discovery are recorded and rejected for the fleet view, because each has every instance pulling from every peer unless only the controller pulls, at which point they are a discovery mechanism for the first candidate rather than a store. (session-settled: user-directed - chosen over deciding the store now: milestone one needs no fleet.) Direction for the fleet milestone.
- KD12. **The audience is operators on Kubernetes, reached through developers.** Operators querying a deployment's runtime and scaling state are the target; developers on laptops are the first users by necessity, since nothing reaches an operator without going through development. (session-settled: user-directed - chosen over framing the developer as the target: the developer is the path, the operator is the destination.) Governs R2, R15.
- KD13. **Four surfaces over one API, and the MCP server first.** The target shape is a web GUI, a CLI tool, an MCP server and the API they all consume; the API is the substrate, and each of the others is a projection of it, the CLI included. The MCP server is the first of the projections built, because the audience with an agent attached is the one it unlocks, and because Parallel Consumer holds internal state - the sparse completion frontier, the offset map's fill, the ordering domains, the admission reasons - that the project believes no comparable library exposes. That belief is a **claimed** differentiator in the adjacent-systems register's evidence vocabulary: nobody has checked it, and the register's question set does not yet ask what a neighbour exposes rather than how it schedules. It must not be stated as fact in announcement material until it is. Pointing an agent at the dashboard's state document was the rejected alternative for the first projection: the discriminator is that MCP tools describe themselves, so an agent with no Hasten knowledge attaches, selects and interprets without a human wiring a URL or explaining the fields. The cost is a JVM MCP dependency inside the user's application classpath, weighed under Dependencies. In milestone one the API is the dashboard's published snapshot and its state-document route, which R6, R7 and R8 extend; that is the contract the CLI's "once the API is stable" precondition refers to, so no second contract is created. The work is a candidate for the 6.1 release. (session-settled: user-directed - chosen over a CLI first and over the state document alone: all four surfaces are wanted, the MCP server is the initial aim, and the CLI follows on the same API.) Governs R1, R5, R17.
- KD14. **The MCP server gets its own module, above the substrate, at a Java 17 floor. The web GUI keeps its own module and its own floor.** Three modules stack: core at whatever the Kafka clients require, the observability substrate above it declaring only core, and each surface above that declaring the substrate. (session-settled: user-directed, 2026-09-12 - this **reverses** the decision of 2026-09-11 that the two surfaces share one module, which was taken on the reasoning that both are HTTP services over the same state. The reversal is recorded rather than quietly replaced: that reasoning still holds and was not wrong, it was outweighed.) **What the reversal buys, and it is more than convenience:** an application that wants the web GUI no longer resolves the MCP SDK and its transitive tree, which is an open question this plan carried and could not answer inside one shared module; only the MCP module needs Java 17, for the SDK, so the dashboard keeps its lower floor and a Java 8 application loses neither the core library nor the GUI, narrowing the compromise accepted on 2026-09-11 to the MCP module alone; and the work can start against master plus the substrate rather than waiting on astubbs#268, a draft, or stacking on its branch. **What it costs:** a third module to maintain, and two HTTP servers rather than one where an application runs both surfaces. What mattered about sharing - one model of the state rather than two - is preserved by the substrate, which is what both now depend on. Governs R1, R12, R14.
- A1. **Developer with a coding agent**, running one Hasten instance on a laptop, attaching the agent to it while writing or debugging.
- A2. **Operator with an agent**, running Hasten instances on Kubernetes, reaching one instance through a port-forward and asking about it. In milestone one the operator's questions are about that instance; the fleet arrives in the next milestone.
- A3. **The agent**, an MCP client that calls the tools, reads what they return, and produces the explanation. It is not part of this product.
- A4. **The processor instance**, one Parallel Consumer running inside an application JVM, whose state the tools answer from. A JVM may hold several: the engine supports multiple processor instances per JVM and tags each one distinctly in its metrics, so "instance" here means the processor, not the JVM and not the application.

### Requirements

**Surface**

- R1. The runtime exposes an MCP server that any conforming MCP client can connect to over a network transport, on its own listener in its own module. It does not mount beside the dashboard's server - KD14 separated them - so an application may run either surface, or both on their own ports.
- R2. Every tool is a read, and that property is enforced at the protocol layer rather than by HTTP method: the server advertises a fixed, enumerated read-only tool set and a read-only resource, registers nothing write-capable and no prompts, declares no capability that lets it act back on the host - no sampling, no elicitation, no roots - and answers any request outside that set as a protocol error. The subscribe capability of KD15 is the one exception to "declares no capability", and it is read-only: it lets the server say a resource changed, never act on the client. The dashboard's rejection of every non-read HTTP method cannot carry this property, because every MCP call is a POST, including a pure read.
- R3. The server is opt-in in the same way the dashboard is: having the module on the classpath starts nothing, and the application starts it.
- R25. The server names which processor instance it answers for, and says what happens when a JVM holds several: whether each gets its own server or one server routes by instance, which instance a tool answers about by default, and what a tool returns once that instance has closed. This precedes the substrate API because it decides its keys and ownership.
- R4. Every tool answers from the published snapshot and never from live engine objects, so that no tool can reproduce the gauge-on-a-foreign-thread failure the dashboard's snapshot design exists to prevent.
- R5. Each tool describes itself well enough that an agent with no prior knowledge of Hasten can choose the right one for "why is this partition paused" and "what is this instance doing", and can interpret what it returns (R17).

**Evidence**

- R26. The instance's current state is exposed as an MCP resource, and the server declares the subscribe capability so a client is notified when it changes. The notification names the resource and carries no data; the client re-reads it. A client that does not subscribe, or does not support resources at all, loses nothing it needs: every question AE1 asks is answerable through the tools alone, and the resource is an improvement on polling rather than a prerequisite.

- R6. The tools return the whole of the instance's snapshot: lifecycle state, work counts, offset-encoding statistics, and per-partition offsets and rates, at the same fidelity the dashboard's state document has.
- R7. The tools return the instance's effective configuration, including the configured concurrency and the ordering and commit modes, so that observed values can be judged against configured ones.
- R8. The tools return a short history of recent snapshots for the instance, not only the latest, so that an agent can see what changed. The history is a wall-clock duration, with samples retained at a fixed cadence independent of the control loop's rate, because the loop samples sub-millisecond apart under load and seconds apart when idle. The duration and cadence are planning decisions.
- R23. Every tool response is bounded and the bound is part of the contract, not a planning detail: a maximum response size, a cursor for continuation, and an explicit truncation marker when a result is cut. Without it a single history call grows as partitions multiplied by retained samples and can exceed a client's context, allocate a large response inside the user's application, or retransmit the same data every call.
- R9. Every explanation the engine computes is returned as a fact beside its evidence. On a base that carries the admission controller (astubbs#333), that includes the current admission target, the would-be target in observe mode, the reason the controller chose it, and whether the admission limit was the binding constraint in the last window. On a base that carries the navigator (astubbs#392, astubbs#456), that includes the resource view: what is resource-ineligible, why, and what rate is available.
- R10. On a base that carries none of those, the tools return the evidence alone and say nothing is computed, rather than failing or inventing a verdict.
- R11. An explanation arriving with a later branch is surfaced by adding one entry to the snapshot's mapping, or one named contribution for state that is not a meter, and costs no change to the MCP tool set or its descriptions. To make that hold, the tools expose the snapshot's sections generically rather than one tool per field, so a snapshot addition needs no tool definition change.
- R21. An entry added under R11 carries, as part of the same entry, both the meaning R17 requires and a declaration of whether it may be exposed to an agent. An entry published without the meaning R17 requires of it fails the build. An entry that does not declare itself agent-exposed is withheld from the tools by default, and that declaration is where R19's allowlist and R20's record-content ban are checked. Without this, the zero-change path of R11 is also the path that bypasses every rule the tools were supposed to carry.
- R17. Every value a tool returns carries its meaning with it. A precise description and the engine term it belongs to are always required. A unit and a limit are required **only where the value has them**: many values are categorical or identifying (lifecycle state, application, topic, partition) or are unbounded counters and absolute offsets, and forcing a limit onto those would make an implementation either fabricate a misleading one or withhold a field R6 requires.

**Posture**

- R12. The server binds to loopback by default, with the same host allowlist and the same opt-in widening the dashboard has.
- R13. The server requires no authentication in this milestone. (session-settled: user-directed, 2026-09-12 - "no Auth for now", taken with the disclosure fact below in view.) **R2 is not the whole argument, and the plan does not pretend it is:** read-only protects integrity, not disclosure, and loopback is not isolation in a Kubernetes pod, where other containers share the network namespace and can reach the listener. So anything that can reach the port can read everything R6, R7 and R8 carry. The milestone accepts that for a developer's own laptop and an operator's own port-forward; an adopter who cannot accept it has to isolate the pod or wait for the write milestone, which brings authentication with it.
- R24. The server bounds what a client can cost it: a cap on concurrent sessions, an idle timeout, and a defined overload response when either is exceeded. It runs inside the message-processing JVM, so an agent that reconnects without closing sessions or enters a tool-call loop must not be able to degrade the processor it is diagnosing. The dashboard already bounds its own long-lived streams this way.
- R14. The server never logs during its port search and logs one line once bound, carrying the URL a client needs, matching the dashboard's start-up contract.
- R19. The configuration tool (R7) returns only an explicit allowlist of non-secret keys: the configured concurrency, the ordering and commit modes, and whatever planning adds by name. It never returns the raw consumer or producer property map, because that map routinely carries SASL and SSL passwords and tool output goes to a remote model service.
- R20. No tool returns a value derived from record content in this milestone. If the deferred per-record detail is ever added, record keys and values are labelled in the tool output as untrusted data carried from the stream, because they come from outside the trust boundary and the agent recommends actions to an operator.
- R22. Everything any tool returns leaves the machine. An MCP client is normally backed by a remote model service, so the application name, consumer group, topic names, partition assignments, offsets and every other value the tools carry reach that vendor. Milestone one accepts this for a developer's own laptop and an operator's own port-forward, and says so rather than leaving an operator in a regulated environment to discover it.

**Acceptance harness**

- R15. A staged scenario runs one instance into the situation "one partition's commit frontier is held behind a single incomplete record while later records on it have already finished" - head-of-line blocking. The stage is built in this module, because the dashboard's showcase scenario and the framework it consumes exist only on the web-GUI branch set and inside the dashboard module, which a module built on master plus the substrate cannot reach (KTD11). What is reused is its recipe - one key pinned to fail on every delivery - not its code. **No engine change is needed for it:** the per-partition committed offset, the sequential-succeeded frontier, the highest completed and highest seen offsets, and the incomplete count are all published today.
- R16. An agent connected only to the MCP server, given the operator's question "Hasten has stopped committing on partition N, why, and what should I do", names the cause and a sensible action, from the tools alone, on the staged scenario.

### Key Flows

- F1. Developer attaches an agent to a local instance
  - **Trigger:** A1 starts an application with the MCP server enabled and points a coding agent at the logged URL.
  - **Actors:** A1, A3, A4
  - **Steps:** The server logs its URL once bound; the agent connects and lists the tools; the developer asks what the instance is doing; the agent calls the snapshot and configuration tools and answers from them.
  - **Outcome:** The developer gets an explanation of the instance's state without opening the dashboard or tailing a log.
  - **Covered by:** R1, R3, R5, R6, R7, R14

- F2. Operator asks why a partition is paused
  - **Trigger:** A2 port-forwards to one pod and asks their agent why Hasten has paused a partition.
  - **Actors:** A2, A3, A4
  - **Steps:** The agent reads the partition's offsets, the incomplete-offset count, the encoding statistics and the recent history; where the base carries it, the agent also reads the admission reason; the agent names the incomplete record's position, the capacity the map reached, and the action.
  - **Outcome:** The operator has the cause and a next step, and the pause is no longer a mystery reported as a number.
  - **Covered by:** R4, R6, R8, R9, R10, R16

- F3. A write is attempted
  - **Trigger:** A3 calls anything that would change state, or a client sends a non-read request to the server.
  - **Actors:** A3, A4
  - **Steps:** The server refuses, because no such tool is registered and no capability permitting it is declared; nothing in the instance changes.
  - **Outcome:** The read-only posture holds by construction of the tool set. It is not the dashboard's mechanism: that one rejects every non-read HTTP method, and MCP carries reads over the same method it would carry writes.
  - **Covered by:** R2, R13

### Acceptance Examples

- AE1. The paused partition is explained
  - **Covers R15, R16.**
  - **Given** the staged scenario has left partition 3's commit frontier held behind one incomplete record, with later records on that partition already finished.
  - **When** an agent connected only to the MCP server is asked why Hasten has stopped committing on partition 3 and what to do.
  - **Then** the agent's answer names the partition, says the commit frontier is held by incomplete work rather than by a lack of progress, quantifies the finished work waiting behind it from the gap between the committed offset and the highest completed offset, and gives an action that addresses that record rather than the partition count or the instance's capacity.

- AE2. Engine verdicts are returned when present
  - **Covers R9, R11.**
  - **Given** the instance's base carries the admission controller and the controller's last window was classified ordering-starved.
  - **When** the agent asks why observed concurrency is below the configured value.
  - **Then** the tools return the admission target, the reason "ordering-starved" and the binding classification as facts, beside the configured concurrency from R7, and the agent's explanation cites them.

- AE3. Nothing computed, evidence still answers
  - **Covers R10.**
  - **Given** the instance's base carries neither the admission controller nor the navigator.
  - **When** the agent asks why observed concurrency is below the configured value.
  - **Then** the tools return the work counts, shard count and configured concurrency, state that no engine explanation is computed, and the agent reasons from the evidence.

- AE4. History shows what changed
  - **Covers R8.**
  - **Given** the paused partition was healthy in the recent window.
  - **When** the agent asks when the partition stopped advancing.
  - **Then** the tools return the recent samples, and the agent can name the sample at which the frontier stopped moving.

- AE5. A write is refused
  - **Covers R2, R13.**
  - **Given** a client connected to the server.
  - **When** it attempts any operation that would change the instance's state.
  - **Then** the server refuses it and the instance's snapshot is unchanged.

- AE6. Nothing starts on its own
  - **Covers R3.**
  - **Given** an application with the module on the classpath that never starts the server.
  - **When** the application runs.
  - **Then** no socket is opened and nothing is logged about MCP.

- AE7. Credentials never leave the instance
  - **Covers R7, R19.**
  - **Given** an instance configured with a SASL password and an SSL keystore password.
  - **When** the agent calls every tool.
  - **Then** neither password, nor any client property outside the allowlist, appears in any tool output.

### Success Criteria

- AE1 passes with a real agent against the staged scenario, graded against its four named elements over repeated runs, with the agent and model named in the record and the transcript kept as the record of what the tools had to carry. **It runs with a control arm, or it proves nothing about this surface:** the same agent, the same staged fault, given the dashboard's existing state document with equivalent field descriptions instead of the tools. A pass on the MCP arm alone shows an agent can paraphrase staged data; it does not show that tool discovery and self-description earned an SDK inside every consuming application, which is KD13's claim. Name the measurable difference before this result is used to choose the first projection. The prompt must also not hand the agent the answer: if it names the partition, "which partition" is free, and a single-partition stage makes it free anyway. Failure is attributed three ways, and only the first grows the evidence surface: the transcript shows the agent sought a fact the tools do not carry, so evidence is missing; the transcript shows the agent never sought a fact the tools do carry, so the tool descriptions and value metadata are at fault and R5 and R17 change instead; otherwise it is the agent's, and nothing changes. The middle branch exists because self-description is the reason this surface was chosen first, and without it the only acceptance test is blind to the property it was chosen for.
- A developer can go from an application start to an agent's first explanation using only the logged URL and the tools' own descriptions.
- An operator reaching one instance of a Kubernetes deployment through a port-forward gets the paused-partition explanation from the tools alone, F2 end to end.

<!-- ce-section: work-relationships -->
### How This Work Fits Together

This plan owns milestone one: one instance, read-only. The breakdown below is the current understanding, not a committed roadmap; later plans may revise it.

- **Milestone two: the fleet view.** Depends on this plan's surface. Still to decide: the fleet store (KD11). Settled direction: KD7, KD8, KD9, KD10. Its acceptance question is the operator's: explain why Hasten has recommended, or declined, a scale-out across the deployment.
  - Depends on the navigator's controller rung (astubbs#228) for the elected controller node, per KD9.
  - Depends on an instance-count recommendation that nothing computes today: the auto-scaling note rules it outside the engine-concurrency stack and no branch's main code produces one, so producing it is a prerequisite of that milestone's acceptance question.
  - Still to decide: whether the first fleet deployment hosts the controller inside an application instance or as the standalone process KD9 allows; the plan for that milestone must show the standalone form costs nothing beyond a host.
  - Still to decide: authentication and authorization, because milestone two changes the two things the no-auth posture rests on: the controller node cannot stay loopback-only once fleet state fans in to it (KD8), and one application's agent can reach another application's state through the redirect or proxy (KD7, KD10).
  - Shares the reachability rule of KD10 with milestone three.
- **Milestone three: the write path.** Depends on milestone two's controller node, and on authentication, which becomes a requirement the moment the first write exists. Each write is an engine API before it is a tool; `docs/inflight/web-control-plane.md` owns that list.
- **The CLI tool.** A later projection of the same API the MCP server projects, for developers in a terminal and for CI. Can proceed independently of this plan once the API is stable; shares R17's rule that values carry their meaning, and the configuration allowlist of R19.
- **Can proceed independently of** the dashboard's own Observe and Explain panels, which draw the same snapshot for a human.

### Scope Boundaries

**Deferred for later**

- The fleet view, the controller node, the redirect and proxy rules, and the fleet store: milestone two, direction in KD7 to KD11.
- A fleet spanning more than one Kafka cluster: wanted, and deferred rather than excluded. The fleet milestone reads one cluster first (KD7), so nothing it builds may treat cluster identity as the fleet boundary.
- Writes, the leader-forwarding path, and authentication: milestone three.
- **Per-partition blocked state and offset-map fill, as new engine telemetry.** The engine retains neither: the encoded payload length is a local inside the encoding step, and what persists is a boolean plus an aggregating distribution summary. Publishing them is core work with its own sampling semantics and owner, and a hello world does not change the engine - so it is a later milestone, and milestone one stages the fault existing state already explains (KD6, R15).
- A scaling recommendation and its reasons: owned by astubbs#333 and the auto-scaling note; this surface returns what those produce and computes nothing.
- The CLI tool, and any API layer distinct from the snapshot the four surfaces already project: a later projection, direction in KD13.
- Per-record detail beyond the snapshot, such as the blocking record's key and attempt count: added only if AE1 cannot pass without it, and labelled per R20 if it is.

**Outside this product's identity**

- An analytics brain in Hasten that computes explanations for this surface. The agent analyses; the engine reports.
- A privileged backend for MCP that the dashboard does not have: both read the same snapshot.

### Dependencies / Assumptions

- **Depends on astubbs#514**, the observability substrate, which carries the reading this surface projects. KD14 replaced the dependency on astubbs#268: the MCP server has its own module and its own listener, so it is built on master plus the substrate and waits on no draft. The dashboard (astubbs#268) is now a sibling rather than a base, and the two agree because both project the same reading.
- **Reads astubbs#333, astubbs#392 and astubbs#456 when present, and works without them** (R9, R10). None is a prerequisite of milestone one.
- **Assumes** the dashboard's snapshot is derived from the meter registry and the control thread's own sampling, so R7 and R8 are additions to what is sampled and retained, not a new sampling path.
- **Assumes** the navigator's contribution (R9) arrives through its read-only view object on the control thread, not through the meter registry, so it is a named non-meter contribution under R11.
- **Verified, no longer assumed:** a maintained JVM MCP server implementation exists and is chosen in KTD1, and its transport cost is settled in KTD2. What follows in this bullet is the reasoning that led there, kept because it records what the choice was weighed against. It no longer has to sit beside the dashboard's server - KD14 gives the MCP server its own module and its own listener - which removes the Vert.x transport constraint from the selection, though transport integration is still a criterion for whatever server this module does run. Its class-file version is why KD14 sets the shared module's floor at Java 17: the module otherwise inherits the project-wide Java 8 bytecode target, and current JVM MCP SDKs are built for 17. The Mutiny module's pom records how a module declares the higher floor and why the build cannot detect a mismatch on its own; the dashboard module (astubbs#268) takes the same declaration when this work lands in it. Two further selection criteria weigh beside the class-file version, because the SDK lands inside somebody else's production application: its **transitive footprint and maintenance record**, since a user who wants only the web GUI resolves whatever the MCP SDK drags in, and **transport integration**, since the dashboard's server is Vert.x while stock JVM MCP transports are servlet- or framework-bound, so the SDK arm costs either a transport provider written against its extension point or a second HTTP stack in the user's classpath. An SDK needing its own container is much closer in cost to implementing the transport directly, which is the comparison planning has to make rather than inherit.

### Outstanding Questions

**Resolved in planning**

- **The module shape**, confirmed against astubbs#514: this plan is written against that module's published API, so the confirmation is closed rather than pending.
- **The history window and cadence** - KTD9, five minutes at a one-second cadence, held in a ring this module owns and fed by a hand-off listener.
- **Which HTTP server this module runs and how it selects a port** - KTD2 and U3. The gate is this module's own, as KTD3 requires: inheriting the dashboard's allowlist by name is not the same as having it.
- **Tool granularity** - KTD5, four tools whose payloads are the reading, which keeps R5's selectability and R11's zero-change path at once.
- **How the staged scenario is driven** - KTD11 and U10: an integration test in this module over core's broker base, asserted against the reading before any agent runs, then a graded agent run with its control arm.

**Still open, as named checks rather than blockers**

- **Which subscription wire shape the pinned SDK emits** (KTD8). The current protocol revision replaced the per-resource subscribe call; the released SDK trails it. Resolved at pin time, against the SDK, and recorded in the PR.
- **Whether the SDK puts a reactive library on a consuming application's compile classpath.** Its build file mentions one and the research could not establish its scope. It decides whether KTD2's footprint argument still holds.
- **Which further non-secret configuration values join the allowlist** (R7, R19). Open by design: the allowlist grows by name in review, never by pattern, which is the property U8 is built to keep.

### Sources / Research

- `docs/inflight/web-control-plane.md`: the Observe/Explain/Act split, the ruling that Observe and Explain ship on the read-only posture and Act cannot, and the control plane running embedded with any instance redirecting to the leader.
- `docs/inflight/core-runtime-services-and-compat.md`: distributed interactive queries as a runtime service, and the rule that the UI uses the same API with no privileged backend.
- `docs/inflight/core-standalone-deployment.md` and `docs/inflight/core-non-kafka-participants.md`: ingesting Prometheus as the way the engine sees a fleet it does not run inside, the origin of the Prometheus candidate in KD11.
- `docs/inflight/core-auto-scaling.md`: the recommendation shape, instance count with hysteresis and rebalance as acknowledgement, and the ruling that instance-count recommendation is outside the engine-concurrency stack.
- `docs/inflight/core-execution-opportunity-model.md`: the gate ladder that the admission reasons read as arithmetic.
- The embedded dashboard, astubbs#268 on branch `feats/web-gui`: the snapshot publisher, the state document and event-stream routes, the read-only 405 handler, the loopback default and host allowlist, and the showcase scenario that R15 extends with a new phase.
- The admission controller, astubbs#333 on branch `feats/ideate-distributed-throttling`: the decision-reason enumeration published as a gauge, and the per-window binding classification.
- The navigator, astubbs#392 and astubbs#456: the resource contract, the allocators, and the read-only view written for the later web GUI to assert against, which R9 returns.
- Verified against the code on 2026-09-11: no branch computes a scale-out recommendation in main code; no branch declares an MCP dependency; a partition refuses records at three quarters of its maximum commit metadata.

## Planning Contract

### Key Technical Decisions

- KTD1. **The SDK is the reference JVM implementation, `io.modelcontextprotocol.sdk:mcp-core`, with a Jackson JSON binding module.** It is MIT licensed, maintained by the protocol's own project, and its class files target Java 17 - the fact KD14's floor rests on, now verified rather than assumed. Pin an exact version and re-verify the coordinate at pin time: the artifact was at 2.0.1 when this plan was written and the project's own development line was already a minor ahead, so the version is under churn and this plan does not treat it as stable. Rejected: the Quarkus MCP server, which is annotation-driven over CDI and brings the Quarkus runtime model into an arbitrary host application; and hand-writing the protocol, which buys nothing a maintained library already carries.
- KTD2. **The module embeds a servlet container and hosts the SDK's bundled servlet transport.** The SDK ships two server transports: stdio, and one servlet class for Streamable HTTP. Neither is a listener, so the module either supplies a container or implements the SDK's transport-provider interface itself. Milestone one supplies the container, Jetty, whose own floor is also 17. **Rejected for milestone one and recorded for later:** a transport provider written over Vert.x Web, which the dashboard already runs and which would keep one HTTP stack in an application running both surfaces. A hello world should not own session handling, stream resumption and event ids, which is most of what a transport is. KD14 is what makes the container acceptable: this module's dependencies reach only an application that asked for MCP. The trigger to revisit is an adopter objecting to the footprint, or the two surfaces shipping together often enough that two listeners become the complaint.
- KTD3. **The Host and Origin check is this module's own, as a servlet filter registered ahead of the MCP servlet.** Origin validation answered with 403 is a protocol MUST and loopback binding is a SHOULD, so the dashboard's posture is also the specification's. The logic is ported from the dashboard's `HostAllowlist`: duplicate Host or Origin headers refused, Host matched against the loopback names plus the bound address plus the configured extras, same origin required, a null Origin treated as cross-origin, and no CORS header ever emitted. Ported rather than shared, because that handler is written against Vert.x routing types and lives in a module this one does not depend on. Register it on a path pattern rather than on the single endpoint, so a later endpoint inherits the gate instead of having to remember it. Streamable HTTP exposes one endpoint today, which makes the gate cheap, not optional: CVE-2026-11624 was a critical Origin-validation failure in an MCP server that believed it was local-only.
- KTD4. **One server answers for one processor instance, and is constructed with that instance's reading publisher.** This is R25's answer, and it needs no routing layer. An application running three processors starts three servers on three ports, each naming its own instance in its own bound log line. Instance identity comes from the reading's own tags rather than from anything this module invents. Once the processor has closed, the tools return the last reading with its age and say the instance is closed, because the substrate's sampling stop has no counterpart and a stale reading carrying its age is more useful to an agent than an error.
- KTD5. **Four tools, whose payloads are the substrate's reading rather than per-field definitions.** One returns the current state, one a window of history, one the allowlisted configuration, one the engine's own verdicts. That shape satisfies both halves that pull against each other: four descriptions let an agent select on the job it has (R5), and a field added to a reading changes a generated schema rather than a tool definition (R11). Rejected: one tool per section, which multiplies the descriptions an agent must read before choosing; and a single sectioned tool, which makes "why has this partition stopped" and "what changed" the same call with the same description.
- KTD6. **Each value's meaning lives in a field registry in this module, projected into the tool's output schema, with the values carried as structured content.** The protocol supports declaring an output schema per tool and returning structured content validated against it, which is where per-field semantics are machine-checkable; a tool description is one block of prose and cannot carry them. One registry entry per exposed field holds the engine term, the unit where the value has one, the limit where it has one, and the agent-exposure declaration R21 requires. A test fails the build when an exposed field has no entry, or an entry has no engine term and description, and that test is R21's enforcement point. **The registry belongs here, not in the substrate:** the substrate has no business knowing that agents exist, and KD14's stack makes each surface own its own projection.
- KTD7. **Every tool declares itself read-only, non-destructive, idempotent and closed-world through the protocol's tool annotations.** All four are accurate for this tool set, and a client that acts on them spares the operator a confirmation prompt on a read. They are hints a server self-reports and the specification says to treat as untrusted, so they are not the read-only boundary; the fixed registry of R2 is.
- KTD8. **The resource declares its subscribe capability through the SDK's own API, and this module never hand-writes the update notification.** The wire shape has moved: the current protocol revision replaced the per-resource subscribe call with a listen call carrying a notification filter, plus an acknowledgement notification, while the released SDK still speaks the previous revision. So the wire is the SDK's to own, and **which shape the pinned version actually emits is a named check at implementation time**, not an assumption this plan makes. **The resource stays useful with no subscription at all:** a client that only re-reads on a timer gets the same document. Client support for live subscriptions could not be confirmed from primary sources, and KD15 already requires the tools to answer every acceptance question on their own.
- KTD9. **A fixed-cadence ring buffer in this module supplies the history of R8, fed by a substrate listener that only hands off.** The substrate publishes a current and a previous reading, which is two samples, and the control loop samples sub-millisecond apart under load and seconds apart when idle. So the module registers a listener that copies a reading reference into a ring slot when the cadence has elapsed and does nothing else: no I/O, no blocking, no allocation beyond the slot. The substrate's listener contract runs on the control thread and is load-bearing rather than stylistic. The window is five minutes at a one-second cadence. What is retained is partitions multiplied by the sample count, which is why R23's bound applies to reading the window back rather than to holding it.
- KTD10. **Bounds live on the options object, are checked in the server, and are reported as protocol errors.** A maximum response size, a cursor on the history tool and a truncation marker when a result is cut satisfy R23. A concurrent-session cap and an idle timeout satisfy R24. Sessions are expressed in whatever the pinned SDK exposes rather than in the wire's session header, because the newer protocol revision removed sessions from the wire while the released SDK still has them.
- KTD11. **The acceptance harness stages head-of-line blocking in this module's own integration tests, and the plan stops claiming the dashboard's showcase scenario.** The showcase and the scenario framework it consumes exist only on the web-GUI branch set, inside the dashboard module, so a module built on master plus the substrate can see neither. What transfers is the recipe, not the code: one key pinned to fail on every delivery, with later records on the same partition completing past it, which is the same lever the showcase's own head-of-line-blocking phase pulls. Core's integration helpers **are** reachable, because the root pom adds the integration source root to every module and produces a test-jar per module, and the substrate module already declares core's tests classifier as the precedent for depending on it.

### High-Level Technical Design

Three modules stack, each declaring only the one below it.

```
parallel-consumer-core                 Kafka's own Java floor
  └── parallel-consumer-observability  the reading: sampled on the control thread, immutable
        ├── parallel-consumer-dashboard   Vert.x, its own port, its own floor  (astubbs#268)
        └── parallel-consumer-mcp         Jetty + MCP SDK, its own port, Java 17
```

One request path, gated before it reaches the protocol.

```
MCP client  --POST-->  Jetty  -->  host/origin filter  -->  MCP servlet transport
                                        |                        |
                                    403 on a bad                 v
                                    Host or Origin        SDK server: tools, resource
                                                                  |
                                                                  v
                                              field registry  -->  reading publisher
                                                                  (current, previous, ring)
```

Two properties the diagram is drawn to make visible. The filter sits on a path pattern ahead of everything, so a second endpoint cannot skip it. And nothing in this module reaches an engine object: every tool reads a published reading, which is what R4 asks for and what the substrate exists to guarantee.

### Implementation Constraints

- **The control thread is not this module's to spend.** The substrate's listener contract permits a hand-off and nothing else. A tool call must never wait on a sample, and the ring's writer must never allocate per reading beyond its slot.
- **The module declares its own Java floor, and the build cannot catch a mistake.** A release level constrains the platform API, not what javac reads off the classpath, so a module depending on Java 17 class files while targeting a lower level compiles cleanly and fails at the adopter's runtime. The Mutiny module's pom records the incident; this module's pom carries the same declaration with its own reason named, the SDK and the container.
- **The parent pom's own dependencies block is inherited.** Lombok, SLF4J, JUnit, AssertJ, Mockito, Truth, Awaitility and ArchUnit arrive already. Redeclaring any of them is the error that broke the substrate module's first build.
- **A module with test sources needs its own test-conventions ArchUnit class or ArchUnit silently does not run**, and a core test walks the tree to fail the build when one is missing.
- **Every public type is marked unstable**, following both existing surfaces, and this module carries its own ArchUnit test for that because nothing enforces it repo-wide.
- **Nothing starts from the classpath.** No service-loader entry, no static initialiser, no scanning. The guarantee holds structurally on both existing surfaces and is asserted by AE6 here.
- **New files take the fork's own copyright header**, never the upstream one.

### Sequencing

U1 gates everything. U2 and U5 are independent of each other and of the transport, so they can run in parallel with U3. U4 needs U1 and U3. U6 needs U4 and U5. U7, U8 and U9 need U6. U10 needs the surface complete.

The first vertical slice that is worth showing anybody is U1 through U6: a client connects, lists the tools, and reads the instance's state with its values carrying their meaning. U7 to U9 widen it; U10 is what decides whether KD13's claim survives.

---

## Implementation Units

| U-ID | Title | Files touched | Depends on |
|---|---|---|---|
| U1 | The module exists and the build accepts it | `pom.xml`, `parallel-consumer-mcp/pom.xml`, the module's arch tests | - |
| U2 | Options, with the posture and the bounds as defaults | `McpServerOptions` | U1 |
| U3 | The listener: container, port walk, one log line, the gate | `McpHttpListener`, `McpHostAllowlistFilter` | U1 |
| U4 | The MCP server: capabilities, lifecycle, the fixed registry | `HastenMcpServer` | U1, U3 |
| U5 | The field registry and its build-time check | `ExposedField`, `FieldRegistry`, `FieldRegistryArchTest` | U1 |
| U6 | The state tool over the reading | `StateTool`, the schema projection | U4, U5 |
| U7 | The history ring and its tool, bounded and cursored | `ReadingRing`, `HistoryTool` | U6 |
| U8 | The configuration tool and its allowlist | `ConfigTool` | U6 |
| U9 | The resource and its subscribe capability | `StateResource` | U6 |
| U10 | The acceptance harness, the graded run and its control arm | the module's integration tests | U6 to U9 |
<!-- file-refs: N/A - the index names the module this plan proposes; none of its paths exist yet -->

Unit file names are intent, not a contract: the implementer may rename, and the path stays `parallel-consumer-mcp/src/main/java/bz/stub/parallelconsumer/mcp/`.
### U1. The module exists and the build accepts it

- **Goal:** A new module that builds, declares a Java 17 floor with its reason, depends on the substrate, and satisfies every gate a new module meets.
- **Requirements:** R1, R3
- **Files:** `pom.xml` (the modules list), `parallel-consumer-mcp/pom.xml`, `parallel-consumer-mcp/src/test/java/.../TestConventionsArchTest.java`, `parallel-consumer-mcp/src/test/java/.../ExperimentalApiArchTest.java`
- **Approach:** Add the module to the root pom's modules list. The module pom declares the substrate, the MCP SDK with a Jackson binding, and the container; it declares nothing the parent's own dependencies block already carries. It overrides the release target to 17 inside its own properties, with a comment naming the SDK and the container as the constraint and stating that the build cannot detect a mismatch - the Mutiny module's pom is the model and the reason. Copy the test-conventions ArchUnit class from the substrate module verbatim. Copy the dashboard module's experimental-API ArchUnit test, which asserts every public type is marked unstable.
- **Test scenarios:** the whole reactor installs; the core test that walks the tree for modules missing a test-conventions class passes; the experimental-API test fails when a public type in this module is unmarked; the copyright gate passes on every new file.
- **Verification:** `./mvnw clean install -DskipTests` from the repo root, then `bin/check-all.sh`.
<!-- file-refs: N/A - the module pom and its arch tests are what this unit creates -->
### U2. Options, with the posture and the bounds as defaults

- **Goal:** One immutable options value that makes the safe posture the default and carries every bound the server enforces.
- **Requirements:** R12, R23, R24, R8
- **Files:** `McpServerOptions`
- **Approach:** Mirror the dashboard's options type: a Lombok value with a builder, defaults and validation applied in the constructor rather than through builder defaults, and hosts normalised to lower case in a set that keeps its order. Carry the bind address defaulting to the loopback address, the port, the number of ports to walk, the extra allowed hosts, the concurrent-session cap, the idle timeout, and the history window and cadence of KTD9. Validate in the constructor: a non-positive window, cadence, port or session cap is an illegal argument at construction rather than a surprise at the first tool call.
- **Test scenarios:** defaults bind loopback and allow no extra host; a widened host list is normalised and order-preserving; each invalid bound is refused at construction with a message naming the field; the defaults name a window and cadence consistent with KTD9.
- **Verification:** `./mvnw test -pl :parallel-consumer-mcp`

### U3. The listener: container, port walk, one log line, the gate

- **Goal:** A listener this module owns, bound to loopback, that refuses a bad Host or Origin before any request reaches the protocol.
- **Requirements:** R1, R12, R14
- **Files:** `McpHttpListener`, `McpHostAllowlistFilter`
- **Approach:** Start an embedded container this module owns and closes, the way the dashboard deliberately owns its own Vert.x instance rather than adopting the application's. Walk upward from the configured port for the configured number of attempts, catching only the bind-unavailable cases and logging nothing per attempt, then log exactly one line carrying the URL a client needs, plus a warning when the bind is not loopback. Port the dashboard's host-allowlist logic to a servlet filter registered on a path pattern ahead of the MCP servlet, per KTD3, and keep its comment that no CORS header is ever emitted. Close the container on close, and make close idempotent.
- **Test scenarios:** a bad Host gets 403; a cross-origin Origin gets 403; a null Origin gets 403; an absent Origin on a loopback Host is allowed; duplicate Host or Origin headers get 400; a widened host from U2 is allowed; the port walk finds a free port above an occupied one with nothing logged for the occupied attempt; exactly one line is logged on a successful bind and it carries the bound URL; close releases the port and a second close is a no-op.
- **Verification:** `./mvnw test -pl :parallel-consumer-mcp`

### U4. The MCP server: capabilities, lifecycle, the fixed registry

- **Goal:** A server that advertises only what R2 permits, answers for exactly one processor instance, and starts only when asked.
- **Requirements:** R1, R2, R3, R25, R24
- **Files:** `HastenMcpServer`
- **Approach:** Construct with a reading publisher and options, per KTD4, so the instance the server answers for is decided at construction and cannot drift. Build the SDK server over the servlet transport of U3, declaring the tools capability and the resources capability with subscribe, and declaring nothing else: no prompts, no sampling, no elicitation, no roots. Register the tool set from a fixed collection, so the set is enumerable in a test rather than accumulated by whoever calls an add method. Apply the annotations of KTD7 to every tool. Enforce the session cap and idle timeout through whatever the pinned SDK exposes, and answer an exceeded cap as a protocol error naming the bound. Nothing happens until start is called, stated in the class javadoc as both existing surfaces state it.
- **Test scenarios:** the advertised capability set contains tools and resources-with-subscribe and nothing else; the registered tool set equals the fixed collection; a call naming an unregistered tool is a protocol error; every registered tool carries the four annotations of KTD7; constructing the server and never starting it opens no socket and logs nothing; exceeding the session cap is refused with a bound-naming error; an idle session is closed after the timeout; closing the processor leaves the tools answering with the last reading and its age.
- **Verification:** `./mvnw test -pl :parallel-consumer-mcp`

### U5. The field registry and its build-time check

- **Goal:** Every value an agent can see carries its engine term and description, its unit and limit where it has them, and an explicit decision that it may be exposed - with a red test when one does not.
- **Requirements:** R17, R21, R11, R20
- **Files:** `ExposedField`, `FieldRegistry`, `FieldRegistryArchTest`
- **Approach:** One entry per exposed field: the reading accessor it projects, the engine term, a description, an optional unit, an optional limit, and the agent-exposure declaration. Unit and limit are optional by R17's own reasoning - a lifecycle state, a topic name and an absolute offset have neither, and forcing one would make the implementation fabricate it or drop a field R6 requires. Default to withheld: a field with no entry is not exposed, which is what makes R11's zero-change path safe rather than a bypass. Project the registry into each tool's output schema, carrying the term, unit and limit in the per-field schema description, and return values as structured content against it. The test walks the reading types' accessors and fails on any that is exposed without a complete entry; it also fails an entry whose accessor no longer exists, which is what keeps the registry from rotting as the substrate grows.
- **Test scenarios:** a reading field with no entry does not appear in any tool output; an entry missing its engine term or description fails the test; an entry naming an accessor that no longer exists fails the test; a categorical field with no unit or limit passes; the generated schema carries the term, unit and limit for a field that has all three; nothing derived from record content has an entry, per R20.
- **Verification:** `./mvnw test -pl :parallel-consumer-mcp`

### U6. The state tool over the reading

- **Goal:** One tool returns the whole of the instance's current state at the fidelity the dashboard's state document has, every value carrying its meaning.
- **Requirements:** R4, R5, R6, R17, R25
- **Files:** `StateTool`, the schema projection from U5
- **Approach:** Read the publisher's current reading and project it through the registry. Take both readings together through the publisher's paired accessor wherever the tool reports anything derived from a delta, never two separate volatile reads. Preserve the substrate's absent-is-not-zero rule: a field no meter supplied is reported absent, not as zero, because an agent told a count is zero will reason from it. Carry the instance identity of KTD4 and the reading's capture time and age. Write the tool description for an agent with no knowledge of this engine, which is the property R5 asks for and the control arm of U10 measures.
- **Test scenarios:** the tool returns lifecycle, work, encoding and per-partition sections; a reading with an absent field reports it absent rather than zero; an empty reading, before the engine has bound its meters, is reported as such rather than as an idle instance; the response carries the instance identity, the capture time and the age; anything delta-derived comes from the paired accessor; every returned field resolves to a registry entry.
- **Verification:** `./mvnw test -pl :parallel-consumer-mcp`

### U7. The history ring and its tool, bounded and cursored

- **Goal:** A window of recent readings an agent can walk to see what changed, held without costing the control thread and read back within a bound.
- **Requirements:** R8, R23
- **Files:** `ReadingRing`, `HistoryTool`
- **Approach:** A fixed-size ring written by a substrate listener that only hands off, per KTD9: compare the reading's capture time against the last stored slot and store the reference when the cadence has elapsed, otherwise return. Size the ring from the window and cadence in the options. The tool returns oldest-first from a cursor, stops at the response-size bound, and sets an explicit truncation marker with the cursor to continue from. An empty or partly filled ring is reported as the window it actually has rather than padded.
- **Test scenarios:** readings arriving faster than the cadence store one slot, not many; readings arriving slower than the cadence leave gaps reported as gaps; the ring never exceeds its size and overwrites oldest-first; a window larger than the response bound truncates with a marker and a usable cursor; walking the cursor to the end returns every retained sample once; the listener does no I/O and no blocking, asserted by the listener running on the test's own thread with a latch rather than by inspection; a request against an empty ring returns an empty window rather than an error.
- **Verification:** `./mvnw test -pl :parallel-consumer-mcp`

### U8. The configuration tool and its allowlist

- **Goal:** The effective configuration an agent needs to judge observed against configured, with no credential able to reach it.
- **Requirements:** R7, R19, R22
- **Files:** `ConfigTool`
- **Approach:** Return an explicit allowlist of non-secret keys by name: the configured concurrency, the ordering mode and the commit mode, plus whatever else is added by name in review. Never the raw consumer or producer property map, which routinely carries SASL and SSL passwords, and never a key matched by a deny-pattern, because a deny list is the shape that leaks when a new property arrives. The tool description states what R22 states: everything returned leaves the machine for the client's model vendor.
- **Test scenarios:** an instance configured with a SASL password and a keystore password returns neither, and returns no key outside the allowlist, with every tool called - AE7; the allowlisted keys are returned with their registry meaning; a key added to the engine's configuration does not appear until it is added to the allowlist by name.
- **Verification:** `./mvnw test -pl :parallel-consumer-mcp`

### U9. The resource and its subscribe capability

- **Goal:** The instance's state as an addressable resource a client is told about when it changes, and which loses nothing for a client that cannot subscribe.
- **Requirements:** R26, R2
- **Files:** `StateResource`
- **Approach:** Register one resource whose read returns the same projection the state tool returns, so the two cannot drift. Declare the subscribe capability through the SDK's API and let the SDK own the notification wire, per KTD8. **Verify against the pinned SDK which subscription shape it emits before building anything around it** - the specification has moved and the released SDK trails it. Notify on a change in the reading that the registry actually exposes, not on every sample, so a client is not woken for a field it cannot see. Carry no data in the notification: it names the resource and the client re-reads.
- **Test scenarios:** the declared capability includes subscribe; a read returns the same projection as the state tool for the same reading; a change to an exposed field produces one notification naming the resource and carrying no data; a sample that changes nothing exposed produces no notification; a client that never subscribes and only re-reads gets the current document every time; an unsubscribe stops the notifications.
- **Verification:** `./mvnw test -pl :parallel-consumer-mcp`

### U10. The acceptance harness, the graded run and its control arm

- **Goal:** Prove that an agent with no knowledge of this engine explains a staged, engine-specific fault from the tools alone - and measure whether the tools beat the state document an agent could already have been pointed at.
- **Requirements:** R15, R16, R5, R17
- **Files:** `parallel-consumer-mcp/src/test-integration/java/.../HeadOfLineBlockingStage.java`, `.../McpAcceptanceIT.java`
- **Approach:** Stage the fault in this module, per KTD11. One key pinned to fail on every delivery; records on other keys of the same partition complete past it; the partition's committed offset then sits below its highest completed offset with a band of finished, uncommittable work between them. Drive it on core's broker integration base, reached through core's tests classifier the way the substrate module reaches core's test conventions. Assert the staged state against the reading itself before any agent is involved, so a failed run is attributable: the stage failed, or the explanation did. Then run the graded agent against the tools, and run the control arm - the same agent, the same stage, given the dashboard's state document with equivalent field descriptions instead of the tools. Grade on the four named elements of AE1. Record the agent, the model and the transcript. Attribute a failure the three ways Success Criteria names, and act only on the first two: a fact the agent sought and the tools do not carry changes the evidence; a fact the agent never sought and the tools do carry changes R5 and R17.
- **Test scenarios:** the stage reaches a stranded band and fails loudly when it does not, so a non-event is a failure rather than a pass; the staged state is visible through the tools without an engine change; the graded run names the partition, attributes the hold to incomplete work rather than to a lack of progress, quantifies the band from the committed-to-highest-completed gap, and recommends an action addressing the record rather than the partition count - AE1; the control arm runs and its result is recorded beside the MCP arm; the prompt does not name the partition and the stage uses more than one partition, so that naming it is not free; an agent asked to change state is refused - AE5.
- **Verification:** `./mvnw verify -pl :parallel-consumer-mcp` with Docker available, plus the graded run recorded in the PR.

---

## Verification Contract

- **Whole reactor first, always.** `./mvnw clean install -DskipTests` from the repo root before anything narrower: a narrowed or compile-only invocation skips the generated Truth assertion classes and then fails as a missing symbol, which reads as a broken repository.
- **Unit suite:** `./mvnw test -pl :parallel-consumer-mcp` for U1 to U9. `bin/ci-unit-test.sh` for the repo-wide check, which is also what catches the core test that walks the tree for a module missing its test-conventions class.
- **Integration suite:** `./mvnw verify -pl :parallel-consumer-mcp`, Docker required. `bin/ci-integration-test.sh` repo-wide.
- **Gates:** `bin/check-all.sh` before every push. It globs every gate so the set cannot drift from whatever was remembered.
- **PR analysis surfaces:** `bin/check-pr-analysis-surfaces.sh` once the PR is open, read before asking for review. A new module turns on static analysis that has never run over this code.
- **A failing test is never loosened to go green.** A test failing under load is either test-infrastructure contention or a real concurrency bug, and which one must be established before anything is changed. This module starts a listener and a container inside a test JVM, so port and container contention is the likely first suspect and must be proven rather than assumed.
- **Named checks that are not tests**, each resolved at implementation time and recorded in the PR: which subscription wire shape the pinned SDK emits (KTD8); whether the SDK drags a reactive library into a consuming application's compile classpath, which the research could not settle from the build file alone; and the exact SDK coordinate and version at pin time (KTD1).

## Definition of Done

**Global**

- The four tools and the resource answer on a running instance, and a client that only re-reads the resource loses nothing.
- AE1 passes against the staged fault with a real agent, **with its control arm run and recorded**. A pass on the MCP arm alone does not close this item: it shows an agent can paraphrase staged data, not that tool self-description earned an SDK inside every consuming application, which is KD13's claim.
- AE5, AE6 and AE7 pass as tests rather than as arguments: a write is refused, a classpath with no call opens nothing, and no credential appears in any tool output.
- Every value any tool returns resolves to a registry entry carrying its engine term and description, and the build fails when one does not.
- The module declares its own Java 17 floor with its reason named in its pom, and the dashboard's floor is untouched.
- `bin/check-all.sh` green, and `bin/check-pr-analysis-surfaces.sh` read with every finding on a line this work wrote either fixed or answered.
- The three named checks of the Verification Contract are resolved in the PR, not left as assumptions.
- A feature record under `docs/features/` describes the new capability, and the roadmap entry's stage moves in the same change.
- **Cleanup:** no abandoned transport experiment, no scratch server, no debug logging, and no commented-out approach left in the diff. If the Vert.x transport of KTD2 was attempted and set aside, it leaves a note rather than code.

**Per unit:** the unit's test scenarios pass, its files carry the fork's own copyright header, and its public types are marked unstable.

## Deferred / Open Questions

### From 2026-09-11 review

- **A web-GUI user cannot decline the MCP dependency** - *resolved 2026-09-12.* KD14 now gives the MCP server its own module, so a web-GUI application never resolves the MCP SDK. Kept as the record of why the module shape changed.

- **Nothing decides when the fleet milestone starts** — Success Criteria (P2, product-lens, confidence 75)

  The decision to start milestone two, a fleet view with an elected controller node, has no evidence input, because every success criterion measures that the staged demo works rather than that a developer or operator reached for the tools on a real incident. STRATEGY.md already accepts a hand-counted lagging signal for exactly this problem, so a criterion in that shape, at least one reported real use before milestone two is planned, would give the fleet milestone something to be decided on. Left open because gating milestone two on real use is a product choice the owner has not weighed.
