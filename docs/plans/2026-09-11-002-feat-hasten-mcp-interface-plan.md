---
title: Hasten MCP Interface - Plan
type: feat
date: 2026-09-11
topic: hasten-mcp-interface
artifact_contract: ce-unified-plan/v1
artifact_readiness: requirements-only
product_contract_source: ce-brainstorm
execution: code
---

# Hasten MCP Interface - Plan

## Goal Capsule

- **Objective:** An operator's or developer's agent, connected over MCP (Model Context Protocol) to one running Hasten instance, can explain a situation that Hasten reports mechanically today, from evidence the instance already holds. Milestone one is that single instance, read-only: the hello world. The fleet view and the write path are later milestones whose direction this document records but does not specify.
- **Product authority:** This document, for the surface and its behaviours in milestone one, and for the recorded direction of the fleet and write milestones. It extends the embedded dashboard (astubbs#268), whose snapshot it projects, and it inherits that PR's read-only, loopback, unauthenticated posture unchanged. The control-plane note (`docs/inflight/web-control-plane.md`) owns the Observe/Explain/Act split this work sits inside; the admission controller (astubbs#333) and the navigator (astubbs#392, astubbs#456) own the explanations this surface returns. The fleet and write milestones are not active scope.
- **Open blockers:** None. Every open item is deferred to planning, named in Outstanding Questions.

---

## Product Contract

### Summary

Embed a read-only MCP server in the Hasten runtime, beside the existing dashboard server, exposing everything one instance knows: its state snapshot, a short history of recent samples, its configuration, and every explanation the engine already computes. An agent attached to that instance does the analysis; the tools return evidence and the engine's own verdicts, never a verdict computed for this surface. The acceptance test is an agent explaining, from the tools alone, why a partition on a staged instance is paused.

### Problem Frame

Hasten already reports things a plain Kafka consumer cannot: a partition paused because its offset map reached three quarters of the metadata it may commit, a commit frontier held behind one incomplete record with thousands of later records already done, observed concurrency far below what was configured, a load factor that stepped up because workers were starving. On the branches that carry them, the admission controller names why the concurrency target is what it is, and the navigator names why a record is waiting on a shared resource and what rate is available. The dashboard (astubbs#268) draws all of this for a human on one instance, on one loopback port.

None of it reaches an agent. The person who meets these reports first is a developer on a laptop with a coding agent open, and the person who most needs them explained is an operator on Kubernetes asking why Hasten has paused, throttled or held something across a deployment. Both have an agent that could read the evidence and reason about it. Today that agent can only tail a log or scrape a page that was drawn for eyes. The point of an MCP surface is that the machine-readable feed comes first and the human query interface second: the state exists, the analytics on top are what an agent does best, and Hasten never has to grow an analytics brain of its own.

### Key Decisions

- KD1. **Milestone one is read-only, and it is one instance.** The hello world proves the surface and the acceptance shape on a single running instance with no new infrastructure. (session-settled: user-directed - chosen over a fleet-first milestone and over including the write path: the simplest version first, and the fleet and writes each carry a design the hello world does not need.) Governs R1, R2, R12, R13.
- KD2. **The MCP surface precedes any analytics query interface.** The machine-readable feed is built before a human query UI on top of the dashboard, because the agent does the querying. (session-settled: user-directed - chosen over building analytics queries into the dashboard first: an MCP server is the first consumer of the analytics that already exist.) Governs R1, R6.
- KD3. **Tools return everything the engine has, evidence and verdicts alike, and the agent analyses.** Nothing is computed for this surface. Where the engine already computes an explanation, that explanation is returned as a fact beside the evidence it rests on. (session-settled: user-directed - chosen over tools that compute explanations for the agent, and over evidence-only tools that hide engine verdicts: the value is agentic analysis of state and stats, and a computed explanation that exists is part of the state.) Governs R6, R7, R8, R9, R10, R11.
- KD4. **Not net new: the surface projects the dashboard's snapshot.** The MCP tools read the same published snapshot the dashboard's state document and event stream read, extended where R7 and R8 need more. (session-settled: user-directed - chosen over a separate state model for MCP: the analytics underneath are already exposed, and two models would drift.) Governs R6, R7, R8.
- KD5. **The posture is inherited unchanged: read-only, loopback, no authentication, nothing starts without being asked.** The dashboard's security argument is that there are no write paths, and milestone one adds none. (session-settled: user-approved - chosen over opening the bind or adding a bearer token now: reachability comes from a port-forward, and authentication is a requirement of the first write path, not of this milestone.) Governs R12, R13, R14.
- KD6. **Acceptance is an agent explaining a staged, Hasten-specific situation from the tools alone.** The stage is a partition paused at offset-map capacity behind one incomplete record. (session-settled: user-directed - chosen over plumbing-only acceptance and over a scaling verdict as the acceptance question: the question must be one only Hasten can raise, and the mechanical fact is Hasten's while the explanation is the agent's; the paused-partition stage was proposed with the hot-key stall as the alternative and accepted.) Governs R15, R16.
- KD7. **The fleet is every application running Hasten on one Kafka cluster.** An instance carries its application and consumer group as attributes; the fleet is not one group. (session-settled: user-directed - chosen over fleet-as-one-consumer-group: operators run deployments, not groups.) Direction for the fleet milestone; governs nothing in milestone one.
- KD8. **Fleet state fans in to the elected controller node only, never to every instance.** An instance never pulls from every peer and never consumes every peer's snapshot: both are quadratic in fleet size. Fleet questions go to the controller; instance questions are answered locally. (session-settled: user-directed - chosen over every instance holding the whole fleet from a shared topic: "that would explode".) Direction for the fleet milestone.
- KD9. **The controller node is the navigator's controller rung, not a second coordinator.** The node that will coordinate named shared resources across applications (the rung astubbs#228 keeps open) is the node that holds the fleet view and, later, receives writes. The controller role runs either inside one of the application instances or as a standalone process that hosts nobody's application; both are the same code, so the standalone form is expected to be trivial, and it is the option for a fleet whose owner does not want a controller running inside an application. (session-settled: user-directed - chosen over a separate fleet aggregator, and over embedded-only hosting: the MCP surface meshes with the named-resource controller system rather than duplicating its election and its view, and the standalone host was asked for as an option that must stay cheap.) Direction for the fleet milestone.
- KD10. **Redirect where the client can reach the target; proxy only where it cannot.** Fleet reads and all writes redirect to the controller when the fleet is reachable from the client, and are proxied by the instance the client reached when it is not, as through a port-forward to one pod. (session-settled: user-directed - chosen over proxying always: a redirect is cheaper and the operator prefers it; the proxy is forced by reachability, not by reads or writes.) Direction for the fleet milestone.
- KD11. **The fleet store is deferred to the fleet milestone.** Two candidates fan in at one place and are recorded: the controller consuming a Kafka topic that every instance publishes to, and the controller or the MCP server querying the operator's Prometheus, which already scrapes the fleet and holds its history. Kubernetes label discovery and consumer-group membership discovery are recorded and rejected for the fleet view, because each has every instance pulling from every peer unless only the controller pulls, at which point they are a discovery mechanism for the first candidate rather than a store. (session-settled: user-directed - chosen over deciding the store now: milestone one needs no fleet.) Direction for the fleet milestone.
- KD12. **The audience is operators on Kubernetes, reached through developers.** Operators querying a deployment's runtime and scaling state are the target; developers on laptops are the first users by necessity, since nothing reaches an operator without going through development. (session-settled: user-directed - chosen over framing the developer as the target: the developer is the path, the operator is the destination.) Governs R2, R15.

### Actors

- A1. **Developer with a coding agent**, running one Hasten instance on a laptop, attaching the agent to it while writing or debugging.
- A2. **Operator with an agent**, running Hasten instances on Kubernetes, reaching one instance through a port-forward and asking about it. In milestone one the operator's questions are about that instance; the fleet arrives in the next milestone.
- A3. **The agent**, an MCP client that calls the tools, reads what they return, and produces the explanation. It is not part of this product.
- A4. **The Hasten instance**, one JVM running Parallel Consumer with the dashboard module and the MCP server, publishing its snapshot and answering the tools from it.

### Requirements

**Surface**

- R1. The runtime exposes an MCP server that any conforming MCP client can connect to over a network transport, beside the existing dashboard server, so that a client reaching the instance's port can use both.
- R2. Every tool is a read: no tool changes the instance's state, and the server refuses any operation that would.
- R3. The server is opt-in in the same way the dashboard is: having the module on the classpath starts nothing, and the application starts it.
- R4. Every tool answers from the published snapshot and never from live engine objects, so that no tool can reproduce the gauge-on-a-foreign-thread failure the dashboard's snapshot design exists to prevent.
- R5. Each tool describes itself well enough that an agent with no prior knowledge of Hasten can choose the right one for "why is this partition paused" and "what is this instance doing".

**Evidence**

- R6. The tools return the whole of the instance's snapshot: lifecycle state, work counts, offset-encoding statistics, and per-partition offsets and rates, at the same fidelity the dashboard's state document has.
- R7. The tools return the instance's effective configuration, including the configured concurrency and the ordering and commit modes, so that observed values can be judged against configured ones.
- R8. The tools return a short history of recent snapshots for the instance, not only the latest, so that an agent can see what changed. The window length is a planning decision.
- R9. Every explanation the engine computes is returned as a fact beside its evidence. On a base that carries the admission controller (astubbs#333), that includes the current admission target, the would-be target in observe mode, the reason the controller chose it, and whether the admission limit was the binding constraint in the last window. On a base that carries the navigator (astubbs#392, astubbs#456), that includes the resource view: what is resource-ineligible, why, and what rate is available.
- R10. On a base that carries none of those, the tools return the evidence alone and say nothing is computed, rather than failing or inventing a verdict.
- R11. The evidence surface is shaped so that an explanation arriving with a later branch appears in the tools without an MCP change, in the same way a new meter appears in the dashboard's snapshot.

**Posture**

- R12. The server binds to loopback by default, with the same host allowlist and the same opt-in widening the dashboard has.
- R13. The server requires no authentication in this milestone; the posture's argument is R2.
- R14. The server never logs during its port search and logs one line once bound, carrying the URL a client needs, matching the dashboard's start-up contract.

**Acceptance harness**

- R15. A staged scenario runs one instance into the situation "a partition is paused because its offset map reached capacity behind one incomplete record", reusing the dashboard's showcase scenario where it already reaches that state.
- R16. An agent connected only to the MCP server, given the operator's question "Hasten says partition N is paused, why, and what should I do", names the cause and a sensible action, from the tools alone, on the staged scenario.

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
  - **Steps:** The server refuses; nothing in the instance changes.
  - **Outcome:** The read-only posture holds structurally, as the dashboard's does.
  - **Covered by:** R2, R13

### Acceptance Examples

- AE1. The paused partition is explained
  - **Covers R15, R16.**
  - **Given** the staged scenario has paused partition 3 because its offset map reached capacity behind one incomplete record.
  - **When** an agent connected only to the MCP server is asked why partition 3 is paused and what to do.
  - **Then** the agent's answer names the paused partition, the incomplete record holding the frontier, the offset-map capacity as the cause, and an action that addresses the incomplete record rather than the partition count.

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

### Success Criteria

- AE1 passes with a real agent against the staged scenario, and the transcript is kept as the record of what the tools had to carry for the explanation to succeed.
- A developer can go from an application start to an agent's first explanation using only the logged URL and the tools' own descriptions.

<!-- ce-section: work-relationships -->
### How This Work Fits Together

This plan owns milestone one: one instance, read-only. The breakdown below is the current understanding, not a committed roadmap; later plans may revise it.

- **Milestone two: the fleet view.** Depends on this plan's surface. Still to decide: the fleet store (KD11). Settled direction: KD7, KD8, KD9, KD10. Its acceptance question is the operator's: explain why Hasten has recommended, or declined, a scale-out across the deployment.
  - Depends on the navigator's controller rung (astubbs#228) for the elected controller node, per KD9.
  - Still to decide: whether the first fleet deployment hosts the controller inside an application instance or as the standalone process KD9 allows; the plan for that milestone must show the standalone form costs nothing beyond a host.
  - Shares the reachability rule of KD10 with milestone three.
- **Milestone three: the write path.** Depends on milestone two's controller node, and on authentication, which becomes a requirement the moment the first write exists. Each write is an engine API before it is a tool; `docs/inflight/web-control-plane.md` owns that list.
- **Can proceed independently of** the dashboard's own Observe and Explain panels, which draw the same snapshot for a human.

### Scope Boundaries

**Deferred for later**

- The fleet view, the controller node, the redirect and proxy rules, and the fleet store: milestone two, direction in KD7 to KD11.
- Writes, the leader-forwarding path, and authentication: milestone three.
- A scaling recommendation and its reasons: owned by astubbs#333 and the auto-scaling note; this surface returns what those produce and computes nothing.
- Per-record detail beyond the snapshot, such as the blocking record's key and attempt count: added only if AE1 cannot pass without it.

**Outside this product's identity**

- An analytics brain in Hasten that computes explanations for this surface. The agent analyses; the engine reports.
- A fleet spanning more than one Kafka cluster.
- A privileged backend for MCP that the dashboard does not have: both read the same snapshot.

### Dependencies / Assumptions

- **Depends on astubbs#268**, the embedded dashboard, which is a draft PR: the MCP server sits beside its server and reads its snapshot, so milestone one is built on that branch or after it merges.
- **Reads astubbs#333, astubbs#392 and astubbs#456 when present, and works without them** (R9, R10). None is a prerequisite of milestone one.
- **Assumes** the dashboard's snapshot is derived from the meter registry and the control thread's own sampling, so R7 and R8 are additions to what is sampled and retained, not a new sampling path.
- **Assumes** a maintained MCP server implementation for the JVM exists that can be hosted beside the dashboard's server; planning verifies this and chooses.

### Outstanding Questions

**Resolve Before Planning**

- None.

**Deferred to Planning**

- The history window length and what it costs to retain (R8).
- Whether the MCP server shares the dashboard's HTTP server and port or takes the next port in the same search (R1, R14).
- Which configuration values R7 exposes beyond concurrency and the ordering and commit modes, and how secrets in the client configuration are kept out.
- Tool granularity: one snapshot tool with sections, or one tool per section; decided against what makes AE1 pass with the least description text (R5).
- How the staged scenario is driven for AE1 and whether it runs as an integration test with a scripted agent, a recorded transcript, or both (R15, R16).

### Sources / Research

- `docs/inflight/web-control-plane.md`: the Observe/Explain/Act split, the ruling that Observe and Explain ship on the read-only posture and Act cannot, and the control plane running embedded with any instance redirecting to the leader.
- `docs/inflight/core-runtime-services-and-compat.md`: distributed interactive queries as a runtime service, and the rule that the UI uses the same API with no privileged backend.
- `docs/inflight/core-standalone-deployment.md` and `docs/inflight/core-non-kafka-participants.md`: ingesting Prometheus as the way the engine sees a fleet it does not run inside, the origin of the Prometheus candidate in KD11.
- `docs/inflight/core-auto-scaling.md`: the recommendation shape, instance count with hysteresis and rebalance as acknowledgement, and the ruling that instance-count recommendation is outside the engine-concurrency stack.
- `docs/inflight/core-execution-opportunity-model.md`: the gate ladder that the admission reasons read as arithmetic.
- The embedded dashboard, astubbs#268 on branch `feats/web-gui`: the snapshot publisher, the state document and event-stream routes, the read-only 405 handler, the loopback default and host allowlist, and the showcase scenario that R15 reuses.
- The admission controller, astubbs#333 on branch `feats/ideate-distributed-throttling`: the decision-reason enumeration published as a gauge, and the per-window binding classification.
- The navigator, astubbs#392 and astubbs#456: the resource contract, the allocators, and the read-only view written for the later web GUI to assert against, which R9 returns.
- Verified against the code on 2026-09-11: no branch computes a scale-out recommendation in main code; no branch declares an MCP dependency; a partition refuses records at three quarters of its maximum commit metadata.
