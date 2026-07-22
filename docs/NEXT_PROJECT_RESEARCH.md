# Next Project Research

Decision date: 2026-07-22

## Executive verdict

Build **effect-broker**, a framework-neutral correctness boundary for irreversible
agent tool calls. Do not build another agent runtime. Durable execution, checkpointing,
human approval, deterministic replay, and time travel are already crowded product
categories. The unresolved seam is narrower: a remote service can commit a charge,
email, ticket, or deployment and the worker can die before recording the response.
The local runtime then cannot know whether retrying is safe.

The leading hypothesis therefore wins only after surgery. "Exactly once for arbitrary
tools" is impossible and must not be claimed. The defensible project classifies each
effect by its real downstream contract, propagates stable idempotency keys, records
intent before dispatch, represents ambiguous outcomes as `outcome_unknown`, reconciles
queryable systems, and refuses blind retries when proof is impossible. That is a real
correctness product. A second `agent-runtime` with a nicer debugger is not.

## Selection standard

Every candidate was tested against five filters:

1. **Model-erasure:** would a much stronger model remove the need? If yes, reject it.
2. **Portfolio duplication:** is this materially different from the existing gateway,
   runtime, mesh, RAG engine, red-team system, CVE analyzer, and Trustdesk?
3. **Platform substitution:** do mature infrastructure products already solve the
   claimed core, rather than merely an adjacent problem?
4. **Solo credibility:** can one strong engineer ship and prove the hard property in a
   production-grade MVP, instead of drawing an architecture that needs a company?
5. **Guarantee honesty:** can the README state the exact failure model and guarantee
   without hiding an impossible distributed-systems claim?

The weighted score is:

`35% real usefulness + 30% model-resistant defensibility + 20% portfolio fit +
15% solo buildability`.

## Ranked shortlist

| Rank | Candidate | Score | Decision | Blunt assessment |
| ---: | --- | ---: | --- | --- |
| 1 | **effect-broker**: remote-effect correctness and reconciliation | 9.2 | **Build** | Generic durability is crowded; the unacknowledged remote-commit gap is not. |
| 2 | **runshift**: persisted-run upgrade verifier and migrator | 8.2 | Fallback | Excellent systems work, but fewer teams have enough long-lived runs to feel it yet. |
| 3 | **agent-attest**: signed execution provenance | 7.8 | Watch | Defensible and audit-friendly, but many buyers will accept ordinary traces until forced not to. |
| 4 | **statecell**: transactional shared state for agents | 7.6 | Watch | The pain is real; the weak answer to "why not Postgres?" keeps it out of first place. |
| 5 | **mcp-capability-broker**: per-invocation tool authorization | 7.1 | Reject now | Useful, but cloud vendors and gateways are already consuming this surface quickly. |
| 6 | **deadline-broker**: end-to-end cost and latency reservations | 7.0 | Reject as a new repo | This should become a hard feature in `llm-gateway`, not portfolio duplication. |

## 1. effect-broker — remote-effect correctness and reconciliation

### Sharp problem

An agent invokes a mutating tool. The remote system commits, but the response is lost or
the worker dies before the local journal records success. On recovery, the runtime sees
an incomplete call. Retrying may double-charge, double-email, open a duplicate ticket,
or repeat a deployment; not retrying may silently lose the requested action.

This is the exact hole in the current `agent-runtime`: it persists `TOOL_REQUESTED`,
calls `tool.handler(...)`, and only then appends `TOOL_SUCCEEDED`. The call and the
journal commit cannot be atomic when the tool is a third-party service.

### Who has the pain, and how expensive is it?

Teams letting agents mutate payments, CRM records, support systems, cloud resources,
inventory, identity, or outbound communications have it. The cost is not an abstract
evaluation score: it is duplicated money movement, irreversible customer contact,
inconsistent external state, manual reconciliation, incident response, and loss of
trust in automation. The failures are rare on the happy path and concentrated under
timeouts, deploys, retries, and worker crashes, which makes them expensive to reproduce.

### Why a better model does not solve it

The model is not present at the atomicity boundary. It cannot distinguish "request never
arrived" from "request committed and response was lost" because both produce the same
timeout observation. No amount of reasoning recovers information that the distributed
system did not retain. The solution requires a stable operation identity and cooperation
from the target through an idempotency key, an atomic local transaction, or an
authoritative status query. For a non-idempotent and non-queryable API, exactly-once
execution is impossible; even Temporal maintainers state this plainly in the
[Temporal discussion of external-call idempotency](https://community.temporal.io/t/activity-external-call-idempotency/7543).

### Closest tools and the specific gap

- General durable execution is no longer open territory. Pydantic AI officially supports
  [Temporal, DBOS, Prefect, and Restate](https://pydantic.dev/docs/ai/integrations/durable_execution/overview/).
  LangGraph persists progress and supports time travel, but its documentation still
  requires side effects to be idempotent because tasks can re-execute
  ([LangGraph functional API](https://docs.langchain.com/oss/javascript/langgraph/functional-api)).
  DBOS likewise says steps are attempted at least once and should be idempotent
  ([DBOS architecture](https://docs.dbos.dev/architecture)). These systems host the
  effect protocol; they do not create a missing downstream guarantee.
- [`agent-ledger` 0.2.1](https://pypi.org/project/agent-ledger/) is the closest direct
  competitor. It already offers a Python effect ledger, concurrency control, approvals,
  and result replay. Its own design constraints admit that exactly-once execution still
  depends on an idempotent handler or downstream idempotency key, and tenant isolation is
  left to the application. It does not make reconciliation and `outcome_unknown` the
  first-class product, nor does it prove the remote-commit crash window across contract
  classes.
- AWS Lambda Powertools has a mature
  [idempotency utility](https://docs.aws.amazon.com/powertools/python/latest/utilities/idempotency/)
  with persistent states and concurrent-request protection. It is function middleware,
  not a cross-framework MCP/HTTP effect broker with downstream contract adapters.
- Stripe shows the cooperative half of the solution: its
  [idempotency keys](https://docs.stripe.com/api/idempotent_requests) return the original
  result for repeated writes. The missing product is the agent-facing layer that derives,
  preserves, audits, and enforces those semantics across heterogeneous tools.
- MCP's experimental tasks add durable handles and polling, but the
  [task protocol](https://modelcontextprotocol.io/specification/2025-11-25/basic/utilities/tasks)
  does not define exactly-once business effects or a general idempotency contract.

### Defensibility as models improve

The moat is not a decorator or a retry loop. It is the contract system and proof suite:
versioned adapter contracts, canonical operation identity, target-specific idempotency
and reconciliation, explicit uncertainty, a crash-point test harness, immutable receipts,
and integration at the MCP boundary so it works across agent frameworks. Better models
increase the number and value of autonomous effects, which increases demand for this
layer. They do not collapse it.

### Solo MVP reality

Credible if tightly scoped to Postgres, one generic HTTP contract, one real idempotent
adapter, a probeable mock payment service, an MCP proxy, and a deterministic crash
matrix. Not credible if it expands into a general workflow engine, a policy platform, or
dozens of SaaS connectors.

### Verdict

**Build it, but never market it as unconditional exactly once.** The honest product is a
"provable effect-safety envelope": exactly-once observable business effects where the
downstream contract permits them, convergence through reconciliation where it can be
proved, and an explicit stop where it cannot.

## 2. runshift — persisted-run upgrade verifier and migrator

### Sharp problem

Long-lived agent runs survive longer than a deployment. Renaming a node, changing state
types, reordering replayed calls, changing a tool schema, or removing a prompt dependency
can strand or corrupt in-flight runs. Teams either pin old workers indefinitely, drain all
runs before deploy, or discover incompatibility during recovery.

### Who has the pain, and how expensive is it?

Teams with approval waits, multi-day research jobs, support workflows, background coding
agents, or regulated processes. The cost is blocked deployments, duplicate worker fleets,
manual repair of checkpoints, failed resumptions, and the worst case: replay applying an
old recorded result to new logic.

### Why a better model does not solve it

Persisted bytes, event ordering, code versions, and schema compatibility are software
contracts. A model can propose a migration; it cannot make an incompatible history
compatible or prove that a fleet of stored runs will resume without side effects.

### Closest tools and the specific gap

- LangGraph documents that the newest graph code runs against old checkpoints, that node
  removal and state changes can break resumed threads, and that it lacks a general search
  index for all in-flight thread state
  ([backward compatibility](https://docs.langchain.com/oss/python/langgraph/backward-compatibility)).
- Restate uses immutable deployments and pins existing invocations to their original
  version; old deployments must remain available or invocations must be moved carefully
  ([Restate versioning](https://docs.restate.dev/services/versioning)).
- DBOS recommends versioning and blue-green draining because breaking workflow changes
  can make recovery checkpoints mismatch
  ([DBOS upgrading workflows](https://docs.dbos.dev/typescript/tutorials/upgrading-workflows)).

Those are engine-specific survival mechanisms. The gap is a framework-neutral pre-deploy
tool that inventories stored histories, replays old and new code against recorded I/O,
detects structural and business divergence, applies typed upcasters, and emits a signed
compatibility report before production traffic moves.

### Defensibility as models improve

Versioned history adapters, replay semantics, migration registries, and a corpus of real
breaking changes become more valuable over time. A stronger model may write an upcaster
faster, but the verifier and evidence remain necessary.

### Fit and buildability

This is an unusually good fit for the existing event journals, replay work, typed Python,
FastAPI control planes, CLI tooling, and failure-oriented tests. A credible solo MVP can
support `agent-runtime` and LangGraph histories, a migration registry, offline shadow
replay, and a CI deploy gate. Claiming framework neutrality with only one adapter would
not be credible.

### Why it is not first

It is a strong infrastructure project, but the immediate user base is smaller than the
set of teams exposed to duplicate effects. A useful cross-framework MVP also needs at
least two real history formats; one adapter would look like a feature of `agent-runtime`.

## 3. agent-attest — signed execution provenance

### Sharp problem

Ordinary traces can show what an agent allegedly called, but they usually cannot prove
which exact prompt, tool schema, model identifier, retrieved artifact, policy version,
human approval, and external receipt produced an action—or prove that the record was not
edited afterward.

### Who has the pain, and how expensive is it?

Platform, security, audit, and compliance teams deploying agents into financial,
healthcare, legal, procurement, or production-operations workflows. Without defensible
lineage, incident reconstruction becomes log archaeology and audit answers become manual
claims rather than verifiable evidence.

### Why a better model does not solve it

Cryptographic integrity, content addressing, identity, retention, and chain of custody are
external facts. A model can summarize evidence; it cannot manufacture trustworthy
provenance after the fact.

### Closest tools and the specific gap

- OpenTelemetry now defines GenAI attributes, including agent-side tool concepts
  ([GenAI semantic attributes](https://opentelemetry.io/docs/specs/semconv/registry/attributes/gen-ai/)),
  while MLflow records traces and linked prompt versions
  ([MLflow trace UI](https://mlflow.org/docs/latest/genai/tracing/observe-with-traces/ui)).
  These are observability records, not tamper-evident execution attestations.
- SPDX 3.0.1 has AI and Dataset profiles
  ([SPDX conformance](https://spdx.github.io/spdx-spec/v3.0.1/conformance/)), and
  [CycloneDX ML-BOM](https://www.cyclonedx.org/capabilities/mlbom/) inventories models,
  datasets, and configurations. They address static system composition better than the
  dynamic, per-run dependency and effect graph.

The specific gap is a small open format and verifier for per-execution manifests: hash
every resolved input, bind approvals to exact intent hashes, attach authoritative effect
receipts, and sign the resulting DAG.

### Defensibility as models improve

Standards compatibility, verifier adoption, and integrations can compound. More capable
agents make the audit trail more important. The commercial weakness is timing: many teams
will continue accepting vendor traces until an incident, customer, or regulator demands
stronger proof.

### Fit and buildability

The cryptographic manifests, event lineage, provider abstraction, API, CLI, and MCP
verification tools match the developer's backend strengths. A solo MVP can ingest OTel
traces, content-address resolved artifacts, sign one execution DAG, and verify it offline.
The hard part is not code volume; it is winning agreement on the evidence schema.

## 4. statecell — transactional shared state for agents

### Sharp problem

Multiple agents and threads update long-term user or business state concurrently. Typical
"memory" APIs expose namespace/key JSON writes and semantic search, but do not make the
business invariants explicit. Lost updates, stale reads, cross-tenant key mistakes,
conflicting facts, incomplete erasure, and untraceable model-authored mutations follow.

### Who has the pain, and how expensive is it?

Teams running customer-facing assistants, account automation, case management, or
multi-agent operations over shared state. Bad state persists beyond one answer: it can
misroute future actions, expose another tenant's data, or require manual cleanup across
indexes and derived memories.

### Why a better model does not solve it

Concurrency control, tenant isolation, schema migration, retention, and deletion are data
system properties. Better extraction may reduce bad writes but cannot prevent two correct
writers from racing or prove that derived copies were erased.

### Closest tools and the specific gap

- LangGraph provides persistent, namespaced stores and semantic search, but its own memory
  guide notes that models can over-insert or over-update and that collection maintenance is
  tricky ([LangGraph memory concepts](https://docs.langchain.com/oss/python/concepts/memory)).
  Changing embedding models also has no automatic migration path in the documented
  deployment flow
  ([LangGraph semantic search](https://docs.langchain.com/langsmith/semantic-search)).
- Letta supports persistent blocks shared by multiple agents
  ([Letta shared memory](https://docs.letta.com/guides/core-concepts/memory/memory-blocks)).

The gap is a typed MVCC state service with compare-and-swap, provenance on every mutation,
policy-checked namespaces, derived-index lineage, temporal reads, and verifiable erasure.

### Defensibility as models improve

The invariants survive better models, but the product moat is weaker than it first appears:
Postgres already supplies transactions, row-level security, and audit primitives. Unless
the agent-specific contract and lineage layer is exceptional, this is a database wrapper.

### Fit and buildability

The work fits the developer's consistency and multi-tenant strengths. A solo MVP can ship
typed schemas, compare-and-swap writes, provenance, temporal reads, erasure propagation,
and an MCP state service on Postgres. The risk is product differentiation, not feasibility.

## 5. mcp-capability-broker — per-invocation tool authorization

### Sharp problem

Agents often receive server-wide credentials and a broad tool catalog when they need one
operation on one resource for one run. A compromised or simply mistaken agent can exceed
the intended tenant, resource, time window, or argument bounds.

### Who has the pain, and how expensive is it?

Enterprise agent-platform teams connecting internal APIs, SaaS systems, and production
operations through MCP. The failure cost is unauthorized mutation, credential leakage,
tenant crossover, or an audit finding.

### Why a better model does not solve it

Authorization must be enforced outside the model. A perfectly aligned model still cannot
turn a bearer token with excessive scope into a least-privilege capability.

### Closest tools and the specific gap

MCP uses OAuth 2.1-oriented authorization
([MCP authorization tutorial](https://modelcontextprotocol.io/docs/tutorials/security/authorization)).
Microsoft Foundry's preview gateway already adds authentication, rate limits, IP policy,
and audit logging to MCP traffic
([Foundry MCP governance](https://learn.microsoft.com/en-us/azure/foundry/agents/how-to/tools/governance?view=foundry)),
and Agent 365 routes tools through a managed gateway with permissions and telemetry
([Agent 365 tool management](https://learn.microsoft.com/en-us/microsoft-agent-365/developer/tooling)).

The remaining gap is a vendor-neutral, self-hosted broker that mints short-lived,
resource-bound capabilities and enforces argument-level policy across MCP servers.

### Defensibility and verdict

The problem survives better models, but this market is moving quickly and the credible
product requires identity-provider, secret-vault, policy-engine, and enterprise audit
integrations. A solo MVP would look thin beside managed gateways. Reject for now.

### Fit and buildability

The FastAPI/MCP gateway and policy core fit, but a production claim requires OIDC, multiple
identity providers, vault integrations, token exchange, tenant isolation, and careful
security review. That is too much integration breadth for the next solo MVP.

## 6. deadline-broker — end-to-end cost and latency reservations

### Sharp problem

A compound agent run fans out across models, retries, tools, and subagents. Per-request
limits do not guarantee that the whole run stays under a monetary budget or finishes by a
deadline. Concurrent branches can all observe remaining budget and oversubscribe it.

### Who has the pain, and how expensive is it?

Teams running high-volume research, coding, support, or document agents. The cost is direct
provider spend, tail-latency SLO violations, queue contention, and one tenant starving
others.

### Why a better model does not solve it

Budgets, reservations, admission control, deadline propagation, and load shedding are
distributed resource-allocation problems. Models getting cheaper would reduce the dollar
pressure but not eliminate finite quotas or latency SLOs.

### Closest tools and the specific gap

Portkey already documents fine-grained spend and token policies grouped by API key,
metadata, provider, model, and other dimensions
([Portkey usage policies](https://portkey.ai/docs/product/enterprise-offering/budget-policies)).
The existing `llm-gateway` already has routing, fallback, circuit breaking, caching, and
per-tenant cost accounting. The specific remaining gap is atomic reservation and
reconciliation for an entire dynamic run, plus deadline-aware fan-out and cancellation.

### Defensibility and verdict

Useful and model-resistant, but not a new project. Add hierarchical budget reservations
and deadline propagation to `llm-gateway` or `agent-mesh`. Spinning up another gateway
would make the portfolio look repetitive.

### Fit and buildability

It is highly buildable with the existing gateway, provider, metrics, and tenant-accounting
code. That is exactly why it should be an extension: the sharp MVP is an atomic reservation
ledger and deadline context inside `llm-gateway`, not another scaffolded repository.

## Explicit verdict on the leading hypothesis

### Hypothesis as stated

> An agent reliability / durable-execution correctness layer guaranteeing exactly-once
> side effects, deterministic replay + time-travel debugging, and crash-safe state for
> tool-calling agents.

### Verdict: accept the problem, reject the bundled product

The problem is real and model-proof. The bundle is not the next project:

- **Generic durable execution loses.** The existing portfolio already has
  `agent-runtime`, and current ecosystems already ship durable agent integrations.
- **Replay and time travel are not the moat.** LangGraph exposes checkpoint history and
  time travel; workflow engines have deterministic recovery. These are expected features.
- **Unconditional exactly once is false.** A local ledger cannot atomically commit with an
  arbitrary remote API. If the remote service is neither idempotent nor queryable, the
  correct state after a lost response is "unknown," not "safe to retry."
- **The remote-effect protocol is still poorly served.** Existing runtimes tell developers
  to make activities idempotent. `agent-ledger` deduplicates local calls but explicitly
  leaves the hard downstream condition to the handler. A focused broker can make those
  contracts executable, observable, testable, and reusable.

Therefore the hypothesis **does win the ranking, but only as `effect-broker`**. Build on
`agent-runtime` for the demo and integrations; do not fork or rename it. The new project's
core must be downstream idempotency propagation, reconciliation, explicit uncertainty,
and crash-proof evidence. If that scope slips back toward "Temporal for agents," stop—the
project has become commodity.

## Ideas killed before the shortlist

- Prompt optimization, better agent planning, response grading, and vertical answer
  generation fail the model-erasure test.
- Another prompt-injection scanner or generic guardrail repeats the `agent-redteam`
  lesson and enters an existing tool market.
- Another LLM observability dashboard competes with LangSmith, MLflow, Langfuse, and
  OpenTelemetry on a crowded surface; signed provenance is the only version worth keeping.
- Another generic agent framework, workflow engine, MCP gateway, or vector-memory product
  is portfolio breadth theater, not a defensible next project.
