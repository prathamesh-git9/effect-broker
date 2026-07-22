# effect-broker: Product and Engineering Specification

Status: proposed next project

Working repository name: `effect-broker`

Python package: `effect_broker`

## Pitch

`effect-broker` is a correctness boundary for side-effecting agent tools. Agents,
workflow engines, and queues may retry; charges, emails, ticket creation, and production
changes must not happen twice. The broker records a stable business intent before
dispatch, binds it to an immutable payload and tool-contract version, propagates the same
downstream idempotency key across every attempt, and reconciles lost responses against an
authoritative target. When the target cannot support idempotency or prove its state, the
broker says `outcome_unknown` and refuses an unsafe retry. It integrates through Python,
HTTP, and MCP, so it complements `agent-runtime`, Temporal, DBOS, Restate, LangGraph, or a
home-grown loop instead of becoming another workflow engine.

## The sharp problem

The dangerous crash window is:

1. local runtime records that it intends to call a tool;
2. remote service commits the mutation;
3. the response is lost or the worker crashes;
4. local runtime has no success record;
5. recovery either retries and risks duplication, or stops and risks omission.

The two external histories—"the request never arrived" and "the request committed but
the acknowledgement was lost"—are identical from the local journal. A local checkpoint
cannot distinguish them. Generic durable execution therefore reduces the window but does
not close it. Its own activity or step can still run at least once; the target must
cooperate or the system must preserve uncertainty.

This is already visible in the portfolio. `agent-runtime` appends `TOOL_REQUESTED`, calls
the handler, then appends `TOOL_SUCCEEDED`. A kill between the handler's remote commit and
the success append leaves exactly this ambiguity.

## Why a better model does not solve it

This is an information and atomicity problem, not a reasoning problem. The model cannot
observe a response that was lost, atomically commit a local row with an unrelated remote
database, or force a third-party API to honor an idempotency key. A stronger model may
choose tools more accurately and still double-charge a customer when infrastructure
retries the correctly chosen call. The only honest solutions are protocol mechanisms:
an atomic shared transaction, downstream idempotency, authoritative reconciliation, or an
explicit stop in an unknown state.

## Target users

Primary users are backend and agent-platform engineers who allow agents or durable
workflows to mutate external systems:

- fintech and commerce teams issuing charges, refunds, credits, or orders;
- support and CRM teams sending customer messages or modifying cases;
- developer-platform teams creating deployments, cloud resources, or access grants;
- internal automation teams connecting agents to SaaS tools through MCP;
- teams already using Temporal, DBOS, Restate, LangGraph, Pydantic AI, Celery, or a custom
  runtime but still hand-rolling idempotency inside every tool.

It is unnecessary for read-only RAG, one-shot prototypes, and workflows where duplicates
are harmless.

## Product boundary

### It is

- an intent ledger and state machine for mutating tool calls;
- a downstream contract registry;
- a dispatcher and reconciler;
- an MCP/HTTP/Python integration boundary;
- a receipt store and deterministic, read-only replay surface;
- a failure-injection proof harness.

### It is not

- an agent framework or agent loop;
- a general workflow engine, queue, or scheduler;
- an LLM gateway;
- a prompt-security or output-quality product;
- a replacement for business-specific compensation logic;
- a claim of unconditional exactly-once execution.

If implementation starts adding planning graphs, agent memory, or a general event bus,
scope has failed.

## Guarantee vocabulary

The project must distinguish execution attempts from observable business effects. An HTTP
request can be transmitted more than once while the target applies one business mutation.
The useful guarantee is exactly one observable effect, not exactly one packet or handler
entry.

| Safety class | Required target contract | Broker behavior | Honest guarantee |
| --- | --- | --- | --- |
| `transactional` | Target mutation and broker receipt share one database transaction | Commit mutation, receipt, and event atomically | Exactly one committed local effect per operation key |
| `idempotent` | Target accepts a stable key, rejects payload drift, and retains the result for a declared horizon | Reuse one downstream key on every attempt and cache the authoritative receipt | Exactly one observable effect while the target contract and retention horizon hold |
| `reconcilable` | Target has an authoritative lookup by stable business key; a negative lookup becomes conclusive only after a declared settlement bound | On ambiguity, probe before any retry; retry only after conclusive `not_committed` | Convergence to one effect if the lookup contract is truthful and bounded; otherwise manual review |
| `unsafe` | No idempotency and no authoritative lookup | Dispatch at most once; a lost result becomes `outcome_unknown` | No broker-issued duplicate, but possible omission and no exactly-once claim |

Important constraints:

- A merely eventually consistent search endpoint is not sufficient for a conclusive
  `not_committed` result. Its contract must define a maximum settlement interval or the
  broker can only discover success, never prove absence.
- Target idempotency-key retention must be at least as long as the broker's retry and
  recovery horizon. Once it expires, automatic retry stops.
- A lease prevents normal concurrent workers; it does not stop a paused or partitioned
  zombie from finishing later. Exactly-once effects come from the downstream contract,
  not from pretending a lease is a distributed lock on the outside world.
- The broker never hashes arguments and assumes identical payloads mean identical
  business intent. Two identical $10 refunds may both be legitimate. The caller must
  supply a stable semantic `operation_key`, normally derived from a workflow run and step
  identifier.

## Architecture

```text
 agent / workflow / queue
          |
          | operation_key + tool + arguments
          v
  Python SDK / MCP proxy / HTTP API
          |
          v
 +------------------- effect-broker --------------------+
 | reserve + bind intent                                 |
 | policy-free state machine                             |
 |                                                       |
 |  Postgres <---- dispatcher ----> contract adapter ----+----> target API
 |     ^              |                                  |
 |     |              v                                  |
 |     +--------- reconciler <------- status probe ------+
 |                                                       |
 | receipt/replay API + metrics + immutable event stream |
 +-------------------------------------------------------+
```

The API process only accepts and reads effects. Separate worker processes claim due work,
dispatch it, and reconcile ambiguous outcomes. They can run in one container for local
development and as independent deployments in production. Postgres is the sole authority.

The model provider abstraction exists for the killer-demo agent and portfolio consistency,
but no model is allowed to decide whether a retry is safe. That decision is pure code over
the pinned contract and persisted state.

## Core module layout

```text
src/effect_broker/
├── __init__.py
├── api.py                  # FastAPI app and request/response schemas
├── auth.py                 # API-key authentication and tenant context
├── canonical.py            # canonical JSON, fingerprints, downstream key derivation
├── cli.py                  # Typer commands
├── config.py               # typed environment and tool-contract configuration
├── contracts.py            # safety classes and versioned contract validation
├── crypto.py               # payload encryption and secret-reference handling
├── engine.py               # submit, deduplicate, inspect, and replay service
├── errors.py               # stable domain error taxonomy
├── events.py               # append-only transition events
├── mcp_server.py           # management tools and configured mutating-tool proxy
├── models.py               # domain dataclasses and enums
├── observability.py        # OpenTelemetry spans and Prometheus metrics
├── providers.py            # Scripted, OpenAI, and Grok demo providers
├── replay.py               # event folding, historical state, and safe forks
├── receipts.py             # immutable receipt construction and verification
├── worker.py               # lease, dispatch, heartbeat, and fencing loop
├── reconcile.py            # unknown-outcome resolution loop
├── adapters/
│   ├── base.py             # adapter protocols and conformance helpers
│   ├── http.py             # configurable HTTP idempotency/probe adapter
│   ├── stripe.py           # optional real idempotent adapter in test mode
│   └── simulated.py        # deterministic crash-demo target adapter
└── store/
    ├── base.py             # persistence protocol
    ├── memory.py           # deterministic unit-test implementation only
    ├── postgres.py         # production implementation
    └── migrations/         # Alembic migrations
```

The root also contains:

```text
examples/crash_matrix/      # target service, demo agent, and kill-point runner
tests/contract/             # runs the same store and adapter contract suites
tests/integration/          # real Postgres, processes, HTTP, and MCP
tests/live/                 # opt-in OpenAI, Grok, and Stripe test-mode checks
docker-compose.yml          # broker, worker, reconciler, Postgres, demo target
Dockerfile
```

## Key interfaces and contracts

The signatures below are normative direction, not copy-paste-complete code.

```python
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import StrEnum
from typing import Any, Mapping, Protocol


JsonObject = Mapping[str, Any]


class SafetyClass(StrEnum):
    TRANSACTIONAL = "transactional"
    IDEMPOTENT = "idempotent"
    RECONCILABLE = "reconcilable"
    UNSAFE = "unsafe"


class EffectStatus(StrEnum):
    PREPARED = "prepared"
    DISPATCHING = "dispatching"
    RETRYABLE = "retryable"
    OUTCOME_UNKNOWN = "outcome_unknown"
    RECONCILING = "reconciling"
    SUCCEEDED = "succeeded"
    FAILED_FINAL = "failed_final"
    MANUAL_REVIEW = "manual_review"
    COMPENSATED = "compensated"


@dataclass(frozen=True, slots=True)
class EffectRequest:
    operation_key: str
    tool: str
    arguments: JsonObject
    requested_by: str
    trace_id: str | None = None


@dataclass(frozen=True, slots=True)
class EffectContract:
    name: str
    version: str
    safety: SafetyClass
    retry_limit: int
    key_retention: timedelta | None = None
    settlement_bound: timedelta | None = None


@dataclass(frozen=True, slots=True)
class EffectRecord:
    effect_id: str
    tenant_id: str
    request: EffectRequest
    request_hash: str
    downstream_key: str
    contract: EffectContract
    status: EffectStatus
    version: int
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class Reservation:
    effect: EffectRecord
    created: bool
    replayed: bool


@dataclass(frozen=True, slots=True)
class DispatchResult:
    external_id: str | None
    output: JsonObject
    committed: bool


class ProbeStatus(StrEnum):
    COMMITTED = "committed"
    NOT_COMMITTED = "not_committed"
    UNKNOWN = "unknown"


@dataclass(frozen=True, slots=True)
class ProbeResult:
    status: ProbeStatus
    external_id: str | None = None
    output: JsonObject | None = None
    evidence: JsonObject | None = None
```

The adapter owns target-specific truth. It does not own retry policy.

```python
class EffectAdapter(Protocol):
    contract: EffectContract

    async def dispatch(
        self,
        effect: EffectRecord,
        *,
        attempt_id: str,
    ) -> DispatchResult:
        """Execute using effect.downstream_key; SDK retries must be disabled."""

    async def probe(self, effect: EffectRecord) -> ProbeResult:
        """Return authoritative evidence, or UNKNOWN when proof is unavailable."""
```

Store operations must make every state transition a compare-and-swap. A stale worker may
finish network I/O, but it cannot overwrite a newer local state.

```python
class EffectStore(Protocol):
    async def reserve(
        self,
        tenant_id: str,
        request: EffectRequest,
        contract: EffectContract,
    ) -> Reservation: ...

    async def claim_due(
        self,
        worker_id: str,
        *,
        now: datetime,
        lease_for: timedelta,
        limit: int,
    ) -> list[EffectRecord]: ...

    async def start_attempt(
        self,
        effect_id: str,
        *,
        expected_version: int,
        worker_id: str,
    ) -> str: ...

    async def transition(
        self,
        effect_id: str,
        *,
        expected_version: int,
        target: EffectStatus,
        data: JsonObject,
    ) -> EffectRecord: ...

    async def get(self, tenant_id: str, effect_id: str) -> EffectRecord | None: ...
```

The public service stays small:

```python
class EffectBroker:
    async def submit(
        self,
        tenant_id: str,
        request: EffectRequest,
    ) -> Reservation: ...

    async def get(self, tenant_id: str, effect_id: str) -> EffectRecord: ...

    async def reconcile(
        self,
        tenant_id: str,
        effect_id: str,
    ) -> EffectRecord: ...

    async def replay_receipt(
        self,
        tenant_id: str,
        effect_id: str,
    ) -> JsonObject: ...
```

The demo agent keeps the portfolio's provider abstraction without contaminating the
correctness path:

```python
class ModelProvider(Protocol):
    async def decide(self, messages: list[JsonObject]) -> JsonObject: ...


class ScriptedProvider: ...
class OpenAIProvider: ...
class GrokProvider: ...
```

`OpenAIProvider` and `GrokProvider` use the official OpenAI client with different base
URLs. Live model tests are opt-in. All crash and correctness tests use `ScriptedProvider`.

## Operation identity and payload binding

`operation_key` is mandatory for mutating calls. Good keys identify business intent, for
example:

```text
order:ord_123:charge:v1
ticket:t_456:send-resolution-email:v1
agent-run:run_789:step:refund_03
```

The broker computes:

```text
request_hash = SHA-256(
    canonical_json({tool, arguments, contract_name, contract_version})
)

downstream_key = base64url(
    HMAC-SHA-256(tenant_key, tenant_id + "\0" + operation_key)
)
```

The unique identity is `(tenant_id, operation_key)`. Reusing it with the same hash returns
the existing effect or receipt. Reusing it with a different hash returns `409 Conflict`
and never dispatches. Canonical JSON rejects NaN, infinity, duplicate keys, non-string
object keys, and values without a declared serialization. The contract version is pinned
at reservation time so a deployment cannot silently change retry semantics for an
in-flight effect.

## State machine

```text
PREPARED ----claim----> DISPATCHING
   ^                       |  |  |  \
   |                       |  |  |   +--> SUCCEEDED
   |                       |  |  +------> FAILED_FINAL
   |                       |  +---------> RETRYABLE --backoff--+
   |                       +------------> OUTCOME_UNKNOWN       |
   |                                         |                  |
   |                                      reconcile             |
   |                                         v                  |
   +-- conclusive not_committed <------ RECONCILING ------------+
                                             |  \
                                             |   +--> SUCCEEDED
                                             +------> MANUAL_REVIEW
```

Rules:

1. A worker commits `DISPATCHING` and an attempt row before network I/O.
2. An explicit target rejection that proves no commit may become `RETRYABLE`.
3. Timeout, connection loss after bytes are sent, worker death, or expired dispatch lease
   becomes `OUTCOME_UNKNOWN`, never an ordinary retry.
4. `idempotent` effects may redispatch with the same key while its retention horizon is
   valid.
5. `reconcilable` effects redispatch only after a contract-valid, conclusive
   `NOT_COMMITTED` probe.
6. `unsafe` effects never redispatch after ambiguity; an operator must resolve them.
7. Terminal receipts are immutable. Correction is a new linked event, not an update that
   rewrites history.
8. Replay only folds stored events and returns stored receipts. It never invokes an
   adapter. A live fork requires a new operation key and explicit operator action.

## Data model

### `tenants`

- `tenant_id` UUID primary key
- `name`
- `api_key_hash`
- `hmac_key_id`
- timestamps and disabled flag

Tenant identity comes from authentication, never from a caller-controlled request field.

### `effect_intents`

- `effect_id` UUID primary key
- `tenant_id` UUID foreign key
- `operation_key` text
- `tool` text
- `request_hash` bytea
- encrypted canonical arguments plus non-sensitive redacted preview
- `contract_name`, `contract_version`, and `safety_class`
- `downstream_key` text
- status, monotonic `version`, attempt count, and next-action time
- lease owner and expiry
- idempotency retention deadline and settlement deadline
- terminal receipt ID, trace ID, creator, and timestamps
- unique constraint on `(tenant_id, operation_key)`

### `effect_attempts`

- `attempt_id` UUID primary key
- `effect_id` and ordinal, unique together
- worker and fencing version
- dispatch start/end timestamps
- request/response digests, sanitized transport metadata, and error category
- whether request bytes may have left the process

### `effect_receipts`

- `receipt_id` UUID primary key
- `effect_id` unique for the terminal success
- external object ID and encrypted result
- evidence/probe digest and target timestamp
- contract version and downstream key digest
- creation timestamp

Application code and a database trigger reject updates to receipt content.

### `effect_events`

- `(effect_id, sequence)` primary key
- old and new status, version, event type, actor, and timestamp
- canonical metadata and previous-event digest

The digest chain detects accidental edits and supports deterministic verification. It is
not marketed as protection against a privileged database administrator; signed and
externally anchored attestations are later scope.

## Durability and consistency mechanics

- Postgres is authoritative; the in-memory store is test-only and is visibly refused by
  production configuration.
- Reservation uses `INSERT ... ON CONFLICT` plus payload-hash comparison. Concurrent
  submissions produce one intent and deterministic replays.
- Workers claim rows with `FOR UPDATE SKIP LOCKED`, a bounded lease, and a monotonic
  version. Every write includes the expected version.
- Intent state, attempt records, receipts, and transition events commit in one local
  transaction.
- HTTP client retries are disabled in adapters. The broker owns retry decisions.
- A stale `DISPATCHING` lease is converted to `OUTCOME_UNKNOWN`; it is never assumed that
  the old worker did nothing.
- Redelivery from MCP, HTTP, the agent runtime, or a queue returns the same effect by
  operation key. The transport can be at least once without duplicating business intent.
- Multi-tenant queries always include tenant scope. Integration tests attempt cross-tenant
  reads, key collisions, and timing attacks.
- Encrypted payloads use AES-GCM with a key ID. Production can supply the master key by
  environment or mounted secret in MVP; KMS-backed envelope encryption is later work.
  Secret values should be passed as resolvable references, not persisted raw.

## Replay and time-travel semantics

Replay is deterministic because it folds persisted transition events and receipts through
versioned event upcasters; it never re-runs a model, dispatcher, adapter, probe, or
notification. An operator can inspect state at any event sequence, compare two points, and
verify the receipt digest. Historical inspection is read-only.

A fork created from historical state defaults to `simulation` and uses recorded receipts
or explicit fake adapters. It cannot inherit a live effect's operation key and dispatch it
again. Turning a fork into live work creates a new business operation, requires a new
operation key, and is an explicit API/CLI action. This makes time travel useful for
debugging without turning the debugger into a duplicate-side-effect button.

## HTTP, CLI, and MCP surfaces

### HTTP API

| Method | Path | Behavior |
| --- | --- | --- |
| `POST` | `/v1/effects` | Reserve an intent; return `202` for new work, existing state for a replay, or `409` on payload drift |
| `GET` | `/v1/effects/{effect_id}` | Return tenant-scoped state and sanitized history |
| `GET` | `/v1/effects/{effect_id}/history` | Fold and inspect state at an optional event sequence |
| `GET` | `/v1/effects/{effect_id}/receipt` | Return an immutable terminal receipt |
| `POST` | `/v1/effects/{effect_id}/reconcile` | Request reconciliation; never forces an unsafe retry |
| `POST` | `/v1/effects/{effect_id}/resolve` | Operator resolution for unknown effects, with reason and evidence |
| `GET` | `/v1/effects` | Filter by status, tool, age, and operation prefix |
| `GET` | `/healthz` and `/readyz` | Liveness and Postgres/migration readiness |
| `GET` | `/metrics` | Prometheus metrics |

Operator resolution has three explicit choices: attach proof of success, attach proof of
non-commit and requeue where the contract permits it, or mark final failure. There is no
"retry anyway" shortcut for `unsafe` effects.

### Typer CLI

```text
effect-broker serve
effect-broker worker
effect-broker reconciler
effect-broker effects list --status outcome_unknown
effect-broker effects inspect <effect-id>
effect-broker effects inspect <effect-id> --at-sequence 7
effect-broker effects reconcile <effect-id>
effect-broker contracts validate
effect-broker doctor
effect-broker demo crash-matrix
```

### MCP server

The MCP surface has two roles:

1. management tools: `get_effect`, `list_unknown_effects`, `get_receipt`, and
   `request_reconciliation`;
2. configured proxy tools: the server mirrors explicitly configured mutating tool schemas
   and requires `operation_key` alongside their arguments before forwarding through the
   broker.

Read-only tools can bypass the broker. A tool cannot be inferred as safe from its name or
description; its versioned contract must be configured. The server rejects an unclassified
mutating tool in strict mode.

## The actual moat

The moat is not "we have an idempotency table." AWS Powertools and `agent-ledger` already
have one. It is the combination of:

1. **Honest contract classes.** The system encodes exactly what the target can prove and
   refuses to promote a timeout into a safe retry.
2. **First-class unknown outcomes.** Ambiguity is a durable operational state with age,
   evidence, alerts, and resolution—not an exception string.
3. **Reusable reconciliation adapters.** Target-specific idempotency retention, lookup
   consistency, settlement bounds, and receipt extraction are versioned and conformance
   tested.
4. **Stable intent across frameworks.** Python, HTTP, MCP, Temporal, DBOS, LangGraph, and
   `agent-runtime` can all map their run/step identity into the same business operation.
5. **Failure proof, not a happy-path demo.** The crash matrix kills processes at every
   boundary, creates concurrent and zombie workers, and checks the authoritative target
   count.
6. **Payload and approval-grade binding.** An operation key cannot be reused with changed
   arguments or a different contract version.

As models improve, agents will be trusted with more consequential effects. The broker's
value rises with that autonomy. None of the six points is replaced by a more accurate tool
choice.

## Killer demo

The demo uses a tiny payment target with an authoritative `charges` table and switchable
contract modes, plus an agent that requests one charge for `order-42`.

### Scenario A: idempotent target

1. The broker reserves `order:42:charge:v1`.
2. The target commits `charge_123` under the downstream key.
3. The test runner kills the broker worker after target commit but before local receipt.
4. Recovery sees `outcome_unknown` and redispatches with the same still-valid key.
5. The target returns `charge_123`; the broker records one receipt.
6. Ten concurrent retries and a zombie worker still leave exactly one target charge.

### Scenario B: bounded, authoritative reconciliation

1. The target does not deduplicate create requests but records a caller correlation key.
2. The response is lost after commit.
3. The broker waits the declared settlement bound and probes by correlation key.
4. It finds the existing charge and records the receipt without redispatch.
5. If the bounded authoritative lookup proves absence, only then may it requeue.

### Scenario C: unsafe target

1. The target has neither an idempotency key nor an authoritative lookup.
2. The response is lost.
3. The broker stays `outcome_unknown`, pages/metrics the condition, and does not retry.

The crash runner repeats each scenario across every failpoint and prints target-side
effect counts and broker receipts. Success means one charge for A and B, and no broker
duplicate for C; C may require manual resolution because physics does not offer a stronger
answer.

### Why a model or generic durable tool cannot reproduce it

From only the local journal, "remote did nothing" and "remote committed" produce the same
history. They are observationally indistinguishable, so no plain LLM, stronger LLM, local
cache, deterministic replay engine, or time-travel UI can choose the safe action. Temporal,
DBOS, Restate, LangGraph, and `agent-runtime` can host the demo, but they need the same
downstream key or reconciliation contract to pass it. `agent-ledger` can pass Scenario A
only when the handler propagates the downstream key; its local ledger alone cannot resolve
the killed-after-commit window. Reproducing all three scenarios requires implementing the
project's core protocol, which is the point.

## MVP scope

### Build first

- Python SDK with mandatory operation keys and payload binding.
- Four safety classes and validated, versioned YAML/TOML contracts.
- Postgres store, migrations, CAS transitions, worker leases, and tenant scoping.
- Dispatcher and reconciler with no hidden HTTP/SDK retry layers.
- Generic HTTP idempotency-header adapter.
- Probeable simulated target and one optional Stripe test-mode adapter.
- Explicit `outcome_unknown` queue and operator evidence resolution.
- Immutable receipts and deterministic read-only replay.
- FastAPI API, Typer CLI, and MCP management/proxy server.
- Scripted, OpenAI, and Grok providers for the demo agent only.
- OpenTelemetry spans and Prometheus metrics without raw argument capture.
- Crash-matrix, concurrency, zombie-worker, tenant-isolation, and adapter-contract tests.
- Docker Compose, production Dockerfile, GitHub Actions, complete threat/failure model,
  and MIT license.

### Later, only after the core is proved

- Temporal, DBOS, Restate, LangGraph, and `agent-runtime` convenience integrations.
- Additional adapters for refunds, email providers, ticketing, cloud changes, and webhooks.
- Transactional outbox and Kafka publication of effect events.
- KMS/Vault-backed envelope encryption and key rotation.
- Signed and externally anchored receipts compatible with `agent-attest` concepts.
- Policy and approval hooks that consume, rather than duplicate, `agent-runtime` gates.
- Business-specific compensation workflows and sagas.
- Web UI, SSO, fine-grained RBAC, retention policies, and audit export.
- Multi-region architecture. Do not claim global active-active correctness before a formal
  design and fault test exists.

### Explicitly excluded from MVP

- a visual workflow builder;
- general agent orchestration;
- model-based retry classification;
- automatic inference of whether a tool mutates state;
- a connector marketplace;
- unconditional "exactly once" marketing;
- automatic compensation for arbitrary effects.

## Technology stack

- Python 3.11+ with `src/` layout and full typing.
- Hatchling build backend and `py.typed` marker.
- Pydantic v2 for API/config validation.
- FastAPI and Uvicorn for HTTP.
- Typer for CLI.
- Official MCP Python SDK for the MCP server/proxy.
- SQLAlchemy 2 Core with psycopg 3, PostgreSQL 16, and Alembic. Critical transitions use
  explicit SQL and transaction tests rather than ORM magic.
- HTTPX with transport retries disabled.
- `cryptography` AES-GCM for stored payloads.
- Official OpenAI Python SDK for both OpenAI and Grok's OpenAI-compatible endpoint.
- OpenTelemetry plus `prometheus-client`.
- Pytest, `pytest-asyncio`, Hypothesis state machines, and Testcontainers or a GitHub
  Actions Postgres service.
- Ruff with line length 90 and rules `E`, `F`, `I`, `UP`, and `B` at minimum.
- Multi-stage Docker image, non-root runtime user, health checks, and Docker Compose demo.
- GitHub Actions for lint, unit, Postgres integration, crash matrix, package build, and
  Docker build. Live provider tests remain opt-in.
- MIT license.

## Required observability

Metrics:

- effects submitted, deduplicated, conflicted, succeeded, failed, and unknown;
- unknown-effect count and oldest age by contract/tool, without tenant-ID cardinality;
- dispatch attempts and reconciliation outcomes;
- idempotency retention remaining when a retry occurs;
- lease expirations, fenced stale writes, and payload conflicts;
- end-to-end reservation, dispatch, and reconciliation latency.

Every effect carries trace context, but spans log only the operation-key digest, tool,
contract version, status, and receipt ID. Raw arguments and outputs are opt-in and redacted.

Alerts should focus on correctness, not generic uptime: any `unsafe` unknown effect,
unknown age beyond its contract threshold, retry attempted after key expiry, payload drift,
or nonzero stale-worker write count.

## Top risks and de-risking

| Risk | Why it can kill the project | De-risking action |
| --- | --- | --- |
| The headline promise is impossible | One dishonest "exactly once for any tool" claim destroys technical credibility | Publish the guarantee table and failure proof before implementation; make `unsafe` and `outcome_unknown` visible in every API |
| It is just `agent-ledger` with more files | A direct Python competitor already handles local deduplication and approvals | Spike the killed-after-remote-commit and reconciliation demo first; if the distinction is not obvious in 60 seconds, stop |
| Callers choose bad operation keys | A random key per retry defeats deduplication; an over-broad key suppresses legitimate work | Require explicit keys, ship runtime-specific key helpers, reject payload drift, and lint key scope in `doctor` |
| Adapter contracts lie | A provider may expire keys early, retry internally, or expose eventually consistent lookup | Pin contract versions, disable SDK retries, run conformance tests, record retention deadlines, and downgrade to unknown on contract violation |
| Leases are mistaken for correctness | Zombie workers can execute after lease expiry | Fence local writes and rely on downstream keys; test paused/zombie workers explicitly |
| Sensitive tool arguments enter the ledger | Payments, support, and identity payloads contain secrets and PII | Encrypt payloads, persist redacted previews, support `SecretRef`, scope every query by authenticated tenant, and test log redaction |
| MCP proxy adoption is awkward | Teams will bypass a boundary that changes every tool signature badly | Mirror configured schemas, add one explicit operation-key convention, and provide thin framework helpers |
| Connector maintenance swallows the project | SaaS API differences can turn the repo into brittle glue | MVP has one generic adapter, one real adapter, and a conformance kit; no marketplace |
| Demand is narrower than expected | Many agents remain read-only or low stakes | Interview five teams with mutating agents before polishing a UI; measure whether they currently hand-roll idempotency/reconciliation |

## Ordered build plan

### 1. Write the failure contract and competitive spike

Create an ADR that proves the remote-commit ambiguity and defines the four safety classes.
Build a minimal target and reproduce the flaw in `agent-runtime` and a local-ledger wrapper.
Exit condition: a process kill causes a duplicate without target cooperation, and the same
test is safe with a stable downstream key.

### 2. Scaffold the repository

Create the house-style package, Hatchling config, Ruff/pytest settings, Typer entry point,
FastAPI factory, MCP stub, Dockerfile, Compose file, CI, license, and typed settings. Keep
all default tests credential-free.

### 3. Freeze domain types and canonicalization

Implement `EffectRequest`, contracts, statuses, strict canonical JSON, request hashes,
HMAC downstream keys, and domain errors. Add property tests for map ordering, Unicode,
numbers, invalid values, payload drift, and tenant scoping.

### 4. Implement the Postgres schema and store contract

Add migrations, unique constraints, compare-and-swap transitions, append-only events,
receipt immutability, and encrypted payload storage. Run one conformance suite against the
in-memory fake and Postgres, while documenting that only Postgres is production-capable.

Exit condition: 1,000 concurrent reservations of one tenant/key produce one intent; a
different payload produces conflicts; the same key in another tenant is independent.

### 5. Implement the pure state machine

Keep transition validation independent of FastAPI, MCP, and HTTP adapters. Exhaustively
test allowed and forbidden transitions with a Hypothesis rule-based state machine.

### 6. Implement worker claims and fencing

Add `SKIP LOCKED` claims, leases, heartbeats, attempt rows, expected-version writes, and
stale-dispatch conversion to `outcome_unknown`. Simulate pauses so an expired worker later
attempts to write and is fenced out.

### 7. Implement adapter contracts

Build the simulated adapter and generic HTTP adapter. Disable all implicit transport
retries. Validate retention and settlement settings. Create a reusable conformance suite
covering stable keys, payload drift, positive probe, negative probe timing, and unknown
probe behavior.

### 8. Implement reconciliation

Resolve idempotent unknowns by safe redispatch, reconcilable unknowns by authoritative
probe, and unsafe unknowns only by operator evidence. Test expiry of downstream key
retention and non-conclusive eventually consistent lookups.

### 9. Build the crash matrix before the polished API

Instrument failpoints before reservation commit, after reservation, before socket write,
after target commit, before receipt commit, and after receipt commit. Kill subprocesses,
restart workers, introduce duplicate submissions and zombies, and assert target-side
counts. This is the project's proof artifact, not a late test task.

### 10. Add FastAPI and authentication

Expose submit, inspect, receipt, reconcile, list, and operator-resolution endpoints.
Derive tenant from hashed API keys. Add problem-detail errors, pagination, request limits,
health/readiness, and OpenAPI examples for replay and conflict behavior.

### 11. Add Typer operations

Implement server/worker/reconciler commands, inspection, contract validation, doctor, and
the crash demo. Make unknown effects operationally obvious and unsafe retry impossible
from the CLI.

### 12. Add the MCP boundary

Expose management tools and a small configured proxy with mirrored schemas. Integrate
`agent-runtime` as the first consumer: its stable run/call IDs become operation keys, and
stored receipts become tool results on replay.

### 13. Add the provider demo and one real adapter

Implement `ScriptedProvider`, `OpenAIProvider`, and `GrokProvider` using the established
portfolio pattern. Add Stripe test mode as the first real idempotent adapter. Models select
the demo tool; they never select safety class or retry policy.

### 14. Add observability and security hardening

Instrument metrics and traces, redact payloads, enforce encrypted storage, add cross-tenant
tests, threat-model operation-key guessing and replay, run dependency scanning, and verify
the container runs non-root.

### 15. Package the proof

Write the README around the failure matrix and guarantee table, not generic AI language.
Publish benchmark numbers for reservation overhead and reconciliation latency, a demo
video showing the target-side charge count, and an integration guide for a plain Python
tool plus `agent-runtime`.

### 16. Apply the stop/go gate

Continue only if all are true:

- the crash demo clearly passes a case that `agent-runtime` and an unmodified local ledger
  fail;
- the code never claims safety for a non-cooperating target;
- at least three external practitioners with mutating agents recognize the failure from
  their own systems;
- the MVP remains an effect boundary rather than a workflow engine;
- the real adapter requires materially less correctness code from its caller than using
  Stripe or AWS idempotency primitives directly.

If these fail, do not add a UI and hope positioning fixes it. Stop and build `runshift`.

## Definition of MVP done

The MVP is done when a fresh clone can run one command that starts Postgres, the broker,
workers, and the simulated target; another command runs the complete crash matrix; and the
result proves the conditional guarantees from authoritative target state. CI must repeat
the Postgres concurrency and subprocess crash tests. The docs must include the unsafe case
and show `outcome_unknown` without euphemism. A polished happy path without that proof is
not an MVP of this project.
