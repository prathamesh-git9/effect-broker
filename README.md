# effect-broker

**A correctness boundary for side-effecting agent tools.** Agents, workflow
engines, and queues retry. Charges, emails, refunds, and production changes must
not happen twice. `effect-broker` sits between them and records a stable business
intent before dispatch, binds it to an immutable payload and a pinned tool
contract, propagates **one downstream idempotency key across every attempt**, and
**reconciles lost responses against an authoritative target**. When a target
cannot support idempotency or prove its state, the broker returns
**`outcome_unknown`** and refuses an unsafe retry.

It never claims exactly-once for a target that cannot honor it. That honesty is
the product.

> **Why a better model does not fix this.** It is an information-and-atomicity
> problem, not a reasoning one. From the local journal alone, *"the request never
> arrived"* and *"the request committed but the acknowledgement was lost"* are
> the same history. A smarter model may pick a better tool and still double-charge
> when infrastructure retries the correctly-chosen call. The only honest
> solutions are protocol mechanisms: a shared transaction, downstream
> idempotency, authoritative reconciliation, or an explicit stop in an unknown
> state.

## The proof (this is the point, not a happy-path demo)

A worker subprocess does a **real `os._exit(137)` after the authoritative target
commits but before the broker records its receipt** — the exact window that
causes real double-charges. Recovery then reconciles, and the runner asserts the
**target-side** effect count:

```
$ python examples/crash_matrix/runner.py
scenario     | failpoint                           | target_count | broker_status | result
idempotent   | after_target_commit_before_receipt  |            1 | succeeded     | PASS
reconcilable | after_target_commit_before_receipt  |            1 | succeeded     | PASS
unsafe       | after_target_commit_before_receipt  |            1 | manual_review | PASS
ALL PASS
```

Recovery runs **real concurrent subprocesses** — exactly one wins and a stale
zombie worker is compare-and-swap fenced. This runs in CI because the durable
store is SQLite (no external service). See `tests/test_crash_matrix.py`.

## The honest guarantee table

The useful guarantee is *exactly one observable business effect* — not exactly
one packet or handler entry. What the broker can promise depends entirely on what
the target can prove:

| Safety class | Required target contract | Honest guarantee |
|---|---|---|
| `transactional` | target mutation + broker receipt share one DB transaction | exactly one committed effect per operation key |
| `idempotent` | stable key, rejects payload drift, retains the result for a declared horizon | exactly one observable effect while the contract and retention hold |
| `reconcilable` | authoritative lookup by business key, with a declared settlement bound | converges to one effect if the lookup is truthful and bounded; else manual review |
| `unsafe` | no idempotency, no authoritative lookup | **no broker-issued duplicate, but possible omission — no exactly-once claim** |

Constraints the broker enforces rather than papers over: an eventually-consistent
search cannot prove absence (only success); a target's key retention must outlast
the broker's recovery horizon or automatic retry stops; a lease is not a
distributed lock — exactly-once comes from the downstream contract, not from
pretending otherwise; and the caller supplies a stable semantic `operation_key`
(two legitimate $10 refunds are different operations, so the broker never hashes
arguments to guess intent).

## Install

```bash
pip install -e ".[dev,server,mcp]"    # Python 3.11+
```

## Quickstart

```python
import asyncio
from effect_broker.config import Settings, build_broker
from effect_broker.models import EffectRequest

# examples/contracts.yaml declares one idempotent, one reconcilable, and one
# unsafe tool — each fixing the honest guarantee for that tool.
broker = build_broker(Settings(contracts_path="examples/contracts.yaml"))
res = asyncio.run(broker.submit("tenant-1", EffectRequest(
    operation_key="agent-run:run_42:step:refund_01",
    tool="charge",
    arguments={"amount_usd": 49.99, "order_id": "A-10428"},
    requested_by="support-agent",
)))
print(res.effect.status)                     # prepared  (a worker dispatches it)
# Re-submitting the same operation_key returns the SAME effect, not a second one.
```

Run the crash-matrix proof, or the API:

```bash
python examples/crash_matrix/runner.py       # the killed-after-commit proof
effect-broker serve                          # FastAPI: POST /effects, GET /effects/{id}, ...
effect-broker worker                         # the dispatch loop (separate process)
effect-broker reconciler                     # the unknown-outcome resolution loop
effect-broker doctor                         # config, contracts, store, key-scope checks
```

## Surfaces

- **HTTP** — `POST /effects` (submit; 201 + dedup replay, **409** on payload
  conflict), `GET /effects/{id}`, `GET /effects/{id}/receipt` (read-only replay,
  never dispatches), `POST /effects/{id}/reconcile`, `GET /effects?status=`,
  `POST /effects/{id}/resolve` (operator resolution of `manual_review`).
- **CLI** — `serve`, `worker`, `reconciler`, `submit`, `inspect`, `list`,
  `contracts validate`, `doctor`, `crash-demo`. Unsafe redispatch is impossible
  from the CLI.
- **MCP** — broker management tools plus a mutating-tool proxy that requires an
  `operation_key`, so an agent routes side-effecting calls through the broker. A
  model never chooses the safety class or the retry policy — that is pure code
  over the pinned contract.

## It is / it is not

**It is** an intent ledger and state machine for mutating tool calls, a
downstream contract registry, a dispatcher and reconciler, and a failure-injection
proof harness. It **complements** agent-runtime, Temporal, DBOS, Restate, and
LangGraph — mapping their run/step identity into one business operation.

**It is not** an agent framework, a workflow engine, an LLM gateway, or a claim
of unconditional exactly-once execution.

## Status

The correctness core, the four-safety-class engine, the durable store, and the
crash-matrix proof are done and green in CI. The production **Postgres** store
(`FOR UPDATE SKIP LOCKED`), provider demo, observability, and Docker Compose are
the next milestones. See [docs/NEXT_PROJECT_SPEC.md](docs/NEXT_PROJECT_SPEC.md).

## License

MIT. See [LICENSE](LICENSE).
