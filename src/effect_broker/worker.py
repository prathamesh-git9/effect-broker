"""Worker loop primitives.

The worker performs no hidden retries. It records ``DISPATCHING`` and an attempt
row before adapter I/O, then persists the only truthful outcome it has evidence
for. Ambiguous transport failures become ``OUTCOME_UNKNOWN`` so reconciliation
can use the pinned contract instead of guessing.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timedelta

from effect_broker.adapters.base import EffectAdapter
from effect_broker.errors import AdapterError
from effect_broker.models import DispatchResult, EffectRecord, EffectStatus, JsonObject
from effect_broker.store.base import EffectStore

AdapterFor = Callable[[EffectRecord], EffectAdapter]


async def dispatch_once(
    store: EffectStore,
    adapter_for: AdapterFor,
    *,
    worker_id: str,
    now: datetime,
    lease_for: timedelta,
    limit: int,
) -> list[EffectRecord]:
    """Claim due work and dispatch each effect at most once."""

    claimed = await store.claim_due(
        worker_id,
        now=now,
        lease_for=lease_for,
        limit=limit,
    )
    results: list[EffectRecord] = []
    for effect in claimed:
        dispatching = await store.transition(
            effect.effect_id,
            expected_version=effect.version,
            target=EffectStatus.DISPATCHING,
            data={"worker_id": worker_id},
        )
        attempt_id = await store.start_attempt(
            dispatching.effect_id,
            expected_version=dispatching.version,
            worker_id=worker_id,
        )
        try:
            adapter = adapter_for(dispatching)
            result = await adapter.dispatch(dispatching, attempt_id=attempt_id)
            if result.committed:
                results.append(
                    await _transition_committed(store, dispatching, attempt_id, result)
                )
            else:
                results.append(
                    await store.transition(
                        dispatching.effect_id,
                        expected_version=dispatching.version,
                        target=EffectStatus.OUTCOME_UNKNOWN,
                        data={"attempt_id": attempt_id, "reason": "not_confirmed"},
                    )
                )
        except AdapterError as exc:
            results.append(
                await _transition_proven_non_commit(
                    store,
                    dispatching,
                    attempt_id,
                    exc,
                )
            )
        except Exception as exc:  # noqa: BLE001 - ambiguity must be persisted.
            results.append(
                await store.transition(
                    dispatching.effect_id,
                    expected_version=dispatching.version,
                    target=EffectStatus.OUTCOME_UNKNOWN,
                    data={
                        "attempt_id": attempt_id,
                        "error": type(exc).__name__,
                        "reason": "ambiguous_dispatch",
                    },
                )
            )
    return results


async def _transition_committed(
    store: EffectStore,
    effect: EffectRecord,
    attempt_id: str,
    result: DispatchResult,
) -> EffectRecord:
    return await store.transition(
        effect.effect_id,
        expected_version=effect.version,
        target=EffectStatus.SUCCEEDED,
        data={
            "attempt_id": attempt_id,
            "external_id": result.external_id,
            "output": dict(result.output),
        },
    )


async def _transition_proven_non_commit(
    store: EffectStore,
    effect: EffectRecord,
    attempt_id: str,
    exc: AdapterError,
) -> EffectRecord:
    attempts = _attempt_ordinal(attempt_id)
    target = (
        EffectStatus.RETRYABLE
        if attempts < effect.contract.retry_limit
        else EffectStatus.FAILED_FINAL
    )
    data: JsonObject = {
        "attempt_id": attempt_id,
        "error": type(exc).__name__,
        "reason": "proven_non_commit",
    }
    return await store.transition(
        effect.effect_id,
        expected_version=effect.version,
        target=target,
        data=data,
    )


def _attempt_ordinal(attempt_id: str) -> int:
    try:
        return int(attempt_id.rsplit("-", maxsplit=1)[1])
    except (IndexError, ValueError):
        return 1
