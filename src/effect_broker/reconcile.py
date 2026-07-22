"""Resolution logic for effects whose dispatch outcome is unknown."""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime

from effect_broker.adapters.base import EffectAdapter
from effect_broker.errors import AdapterError
from effect_broker.models import (
    DispatchResult,
    EffectRecord,
    EffectStatus,
    ProbeStatus,
)
from effect_broker.statemachine import route_unknown
from effect_broker.store.base import EffectStore

AdapterFor = Callable[[EffectRecord], EffectAdapter]


async def reconcile_once(
    store: EffectStore,
    adapter_for: AdapterFor,
    *,
    effect: EffectRecord,
) -> EffectRecord:
    """Resolve one ``OUTCOME_UNKNOWN`` effect according to its safety class."""

    route = route_unknown(effect.contract.safety)
    if route is EffectStatus.DISPATCHING:
        if not _key_retention_valid(effect):
            return await store.transition(
                effect.effect_id,
                expected_version=effect.version,
                target=EffectStatus.MANUAL_REVIEW,
                data={"reason": "idempotency_key_retention_expired"},
            )
        dispatching = await store.transition(
            effect.effect_id,
            expected_version=effect.version,
            target=EffectStatus.DISPATCHING,
            data={"reason": "idempotent_redispatch"},
        )
        attempt_id = await store.start_attempt(
            dispatching.effect_id,
            expected_version=dispatching.version,
            worker_id="reconciler",
        )
        adapter = adapter_for(dispatching)
        try:
            result = await adapter.dispatch(dispatching, attempt_id=attempt_id)
        except AdapterError as exc:
            return await store.transition(
                dispatching.effect_id,
                expected_version=dispatching.version,
                target=EffectStatus.RETRYABLE,
                data={
                    "attempt_id": attempt_id,
                    "error": type(exc).__name__,
                    "reason": "proven_non_commit_after_redispatch",
                },
            )
        except Exception as exc:  # noqa: BLE001 - still ambiguous, do not retry.
            return await store.transition(
                dispatching.effect_id,
                expected_version=dispatching.version,
                target=EffectStatus.OUTCOME_UNKNOWN,
                data={
                    "attempt_id": attempt_id,
                    "error": type(exc).__name__,
                    "reason": "ambiguous_redispatch",
                },
            )
        if result.committed:
            return await _transition_committed(store, dispatching, attempt_id, result)
        return await store.transition(
            dispatching.effect_id,
            expected_version=dispatching.version,
            target=EffectStatus.OUTCOME_UNKNOWN,
            data={"attempt_id": attempt_id, "reason": "not_confirmed"},
        )

    if route is EffectStatus.RECONCILING:
        reconciling = await store.transition(
            effect.effect_id,
            expected_version=effect.version,
            target=EffectStatus.RECONCILING,
            data={"reason": "authoritative_probe"},
        )
        probe = await adapter_for(reconciling).probe(reconciling)
        if probe.status is ProbeStatus.COMMITTED:
            return await store.transition(
                reconciling.effect_id,
                expected_version=reconciling.version,
                target=EffectStatus.SUCCEEDED,
                data={
                    "external_id": probe.external_id,
                    "output": dict(probe.output or {}),
                    "evidence": dict(probe.evidence),
                },
            )
        if probe.status is ProbeStatus.NOT_COMMITTED:
            return await store.transition(
                reconciling.effect_id,
                expected_version=reconciling.version,
                target=EffectStatus.PREPARED,
                data={"evidence": dict(probe.evidence)},
            )
        return await store.transition(
            reconciling.effect_id,
            expected_version=reconciling.version,
            target=EffectStatus.MANUAL_REVIEW,
            data={"evidence": dict(probe.evidence), "reason": "probe_unknown"},
        )

    return await store.transition(
        effect.effect_id,
        expected_version=effect.version,
        target=EffectStatus.MANUAL_REVIEW,
        data={"reason": "unsafe_unknown"},
    )


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


def _key_retention_valid(effect: EffectRecord) -> bool:
    retention = effect.contract.key_retention
    if retention is None:
        return False
    return datetime.now(UTC) <= effect.created_at + retention
