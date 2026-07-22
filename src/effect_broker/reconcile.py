"""Resolution logic for effects whose dispatch outcome is unknown."""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime

from effect_broker.adapters.base import EffectAdapter
from effect_broker.errors import AdapterError, VersionConflictError
from effect_broker.models import (
    DispatchResult,
    EffectRecord,
    EffectStatus,
    JsonObject,
    ProbeStatus,
)
from effect_broker.observability import (
    observe_latency,
    record_dispatch_attempt,
    record_fenced_stale_write,
    record_reconciliation_outcome,
    record_retention_remaining,
    record_transition,
    span,
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

    with observe_latency("reconcile"), span("reconcile", effect):
        return await _reconcile_once(store, adapter_for, effect=effect)


async def _reconcile_once(
    store: EffectStore,
    adapter_for: AdapterFor,
    *,
    effect: EffectRecord,
) -> EffectRecord:
    """Implementation body separated so latency covers every return path."""

    route = route_unknown(effect.contract.safety)
    if route is EffectStatus.DISPATCHING:
        record_retention_remaining(effect)
        if not _key_retention_valid(effect):
            record_reconciliation_outcome("retention_expired")
            return await _transition_and_record(
                store,
                effect,
                EffectStatus.MANUAL_REVIEW,
                {"reason": "idempotency_key_retention_expired"},
            )
        dispatching = await _transition_and_record(
            store,
            effect,
            EffectStatus.DISPATCHING,
            {"reason": "idempotent_redispatch"},
        )
        attempt_id = await store.start_attempt(
            dispatching.effect_id,
            expected_version=dispatching.version,
            worker_id="reconciler",
        )
        record_dispatch_attempt(dispatching)
        adapter = adapter_for(dispatching)
        try:
            with observe_latency("dispatch"), span("dispatch", dispatching):
                result = await adapter.dispatch(dispatching, attempt_id=attempt_id)
        except AdapterError as exc:
            record_reconciliation_outcome("proven_non_commit")
            return await _transition_and_record(
                store,
                dispatching,
                EffectStatus.RETRYABLE,
                {
                    "attempt_id": attempt_id,
                    "error": type(exc).__name__,
                    "reason": "proven_non_commit_after_redispatch",
                },
            )
        except Exception as exc:  # noqa: BLE001 - still ambiguous, do not retry.
            record_reconciliation_outcome("redispatch_unknown")
            return await _transition_and_record(
                store,
                dispatching,
                EffectStatus.OUTCOME_UNKNOWN,
                {
                    "attempt_id": attempt_id,
                    "error": type(exc).__name__,
                    "reason": "ambiguous_redispatch",
                },
            )
        if result.committed:
            record_reconciliation_outcome("redispatch_committed")
            return await _transition_committed(store, dispatching, attempt_id, result)
        record_reconciliation_outcome("redispatch_unknown")
        return await _transition_and_record(
            store,
            dispatching,
            EffectStatus.OUTCOME_UNKNOWN,
            {"attempt_id": attempt_id, "reason": "not_confirmed"},
        )

    if route is EffectStatus.RECONCILING:
        reconciling = await _transition_and_record(
            store,
            effect,
            EffectStatus.RECONCILING,
            {"reason": "authoritative_probe"},
        )
        probe = await adapter_for(reconciling).probe(reconciling)
        record_reconciliation_outcome(probe.status.value)
        if probe.status is ProbeStatus.COMMITTED:
            return await _transition_and_record(
                store,
                reconciling,
                EffectStatus.SUCCEEDED,
                {
                    "external_id": probe.external_id,
                    "output": dict(probe.output or {}),
                    "evidence": dict(probe.evidence),
                },
            )
        if probe.status is ProbeStatus.NOT_COMMITTED:
            return await _transition_and_record(
                store,
                reconciling,
                EffectStatus.PREPARED,
                {"evidence": dict(probe.evidence)},
            )
        return await _transition_and_record(
            store,
            reconciling,
            EffectStatus.MANUAL_REVIEW,
            {"evidence": dict(probe.evidence), "reason": "probe_unknown"},
        )

    record_reconciliation_outcome("unsafe_unknown")
    return await _transition_and_record(
        store,
        effect,
        EffectStatus.MANUAL_REVIEW,
        {"reason": "unsafe_unknown"},
    )


async def _transition_committed(
    store: EffectStore,
    effect: EffectRecord,
    attempt_id: str,
    result: DispatchResult,
) -> EffectRecord:
    return await _transition_and_record(
        store,
        effect,
        EffectStatus.SUCCEEDED,
        {
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


async def _transition_and_record(
    store: EffectStore,
    before: EffectRecord,
    target: EffectStatus,
    data: JsonObject,
) -> EffectRecord:
    try:
        after = await store.transition(
            before.effect_id,
            expected_version=before.version,
            target=target,
            data=data,
        )
    except VersionConflictError:
        record_fenced_stale_write()
        raise
    record_transition(before, after)
    return after
