from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import pytest

from effect_broker.adapters.simulated import SimulatedAdapter, SimulatedTarget
from effect_broker.contracts import ContractRegistry
from effect_broker.engine import EffectBroker
from effect_broker.errors import VersionConflictError
from effect_broker.models import (
    EffectContract,
    EffectRecord,
    EffectRequest,
    EffectStatus,
    SafetyClass,
)
from effect_broker.reconcile import reconcile_once
from effect_broker.store.memory import InMemoryStore
from effect_broker.worker import dispatch_once


def _request(
    operation_key: str,
    *,
    tool: str = "charge",
) -> EffectRequest:
    return EffectRequest(
        operation_key=operation_key,
        tool=tool,
        arguments={"amount": 4200, "currency": "usd"},
        requested_by="test",
    )


def _contract(safety: SafetyClass) -> EffectContract:
    return EffectContract(
        name="charge",
        version="v1",
        safety=safety,
        retry_limit=3 if safety is not SafetyClass.UNSAFE else 0,
        key_retention=timedelta(hours=1)
        if safety is SafetyClass.IDEMPOTENT
        else None,
        settlement_bound=timedelta(seconds=5)
        if safety is SafetyClass.RECONCILABLE
        else None,
    )


def _adapter_for(target: SimulatedTarget):
    def build(effect: EffectRecord) -> SimulatedAdapter:
        return SimulatedAdapter(target, effect.contract)

    return build


async def _reserve(
    store: InMemoryStore,
    target: SimulatedTarget,
    contract: EffectContract,
    operation_key: str,
):
    broker = EffectBroker(
        store,
        ContractRegistry({"charge": contract}),
        _adapter_for(target),
    )
    return await broker.submit("tenant-a", _request(operation_key))


async def _dispatch_lost_response(
    store: InMemoryStore,
    target: SimulatedTarget,
    contract: EffectContract,
    operation_key: str,
) -> EffectRecord:
    reservation = await _reserve(store, target, contract, operation_key)
    target.lose_next_response()
    results = await dispatch_once(
        store,
        _adapter_for(target),
        worker_id="worker-1",
        now=datetime.now(UTC),
        lease_for=timedelta(seconds=30),
        limit=10,
    )
    assert len(results) == 1
    assert results[0].status is EffectStatus.OUTCOME_UNKNOWN
    effect = await store.get("tenant-a", reservation.effect.effect_id)
    assert effect is not None
    return effect


async def test_idempotent_killer_path_redispatches_same_key_once():
    store = InMemoryStore()
    target = SimulatedTarget()
    contract = _contract(SafetyClass.IDEMPOTENT)
    unknown = await _dispatch_lost_response(
        store,
        target,
        contract,
        "order:42:charge:v1",
    )

    assert len(target.charges_for_key(unknown.downstream_key)) == 1

    reconciled = await reconcile_once(
        store,
        _adapter_for(target),
        effect=unknown,
    )

    assert reconciled.status is EffectStatus.SUCCEEDED
    assert len(target.charges_for_key(unknown.downstream_key)) == 1


async def test_idempotent_concurrent_redispatches_and_zombie_are_fenced():
    store = InMemoryStore()
    target = SimulatedTarget()
    contract = _contract(SafetyClass.IDEMPOTENT)
    unknown = await _dispatch_lost_response(
        store,
        target,
        contract,
        "order:43:charge:v1",
    )

    outcomes = await asyncio.gather(
        *[
            reconcile_once(store, _adapter_for(target), effect=unknown)
            for _ in range(5)
        ],
        return_exceptions=True,
    )

    successes = [
        outcome
        for outcome in outcomes
        if isinstance(outcome, EffectRecord)
        and outcome.status is EffectStatus.SUCCEEDED
    ]
    conflicts = [
        outcome for outcome in outcomes if isinstance(outcome, VersionConflictError)
    ]
    assert len(successes) == 1
    assert len(conflicts) == 4
    assert len(target.charges_for_key(unknown.downstream_key)) == 1

    with pytest.raises(VersionConflictError):
        await store.transition(
            unknown.effect_id,
            expected_version=unknown.version,
            target=EffectStatus.DISPATCHING,
            data={"actor": "zombie-worker"},
        )


async def test_reconcilable_lost_response_probes_after_settlement_no_redispatch():
    store = InMemoryStore()
    target = SimulatedTarget()
    contract = _contract(SafetyClass.RECONCILABLE)
    unknown = await _dispatch_lost_response(
        store,
        target,
        contract,
        "order:44:charge:v1",
    )
    target.set_now(unknown.created_at + contract.settlement_bound + timedelta(seconds=1))

    reconciled = await reconcile_once(
        store,
        _adapter_for(target),
        effect=unknown,
    )

    assert reconciled.status is EffectStatus.SUCCEEDED
    assert target.dispatch_count == 1
    assert len(target.charges_for_key(unknown.downstream_key)) == 1


async def test_reconcilable_probe_unknown_before_settlement_manual_review():
    store = InMemoryStore()
    target = SimulatedTarget()
    target.hide_commits_until_settled = True
    contract = _contract(SafetyClass.RECONCILABLE)
    unknown = await _dispatch_lost_response(
        store,
        target,
        contract,
        "order:45:charge:v1",
    )

    reconciled = await reconcile_once(
        store,
        _adapter_for(target),
        effect=unknown,
    )

    assert reconciled.status is EffectStatus.MANUAL_REVIEW
    assert target.dispatch_count == 1
    assert len(target.charges_for_key(unknown.downstream_key)) == 1


async def test_unsafe_unknown_goes_to_manual_review_without_duplicate():
    store = InMemoryStore()
    target = SimulatedTarget()
    contract = _contract(SafetyClass.UNSAFE)
    unknown = await _dispatch_lost_response(
        store,
        target,
        contract,
        "order:46:charge:v1",
    )

    reconciled = await reconcile_once(
        store,
        _adapter_for(target),
        effect=unknown,
    )

    assert reconciled.status is EffectStatus.MANUAL_REVIEW
    assert target.dispatch_count == 1
    assert len(target.charges_for_key(unknown.downstream_key)) <= 1


async def test_transactional_atomic_commit_succeeds_directly():
    store = InMemoryStore()
    target = SimulatedTarget()
    contract = _contract(SafetyClass.TRANSACTIONAL)
    reservation = await _reserve(
        store,
        target,
        contract,
        "order:47:charge:v1",
    )

    results = await dispatch_once(
        store,
        _adapter_for(target),
        worker_id="worker-1",
        now=datetime.now(UTC),
        lease_for=timedelta(seconds=30),
        limit=10,
    )

    assert len(results) == 1
    assert results[0].status is EffectStatus.SUCCEEDED
    assert len(target.charges_for_key(reservation.effect.downstream_key)) == 1
