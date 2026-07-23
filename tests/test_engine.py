from __future__ import annotations

from datetime import timedelta

import pytest

from effect_broker.adapters.simulated import SimulatedAdapter, SimulatedTarget
from effect_broker.contracts import ContractRegistry
from effect_broker.engine import EffectBroker
from effect_broker.errors import PayloadConflictError, VersionConflictError
from effect_broker.models import (
    EffectContract,
    EffectRequest,
    EffectStatus,
    SafetyClass,
)
from effect_broker.store.memory import InMemoryStore


def _request(
    operation_key: str = "order:42:charge:v1",
    *,
    amount: int = 100,
    tool: str = "charge",
) -> EffectRequest:
    return EffectRequest(
        operation_key=operation_key,
        tool=tool,
        arguments={"amount": amount, "currency": "usd"},
        requested_by="test",
    )


def _contract(safety: SafetyClass) -> EffectContract:
    return EffectContract(
        name="charge",
        version="v1",
        safety=safety,
        retry_limit=3 if safety is not SafetyClass.UNSAFE else 0,
        key_retention=timedelta(hours=1) if safety is SafetyClass.IDEMPOTENT else None,
        settlement_bound=timedelta(seconds=5)
        if safety is SafetyClass.RECONCILABLE
        else None,
    )


def _broker(
    store: InMemoryStore,
    contract: EffectContract,
    target: SimulatedTarget | None = None,
) -> EffectBroker:
    def adapter_for(effect):
        if target is None:
            raise AssertionError("adapter should not be requested")
        return SimulatedAdapter(target, effect.contract)

    return EffectBroker(
        store,
        ContractRegistry({"charge": contract}),
        adapter_for,
    )


async def test_dedup_same_operation_and_payload_replays_one_effect():
    store = InMemoryStore()
    broker = _broker(store, _contract(SafetyClass.IDEMPOTENT))
    request = _request()

    first = await broker.submit("tenant-a", request)
    second = await broker.submit("tenant-a", request)

    assert first.created
    assert not first.replayed
    assert not second.created
    assert second.replayed
    assert second.effect.effect_id == first.effect.effect_id
    assert await store.count() == 1


async def test_payload_conflict_refuses_dispatch():
    store = InMemoryStore()
    target = SimulatedTarget()
    broker = _broker(store, _contract(SafetyClass.IDEMPOTENT), target)

    await broker.submit("tenant-a", _request(amount=100))
    with pytest.raises(PayloadConflictError):
        await broker.submit("tenant-a", _request(amount=101))

    assert await store.count() == 1
    assert target.dispatch_count == 0


async def test_version_cas_rejects_stale_transition():
    store = InMemoryStore()
    broker = _broker(store, _contract(SafetyClass.IDEMPOTENT))
    reservation = await broker.submit("tenant-a", _request())

    current = await store.transition(
        reservation.effect.effect_id,
        expected_version=reservation.effect.version,
        target=EffectStatus.DISPATCHING,
        data={},
    )

    with pytest.raises(VersionConflictError):
        await store.transition(
            current.effect_id,
            expected_version=reservation.effect.version,
            target=EffectStatus.SUCCEEDED,
            data={},
        )
