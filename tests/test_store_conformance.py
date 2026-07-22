from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime, timedelta

import pytest

from effect_broker.errors import (
    InvalidTransitionError,
    PayloadConflictError,
    VersionConflictError,
)
from effect_broker.models import (
    EffectContract,
    EffectRequest,
    EffectStatus,
    SafetyClass,
)
from effect_broker.store.memory import InMemoryStore
from effect_broker.store.sqlite import SqliteStore


def _request(
    operation_key: str = "order:42:charge:v1",
    *,
    amount: int = 100,
) -> EffectRequest:
    return EffectRequest(
        operation_key=operation_key,
        tool="charge",
        arguments={"amount": amount, "currency": "usd"},
        requested_by="test",
    )


def _contract() -> EffectContract:
    return EffectContract(
        name="charge",
        version="v1",
        safety=SafetyClass.IDEMPOTENT,
        retry_limit=3,
        key_retention=timedelta(hours=1),
    )


@pytest.fixture(params=["memory", "sqlite"])
def store(request: pytest.FixtureRequest, tmp_path) -> Iterator[object]:
    if request.param == "memory":
        yield InMemoryStore()
        return

    sqlite = SqliteStore.open(tmp_path / "broker.sqlite3", b"test-secret")
    try:
        yield sqlite
    finally:
        sqlite.close()


async def test_reservation_deduplicates_same_payload(store) -> None:
    first = await store.reserve("tenant-a", _request(), _contract())
    second = await store.reserve("tenant-a", _request(), _contract())

    assert first.created
    assert not first.replayed
    assert not second.created
    assert second.replayed
    assert second.effect.effect_id == first.effect.effect_id
    assert await store.count() == 1


async def test_reservation_rejects_payload_conflict(store) -> None:
    await store.reserve("tenant-a", _request(amount=100), _contract())

    with pytest.raises(PayloadConflictError):
        await store.reserve("tenant-a", _request(amount=101), _contract())

    assert await store.count() == 1


async def test_transition_is_version_fenced(store) -> None:
    reservation = await store.reserve("tenant-a", _request(), _contract())
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


async def test_claim_due_honors_lease_expiry(store) -> None:
    reservation = await store.reserve("tenant-a", _request(), _contract())
    now = datetime.now(UTC)

    first = await store.claim_due(
        "worker-1",
        now=now,
        lease_for=timedelta(seconds=30),
        limit=10,
    )
    second = await store.claim_due(
        "worker-2",
        now=now + timedelta(seconds=1),
        lease_for=timedelta(seconds=30),
        limit=10,
    )
    third = await store.claim_due(
        "worker-3",
        now=now + timedelta(seconds=31),
        lease_for=timedelta(seconds=30),
        limit=10,
    )

    assert [effect.effect_id for effect in first] == [reservation.effect.effect_id]
    assert second == []
    assert [effect.effect_id for effect in third] == [reservation.effect.effect_id]
    assert first[0].version == 1
    assert third[0].version == 2


async def test_forbidden_transition_is_rejected(store) -> None:
    reservation = await store.reserve("tenant-a", _request(), _contract())

    with pytest.raises(InvalidTransitionError):
        await store.transition(
            reservation.effect.effect_id,
            expected_version=reservation.effect.version,
            target=EffectStatus.SUCCEEDED,
            data={},
        )
