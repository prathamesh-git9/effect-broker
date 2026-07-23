from __future__ import annotations

import asyncio
from datetime import timedelta

import pytest

pytest.importorskip("fastapi")

from fastapi.testclient import TestClient

from effect_broker.adapters.simulated import SimulatedAdapter, SimulatedTarget
from effect_broker.api import create_app
from effect_broker.auth import APIKeyAuthenticator, hash_api_key
from effect_broker.contracts import ContractRegistry
from effect_broker.engine import EffectBroker
from effect_broker.models import (
    EffectContract,
    EffectRecord,
    EffectStatus,
    SafetyClass,
)
from effect_broker.store.memory import InMemoryStore

API_KEY = "test-api-key"
TENANT_ID = "tenant-a"


@pytest.fixture()
def store() -> InMemoryStore:
    return InMemoryStore()


@pytest.fixture()
def client(store: InMemoryStore) -> TestClient:
    target = SimulatedTarget()

    def adapter_for(effect: EffectRecord) -> SimulatedAdapter:
        return SimulatedAdapter(target, effect.contract)

    broker = EffectBroker(store, _registry(), adapter_for)
    authenticator = APIKeyAuthenticator({hash_api_key(API_KEY): TENANT_ID})
    return TestClient(create_app(broker, authenticator))


def _headers(key: str = API_KEY) -> dict[str, str]:
    return {"X-API-Key": key}


def _registry() -> ContractRegistry:
    return ContractRegistry(
        {
            "charge": EffectContract(
                name="charge",
                version="v1",
                safety=SafetyClass.IDEMPOTENT,
                retry_limit=3,
                key_retention=timedelta(hours=1),
            ),
            "unsafe_charge": EffectContract(
                name="unsafe_charge",
                version="v1",
                safety=SafetyClass.UNSAFE,
                retry_limit=0,
            ),
        }
    )


def _body(operation_key: str = "order:42:charge:v1", amount: int = 100):
    return {
        "operation_key": operation_key,
        "tool": "charge",
        "arguments": {"amount": amount, "currency": "usd"},
        "requested_by": "test",
    }


def test_healthz(client: TestClient) -> None:
    response = client.get("/healthz")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_submit_201_and_dedup_replay(client: TestClient) -> None:
    first = client.post("/effects", json=_body(), headers=_headers())
    second = client.post("/effects", json=_body(), headers=_headers())

    assert first.status_code == 201
    assert second.status_code == 201
    assert first.json()["created"] is True
    assert second.json()["replayed"] is True
    assert second.json()["effect_id"] == first.json()["effect_id"]


def test_payload_conflict_is_409(client: TestClient) -> None:
    created = client.post("/effects", json=_body(amount=100), headers=_headers())
    conflict = client.post("/effects", json=_body(amount=101), headers=_headers())

    assert created.status_code == 201
    assert conflict.status_code == 409
    assert conflict.json()["title"] == "Payload conflict"


def test_unknown_tool_is_422(client: TestClient) -> None:
    body = _body()
    body["tool"] = "missing"
    response = client.post("/effects", json=body, headers=_headers())

    assert response.status_code == 422


def test_get_200_and_404(client: TestClient) -> None:
    created = client.post("/effects", json=_body(), headers=_headers()).json()
    found = client.get(f"/effects/{created['effect_id']}", headers=_headers())
    missing = client.get("/effects/not-real", headers=_headers())

    assert found.status_code == 200
    assert found.json()["status"] == EffectStatus.PREPARED.value
    assert missing.status_code == 404


def test_list_by_status(client: TestClient) -> None:
    client.post("/effects", json=_body("order:1:charge:v1"), headers=_headers())
    client.post("/effects", json=_body("order:2:charge:v1"), headers=_headers())

    response = client.get(
        "/effects",
        params={"status": EffectStatus.PREPARED.value},
        headers=_headers(),
    )

    assert response.status_code == 200
    assert len(response.json()["items"]) == 2


def test_cancel_prepared_effect_and_reject_stale_or_post_dispatch_cancel(
    client: TestClient,
    store: InMemoryStore,
) -> None:
    created = client.post("/effects", json=_body(), headers=_headers()).json()
    cancelled = client.post(
        f"/effects/{created['effect_id']}/cancel",
        json={"expected_version": 0, "actor": "ops", "reason": "customer withdrew"},
        headers=_headers(),
    )
    assert cancelled.status_code == 200
    assert cancelled.json()["status"] == EffectStatus.CANCELLED.value

    stale = client.post(
        f"/effects/{created['effect_id']}/cancel",
        json={"expected_version": 0, "actor": "ops", "reason": "again"},
        headers=_headers(),
    )
    assert stale.status_code == 409

    other = client.post(
        "/effects",
        json=_body("order:43:charge:v1"),
        headers=_headers(),
    ).json()
    effect = asyncio.run(async_get(store, other["effect_id"]))
    asyncio.run(
        store.transition(
            effect.effect_id,
            expected_version=effect.version,
            target=EffectStatus.DISPATCHING,
            data={},
        )
    )
    too_late = client.post(
        f"/effects/{other['effect_id']}/cancel",
        json={"expected_version": 1, "actor": "ops", "reason": "too late"},
        headers=_headers(),
    )
    assert too_late.status_code == 409


def test_reconcile_and_resolve_manual_review(
    client: TestClient,
    store: InMemoryStore,
) -> None:
    body = _body("order:unsafe:charge:v1")
    body["tool"] = "unsafe_charge"
    created = client.post("/effects", json=body, headers=_headers()).json()
    effect = asyncio.run(async_get(store, created["effect_id"]))

    dispatching = asyncio.run(
        store.transition(
            effect.effect_id,
            expected_version=effect.version,
            target=EffectStatus.DISPATCHING,
            data={},
        )
    )
    asyncio.run(
        store.transition(
            dispatching.effect_id,
            expected_version=dispatching.version,
            target=EffectStatus.OUTCOME_UNKNOWN,
            data={"reason": "test"},
        )
    )

    reconciled = client.post(
        f"/effects/{created['effect_id']}/reconcile",
        headers=_headers(),
    )
    assert reconciled.status_code == 200
    assert reconciled.json()["status"] == EffectStatus.MANUAL_REVIEW.value

    resolved = client.post(
        f"/effects/{created['effect_id']}/resolve",
        json={"resolution": "succeeded", "evidence": {"ticket": "ops-1"}},
        headers=_headers(),
    )
    assert resolved.status_code == 200
    assert resolved.json()["status"] == EffectStatus.SUCCEEDED.value

    receipt = client.get(
        f"/effects/{created['effect_id']}/receipt",
        headers=_headers(),
    )
    assert receipt.status_code == 200
    assert receipt.json()["metadata"]["evidence"]["ticket"] == "ops-1"


def test_receipt_404_when_none(client: TestClient) -> None:
    created = client.post("/effects", json=_body(), headers=_headers()).json()
    response = client.get(
        f"/effects/{created['effect_id']}/receipt",
        headers=_headers(),
    )
    assert response.status_code == 404


def test_unauthorized_missing_and_bad_key(client: TestClient) -> None:
    missing = client.post("/effects", json=_body())
    bad = client.post("/effects", json=_body(), headers=_headers("bad"))

    assert missing.status_code == 401
    assert bad.status_code == 401


async def async_get(store: InMemoryStore, effect_id: str):
    effect = await store.get(TENANT_ID, effect_id)
    assert effect is not None
    return effect
