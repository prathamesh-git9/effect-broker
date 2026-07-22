from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

pytest.importorskip("fastapi")

from fastapi.testclient import TestClient

from effect_broker import observability
from effect_broker.adapters.simulated import SimulatedAdapter, SimulatedTarget
from effect_broker.api import create_app
from effect_broker.auth import APIKeyAuthenticator, hash_api_key
from effect_broker.contracts import ContractRegistry
from effect_broker.engine import EffectBroker
from effect_broker.errors import PayloadConflictError
from effect_broker.models import (
    DispatchResult,
    EffectContract,
    EffectRecord,
    EffectRequest,
    SafetyClass,
)
from effect_broker.store.memory import InMemoryStore
from effect_broker.worker import dispatch_once

TENANT_ID = "tenant-secret-id"
API_KEY = "observability-api-key"
RAW_OPERATION_KEY = "order:raw-operation-key:charge:v1"
RAW_ARGUMENT = "super-secret-card-token"


@pytest.fixture(autouse=True)
def reset_observability() -> Iterator[None]:
    observability.reset_for_tests()
    yield
    observability.reset_for_tests()


def _contract() -> EffectContract:
    return EffectContract(
        name="charge",
        version="v1",
        safety=SafetyClass.IDEMPOTENT,
        retry_limit=3,
        key_retention=timedelta(hours=1),
    )


def _registry() -> ContractRegistry:
    return ContractRegistry({"charge": _contract()})


def _request(
    operation_key: str = RAW_OPERATION_KEY,
    *,
    amount: int = 100,
) -> EffectRequest:
    return EffectRequest(
        operation_key=operation_key,
        tool="charge",
        arguments={"amount": amount, "token": RAW_ARGUMENT},
        requested_by="test",
    )


async def test_metrics_move_on_submit_dedup_conflict_success_and_unknown() -> None:
    store = InMemoryStore()
    success_target = SimulatedTarget()

    def success_adapter(effect: EffectRecord) -> SimulatedAdapter:
        return SimulatedAdapter(success_target, effect.contract)

    broker = EffectBroker(store, _registry(), success_adapter)
    await broker.submit(TENANT_ID, _request())
    await broker.submit(TENANT_ID, _request())
    with pytest.raises(PayloadConflictError):
        await broker.submit(TENANT_ID, _request(amount=101))

    succeeded = await dispatch_once(
        store,
        success_adapter,
        worker_id="worker-1",
        now=datetime.now(UTC),
        lease_for=timedelta(seconds=30),
        limit=10,
    )
    assert succeeded[0].status.value == "succeeded"

    unknown_store = InMemoryStore()
    unknown_broker = EffectBroker(unknown_store, _registry(), _not_confirmed_adapter)
    await unknown_broker.submit(TENANT_ID, _request("order:unknown:charge:v1"))
    unknown = await dispatch_once(
        unknown_store,
        _not_confirmed_adapter,
        worker_id="worker-2",
        now=datetime.now(UTC),
        lease_for=timedelta(seconds=30),
        limit=10,
    )
    assert unknown[0].status.value == "outcome_unknown"

    rendered = observability.metrics_text().decode("utf-8")
    assert 'effects_total{result="submitted"} 2.0' in rendered
    assert 'effects_total{result="deduplicated"} 1.0' in rendered
    assert 'effects_total{result="conflicted"} 1.0' in rendered
    assert 'effects_total{result="succeeded"} 1.0' in rendered
    assert 'effects_total{result="unknown"} 1.0' in rendered
    assert 'outcome_unknown_effects{contract="charge",safety="idempotent"} 1.0' in (
        rendered
    )


async def test_metrics_text_renders_expected_metric_names() -> None:
    store = InMemoryStore()
    broker = EffectBroker(store, _registry())
    await broker.submit(TENANT_ID, _request())

    rendered = observability.metrics_text().decode("utf-8")

    for name in (
        "effects_total",
        "outcome_unknown_effects",
        "outcome_unknown_oldest_age_seconds",
        "dispatch_attempts_total",
        "reconciliation_outcomes_total",
        "lease_expirations_total",
        "fenced_stale_writes_total",
        "payload_conflicts_total",
        "op_latency_seconds",
        "idempotency_retention_remaining_seconds",
    ):
        assert name in rendered


async def test_metrics_and_span_attributes_redact_raw_values(monkeypatch) -> None:
    store = InMemoryStore()
    broker = EffectBroker(store, _registry())
    reservation = await broker.submit(TENANT_ID, _request())
    captured: list[dict[str, Any]] = []

    class FakeSpan:
        def __init__(self) -> None:
            self.attributes: dict[str, Any] = {}

        def set_attribute(self, key: str, value: Any) -> None:
            self.attributes[key] = value

    class FakeTracer:
        @contextmanager
        def start_as_current_span(self, _name: str) -> Iterator[FakeSpan]:
            span = FakeSpan()
            captured.append(span.attributes)
            yield span

    monkeypatch.setattr(observability, "_get_tracer", lambda: FakeTracer())

    with observability.span("submit", reservation.effect):
        pass

    rendered = observability.metrics_text().decode("utf-8")
    attributes_text = repr(captured)
    forbidden = (RAW_OPERATION_KEY, RAW_ARGUMENT, TENANT_ID)
    for value in forbidden:
        assert value not in rendered
        assert value not in attributes_text
    assert observability.operation_key_digest(RAW_OPERATION_KEY) in attributes_text


def test_metrics_endpoint_returns_prometheus_text() -> None:
    store = InMemoryStore()
    broker = EffectBroker(store, _registry())
    authenticator = APIKeyAuthenticator({hash_api_key(API_KEY): TENANT_ID})
    client = TestClient(create_app(broker, authenticator))

    response = client.get("/metrics")

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/plain")
    assert b"effects_total" in response.content


class _NotConfirmedAdapter:
    contract: EffectContract

    def __init__(self, contract: EffectContract) -> None:
        self.contract = contract

    async def dispatch(
        self,
        effect: EffectRecord,
        *,
        attempt_id: str,
    ) -> DispatchResult:
        del effect, attempt_id
        return DispatchResult(external_id=None, output={}, committed=False)

    async def probe(self, effect: EffectRecord):
        raise AssertionError(f"probe should not run for {effect.effect_id}")


def _not_confirmed_adapter(effect: EffectRecord) -> _NotConfirmedAdapter:
    return _NotConfirmedAdapter(effect.contract)
