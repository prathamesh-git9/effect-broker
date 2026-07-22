"""Small public service over the store and contract registry."""

from __future__ import annotations

from collections.abc import Callable

from effect_broker.adapters.base import EffectAdapter
from effect_broker.contracts import ContractRegistry
from effect_broker.errors import (
    InvalidTransitionError,
    PayloadConflictError,
    UnknownEffectError,
)
from effect_broker.models import (
    EffectRecord,
    EffectRequest,
    EffectStatus,
    JsonObject,
    Reservation,
)
from effect_broker.observability import (
    observe_latency,
    record_conflict,
    record_submit,
)
from effect_broker.reconcile import reconcile_once
from effect_broker.store.base import EffectStore

AdapterFor = Callable[[EffectRecord], EffectAdapter]


class EffectBroker:
    """Reserve, inspect, and reconcile effects without owning dispatch policy."""

    def __init__(
        self,
        store: EffectStore,
        contracts: ContractRegistry,
        adapter_for: AdapterFor | None = None,
    ) -> None:
        self._store = store
        self._contracts = contracts
        self._adapter_for = adapter_for

    @property
    def store(self) -> EffectStore:
        """Expose the ledger to process runners without changing dispatch policy."""
        return self._store

    async def submit(
        self,
        tenant_id: str,
        request: EffectRequest,
    ) -> Reservation:
        contract = self._contracts.get(request.tool)
        with observe_latency("reserve"):
            try:
                reservation = await self._store.reserve(tenant_id, request, contract)
            except PayloadConflictError:
                record_conflict()
                raise
        record_submit(reservation)
        return reservation

    async def get(self, tenant_id: str, effect_id: str) -> EffectRecord:
        effect = await self._store.get(tenant_id, effect_id)
        if effect is None:
            raise UnknownEffectError(effect_id)
        return effect

    async def list(
        self,
        tenant_id: str,
        *,
        status: EffectStatus | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[EffectRecord]:
        return await self._store.list(
            tenant_id,
            status=status,
            limit=limit,
            offset=offset,
        )

    async def replay_receipt(
        self,
        tenant_id: str,
        effect_id: str,
    ) -> JsonObject:
        receipt = await self._store.receipt(tenant_id, effect_id)
        if receipt is None:
            raise UnknownEffectError(effect_id)
        return receipt

    async def reconcile(self, tenant_id: str, effect_id: str) -> EffectRecord:
        if self._adapter_for is None:
            raise RuntimeError("EffectBroker.reconcile requires adapter_for")
        effect = await self.get(tenant_id, effect_id)
        return await reconcile_once(self._store, self._adapter_for, effect=effect)

    async def resolve(
        self,
        tenant_id: str,
        effect_id: str,
        *,
        resolution: EffectStatus,
        evidence: JsonObject,
    ) -> EffectRecord:
        effect = await self.get(tenant_id, effect_id)
        if effect.status is not EffectStatus.MANUAL_REVIEW:
            raise InvalidTransitionError(effect.status, resolution)
        if resolution not in {
            EffectStatus.SUCCEEDED,
            EffectStatus.FAILED_FINAL,
            EffectStatus.COMPENSATED,
        }:
            raise InvalidTransitionError(effect.status, resolution)
        return await self._store.transition(
            effect.effect_id,
            expected_version=effect.version,
            target=resolution,
            data={"evidence": dict(evidence), "actor": "operator"},
        )
