"""Small public service over the store and contract registry."""

from __future__ import annotations

from collections.abc import Callable

from effect_broker.adapters.base import EffectAdapter
from effect_broker.contracts import ContractRegistry
from effect_broker.errors import UnknownEffectError
from effect_broker.models import EffectRecord, EffectRequest, Reservation
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

    async def submit(
        self,
        tenant_id: str,
        request: EffectRequest,
    ) -> Reservation:
        contract = self._contracts.get(request.tool)
        return await self._store.reserve(tenant_id, request, contract)

    async def get(self, tenant_id: str, effect_id: str) -> EffectRecord:
        effect = await self._store.get(tenant_id, effect_id)
        if effect is None:
            raise UnknownEffectError(effect_id)
        return effect

    async def reconcile(self, tenant_id: str, effect_id: str) -> EffectRecord:
        if self._adapter_for is None:
            raise RuntimeError("EffectBroker.reconcile requires adapter_for")
        effect = await self.get(tenant_id, effect_id)
        return await reconcile_once(self._store, self._adapter_for, effect=effect)
