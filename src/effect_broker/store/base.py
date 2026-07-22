"""Persistence contract for the effect ledger.

The store is the broker's local authority, but only for local facts: intent,
versions, attempts, receipts, and transition history. It never upgrades an
ambiguous remote call into success. Every transition is a compare-and-swap so a
paused worker can finish network I/O later without overwriting newer evidence.
"""

from __future__ import annotations

import hashlib
import hmac
from collections.abc import Callable
from datetime import datetime, timedelta
from typing import Protocol

from effect_broker.models import (
    EffectContract,
    EffectRecord,
    EffectRequest,
    EffectStatus,
    JsonObject,
    Reservation,
)

TenantKeyProvider = Callable[[str], bytes]


def tenant_key_provider_from_secret(
    broker_secret: bytes,
) -> TenantKeyProvider:
    """Derive deterministic per-tenant HMAC keys from one broker secret.

    The memory store uses this as a convenient default for tests. Production
    stores should load explicit tenant keys, but the derivation keeps the
    downstream key stable across process restarts in local conformance tests.
    """

    def provide(tenant_id: str) -> bytes:
        return hmac.new(
            broker_secret,
            tenant_id.encode("utf-8"),
            hashlib.sha256,
        ).digest()

    return provide


class EffectStore(Protocol):
    async def reserve(
        self,
        tenant_id: str,
        request: EffectRequest,
        contract: EffectContract,
    ) -> Reservation: ...

    async def claim_due(
        self,
        worker_id: str,
        *,
        now: datetime,
        lease_for: timedelta,
        limit: int,
    ) -> list[EffectRecord]: ...

    async def start_attempt(
        self,
        effect_id: str,
        *,
        expected_version: int,
        worker_id: str,
    ) -> str: ...

    async def transition(
        self,
        effect_id: str,
        *,
        expected_version: int,
        target: EffectStatus,
        data: JsonObject,
    ) -> EffectRecord: ...

    async def get(self, tenant_id: str, effect_id: str) -> EffectRecord | None: ...

    async def list(
        self,
        tenant_id: str,
        *,
        status: EffectStatus | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[EffectRecord]: ...

    async def receipt(
        self,
        tenant_id: str,
        effect_id: str,
    ) -> JsonObject | None: ...
