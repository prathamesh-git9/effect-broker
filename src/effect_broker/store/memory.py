"""Deterministic in-memory store for tests only.

Postgres is the sole production store for this project. This implementation is
not durable, does not survive process death, and exists only to exercise the
engine, worker, reconciler, and adapter contracts offline. It still preserves
the important correctness mechanics: payload binding, per-tenant downstream
keys, compare-and-swap transitions, leases, attempt rows, and append-only events.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta

from effect_broker.canonical import downstream_key, request_hash
from effect_broker.errors import (
    PayloadConflictError,
    UnknownEffectError,
    VersionConflictError,
)
from effect_broker.models import (
    EffectContract,
    EffectRecord,
    EffectRequest,
    EffectStatus,
    JsonObject,
    Reservation,
)
from effect_broker.statemachine import assert_transition
from effect_broker.store.base import (
    EffectStore,
    TenantKeyProvider,
    tenant_key_provider_from_secret,
)

_DEFAULT_BROKER_SECRET = b"effect-broker-memory-store-test-secret"


@dataclass(frozen=True, slots=True)
class MemoryEvent:
    """A compact append-only fact used by tests to inspect store history."""

    effect_id: str
    sequence: int
    event_type: str
    version: int
    at: datetime
    data: JsonObject


@dataclass(frozen=True, slots=True)
class _Lease:
    worker_id: str
    expires_at: datetime


@dataclass(frozen=True, slots=True)
class _Attempt:
    attempt_id: str
    effect_id: str
    ordinal: int
    worker_id: str
    version: int
    started_at: datetime


class InMemoryStore(EffectStore):
    """Async, lock-guarded fake store with production-like fencing semantics."""

    def __init__(
        self,
        tenant_key_provider: TenantKeyProvider | None = None,
        *,
        broker_secret: bytes = _DEFAULT_BROKER_SECRET,
    ) -> None:
        self._tenant_key_provider = (
            tenant_key_provider or tenant_key_provider_from_secret(broker_secret)
        )
        self._lock = asyncio.Lock()
        self._records: dict[str, EffectRecord] = {}
        self._identity: dict[tuple[str, str], str] = {}
        self._leases: dict[str, _Lease] = {}
        self._attempts: dict[str, list[_Attempt]] = {}
        self._events: dict[str, list[MemoryEvent]] = {}
        self._next_effect_id = 1

    async def reserve(
        self,
        tenant_id: str,
        request: EffectRequest,
        contract: EffectContract,
    ) -> Reservation:
        digest = request_hash(
            request.tool,
            request.arguments,
            contract.name,
            contract.version,
        )
        identity = (tenant_id, request.operation_key)
        async with self._lock:
            existing_id = self._identity.get(identity)
            if existing_id is not None:
                existing = self._records[existing_id]
                if existing.request_hash != digest:
                    raise PayloadConflictError(
                        request.operation_key,
                        existing.request_hash,
                        digest,
                    )
                return Reservation(existing, created=False, replayed=True)

            now = datetime.now(UTC)
            effect_id = f"eff_{self._next_effect_id:06d}"
            self._next_effect_id += 1
            record = EffectRecord(
                effect_id=effect_id,
                tenant_id=tenant_id,
                request=request,
                request_hash=digest,
                downstream_key=downstream_key(
                    self._tenant_key_provider(tenant_id),
                    tenant_id,
                    request.operation_key,
                ),
                contract=contract,
                status=EffectStatus.PREPARED,
                version=0,
                created_at=now,
                updated_at=now,
            )
            self._identity[identity] = effect_id
            self._records[effect_id] = record
            self._attempts[effect_id] = []
            self._events[effect_id] = []
            self._append_event_locked(
                effect_id,
                "reserved",
                record.version,
                {"status": record.status.value},
                at=now,
            )
            return Reservation(record, created=True, replayed=False)

    async def claim_due(
        self,
        worker_id: str,
        *,
        now: datetime,
        lease_for: timedelta,
        limit: int,
    ) -> list[EffectRecord]:
        if limit <= 0:
            return []
        claimed: list[EffectRecord] = []
        async with self._lock:
            for effect_id, record in self._records.items():
                if len(claimed) >= limit:
                    break
                if record.status not in {
                    EffectStatus.PREPARED,
                    EffectStatus.RETRYABLE,
                }:
                    continue
                lease = self._leases.get(effect_id)
                if lease is not None and lease.expires_at > now:
                    continue
                updated = replace(
                    record,
                    version=record.version + 1,
                    updated_at=now,
                )
                self._records[effect_id] = updated
                self._leases[effect_id] = _Lease(
                    worker_id=worker_id,
                    expires_at=now + lease_for,
                )
                self._append_event_locked(
                    effect_id,
                    "claimed",
                    updated.version,
                    {
                        "worker_id": worker_id,
                        "lease_expires_at": self._format_time(now + lease_for),
                    },
                    at=now,
                )
                claimed.append(updated)
        return claimed

    async def start_attempt(
        self,
        effect_id: str,
        *,
        expected_version: int,
        worker_id: str,
    ) -> str:
        async with self._lock:
            record = self._record_for_update_locked(effect_id, expected_version)
            attempts = self._attempts[effect_id]
            ordinal = len(attempts) + 1
            attempt_id = f"{effect_id}-attempt-{ordinal}"
            attempt = _Attempt(
                attempt_id=attempt_id,
                effect_id=effect_id,
                ordinal=ordinal,
                worker_id=worker_id,
                version=expected_version,
                started_at=datetime.now(UTC),
            )
            attempts.append(attempt)
            self._append_event_locked(
                effect_id,
                "attempt_started",
                record.version,
                {
                    "attempt_id": attempt_id,
                    "ordinal": ordinal,
                    "worker_id": worker_id,
                },
                at=attempt.started_at,
            )
            return attempt_id

    async def transition(
        self,
        effect_id: str,
        *,
        expected_version: int,
        target: EffectStatus,
        data: JsonObject,
    ) -> EffectRecord:
        async with self._lock:
            record = self._record_for_update_locked(effect_id, expected_version)
            assert_transition(record.status, target)
            now = datetime.now(UTC)
            updated = replace(
                record,
                status=target,
                version=record.version + 1,
                updated_at=now,
            )
            self._records[effect_id] = updated
            if target not in {EffectStatus.PREPARED, EffectStatus.RETRYABLE}:
                self._leases.pop(effect_id, None)
            self._append_event_locked(
                effect_id,
                "transition",
                updated.version,
                {
                    "from": record.status.value,
                    "to": target.value,
                    "metadata": dict(data),
                },
                at=now,
            )
            return updated

    async def get(self, tenant_id: str, effect_id: str) -> EffectRecord | None:
        async with self._lock:
            record = self._records.get(effect_id)
            if record is None or record.tenant_id != tenant_id:
                return None
            return record

    async def list(
        self,
        tenant_id: str,
        *,
        status: EffectStatus | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[EffectRecord]:
        async with self._lock:
            records = [
                record
                for record in self._records.values()
                if record.tenant_id == tenant_id
                and (status is None or record.status is status)
            ]
            records.sort(key=lambda record: (record.created_at, record.effect_id))
            return records[offset : offset + limit]

    async def receipt(
        self,
        tenant_id: str,
        effect_id: str,
    ) -> JsonObject | None:
        async with self._lock:
            record = self._records.get(effect_id)
            if record is None or record.tenant_id != tenant_id:
                return None
            for event in reversed(self._events.get(effect_id, [])):
                if event.event_type != "transition":
                    continue
                if event.data.get("to") != EffectStatus.SUCCEEDED.value:
                    continue
                metadata = event.data.get("metadata", {})
                return {
                    "effect_id": effect_id,
                    "status": EffectStatus.SUCCEEDED.value,
                    "contract": record.contract.name,
                    "contract_version": record.contract.version,
                    "downstream_key": record.downstream_key,
                    "metadata": dict(metadata),
                    "recorded_at": self._format_time(event.at),
                }
            return None

    async def events(self, effect_id: str) -> list[MemoryEvent]:
        """Return a copy of an effect's event stream for offline assertions."""
        async with self._lock:
            return list(self._events.get(effect_id, []))

    async def attempt_count(self, effect_id: str) -> int:
        """Return the number of local attempt rows for tests and diagnostics."""
        async with self._lock:
            return len(self._attempts.get(effect_id, []))

    async def count(self) -> int:
        """Return the number of reserved effects."""
        async with self._lock:
            return len(self._records)

    def _record_for_update_locked(
        self,
        effect_id: str,
        expected_version: int,
    ) -> EffectRecord:
        try:
            record = self._records[effect_id]
        except KeyError as exc:
            raise UnknownEffectError(effect_id) from exc
        if record.version != expected_version:
            raise VersionConflictError(
                f"{effect_id} expected version {expected_version}, "
                f"found {record.version}"
            )
        return record

    def _append_event_locked(
        self,
        effect_id: str,
        event_type: str,
        version: int,
        data: JsonObject,
        *,
        at: datetime,
    ) -> None:
        events = self._events[effect_id]
        events.append(
            MemoryEvent(
                effect_id=effect_id,
                sequence=len(events) + 1,
                event_type=event_type,
                version=version,
                at=at,
                data=data,
            )
        )

    @staticmethod
    def _format_time(value: datetime) -> str:
        return value.isoformat()
