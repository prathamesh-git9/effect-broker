"""SQLite store used by the proof-of-concept crash matrix.

This is a durable, file-backed implementation of the same store contract as
``InMemoryStore``. It is deliberately small and explicit: SQLite serializes
writers, so ``claim_due`` uses a short ``BEGIN IMMEDIATE`` transaction to avoid
lost updates while claiming a batch. The Postgres store should use
``FOR UPDATE SKIP LOCKED`` instead; this module exists to prove process-crash
semantics without requiring an external service.
"""

from __future__ import annotations

import json
import sqlite3
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Self

from effect_broker.canonical import canonical_json, downstream_key, request_hash
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
    SafetyClass,
)
from effect_broker.statemachine import assert_transition
from effect_broker.store.base import (
    EffectStore,
    TenantKeyProvider,
    tenant_key_provider_from_secret,
)

_DEFAULT_BUSY_TIMEOUT_MS = 5_000


@dataclass(frozen=True, slots=True)
class SqliteEvent:
    """A persisted append-only fact used by crash-matrix assertions."""

    effect_id: str
    sequence: int
    event_type: str
    version: int
    at: datetime
    data: JsonObject


class SqliteStore(EffectStore):
    """Durable SQLite ledger with production-like fencing semantics."""

    def __init__(
        self,
        connection: sqlite3.Connection,
        tenant_key_provider: TenantKeyProvider,
    ) -> None:
        self._conn = connection
        self._tenant_key_provider = tenant_key_provider

    @classmethod
    def open(
        cls,
        path: str | Path,
        tenant_secret: bytes,
        *,
        busy_timeout_ms: int = _DEFAULT_BUSY_TIMEOUT_MS,
    ) -> Self:
        db_path = Path(path)
        db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(
            db_path,
            timeout=busy_timeout_ms / 1000,
            isolation_level=None,
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute(f"PRAGMA busy_timeout={busy_timeout_ms}")
        store = cls(conn, tenant_key_provider_from_secret(tenant_secret))
        store._create_schema()
        return store

    def close(self) -> None:
        self._conn.close()

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
        now = datetime.now(UTC)
        effect_id = f"eff_{uuid.uuid4().hex}"
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

        with self._transaction():
            try:
                self._conn.execute(
                    """
                    INSERT INTO effects (
                        effect_id, tenant_id, operation_key, tool,
                        arguments_json, requested_by, trace_id, request_hash,
                        downstream_key, contract_name, contract_version,
                        safety_class, retry_limit, key_retention_seconds,
                        settlement_bound_seconds, status, version, created_at,
                        updated_at, lease_worker_id, lease_until
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                            ?, ?, NULL, NULL)
                    """,
                    self._record_params(record),
                )
            except sqlite3.IntegrityError as exc:
                existing = self._select_by_identity(
                    tenant_id,
                    request.operation_key,
                )
                if existing is None:
                    raise
                if existing.request_hash != digest:
                    raise PayloadConflictError(
                        request.operation_key,
                        existing.request_hash,
                        digest,
                    ) from exc
                return Reservation(existing, created=False, replayed=True)
            self._append_event(
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
        lease_until = now + lease_for
        with self._transaction():
            rows = self._conn.execute(
                """
                SELECT *
                FROM effects
                WHERE status IN (?, ?)
                ORDER BY created_at, effect_id
                """,
                (EffectStatus.PREPARED.value, EffectStatus.RETRYABLE.value),
            ).fetchall()
            for row in rows:
                if len(claimed) >= limit:
                    break
                lease_text = row["lease_until"]
                if lease_text is not None and _parse_time(lease_text) > now:
                    continue
                self._conn.execute(
                    """
                    UPDATE effects
                    SET worker_id = ?,
                        lease_worker_id = ?,
                        lease_until = ?,
                        version = version + 1,
                        updated_at = ?
                    WHERE effect_id = ?
                    """,
                    (
                        worker_id,
                        worker_id,
                        _format_time(lease_until),
                        _format_time(now),
                        row["effect_id"],
                    ),
                )
                updated = self._select_by_effect_id(row["effect_id"])
                if updated is None:
                    raise UnknownEffectError(row["effect_id"])
                self._append_event(
                    updated.effect_id,
                    "claimed",
                    updated.version,
                    {
                        "worker_id": worker_id,
                        "lease_expires_at": _format_time(lease_until),
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
        with self._transaction():
            record = self._record_for_expected_version(effect_id, expected_version)
            ordinal = int(
                self._conn.execute(
                    "SELECT COUNT(*) FROM attempts WHERE effect_id = ?",
                    (effect_id,),
                ).fetchone()[0]
            ) + 1
            attempt_id = f"{effect_id}-attempt-{ordinal}"
            now = datetime.now(UTC)
            self._conn.execute(
                """
                INSERT INTO attempts (
                    attempt_id, effect_id, ordinal, worker_id, version,
                    started_at
                )
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    attempt_id,
                    effect_id,
                    ordinal,
                    worker_id,
                    expected_version,
                    _format_time(now),
                ),
            )
            self._append_event(
                effect_id,
                "attempt_started",
                record.version,
                {
                    "attempt_id": attempt_id,
                    "ordinal": ordinal,
                    "worker_id": worker_id,
                },
                at=now,
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
        with self._transaction():
            record = self._record_for_expected_version(effect_id, expected_version)
            assert_transition(record.status, target)
            now = datetime.now(UTC)
            cursor = self._conn.execute(
                """
                UPDATE effects
                SET status = ?,
                    version = version + 1,
                    updated_at = ?,
                    worker_id = NULL,
                    lease_worker_id = NULL,
                    lease_until = ?
                WHERE effect_id = ? AND version = ?
                """,
                (
                    target.value,
                    _format_time(now),
                    None,
                    effect_id,
                    expected_version,
                ),
            )
            if cursor.rowcount == 0:
                raise VersionConflictError(
                    f"{effect_id} expected version {expected_version}"
                )
            updated = self._select_by_effect_id(effect_id)
            if updated is None:
                raise UnknownEffectError(effect_id)
            self._append_event(
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
        row = self._conn.execute(
            "SELECT * FROM effects WHERE tenant_id = ? AND effect_id = ?",
            (tenant_id, effect_id),
        ).fetchone()
        return None if row is None else _row_to_record(row)

    async def list(
        self,
        tenant_id: str,
        *,
        status: EffectStatus | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[EffectRecord]:
        params: list[Any] = [tenant_id]
        where = "tenant_id = ?"
        if status is not None:
            where = f"{where} AND status = ?"
            params.append(status.value)
        params.extend([limit, offset])
        rows = self._conn.execute(
            f"""
            SELECT *
            FROM effects
            WHERE {where}
            ORDER BY created_at, effect_id
            LIMIT ? OFFSET ?
            """,
            params,
        ).fetchall()
        return [_row_to_record(row) for row in rows]

    async def receipt(
        self,
        tenant_id: str,
        effect_id: str,
    ) -> JsonObject | None:
        record = await self.get(tenant_id, effect_id)
        if record is None:
            return None
        row = self._conn.execute(
            """
            SELECT *
            FROM events
            WHERE effect_id = ? AND event_type = ?
            ORDER BY sequence DESC
            """,
            (effect_id, "transition"),
        ).fetchall()
        for event in row:
            data = json.loads(event["data_json"])
            if data.get("to") != EffectStatus.SUCCEEDED.value:
                continue
            return {
                "effect_id": effect_id,
                "status": EffectStatus.SUCCEEDED.value,
                "contract": record.contract.name,
                "contract_version": record.contract.version,
                "downstream_key": record.downstream_key,
                "metadata": dict(data.get("metadata", {})),
                "recorded_at": event["at"],
            }
        return None

    async def events(self, effect_id: str) -> list[SqliteEvent]:
        rows = self._conn.execute(
            """
            SELECT *
            FROM events
            WHERE effect_id = ?
            ORDER BY sequence
            """,
            (effect_id,),
        ).fetchall()
        return [
            SqliteEvent(
                effect_id=row["effect_id"],
                sequence=row["sequence"],
                event_type=row["event_type"],
                version=row["version"],
                at=_parse_time(row["at"]),
                data=json.loads(row["data_json"]),
            )
            for row in rows
        ]

    async def attempt_count(self, effect_id: str) -> int:
        return int(
            self._conn.execute(
                "SELECT COUNT(*) FROM attempts WHERE effect_id = ?",
                (effect_id,),
            ).fetchone()[0]
        )

    async def count(self) -> int:
        return int(self._conn.execute("SELECT COUNT(*) FROM effects").fetchone()[0])

    def _create_schema(self) -> None:
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS effects (
                effect_id TEXT PRIMARY KEY,
                tenant_id TEXT NOT NULL,
                operation_key TEXT NOT NULL,
                tool TEXT NOT NULL,
                arguments_json TEXT NOT NULL,
                requested_by TEXT NOT NULL,
                trace_id TEXT,
                request_hash TEXT NOT NULL,
                downstream_key TEXT NOT NULL,
                contract_name TEXT NOT NULL,
                contract_version TEXT NOT NULL,
                safety_class TEXT NOT NULL,
                retry_limit INTEGER NOT NULL,
                key_retention_seconds REAL,
                settlement_bound_seconds REAL,
                status TEXT NOT NULL,
                version INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                worker_id TEXT,
                lease_worker_id TEXT,
                lease_until TEXT,
                UNIQUE (tenant_id, operation_key)
            )
            """
        )
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS attempts (
                attempt_id TEXT PRIMARY KEY,
                effect_id TEXT NOT NULL REFERENCES effects(effect_id),
                ordinal INTEGER NOT NULL,
                worker_id TEXT NOT NULL,
                version INTEGER NOT NULL,
                started_at TEXT NOT NULL,
                UNIQUE (effect_id, ordinal)
            )
            """
        )
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS events (
                effect_id TEXT NOT NULL REFERENCES effects(effect_id),
                sequence INTEGER NOT NULL,
                event_type TEXT NOT NULL,
                version INTEGER NOT NULL,
                at TEXT NOT NULL,
                data_json TEXT NOT NULL,
                PRIMARY KEY (effect_id, sequence)
            )
            """
        )

    def _record_params(self, record: EffectRecord) -> tuple[Any, ...]:
        return (
            record.effect_id,
            record.tenant_id,
            record.request.operation_key,
            record.request.tool,
            canonical_json(record.request.arguments),
            record.request.requested_by,
            record.request.trace_id,
            record.request_hash,
            record.downstream_key,
            record.contract.name,
            record.contract.version,
            record.contract.safety.value,
            record.contract.retry_limit,
            _duration_seconds(record.contract.key_retention),
            _duration_seconds(record.contract.settlement_bound),
            record.status.value,
            record.version,
            _format_time(record.created_at),
            _format_time(record.updated_at),
        )

    def _select_by_identity(
        self,
        tenant_id: str,
        operation_key: str,
    ) -> EffectRecord | None:
        row = self._conn.execute(
            """
            SELECT *
            FROM effects
            WHERE tenant_id = ? AND operation_key = ?
            """,
            (tenant_id, operation_key),
        ).fetchone()
        return None if row is None else _row_to_record(row)

    def _select_by_effect_id(self, effect_id: str) -> EffectRecord | None:
        row = self._conn.execute(
            "SELECT * FROM effects WHERE effect_id = ?",
            (effect_id,),
        ).fetchone()
        return None if row is None else _row_to_record(row)

    def _record_for_expected_version(
        self,
        effect_id: str,
        expected_version: int,
    ) -> EffectRecord:
        record = self._select_by_effect_id(effect_id)
        if record is None:
            raise UnknownEffectError(effect_id)
        if record.version != expected_version:
            raise VersionConflictError(
                f"{effect_id} expected version {expected_version}, "
                f"found {record.version}"
            )
        return record

    def _append_event(
        self,
        effect_id: str,
        event_type: str,
        version: int,
        data: JsonObject,
        *,
        at: datetime,
    ) -> None:
        sequence = int(
            self._conn.execute(
                "SELECT COUNT(*) FROM events WHERE effect_id = ?",
                (effect_id,),
            ).fetchone()[0]
        ) + 1
        self._conn.execute(
            """
            INSERT INTO events (
                effect_id, sequence, event_type, version, at, data_json
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                effect_id,
                sequence,
                event_type,
                version,
                _format_time(at),
                canonical_json(data),
            ),
        )

    @contextmanager
    def _transaction(self) -> Iterator[None]:
        self._conn.execute("BEGIN IMMEDIATE")
        try:
            yield
        except Exception:
            self._conn.execute("ROLLBACK")
            raise
        else:
            self._conn.execute("COMMIT")


def _row_to_record(row: sqlite3.Row) -> EffectRecord:
    return EffectRecord(
        effect_id=row["effect_id"],
        tenant_id=row["tenant_id"],
        request=EffectRequest(
            operation_key=row["operation_key"],
            tool=row["tool"],
            arguments=json.loads(row["arguments_json"]),
            requested_by=row["requested_by"],
            trace_id=row["trace_id"],
        ),
        request_hash=row["request_hash"],
        downstream_key=row["downstream_key"],
        contract=EffectContract(
            name=row["contract_name"],
            version=row["contract_version"],
            safety=SafetyClass(row["safety_class"]),
            retry_limit=row["retry_limit"],
            key_retention=_seconds_to_duration(row["key_retention_seconds"]),
            settlement_bound=_seconds_to_duration(row["settlement_bound_seconds"]),
        ),
        status=EffectStatus(row["status"]),
        version=row["version"],
        created_at=_parse_time(row["created_at"]),
        updated_at=_parse_time(row["updated_at"]),
    )


def _duration_seconds(value: timedelta | None) -> float | None:
    return None if value is None else value.total_seconds()


def _seconds_to_duration(value: float | None) -> timedelta | None:
    return None if value is None else timedelta(seconds=value)


def _format_time(value: datetime) -> str:
    return value.isoformat()


def _parse_time(value: str) -> datetime:
    return datetime.fromisoformat(value)
