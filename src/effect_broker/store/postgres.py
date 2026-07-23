"""Production Postgres store for the effect ledger.

The store uses SQLAlchemy 2 Core over psycopg 3's async driver. Transactions run
at Postgres' default ``READ COMMITTED`` isolation level: every correctness path
uses either a unique constraint, a row lock, or an explicit version
compare-and-swap, so stronger snapshot isolation would not add the guard that
prevents lost updates. Driver-level retries are not enabled; a failed statement
surfaces to the caller instead of being replayed behind the broker's state
machine.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Self

from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    MetaData,
    PrimaryKeyConstraint,
    Table,
    Text,
    UniqueConstraint,
    and_,
    func,
    insert,
    select,
    text,
    update,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.engine import RowMapping
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

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
from effect_broker.store.base import EffectStore, TenantKeyProvider

metadata = MetaData()

tenants = Table(
    "tenants",
    metadata,
    # The domain model accepts string tenant ids, so production storage does too.
    # Auth owns identity validation above this layer.
    Column("tenant_id", Text, primary_key=True),
    Column("name", Text),
    Column("api_key_hash", Text),
    Column("hmac_key_id", Text),
    Column(
        "disabled",
        Boolean,
        nullable=False,
        server_default=text("false"),
    ),
    Column(
        "created_at",
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    ),
    Column(
        "updated_at",
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    ),
)

effect_intents = Table(
    "effect_intents",
    metadata,
    Column("effect_id", Text, primary_key=True),
    Column(
        "tenant_id",
        Text,
        ForeignKey("tenants.tenant_id", ondelete="RESTRICT"),
        nullable=False,
    ),
    Column("operation_key", Text, nullable=False),
    Column("tool", Text, nullable=False),
    Column("arguments_json", JSONB, nullable=False),
    Column("requested_by", Text, nullable=False),
    Column("trace_id", Text),
    Column("request_hash", Text, nullable=False),
    Column("downstream_key", Text, nullable=False),
    Column("contract_name", Text, nullable=False),
    Column("contract_version", Text, nullable=False),
    Column("safety_class", Text, nullable=False),
    Column("retry_limit", Integer, nullable=False),
    Column("key_retention_seconds", BigInteger),
    Column("settlement_bound_seconds", BigInteger),
    Column("status", Text, nullable=False),
    Column("version", Integer, nullable=False),
    Column(
        "created_at",
        DateTime(timezone=True),
        nullable=False,
    ),
    Column(
        "updated_at",
        DateTime(timezone=True),
        nullable=False,
    ),
    Column("worker_id", Text),
    Column("lease_until", DateTime(timezone=True)),
    UniqueConstraint(
        "tenant_id",
        "operation_key",
        name="uq_effect_intents_tenant_operation_key",
    ),
)

effect_attempts = Table(
    "effect_attempts",
    metadata,
    Column("attempt_id", Text, primary_key=True),
    Column(
        "effect_id",
        Text,
        ForeignKey("effect_intents.effect_id", ondelete="CASCADE"),
        nullable=False,
    ),
    Column("ordinal", Integer, nullable=False),
    Column("worker_id", Text, nullable=False),
    Column("version", Integer, nullable=False),
    Column(
        "started_at",
        DateTime(timezone=True),
        nullable=False,
    ),
    UniqueConstraint("effect_id", "ordinal", name="uq_effect_attempts_ordinal"),
)

effect_receipts = Table(
    "effect_receipts",
    metadata,
    Column(
        "receipt_id",
        Text,
        primary_key=True,
    ),
    Column(
        "effect_id",
        Text,
        ForeignKey("effect_intents.effect_id", ondelete="CASCADE"),
        nullable=False,
        unique=True,
    ),
    Column("external_id", Text),
    Column("contract_name", Text, nullable=False),
    Column("contract_version", Text, nullable=False),
    Column("downstream_key", Text, nullable=False),
    Column("metadata_json", JSONB, nullable=False),
    Column(
        "created_at",
        DateTime(timezone=True),
        nullable=False,
    ),
)

effect_events = Table(
    "effect_events",
    metadata,
    Column(
        "effect_id",
        Text,
        ForeignKey("effect_intents.effect_id", ondelete="CASCADE"),
        nullable=False,
    ),
    Column("sequence", Integer, nullable=False),
    Column("event_type", Text, nullable=False),
    Column("version", Integer, nullable=False),
    Column("at", DateTime(timezone=True), nullable=False),
    Column("data_json", JSONB, nullable=False),
    PrimaryKeyConstraint(
        "effect_id",
        "sequence",
        name="pk_effect_events",
    ),
)

Index(
    "ix_effect_intents_status_lease",
    effect_intents.c.status,
    effect_intents.c.lease_until,
)
Index("ix_effect_intents_tenant_id", effect_intents.c.tenant_id)
Index("ix_effect_attempts_effect_id", effect_attempts.c.effect_id)
Index("ix_effect_receipts_effect_id", effect_receipts.c.effect_id)
Index("ix_effect_events_effect_id", effect_events.c.effect_id)


@dataclass(frozen=True, slots=True)
class PostgresEvent:
    """A persisted append-only fact used by conformance diagnostics."""

    effect_id: str
    sequence: int
    event_type: str
    version: int
    at: datetime
    data: JsonObject


class PostgresStore(EffectStore):
    """Durable Postgres ledger with CAS fencing and SKIP LOCKED leases."""

    def __init__(
        self,
        engine: AsyncEngine,
        tenant_key_provider: TenantKeyProvider,
    ) -> None:
        self._engine = engine
        self._tenant_key_provider = tenant_key_provider

    @classmethod
    def connect(
        cls,
        dsn: str,
        tenant_key_provider: TenantKeyProvider,
    ) -> Self:
        """Build an async pooled store without touching the database yet."""

        engine = create_async_engine(
            _async_dsn(dsn),
            isolation_level="READ COMMITTED",
            pool_pre_ping=True,
        )
        return cls(engine, tenant_key_provider)

    async def aclose(self) -> None:
        await self._engine.dispose()

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

        async with self._engine.begin() as conn:
            await conn.execute(
                pg_insert(tenants)
                .values(tenant_id=tenant_id, created_at=now, updated_at=now)
                .on_conflict_do_nothing(index_elements=[tenants.c.tenant_id])
            )
            inserted = (
                (
                    await conn.execute(
                        pg_insert(effect_intents)
                        .values(_record_values(record))
                        .on_conflict_do_nothing(
                            index_elements=[
                                effect_intents.c.tenant_id,
                                effect_intents.c.operation_key,
                            ]
                        )
                        .returning(*effect_intents.c)
                    )
                )
                .mappings()
                .one_or_none()
            )
            if inserted is None:
                existing = await self._select_by_identity(
                    conn,
                    tenant_id,
                    request.operation_key,
                )
                if existing is None:
                    raise VersionConflictError(
                        f"{tenant_id}/{request.operation_key} disappeared"
                    )
                if existing.request_hash != digest:
                    raise PayloadConflictError(
                        request.operation_key,
                        existing.request_hash,
                        digest,
                    )
                return Reservation(existing, created=False, replayed=True)

            await self._append_event(
                conn,
                effect_id,
                "reserved",
                record.version,
                {"status": record.status.value},
                at=now,
            )
            return Reservation(_row_to_record(inserted), created=True, replayed=False)

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
        lease_until = now + lease_for
        claimed: list[EffectRecord] = []
        async with self._engine.begin() as conn:
            rows = (
                (
                    await conn.execute(
                        select(effect_intents)
                        .where(
                            and_(
                                effect_intents.c.status.in_(
                                    [
                                        EffectStatus.PREPARED.value,
                                        EffectStatus.RETRYABLE.value,
                                    ]
                                ),
                                (effect_intents.c.lease_until.is_(None))
                                | (effect_intents.c.lease_until < now),
                            )
                        )
                        .order_by(effect_intents.c.created_at, effect_intents.c.effect_id)
                        .with_for_update(skip_locked=True)
                        .limit(limit)
                    )
                )
                .mappings()
                .all()
            )
            for row in rows:
                updated = (
                    (
                        await conn.execute(
                            update(effect_intents)
                            .where(effect_intents.c.effect_id == row["effect_id"])
                            .values(
                                worker_id=worker_id,
                                lease_until=lease_until,
                                version=effect_intents.c.version + 1,
                                updated_at=now,
                            )
                            .returning(*effect_intents.c)
                        )
                    )
                    .mappings()
                    .one()
                )
                await self._append_event(
                    conn,
                    updated["effect_id"],
                    "claimed",
                    updated["version"],
                    {
                        "worker_id": worker_id,
                        "lease_expires_at": _format_time(lease_until),
                    },
                    at=now,
                )
                claimed.append(_row_to_record(updated))
        return claimed

    async def start_attempt(
        self,
        effect_id: str,
        *,
        expected_version: int,
        worker_id: str,
    ) -> str:
        async with self._engine.begin() as conn:
            record = await self._record_for_expected_version(
                conn,
                effect_id,
                expected_version,
                lock=True,
            )
            ordinal = (
                int(
                    (
                        await conn.execute(
                            select(func.count())
                            .select_from(effect_attempts)
                            .where(effect_attempts.c.effect_id == effect_id)
                        )
                    ).scalar_one()
                )
                + 1
            )
            attempt_id = f"{effect_id}-attempt-{ordinal}"
            now = datetime.now(UTC)
            await conn.execute(
                insert(effect_attempts).values(
                    attempt_id=attempt_id,
                    effect_id=effect_id,
                    ordinal=ordinal,
                    worker_id=worker_id,
                    version=expected_version,
                    started_at=now,
                )
            )
            await self._append_event(
                conn,
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
        async with self._engine.begin() as conn:
            record = await self._record_for_expected_version(
                conn,
                effect_id,
                expected_version,
            )
            assert_transition(record.status, target)
            now = datetime.now(UTC)
            updated = (
                (
                    await conn.execute(
                        update(effect_intents)
                        .where(
                            and_(
                                effect_intents.c.effect_id == effect_id,
                                effect_intents.c.version == expected_version,
                            )
                        )
                        .values(
                            status=target.value,
                            version=effect_intents.c.version + 1,
                            updated_at=now,
                            worker_id=None,
                            lease_until=None,
                        )
                        .returning(*effect_intents.c)
                    )
                )
                .mappings()
                .one_or_none()
            )
            if updated is None:
                raise VersionConflictError(
                    f"{effect_id} expected version {expected_version}"
                )
            await self._append_event(
                conn,
                effect_id,
                "transition",
                updated["version"],
                {
                    "from": record.status.value,
                    "to": target.value,
                    "metadata": dict(data),
                },
                at=now,
            )
            if target is EffectStatus.SUCCEEDED:
                await self._insert_receipt(conn, _row_to_record(updated), data, at=now)
            return _row_to_record(updated)

    async def get(self, tenant_id: str, effect_id: str) -> EffectRecord | None:
        async with self._engine.connect() as conn:
            row = (
                (
                    await conn.execute(
                        select(effect_intents).where(
                            and_(
                                effect_intents.c.tenant_id == tenant_id,
                                effect_intents.c.effect_id == effect_id,
                            )
                        )
                    )
                )
                .mappings()
                .one_or_none()
            )
        return None if row is None else _row_to_record(row)

    async def list(
        self,
        tenant_id: str,
        *,
        status: EffectStatus | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[EffectRecord]:
        statement = select(effect_intents).where(effect_intents.c.tenant_id == tenant_id)
        if status is not None:
            statement = statement.where(effect_intents.c.status == status.value)
        statement = (
            statement.order_by(effect_intents.c.created_at, effect_intents.c.effect_id)
            .limit(limit)
            .offset(offset)
        )
        async with self._engine.connect() as conn:
            rows = (await conn.execute(statement)).mappings().all()
        return [_row_to_record(row) for row in rows]

    async def receipt(
        self,
        tenant_id: str,
        effect_id: str,
    ) -> JsonObject | None:
        async with self._engine.connect() as conn:
            row = (
                (
                    await conn.execute(
                        select(effect_receipts, effect_intents.c.tenant_id)
                        .join(
                            effect_intents,
                            effect_receipts.c.effect_id == effect_intents.c.effect_id,
                        )
                        .where(
                            and_(
                                effect_intents.c.tenant_id == tenant_id,
                                effect_receipts.c.effect_id == effect_id,
                            )
                        )
                    )
                )
                .mappings()
                .one_or_none()
            )
        if row is None:
            return None
        return {
            "effect_id": effect_id,
            "status": EffectStatus.SUCCEEDED.value,
            "contract": row["contract_name"],
            "contract_version": row["contract_version"],
            "downstream_key": row["downstream_key"],
            "metadata": dict(row["metadata_json"]),
            "recorded_at": _format_time(row["created_at"]),
        }

    async def events(self, effect_id: str) -> list[PostgresEvent]:
        async with self._engine.connect() as conn:
            rows = (
                (
                    await conn.execute(
                        select(effect_events)
                        .where(effect_events.c.effect_id == effect_id)
                        .order_by(effect_events.c.sequence)
                    )
                )
                .mappings()
                .all()
            )
        return [
            PostgresEvent(
                effect_id=row["effect_id"],
                sequence=row["sequence"],
                event_type=row["event_type"],
                version=row["version"],
                at=_ensure_tz(row["at"]),
                data=dict(row["data_json"]),
            )
            for row in rows
        ]

    async def attempt_count(self, effect_id: str) -> int:
        async with self._engine.connect() as conn:
            return int(
                (
                    await conn.execute(
                        select(func.count())
                        .select_from(effect_attempts)
                        .where(effect_attempts.c.effect_id == effect_id)
                    )
                ).scalar_one()
            )

    async def count(self) -> int:
        async with self._engine.connect() as conn:
            return int(
                (
                    await conn.execute(select(func.count()).select_from(effect_intents))
                ).scalar_one()
            )

    async def _record_for_expected_version(
        self,
        conn,
        effect_id: str,
        expected_version: int,
        *,
        lock: bool = False,
    ) -> EffectRecord:
        statement = select(effect_intents).where(effect_intents.c.effect_id == effect_id)
        if lock:
            statement = statement.with_for_update()
        row = (await conn.execute(statement)).mappings().one_or_none()
        if row is None:
            raise UnknownEffectError(effect_id)
        record = _row_to_record(row)
        if record.version != expected_version:
            raise VersionConflictError(
                f"{effect_id} expected version {expected_version}, found {record.version}"
            )
        return record

    async def _select_by_identity(
        self,
        conn,
        tenant_id: str,
        operation_key: str,
    ) -> EffectRecord | None:
        row = (
            (
                await conn.execute(
                    select(effect_intents).where(
                        and_(
                            effect_intents.c.tenant_id == tenant_id,
                            effect_intents.c.operation_key == operation_key,
                        )
                    )
                )
            )
            .mappings()
            .one_or_none()
        )
        return None if row is None else _row_to_record(row)

    async def _append_event(
        self,
        conn,
        effect_id: str,
        event_type: str,
        version: int,
        data: JsonObject,
        *,
        at: datetime,
    ) -> None:
        sequence = int(
            (
                await conn.execute(
                    select(func.coalesce(func.max(effect_events.c.sequence), 0) + 1)
                    .select_from(effect_events)
                    .where(effect_events.c.effect_id == effect_id)
                )
            ).scalar_one()
        )
        await conn.execute(
            insert(effect_events).values(
                effect_id=effect_id,
                sequence=sequence,
                event_type=event_type,
                version=version,
                at=at,
                data_json=_json_value(data),
            )
        )

    async def _insert_receipt(
        self,
        conn,
        record: EffectRecord,
        data: JsonObject,
        *,
        at: datetime,
    ) -> None:
        await conn.execute(
            insert(effect_receipts).values(
                receipt_id=f"rcpt_{uuid.uuid4().hex}",
                effect_id=record.effect_id,
                external_id=data.get("external_id"),
                contract_name=record.contract.name,
                contract_version=record.contract.version,
                downstream_key=record.downstream_key,
                metadata_json=_json_value(data),
                created_at=at,
            )
        )


async def create_schema(dsn: str, *, reset: bool = False) -> None:
    """Create the production schema for tests and local bootstrap.

    Alembic remains the deployment path. This helper is intentionally thin for
    integration tests that need to prepare a disposable database without shelling
    out to the Alembic CLI.
    """

    engine = create_async_engine(_async_dsn(dsn), isolation_level="READ COMMITTED")
    try:
        async with engine.begin() as conn:
            if reset:
                await conn.run_sync(metadata.drop_all)
            await conn.run_sync(metadata.create_all)
            for statement in _receipt_immutability_sql():
                await conn.execute(text(statement))
    finally:
        await engine.dispose()


def _record_values(record: EffectRecord) -> dict[str, Any]:
    return {
        "effect_id": record.effect_id,
        "tenant_id": record.tenant_id,
        "operation_key": record.request.operation_key,
        "tool": record.request.tool,
        "arguments_json": _json_value(record.request.arguments),
        "requested_by": record.request.requested_by,
        "trace_id": record.request.trace_id,
        "request_hash": record.request_hash,
        "downstream_key": record.downstream_key,
        "contract_name": record.contract.name,
        "contract_version": record.contract.version,
        "safety_class": record.contract.safety.value,
        "retry_limit": record.contract.retry_limit,
        "key_retention_seconds": _duration_seconds(record.contract.key_retention),
        "settlement_bound_seconds": _duration_seconds(record.contract.settlement_bound),
        "status": record.status.value,
        "version": record.version,
        "created_at": record.created_at,
        "updated_at": record.updated_at,
    }


def _row_to_record(row: RowMapping) -> EffectRecord:
    return EffectRecord(
        effect_id=row["effect_id"],
        tenant_id=row["tenant_id"],
        request=EffectRequest(
            operation_key=row["operation_key"],
            tool=row["tool"],
            arguments=dict(row["arguments_json"]),
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
        created_at=_ensure_tz(row["created_at"]),
        updated_at=_ensure_tz(row["updated_at"]),
    )


def _json_value(value: Any) -> Any:
    return json.loads(canonical_json(value))


def _duration_seconds(value: timedelta | None) -> int | None:
    return None if value is None else int(value.total_seconds())


def _seconds_to_duration(value: int | None) -> timedelta | None:
    return None if value is None else timedelta(seconds=value)


def _format_time(value: datetime) -> str:
    return value.astimezone(UTC).isoformat()


def _ensure_tz(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value


def _async_dsn(dsn: str) -> str:
    if dsn.startswith("postgres://"):
        dsn = f"postgresql://{dsn.removeprefix('postgres://')}"
    if dsn.startswith("postgresql://"):
        return f"postgresql+psycopg://{dsn.removeprefix('postgresql://')}"
    return dsn


def _receipt_immutability_sql() -> tuple[str, ...]:
    return (
        """
        CREATE OR REPLACE FUNCTION effect_receipts_no_update()
        RETURNS trigger AS $$
        BEGIN
            RAISE EXCEPTION 'effect_receipts are immutable';
        END;
        $$ LANGUAGE plpgsql;
        """,
        """
        DROP TRIGGER IF EXISTS trg_effect_receipts_no_update ON effect_receipts;
        """,
        """
        CREATE TRIGGER trg_effect_receipts_no_update
        BEFORE UPDATE ON effect_receipts
        FOR EACH ROW EXECUTE FUNCTION effect_receipts_no_update();
        """,
    )
