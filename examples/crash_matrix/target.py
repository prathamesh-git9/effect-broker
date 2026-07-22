"""Durable target service used by the subprocess crash matrix.

The broker database is not the authority for whether money moved. This target
has its own SQLite file and its own committed-effect table. ``commit`` is
idempotent on ``downstream_key`` so a broker redispatch can prove "same target
effect" by reading the target's durable state, not by trusting local receipts.
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Self

from effect_broker.adapters.base import EffectAdapter
from effect_broker.canonical import canonical_json
from effect_broker.models import (
    DispatchResult,
    EffectRecord,
    ProbeResult,
    ProbeStatus,
    SafetyClass,
)


@dataclass(frozen=True, slots=True)
class TargetCommit:
    external_id: str
    downstream_key: str
    payload: dict[str, object]
    committed_at: datetime


class DurableTarget:
    """Small authoritative target shared by killed worker subprocesses."""

    def __init__(self, connection: sqlite3.Connection) -> None:
        self._conn = connection

    @classmethod
    def open(cls, path: str | Path) -> Self:
        db_path = Path(path)
        db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(db_path, isolation_level=None, timeout=5)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=5000")
        target = cls(conn)
        target._create_schema()
        return target

    def close(self) -> None:
        self._conn.close()

    def commit(self, downstream_key: str, payload: dict[str, object]) -> str:
        """Commit once by key; repeated commits return the same external id."""

        now = datetime.now(UTC).isoformat()
        payload_json = canonical_json(payload)
        self._conn.execute("BEGIN IMMEDIATE")
        try:
            self._conn.execute(
                """
                INSERT INTO commit_calls (downstream_key, payload_json, called_at)
                VALUES (?, ?, ?)
                """,
                (downstream_key, payload_json, now),
            )
            row = self._conn.execute(
                """
                SELECT external_id
                FROM target_effects
                WHERE downstream_key = ?
                """,
                (downstream_key,),
            ).fetchone()
            if row is not None:
                self._conn.execute("COMMIT")
                return row["external_id"]

            external_id = f"charge_{self._next_id():06d}"
            self._conn.execute(
                """
                INSERT INTO target_effects (
                    external_id, downstream_key, payload_json, committed_at
                )
                VALUES (?, ?, ?, ?)
                """,
                (external_id, downstream_key, payload_json, now),
            )
            self._conn.execute("COMMIT")
            return external_id
        except Exception:
            self._conn.execute("ROLLBACK")
            raise

    def count(self, downstream_key: str) -> int:
        return int(
            self._conn.execute(
                """
                SELECT COUNT(*)
                FROM target_effects
                WHERE downstream_key = ?
                """,
                (downstream_key,),
            ).fetchone()[0]
        )

    def commit_call_count(self, downstream_key: str) -> int:
        return int(
            self._conn.execute(
                """
                SELECT COUNT(*)
                FROM commit_calls
                WHERE downstream_key = ?
                """,
                (downstream_key,),
            ).fetchone()[0]
        )

    def probe(self, downstream_key: str) -> ProbeResult:
        row = self._conn.execute(
            """
            SELECT external_id
            FROM target_effects
            WHERE downstream_key = ?
            """,
            (downstream_key,),
        ).fetchone()
        if row is None:
            return ProbeResult(
                status=ProbeStatus.NOT_COMMITTED,
                evidence={"downstream_key": downstream_key},
            )
        return ProbeResult(
            status=ProbeStatus.COMMITTED,
            external_id=row["external_id"],
            output={"external_id": row["external_id"]},
            evidence={"downstream_key": downstream_key},
        )

    def _create_schema(self) -> None:
        self._conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS target_effects (
                external_id TEXT PRIMARY KEY,
                downstream_key TEXT NOT NULL UNIQUE,
                payload_json TEXT NOT NULL,
                committed_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS commit_calls (
                call_id INTEGER PRIMARY KEY AUTOINCREMENT,
                downstream_key TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                called_at TEXT NOT NULL
            );
            """
        )

    def _next_id(self) -> int:
        return int(
            self._conn.execute("SELECT COUNT(*) FROM target_effects").fetchone()[0]
        ) + 1


class DurableTargetAdapter(EffectAdapter):
    """Effect adapter over the durable crash-matrix target."""

    def __init__(self, target: DurableTarget) -> None:
        self.target = target

    async def dispatch(
        self,
        effect: EffectRecord,
        *,
        attempt_id: str,
    ) -> DispatchResult:
        del attempt_id
        external_id = self.target.commit(
            effect.downstream_key,
            {
                "request_hash": effect.request_hash,
                "arguments": dict(effect.request.arguments),
            },
        )
        return DispatchResult(
            external_id=external_id,
            output={"external_id": external_id},
            committed=True,
        )

    async def probe(self, effect: EffectRecord) -> ProbeResult:
        if effect.contract.safety is SafetyClass.UNSAFE:
            return ProbeResult(
                status=ProbeStatus.UNKNOWN,
                evidence={"reason": "unsafe target has no authoritative lookup"},
            )
        probe = self.target.probe(effect.downstream_key)
        if (
            probe.status is ProbeStatus.NOT_COMMITTED
            and effect.contract.safety is SafetyClass.IDEMPOTENT
        ):
            return ProbeResult(
                status=ProbeStatus.UNKNOWN,
                evidence={"reason": "idempotent recovery redispatches by key"},
            )
        return probe


def decode_payload(payload_json: str) -> dict[str, object]:
    return json.loads(payload_json)
