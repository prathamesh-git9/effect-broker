from __future__ import annotations

import asyncio
import os
import subprocess
import sys
from pathlib import Path

import pytest
from examples.crash_matrix.runner import (
    FAILPOINT,
    TENANT_ID,
    _mark_unknown,
    _submit,
)
from examples.crash_matrix.target import DurableTarget
from examples.crash_matrix.worker import TENANT_SECRET

from effect_broker.models import EffectStatus, SafetyClass
from effect_broker.store.base import tenant_key_provider_from_secret
from effect_broker.store.postgres import PostgresStore, create_schema


@pytest.mark.skipif(
    not (os.environ.get("EFFECT_BROKER_TEST_DSN") or os.environ.get("DATABASE_URL")),
    reason="EFFECT_BROKER_TEST_DSN or DATABASE_URL is not set",
)
def test_postgres_crash_recovery_is_idempotent_and_version_fenced(
    tmp_path: Path,
) -> None:
    dsn = os.environ.get("EFFECT_BROKER_TEST_DSN") or os.environ["DATABASE_URL"]
    asyncio.run(create_schema(dsn, reset=True))

    target_path = tmp_path / "target.sqlite3"
    store = PostgresStore.connect(
        dsn,
        tenant_key_provider_from_secret(TENANT_SECRET),
    )
    target = DurableTarget.open(target_path)
    try:
        reservation = asyncio.run(_submit(store, SafetyClass.IDEMPOTENT))
        crashed = _run_worker(dsn, target_path, FAILPOINT)
        assert crashed.returncode != 0, crashed.stderr

        unknown = asyncio.run(_mark_unknown(store, reservation.effect))
    finally:
        target.close()
        asyncio.run(store.aclose())

    recovery = _run_recovery_processes(dsn, target_path, unknown.effect_id)
    zombie = _run_zombie(dsn, unknown.effect_id, unknown.version)

    store = PostgresStore.connect(
        dsn,
        tenant_key_provider_from_secret(TENANT_SECRET),
    )
    target = DurableTarget.open(target_path)
    try:
        current = asyncio.run(store.get(TENANT_ID, unknown.effect_id))
        assert current is not None
        assert current.status is EffectStatus.SUCCEEDED
        assert target.count(current.downstream_key) == 1
        assert recovery.count(0) == 1
        assert zombie.returncode == 23, zombie.stderr
        assert asyncio.run(store.attempt_count(current.effect_id)) == 2
    finally:
        target.close()
        asyncio.run(store.aclose())


def _run_worker(
    dsn: str,
    target_path: Path,
    failpoint: str,
) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env.update(
        {
            "BROKER_DSN": dsn,
            "TARGET_DB": str(target_path),
            "FAILPOINT": failpoint,
        }
    )
    return subprocess.run(
        [sys.executable, "-m", "examples.crash_matrix.worker"],
        check=False,
        cwd=Path(__file__).resolve().parents[2],
        env=env,
        text=True,
        capture_output=True,
        timeout=10,
    )


def _run_recovery_processes(
    dsn: str,
    target_path: Path,
    effect_id: str,
) -> list[int]:
    env = os.environ.copy()
    env["BROKER_DSN"] = dsn
    commands = [
        [
            sys.executable,
            "-m",
            "examples.crash_matrix.runner",
            "--recover-one",
            "unused.sqlite3",
            str(target_path),
            effect_id,
        ]
        for _ in range(3)
    ]
    procs = [
        subprocess.Popen(
            command,
            cwd=Path(__file__).resolve().parents[2],
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        for command in commands
    ]
    return [proc.communicate(timeout=10) and proc.returncode for proc in procs]


def _run_zombie(
    dsn: str,
    effect_id: str,
    expected_version: int,
) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env["BROKER_DSN"] = dsn
    return subprocess.run(
        [
            sys.executable,
            "-m",
            "examples.crash_matrix.runner",
            "--zombie-transition",
            "unused.sqlite3",
            effect_id,
            str(expected_version),
        ],
        check=False,
        cwd=Path(__file__).resolve().parents[2],
        env=env,
        text=True,
        capture_output=True,
        timeout=10,
    )
