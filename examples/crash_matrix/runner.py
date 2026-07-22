"""Run the durable crash matrix with killed worker subprocesses."""

from __future__ import annotations

import argparse
import asyncio
import os
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path

from examples.crash_matrix.target import DurableTarget, DurableTargetAdapter
from examples.crash_matrix.worker import TENANT_SECRET

from effect_broker.contracts import ContractRegistry
from effect_broker.engine import EffectBroker
from effect_broker.errors import VersionConflictError
from effect_broker.models import (
    EffectContract,
    EffectRecord,
    EffectRequest,
    EffectStatus,
    SafetyClass,
)
from effect_broker.reconcile import reconcile_once
from effect_broker.store.base import tenant_key_provider_from_secret
from effect_broker.store.postgres import PostgresStore
from effect_broker.store.sqlite import SqliteStore

TENANT_ID = "tenant-a"
TOOL = "charge"
FAILPOINT = "after_target_commit_before_receipt"


@dataclass(frozen=True, slots=True)
class ScenarioResult:
    scenario: str
    failpoint: str
    target_count: int
    broker_status: EffectStatus
    passed: bool
    detail: str


def run_matrix(base_dir: Path | None = None) -> list[ScenarioResult]:
    root = Path(tempfile.mkdtemp(prefix="effect-broker-crash-", dir=base_dir))
    results = [
        _run_scenario(root, safety)
        for safety in (
            SafetyClass.IDEMPOTENT,
            SafetyClass.RECONCILABLE,
            SafetyClass.UNSAFE,
        )
    ]
    _print_table(results)
    return results


def _run_scenario(root: Path, safety: SafetyClass) -> ScenarioResult:
    scenario_dir = root / safety.value
    scenario_dir.mkdir(parents=True, exist_ok=True)
    broker_path = scenario_dir / "broker.sqlite3"
    target_path = scenario_dir / "target.sqlite3"

    store = SqliteStore.open(broker_path, TENANT_SECRET)
    target = DurableTarget.open(target_path)
    try:
        reservation = asyncio.run(_submit(store, safety))
        crashed = _run_worker(broker_path, target_path, FAILPOINT)
        if crashed.returncode == 0:
            return _result(safety, FAILPOINT, target, reservation.effect, False, "live")

        unknown = asyncio.run(_mark_unknown(store, reservation.effect))
    finally:
        target.close()
        store.close()

    recovery = _run_recovery_processes(broker_path, target_path, unknown.effect_id)
    zombie = _run_zombie(broker_path, unknown.effect_id, unknown.version)

    store = SqliteStore.open(broker_path, TENANT_SECRET)
    target = DurableTarget.open(target_path)
    try:
        current = asyncio.run(store.get(TENANT_ID, unknown.effect_id))
        if current is None:
            raise AssertionError("reserved effect disappeared")
        target_count = target.count(current.downstream_key)
        target_calls = target.commit_call_count(current.downstream_key)
        attempts = asyncio.run(store.attempt_count(current.effect_id))
        passed, detail = _judge(safety, current, target_count, target_calls, attempts)
        if recovery.count(0) != 1 or zombie.returncode != 23:
            passed = False
            detail = (
                f"{detail}; recovery={recovery}; "
                f"zombie={zombie.returncode}"
            )
        return ScenarioResult(
            scenario=safety.value,
            failpoint=FAILPOINT,
            target_count=target_count,
            broker_status=current.status,
            passed=passed,
            detail=detail,
        )
    finally:
        target.close()
        store.close()


async def _submit(store, safety: SafetyClass):
    broker = EffectBroker(
        store,
        ContractRegistry({TOOL: _contract(safety)}),
        _adapter_for,
    )
    return await broker.submit(
        TENANT_ID,
        EffectRequest(
            operation_key=f"order:42:{safety.value}:charge:v1",
            tool=TOOL,
            arguments={"amount": 4200, "currency": "usd"},
            requested_by="crash-matrix",
        ),
    )


async def _mark_unknown(
    store,
    effect: EffectRecord,
) -> EffectRecord:
    current = await store.get(TENANT_ID, effect.effect_id)
    if current is None:
        raise AssertionError("reserved effect disappeared")
    if current.status is not EffectStatus.DISPATCHING:
        raise AssertionError(f"expected DISPATCHING after crash, got {current.status}")
    return await store.transition(
        current.effect_id,
        expected_version=current.version,
        target=EffectStatus.OUTCOME_UNKNOWN,
        data={"reason": "stale_dispatching_worker_died"},
    )


def _run_worker(
    broker_path: Path,
    target_path: Path,
    failpoint: str,
) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env.update(
        {
            "BROKER_DB": str(broker_path),
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
    broker_path: Path,
    target_path: Path,
    effect_id: str,
) -> list[int]:
    commands = [
        [
            sys.executable,
            "-m",
            "examples.crash_matrix.runner",
            "--recover-one",
            str(broker_path),
            str(target_path),
            effect_id,
        ]
        for _ in range(3)
    ]
    procs = [
        subprocess.Popen(
            command,
            cwd=Path(__file__).resolve().parents[2],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        for command in commands
    ]
    return [proc.communicate(timeout=10) and proc.returncode for proc in procs]


def _run_zombie(
    broker_path: Path,
    effect_id: str,
    expected_version: int,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            sys.executable,
            "-m",
            "examples.crash_matrix.runner",
            "--zombie-transition",
            str(broker_path),
            effect_id,
            str(expected_version),
        ],
        check=False,
        cwd=Path(__file__).resolve().parents[2],
        text=True,
        capture_output=True,
        timeout=10,
    )


async def recover_one(
    broker_path: Path,
    target_path: Path,
    effect_id: str,
) -> EffectRecord:
    store = _open_store(broker_path)
    target = DurableTarget.open(target_path)
    try:
        effect = await store.get(TENANT_ID, effect_id)
        if effect is None:
            raise AssertionError("reserved effect disappeared")
        return await reconcile_once(
            store,
            lambda candidate: DurableTargetAdapter(target),
            effect=effect,
        )
    finally:
        target.close()
        await _close_store(store)


async def zombie_transition(
    broker_path: Path,
    effect_id: str,
    expected_version: int,
) -> None:
    store = _open_store(broker_path)
    try:
        await store.transition(
            effect_id,
            expected_version=expected_version,
            target=EffectStatus.DISPATCHING,
            data={"actor": "zombie-worker"},
        )
    finally:
        await _close_store(store)


def _open_store(broker_path: Path):
    dsn = os.environ.get("BROKER_DSN")
    if dsn:
        return PostgresStore.connect(
            dsn,
            tenant_key_provider_from_secret(TENANT_SECRET),
        )
    return SqliteStore.open(broker_path, TENANT_SECRET)


async def _close_store(store) -> None:
    close = getattr(store, "close", None)
    if close is not None:
        close()
        return
    await store.aclose()


def _adapter_for(effect: EffectRecord) -> DurableTargetAdapter:
    del effect
    raise AssertionError("adapter is only needed by subprocess recovery")


def _contract(safety: SafetyClass) -> EffectContract:
    return EffectContract(
        name=TOOL,
        version="v1",
        safety=safety,
        retry_limit=3 if safety is not SafetyClass.UNSAFE else 0,
        key_retention=timedelta(hours=1)
        if safety is SafetyClass.IDEMPOTENT
        else None,
        settlement_bound=timedelta(0)
        if safety is SafetyClass.RECONCILABLE
        else None,
    )


def _judge(
    safety: SafetyClass,
    effect: EffectRecord,
    target_count: int,
    target_calls: int,
    attempts: int,
) -> tuple[bool, str]:
    if safety is SafetyClass.IDEMPOTENT:
        return (
            target_count == 1 and effect.status is EffectStatus.SUCCEEDED,
            f"target_calls={target_calls}; attempts={attempts}",
        )
    if safety is SafetyClass.RECONCILABLE:
        return (
            target_count == 1
            and target_calls == 1
            and effect.status is EffectStatus.SUCCEEDED,
            f"target_calls={target_calls}; attempts={attempts}",
        )
    return (
        target_count <= 1
        and target_calls == 1
        and attempts == 1
        and effect.status is EffectStatus.MANUAL_REVIEW,
        f"target_calls={target_calls}; attempts={attempts}",
    )


def _result(
    safety: SafetyClass,
    failpoint: str,
    target: DurableTarget,
    effect: EffectRecord,
    passed: bool,
    detail: str,
) -> ScenarioResult:
    return ScenarioResult(
        scenario=safety.value,
        failpoint=failpoint,
        target_count=target.count(effect.downstream_key),
        broker_status=effect.status,
        passed=passed,
        detail=detail,
    )


def _print_table(results: list[ScenarioResult]) -> None:
    print("scenario | failpoint | target_count | broker_status | result")
    print("--- | --- | ---: | --- | ---")
    for result in results:
        verdict = "PASS" if result.passed else f"FAIL ({result.detail})"
        print(
            f"{result.scenario} | {result.failpoint} | "
            f"{result.target_count} | {result.broker_status.value} | {verdict}"
        )
    if all(result.passed for result in results):
        print("ALL PASS")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--recover-one", nargs=3, metavar=("BROKER", "TARGET", "ID"))
    parser.add_argument("--zombie-transition", nargs=3, metavar=("BROKER", "ID", "VER"))
    args = parser.parse_args()

    if args.recover_one is not None:
        try:
            asyncio.run(
                recover_one(
                    Path(args.recover_one[0]),
                    Path(args.recover_one[1]),
                    args.recover_one[2],
                )
            )
        except VersionConflictError as exc:
            raise SystemExit(23) from exc
        raise SystemExit(0)

    if args.zombie_transition is not None:
        try:
            asyncio.run(
                zombie_transition(
                    Path(args.zombie_transition[0]),
                    args.zombie_transition[1],
                    int(args.zombie_transition[2]),
                )
            )
        except VersionConflictError as exc:
            raise SystemExit(23) from exc
        raise SystemExit(0)

    temp_dir = Path(
        os.environ.get("TMP") or os.environ.get("TEMP") or tempfile.gettempdir()
    )
    results = run_matrix(temp_dir)
    raise SystemExit(0 if all(result.passed for result in results) else 1)


if __name__ == "__main__":
    main()
