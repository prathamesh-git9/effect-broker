"""Crash-matrix worker subprocess entrypoint.

This process performs the dangerous sequence with real ``os._exit`` failpoints:
claim, persist ``DISPATCHING``, persist an attempt row, touch the target, then
maybe die before the broker receipt. A hard exit is essential because exception
tests do not prove durability across process death.
"""

from __future__ import annotations

import asyncio
import os
from datetime import UTC, datetime, timedelta

from examples.crash_matrix.target import DurableTarget

from effect_broker.models import EffectStatus
from effect_broker.store.base import tenant_key_provider_from_secret
from effect_broker.store.postgres import PostgresStore
from effect_broker.store.sqlite import SqliteStore

TENANT_SECRET = b"crash-matrix-tenant-secret"


async def run_once() -> int:
    broker_dsn = os.environ.get("BROKER_DSN")
    broker_path = os.environ.get("BROKER_DB")
    target_path = _required_env("TARGET_DB")
    failpoint = os.environ.get("FAILPOINT", "none")
    worker_id = os.environ.get("WORKER_ID", f"worker-{os.getpid()}")

    if broker_dsn:
        store = PostgresStore.connect(
            broker_dsn,
            tenant_key_provider_from_secret(TENANT_SECRET),
        )
    elif broker_path:
        store = SqliteStore.open(broker_path, TENANT_SECRET)
    else:
        raise RuntimeError("BROKER_DSN or BROKER_DB is required")
    target = DurableTarget.open(target_path)
    try:
        claimed = await store.claim_due(
            worker_id,
            now=datetime.now(UTC),
            lease_for=timedelta(seconds=1),
            limit=1,
        )
        if not claimed:
            return 2

        effect = claimed[0]
        dispatching = await store.transition(
            effect.effect_id,
            expected_version=effect.version,
            target=EffectStatus.DISPATCHING,
            data={"worker_id": worker_id},
        )
        attempt_id = await store.start_attempt(
            dispatching.effect_id,
            expected_version=dispatching.version,
            worker_id=worker_id,
        )
        if failpoint == "before_target_commit":
            os._exit(137)

        external_id = target.commit(
            dispatching.downstream_key,
            {
                "request_hash": dispatching.request_hash,
                "arguments": dict(dispatching.request.arguments),
            },
        )
        if failpoint == "after_target_commit_before_receipt":
            os._exit(137)
        if failpoint != "none":
            raise ValueError(f"unknown FAILPOINT {failpoint!r}")

        await store.transition(
            dispatching.effect_id,
            expected_version=dispatching.version,
            target=EffectStatus.SUCCEEDED,
            data={
                "attempt_id": attempt_id,
                "external_id": external_id,
                "output": {"external_id": external_id},
            },
        )
        return 0
    finally:
        target.close()
        close = getattr(store, "close", None)
        if close is not None:
            close()
        else:
            await store.aclose()


def _required_env(name: str) -> str:
    value = os.environ.get(name)
    if value is None:
        raise RuntimeError(f"{name} is required")
    return value


def main() -> None:
    raise SystemExit(asyncio.run(run_once()))


if __name__ == "__main__":
    main()
