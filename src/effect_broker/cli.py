"""Operator CLI for effect-broker."""

from __future__ import annotations

import asyncio
import importlib
import json
import re
import subprocess
import sys
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import typer
from rich.console import Console
from rich.table import Table

from effect_broker.config import Settings, StoreKind, build_broker, load_contracts
from effect_broker.contracts import ContractRegistry
from effect_broker.engine import AdapterFor, EffectBroker
from effect_broker.errors import EffectBrokerError
from effect_broker.models import EffectRequest, EffectStatus
from effect_broker.worker import dispatch_once

app = typer.Typer(no_args_is_help=True)
contracts_app = typer.Typer(no_args_is_help=True)
app.add_typer(contracts_app, name="contracts")
console = Console()

_BROKER_CACHE: dict[tuple[str, str, str], EffectBroker] = {}


def _settings() -> Settings:
    return Settings()


def _broker(
    settings: Settings | None = None,
    adapter_for: AdapterFor | None = None,
) -> EffectBroker:
    settings = settings or _settings()
    key = (
        settings.store.value,
        str(settings.contracts_path.resolve()),
        str(settings.sqlite_path.resolve()),
        settings.adapter_factory or "",
    )
    if settings.store is StoreKind.MEMORY and adapter_for is None:
        broker = _BROKER_CACHE.get(key)
        if broker is None:
            broker = build_broker(settings)
            _BROKER_CACHE[key] = broker
        return broker
    return build_broker(settings, adapter_for)


@app.command()
def serve(
    host: str = "127.0.0.1",
    port: int = 8000,
) -> None:
    """Run the FastAPI process."""
    import uvicorn

    from effect_broker.api import create_app

    uvicorn.run(create_app(_broker()), host=host, port=port)


@app.command()
def worker(
    once: bool = False,
    interval_seconds: float = 1.0,
    adapter_factory: str | None = None,
    worker_id: str = "cli-worker",
    limit: int = 10,
) -> None:
    """Run the dispatch loop when an adapter is configured by the embedding app."""
    settings = _settings()
    factory = _adapter_factory(adapter_factory or settings.adapter_factory)
    broker = _broker(settings, factory)
    _run(
        _worker_loop(
            broker,
            factory,
            once=once,
            interval_seconds=interval_seconds,
            worker_id=worker_id,
            limit=limit,
        )
    )


@app.command()
def reconciler(
    once: bool = False,
    interval_seconds: float = 1.0,
    adapter_factory: str | None = None,
    limit: int = 10,
) -> None:
    """Run the reconcile loop when an adapter is configured by the embedding app."""
    settings = _settings()
    factory = _adapter_factory(adapter_factory or settings.adapter_factory)
    broker = _broker(settings, factory)
    _run(
        _reconciler_loop(
            broker,
            once=once,
            interval_seconds=interval_seconds,
            tenant_id=settings.dev_tenant_id,
            limit=limit,
        )
    )


@app.command()
def submit(
    operation_key: str = typer.Option(..., help="Stable business operation key."),
    tool: str = typer.Option(..., help="Registered tool name."),
    arguments: str = typer.Option("{}", help="JSON object with tool arguments."),
    requested_by: str = typer.Option("operator"),
    trace_id: str | None = None,
    json_output: bool = typer.Option(False, "--json"),
) -> None:
    """Submit one effect intent from JSON arguments."""
    try:
        parsed = json.loads(arguments)
        if not isinstance(parsed, dict):
            raise ValueError("arguments must decode to a JSON object")
        reservation = _run(
            _broker().submit(
                _settings().dev_tenant_id,
                EffectRequest(
                    operation_key=operation_key,
                    tool=tool,
                    arguments=parsed,
                    requested_by=requested_by,
                    trace_id=trace_id,
                ),
            )
        )
    except (EffectBrokerError, ValueError, json.JSONDecodeError) as exc:
        _fail(exc)
    payload = {
        "effect_id": reservation.effect.effect_id,
        "status": reservation.effect.status.value,
        "created": reservation.created,
        "replayed": reservation.replayed,
    }
    _emit(payload, json_output=json_output, table_factory=_reservation_table)


@app.command("inspect")
def inspect_effect(
    effect_id: str,
    json_output: bool = typer.Option(False, "--json"),
) -> None:
    """Inspect one effect without dispatching it."""
    try:
        effect = _run(_broker().get(_settings().dev_tenant_id, effect_id))
    except EffectBrokerError as exc:
        _fail(exc)
    _emit(_effect_dict(effect), json_output=json_output, table_factory=_effect_table)


@app.command("list")
def list_effects(
    status: EffectStatus | None = None,
    limit: int = 50,
    offset: int = 0,
    json_output: bool = typer.Option(False, "--json"),
) -> None:
    """List tenant-scoped effects, optionally filtered by status."""
    try:
        effects = _run(
            _broker().list(
                _settings().dev_tenant_id,
                status=status,
                limit=limit,
                offset=offset,
            )
        )
    except EffectBrokerError as exc:
        _fail(exc)
    payload = {"items": [_effect_dict(effect) for effect in effects]}
    _emit(payload, json_output=json_output, table_factory=_list_table)


@app.command()
def reconcile(
    effect_id: str,
    json_output: bool = typer.Option(False, "--json"),
) -> None:
    """Request reconciliation through the safety-class router."""
    try:
        effect = _run(_broker().reconcile(_settings().dev_tenant_id, effect_id))
    except (EffectBrokerError, RuntimeError) as exc:
        _fail(exc)
    _emit(_effect_dict(effect), json_output=json_output, table_factory=_effect_table)


@app.command("cancel")
def cancel_effect(
    effect_id: str,
    expected_version: int = typer.Option(..., min=0),
    actor: str = typer.Option("operator"),
    reason: str = typer.Option(...),
    json_output: bool = typer.Option(False, "--json"),
) -> None:
    """Cancel work that has not entered dispatch; races fail closed."""
    try:
        effect = _run(
            _broker().cancel(
                _settings().dev_tenant_id,
                effect_id,
                expected_version=expected_version,
                actor=actor,
                reason=reason,
            )
        )
    except EffectBrokerError as exc:
        _fail(exc)
    _emit(_effect_dict(effect), json_output=json_output, table_factory=_effect_table)


@contracts_app.command("validate")
def validate_contracts(path: Path) -> None:
    """Validate a contracts YAML file."""
    try:
        registry = ContractRegistry.from_yaml(path)
    except EffectBrokerError as exc:
        _fail(exc)
    console.print(f"contracts ok: {', '.join(registry.tools()) or '(none)'}")


@app.command()
def doctor(json_output: bool = typer.Option(False, "--json")) -> None:
    """Check config, contracts, store connectivity, and operation-key hygiene."""
    settings = _settings()
    warnings: list[str] = []
    try:
        settings.broker_secret_bytes()
    except (ValueError, TypeError) as exc:
        _fail(exc)
    if not settings.contracts_path.exists():
        warnings.append(f"contracts file not found: {settings.contracts_path}")
        registry = load_contracts(settings.contracts_path)
    else:
        try:
            registry = load_contracts(settings.contracts_path)
        except EffectBrokerError as exc:
            _fail(exc)
    try:
        broker = _broker(settings)
        effects = _run(broker.list(settings.dev_tenant_id, limit=500, offset=0))
    except EffectBrokerError as exc:
        _fail(exc)
    for effect in effects:
        operation_key = effect.request.operation_key
        if _risky_operation_key(operation_key):
            warnings.append(f"risky operation_key pattern: {operation_key}")
    payload = {
        "status": "ok" if not warnings else "warning",
        "store": settings.store.value,
        "contracts": registry.tools(),
        "warnings": warnings,
    }
    _emit(payload, json_output=json_output, table_factory=_doctor_table)


@app.command("crash-demo")
def crash_demo() -> None:
    """Run the subprocess crash matrix proof harness."""
    completed = subprocess.run(
        [sys.executable, "-m", "examples.crash_matrix.runner"],
        check=False,
        text=True,
    )
    raise typer.Exit(completed.returncode)


def _run(coro):  # noqa: ANN001, ANN201 - small asyncio bridge for Typer commands.
    return asyncio.run(coro)


async def _worker_loop(
    broker: EffectBroker,
    adapter_for: AdapterFor,
    *,
    once: bool,
    interval_seconds: float,
    worker_id: str,
    limit: int,
) -> None:
    while True:
        effects = await dispatch_once(
            broker.store,
            adapter_for,
            worker_id=worker_id,
            now=datetime.now(UTC),
            lease_for=timedelta(seconds=30),
            limit=limit,
        )
        for effect in effects:
            console.print(f"{effect.effect_id} {_status_text(effect.status.value)}")
        if once:
            return
        await asyncio.sleep(interval_seconds)


async def _reconciler_loop(
    broker: EffectBroker,
    *,
    once: bool,
    interval_seconds: float,
    tenant_id: str,
    limit: int,
) -> None:
    while True:
        effects = await broker.list(
            tenant_id,
            status=EffectStatus.OUTCOME_UNKNOWN,
            limit=limit,
            offset=0,
        )
        for effect in effects:
            reconciled = await broker.reconcile(tenant_id, effect.effect_id)
            console.print(
                f"{reconciled.effect_id} {_status_text(reconciled.status.value)}"
            )
        if once:
            return
        await asyncio.sleep(interval_seconds)


def _adapter_factory(path: str | None) -> AdapterFor:
    if not path:
        raise typer.BadParameter(
            "No adapter factory configured. Set EFFECT_BROKER_ADAPTER_FACTORY "
            "or pass --adapter-factory module:function."
        )
    try:
        module_name, function_name = path.split(":", maxsplit=1)
        factory = getattr(importlib.import_module(module_name), function_name)
    except (ImportError, AttributeError, ValueError) as exc:
        raise typer.BadParameter(f"invalid adapter factory import path: {path}") from exc
    return factory


def _fail(exc: BaseException) -> None:
    console.print(f"[red]{type(exc).__name__}[/red]: {exc}")
    raise typer.Exit(1) from exc


def _emit(
    payload: dict[str, Any],
    *,
    json_output: bool,
    table_factory: Callable[[dict[str, Any]], Table],
) -> None:
    if json_output:
        console.print(json.dumps(payload, sort_keys=True))
        return
    console.print(table_factory(payload))


def _reservation_table(payload: dict[str, Any]) -> Table:
    table = Table("effect_id", "status", "created", "replayed")
    table.add_row(
        payload["effect_id"],
        _status_text(payload["status"]),
        str(payload["created"]),
        str(payload["replayed"]),
    )
    return table


def _effect_table(payload: dict[str, Any]) -> Table:
    table = Table("field", "value")
    for key in (
        "effect_id",
        "status",
        "operation_key",
        "tool",
        "contract",
        "version",
        "created_at",
        "updated_at",
    ):
        value = payload[key]
        table.add_row(key, _status_text(value) if key == "status" else str(value))
    return table


def _list_table(payload: dict[str, Any]) -> Table:
    table = Table("effect_id", "status", "operation_key", "tool", "updated_at")
    for effect in payload["items"]:
        table.add_row(
            effect["effect_id"],
            _status_text(effect["status"]),
            effect["operation_key"],
            effect["tool"],
            effect["updated_at"],
        )
    return table


def _doctor_table(payload: dict[str, Any]) -> Table:
    table = Table("check", "value")
    table.add_row("status", payload["status"])
    table.add_row("store", payload["store"])
    table.add_row("contracts", ", ".join(payload["contracts"]) or "(none)")
    table.add_row("warnings", "\n".join(payload["warnings"]) or "(none)")
    return table


def _effect_dict(effect) -> dict[str, Any]:
    return {
        "effect_id": effect.effect_id,
        "status": effect.status.value,
        "operation_key": effect.request.operation_key,
        "tool": effect.request.tool,
        "contract": f"{effect.contract.name}@{effect.contract.version}",
        "safety": effect.contract.safety.value,
        "version": effect.version,
        "created_at": _format_time(effect.created_at),
        "updated_at": _format_time(effect.updated_at),
        "requested_by": effect.request.requested_by,
        "trace_id": effect.request.trace_id,
    }


def _status_text(status: str) -> str:
    if status in {EffectStatus.OUTCOME_UNKNOWN.value, EffectStatus.MANUAL_REVIEW.value}:
        return f"[bold yellow]{status}[/bold yellow]"
    return status


def _format_time(value: datetime) -> str:
    return value.astimezone(UTC).isoformat()


def _risky_operation_key(operation_key: str) -> bool:
    uuid_like = re.fullmatch(
        r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-"
        r"[0-9a-fA-F]{4}-[0-9a-fA-F]{12}",
        operation_key,
    )
    return ":" not in operation_key or uuid_like is not None
