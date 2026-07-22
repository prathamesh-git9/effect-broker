"""Prometheus metrics and redacted tracing for correctness events.

Telemetry must not become a second data leak. Metrics intentionally use only
low-cardinality labels, and spans expose a short digest of the operation key
instead of the raw key, arguments, outputs, or tenant id.
"""

from __future__ import annotations

import hashlib
import time
from collections.abc import Iterator
from contextlib import contextmanager, nullcontext
from datetime import UTC, datetime
from typing import Any

from prometheus_client import (
    CONTENT_TYPE_LATEST,
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
)

from effect_broker.models import EffectRecord, EffectStatus, Reservation

PROMETHEUS_CONTENT_TYPE = CONTENT_TYPE_LATEST

registry = CollectorRegistry(auto_describe=True)

_effects_total = Counter(
    "effects_total",
    "Effects observed by correctness result.",
    ("result",),
    registry=registry,
)
_outcome_unknown_effects = Gauge(
    "outcome_unknown_effects",
    "Current outcome_unknown effects by pinned contract and safety class.",
    ("contract", "safety"),
    registry=registry,
)
_outcome_unknown_oldest_age_seconds = Gauge(
    "outcome_unknown_oldest_age_seconds",
    "Age of the oldest observed outcome_unknown effect by contract and safety.",
    ("contract", "safety"),
    registry=registry,
)
_dispatch_attempts_total = Counter(
    "dispatch_attempts_total",
    "Target dispatch attempts by tool.",
    ("tool",),
    registry=registry,
)
_reconciliation_outcomes_total = Counter(
    "reconciliation_outcomes_total",
    "Reconciliation outcomes.",
    ("outcome",),
    registry=registry,
)
_lease_expirations_total = Counter(
    "lease_expirations_total",
    "Expired dispatch leases observed by workers or recovery.",
    registry=registry,
)
_fenced_stale_writes_total = Counter(
    "fenced_stale_writes_total",
    "Stale compare-and-swap writes rejected by fencing.",
    registry=registry,
)
_payload_conflicts_total = Counter(
    "payload_conflicts_total",
    "Operation-key payload drift conflicts.",
    registry=registry,
)
_op_latency_seconds = Histogram(
    "op_latency_seconds",
    "Broker operation latency.",
    ("op",),
    registry=registry,
)
_idempotency_retention_remaining_seconds = Gauge(
    "idempotency_retention_remaining_seconds",
    "Remaining target idempotency-key retention when retry is evaluated.",
    registry=registry,
)

_unknown_by_effect_id: dict[str, tuple[str, str, datetime]] = {}
_TRACER: Any | None = None
_TRACER_LOADED = False


def record_submit(reservation: Reservation) -> None:
    """Record reservation outcomes without tenant or operation-key labels."""
    result = "submitted" if reservation.created else "deduplicated"
    _effects_total.labels(result=result).inc()


def record_conflict() -> None:
    """Record payload drift, the correctness failure a 409 protects against."""
    _effects_total.labels(result="conflicted").inc()
    _payload_conflicts_total.inc()


def record_transition(before: EffectRecord, after: EffectRecord) -> None:
    """Record correctness-visible state transitions with low-cardinality labels."""
    if after.status is EffectStatus.SUCCEEDED and before.status is not after.status:
        _effects_total.labels(result="succeeded").inc()
    elif after.status is EffectStatus.FAILED_FINAL and before.status is not after.status:
        _effects_total.labels(result="failed").inc()
    elif (
        after.status is EffectStatus.OUTCOME_UNKNOWN
        and before.status is not after.status
    ):
        _effects_total.labels(result="unknown").inc()

    if before.status is not after.status:
        _track_unknown(before, after)


def record_dispatch_attempt(effect: EffectRecord) -> None:
    """Record a target call attempt by tool only."""
    _dispatch_attempts_total.labels(tool=effect.request.tool).inc()


def record_reconciliation_outcome(outcome: str) -> None:
    """Record a bounded reconciliation outcome label."""
    _reconciliation_outcomes_total.labels(outcome=outcome).inc()


def record_lease_expiration() -> None:
    """Record a dispatch lease expiry converted by recovery code."""
    _lease_expirations_total.inc()


def record_fenced_stale_write() -> None:
    """Record a stale worker write rejected by the store's CAS fence."""
    _fenced_stale_writes_total.inc()


def record_retention_remaining(effect: EffectRecord) -> None:
    """Record target key-retention remaining when an idempotent retry is checked."""
    retention = effect.contract.key_retention
    if retention is None:
        _idempotency_retention_remaining_seconds.set(0)
        return
    remaining = (effect.created_at + retention - datetime.now(UTC)).total_seconds()
    _idempotency_retention_remaining_seconds.set(remaining)


@contextmanager
def observe_latency(op: str) -> Iterator[None]:
    """Time a broker operation using a closed set of operation labels."""
    start = time.perf_counter()
    try:
        yield
    finally:
        _op_latency_seconds.labels(op=op).observe(time.perf_counter() - start)


def metrics_text() -> bytes:
    """Render the private Prometheus registry."""
    return generate_latest(registry)


@contextmanager
def span(
    name: str,
    effect: EffectRecord,
    *,
    receipt_id: str | None = None,
) -> Iterator[Any | None]:
    """Start an optional OpenTelemetry span with mandatory redaction."""
    tracer = _get_tracer()
    context = (
        nullcontext(None)
        if tracer is None
        else tracer.start_as_current_span(name)
    )
    with context as active:
        if active is not None:
            active.set_attribute(
                "effect_broker.operation_key_digest",
                operation_key_digest(effect.request.operation_key),
            )
            active.set_attribute("effect_broker.tool", effect.request.tool)
            active.set_attribute(
                "effect_broker.contract_version",
                effect.contract.version,
            )
            active.set_attribute("effect_broker.status", effect.status.value)
            active.set_attribute("effect_broker.receipt_id", receipt_id or "")
        yield active


def operation_key_digest(operation_key: str) -> str:
    """Return a short digest for telemetry; never emit the raw operation key."""
    return hashlib.sha256(operation_key.encode("utf-8")).hexdigest()[:16]


def reset_for_tests() -> None:
    """Reset metric samples and observed unknown state for isolated tests."""
    global registry
    global _dispatch_attempts_total
    global _effects_total
    global _fenced_stale_writes_total
    global _idempotency_retention_remaining_seconds
    global _lease_expirations_total
    global _op_latency_seconds
    global _outcome_unknown_effects
    global _outcome_unknown_oldest_age_seconds
    global _payload_conflicts_total
    global _reconciliation_outcomes_total

    registry = CollectorRegistry(auto_describe=True)
    _effects_total = Counter(
        "effects_total",
        "Effects observed by correctness result.",
        ("result",),
        registry=registry,
    )
    _outcome_unknown_effects = Gauge(
        "outcome_unknown_effects",
        "Current outcome_unknown effects by pinned contract and safety class.",
        ("contract", "safety"),
        registry=registry,
    )
    _outcome_unknown_oldest_age_seconds = Gauge(
        "outcome_unknown_oldest_age_seconds",
        "Age of the oldest observed outcome_unknown effect by contract and safety.",
        ("contract", "safety"),
        registry=registry,
    )
    _dispatch_attempts_total = Counter(
        "dispatch_attempts_total",
        "Target dispatch attempts by tool.",
        ("tool",),
        registry=registry,
    )
    _reconciliation_outcomes_total = Counter(
        "reconciliation_outcomes_total",
        "Reconciliation outcomes.",
        ("outcome",),
        registry=registry,
    )
    _lease_expirations_total = Counter(
        "lease_expirations_total",
        "Expired dispatch leases observed by workers or recovery.",
        registry=registry,
    )
    _fenced_stale_writes_total = Counter(
        "fenced_stale_writes_total",
        "Stale compare-and-swap writes rejected by fencing.",
        registry=registry,
    )
    _payload_conflicts_total = Counter(
        "payload_conflicts_total",
        "Operation-key payload drift conflicts.",
        registry=registry,
    )
    _op_latency_seconds = Histogram(
        "op_latency_seconds",
        "Broker operation latency.",
        ("op",),
        registry=registry,
    )
    _idempotency_retention_remaining_seconds = Gauge(
        "idempotency_retention_remaining_seconds",
        "Remaining target idempotency-key retention when retry is evaluated.",
        registry=registry,
    )
    _unknown_by_effect_id.clear()


def _track_unknown(before: EffectRecord, after: EffectRecord) -> None:
    if after.status is EffectStatus.OUTCOME_UNKNOWN:
        _unknown_by_effect_id[after.effect_id] = _unknown_key(after)
    elif before.status is EffectStatus.OUTCOME_UNKNOWN:
        _unknown_by_effect_id.pop(before.effect_id, None)

    labels = {
        _unknown_key(before)[:2],
        _unknown_key(after)[:2],
        *{
            (contract, safety)
            for contract, safety, _created_at in _unknown_by_effect_id.values()
        },
    }
    now = datetime.now(UTC)
    for contract, safety in labels:
        created_times = [
            created_at
            for item_contract, item_safety, created_at in _unknown_by_effect_id.values()
            if item_contract == contract and item_safety == safety
        ]
        _outcome_unknown_effects.labels(contract=contract, safety=safety).set(
            len(created_times)
        )
        age = 0.0
        if created_times:
            oldest = min(created_times)
            age = max(0.0, (now - oldest).total_seconds())
        _outcome_unknown_oldest_age_seconds.labels(
            contract=contract,
            safety=safety,
        ).set(age)


def _unknown_key(effect: EffectRecord) -> tuple[str, str, datetime]:
    return (
        effect.contract.name,
        effect.contract.safety.value,
        effect.created_at,
    )


def _get_tracer() -> Any | None:
    global _TRACER
    global _TRACER_LOADED
    if _TRACER_LOADED:
        return _TRACER
    _TRACER_LOADED = True
    try:
        from opentelemetry import trace
    except ImportError:
        _TRACER = None
    else:
        _TRACER = trace.get_tracer("effect_broker")
    return _TRACER
