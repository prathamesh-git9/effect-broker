"""Domain vocabulary for the effect broker.

These types encode the project's central honesty: an *attempt* (an HTTP request,
a handler entry) is not the same as an observable *business effect* (a charge, an
email). The guarantee the broker offers is "exactly one observable effect", and
only under a target contract that can actually support it. The four
``SafetyClass`` values name exactly how much a target can prove, and the status
machine keeps ``OUTCOME_UNKNOWN`` a first-class, durable state rather than an
exception string — because for an uncooperative target that is the only truthful
answer.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import StrEnum
from typing import Any

# A JSON object with string keys. We never accept non-string keys, NaN, or
# infinities (see canonical.py) because a fingerprint that cannot be reproduced
# byte-for-byte across processes would make deduplication unsound.
JsonObject = Mapping[str, Any]


class SafetyClass(StrEnum):
    """How much the downstream target can prove about a committed effect.

    The broker's behaviour is a pure function of this class and persisted state —
    never of a model's judgement. A more capable model may pick a better tool and
    still double-charge when infrastructure retries the correctly chosen call, so
    the safety decision must live in code over the pinned contract.
    """

    TRANSACTIONAL = "transactional"  # shares one DB transaction with the broker
    IDEMPOTENT = "idempotent"        # honours a stable key for a retention horizon
    RECONCILABLE = "reconcilable"    # authoritative lookup, bounded settlement
    UNSAFE = "unsafe"                # no key, no lookup -> at most once, else unknown


class EffectStatus(StrEnum):
    PREPARED = "prepared"
    DISPATCHING = "dispatching"
    RETRYABLE = "retryable"
    OUTCOME_UNKNOWN = "outcome_unknown"
    RECONCILING = "reconciling"
    SUCCEEDED = "succeeded"
    FAILED_FINAL = "failed_final"
    MANUAL_REVIEW = "manual_review"
    COMPENSATED = "compensated"


#: Statuses from which no automatic transition is allowed.
TERMINAL_STATUSES = frozenset(
    {EffectStatus.SUCCEEDED, EffectStatus.FAILED_FINAL, EffectStatus.COMPENSATED}
)


@dataclass(frozen=True, slots=True)
class EffectRequest:
    """What a caller asks the broker to make happen, exactly once observably.

    ``operation_key`` is mandatory and must name *business intent* (e.g.
    ``agent-run:run_789:step:refund_03``). The broker never hashes arguments and
    assumes identical payloads mean identical intent: two legitimate $10 refunds
    are different operations, so the caller — not the broker — owns the key.
    """

    operation_key: str
    tool: str
    arguments: JsonObject
    requested_by: str
    trace_id: str | None = None


@dataclass(frozen=True, slots=True)
class EffectContract:
    """The pinned, versioned promise a target makes for one tool.

    Pinned at reservation time so a redeploy cannot silently change retry
    semantics for an in-flight effect. ``key_retention`` and ``settlement_bound``
    are required for the classes that depend on them (validated in contracts.py):
    an ``idempotent`` target whose key expires before the broker's recovery
    horizon is not safe to retry, and a ``reconcilable`` probe that can only ever
    discover success (never prove absence) cannot conclude ``not_committed``.
    """

    name: str
    version: str
    safety: SafetyClass
    retry_limit: int
    key_retention: timedelta | None = None
    settlement_bound: timedelta | None = None


@dataclass(frozen=True, slots=True)
class EffectRecord:
    """The full persisted state of one effect. Returned by the store."""

    effect_id: str
    tenant_id: str
    request: EffectRequest
    request_hash: str
    downstream_key: str
    contract: EffectContract
    status: EffectStatus
    version: int
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class Reservation:
    """The outcome of ``submit``.

    ``created`` is False and ``replayed`` True when an identical
    ``(tenant_id, operation_key)`` with the same request hash was already
    reserved — the caller re-submitting after a crash gets the existing effect,
    not a second one.
    """

    effect: EffectRecord
    created: bool
    replayed: bool


@dataclass(frozen=True, slots=True)
class DispatchResult:
    """What an adapter reports after attempting the target call.

    ``committed`` means the adapter has authoritative evidence the effect
    happened (a success response tied to the downstream key). A timeout or a
    connection dropped after bytes were sent is NOT ``committed`` and is NOT a
    failure — it is ambiguity, which the worker turns into ``OUTCOME_UNKNOWN``.
    """

    external_id: str | None
    output: JsonObject
    committed: bool


class ProbeStatus(StrEnum):
    COMMITTED = "committed"
    NOT_COMMITTED = "not_committed"  # only valid after the settlement bound
    UNKNOWN = "unknown"


@dataclass(frozen=True, slots=True)
class ProbeResult:
    """Authoritative evidence from reconciliation, or UNKNOWN when unavailable.

    A ``NOT_COMMITTED`` result is only permitted for a ``reconcilable`` target
    whose contract declares a maximum settlement interval and only after that
    interval has elapsed; an eventually-consistent search that can lag is not
    enough to prove absence, so it must return UNKNOWN instead.
    """

    status: ProbeStatus
    external_id: str | None = None
    output: JsonObject | None = None
    evidence: JsonObject = field(default_factory=dict)
