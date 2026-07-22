"""The pure effect state machine.

Deliberately free of Postgres, HTTP, and FastAPI so every allowed and forbidden
transition can be checked exhaustively (including with a Hypothesis rule-based
machine). The single most important rule it enforces: a ``DISPATCHING`` effect
whose outcome is *ambiguous* (timeout, connection lost after send, worker death)
goes to ``OUTCOME_UNKNOWN`` — never straight back to a retry. Turning ambiguity
into a blind retry is exactly the bug the whole project exists to prevent.
"""

from __future__ import annotations

from effect_broker.errors import InvalidTransitionError
from effect_broker.models import (
    TERMINAL_STATUSES,
    EffectStatus,
    SafetyClass,
)

S = EffectStatus

# The transition graph. Each key maps to the statuses reachable from it.
_ALLOWED: dict[EffectStatus, frozenset[EffectStatus]] = {
    S.PREPARED: frozenset({S.DISPATCHING}),
    # A dispatch ends in exactly one of: proven success, proven non-commit
    # (final or retryable), or ambiguity.
    S.DISPATCHING: frozenset(
        {S.SUCCEEDED, S.FAILED_FINAL, S.RETRYABLE, S.OUTCOME_UNKNOWN}
    ),
    # A retryable (proven-not-committed) effect may be re-dispatched after
    # backoff, or given up on.
    S.RETRYABLE: frozenset({S.DISPATCHING, S.FAILED_FINAL}),
    # Ambiguity is resolved per safety class (see route_unknown): idempotent
    # re-dispatches with the same key, reconcilable probes, unsafe waits for an
    # operator.
    S.OUTCOME_UNKNOWN: frozenset(
        {S.DISPATCHING, S.RECONCILING, S.MANUAL_REVIEW}
    ),
    # A probe yields: committed (success), a conclusive not-committed (safe to
    # requeue), or no proof (operator review).
    S.RECONCILING: frozenset(
        {S.SUCCEEDED, S.PREPARED, S.MANUAL_REVIEW, S.FAILED_FINAL}
    ),
    # An operator resolves an unknown/review with evidence.
    S.MANUAL_REVIEW: frozenset({S.SUCCEEDED, S.FAILED_FINAL, S.COMPENSATED}),
    S.SUCCEEDED: frozenset(),
    S.FAILED_FINAL: frozenset(),
    S.COMPENSATED: frozenset(),
}


def is_terminal(status: EffectStatus) -> bool:
    return status in TERMINAL_STATUSES


def is_allowed(current: EffectStatus, target: EffectStatus) -> bool:
    return target in _ALLOWED[current]


def assert_transition(current: EffectStatus, target: EffectStatus) -> None:
    """Raise :class:`InvalidTransitionError` unless the transition is allowed."""
    if not is_allowed(current, target):
        raise InvalidTransitionError(current, target)


def route_unknown(safety: SafetyClass) -> EffectStatus:
    """Where an ``OUTCOME_UNKNOWN`` effect goes next, decided purely by class.

    This is the crux of the honesty guarantee, and it is code — never a model:

    - ``idempotent``: re-dispatch with the same downstream key (safe while the
      target's retention horizon holds); the caller of this function must still
      check retention before taking the ``DISPATCHING`` edge.
    - ``reconcilable``: probe the authoritative target before any re-dispatch.
    - ``transactional``: the shared transaction is authority; reconcile by
      reading it (modelled as a probe).
    - ``unsafe``: never re-dispatch — an operator resolves it. This is the only
      truthful answer when the target can neither dedupe nor prove its state.
    """
    if safety is SafetyClass.IDEMPOTENT:
        return S.DISPATCHING
    if safety in (SafetyClass.RECONCILABLE, SafetyClass.TRANSACTIONAL):
        return S.RECONCILING
    return S.MANUAL_REVIEW
