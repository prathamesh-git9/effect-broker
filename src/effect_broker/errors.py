"""Stable domain error taxonomy.

Errors are part of the contract, not incidental strings. In particular, a
payload conflict and an invalid transition are *expected* operational outcomes
the API surfaces deliberately (a 409, a refusal to dispatch) — never swallowed —
because the whole product is about refusing to do the unsafe thing rather than
guessing.
"""

from __future__ import annotations


class EffectBrokerError(Exception):
    """Base class for all domain errors."""


class CanonicalizationError(EffectBrokerError):
    """A value cannot be canonicalized reproducibly (bad key, NaN, unknown type)."""


class ContractError(EffectBrokerError):
    """A tool contract is missing, malformed, or self-inconsistent.

    e.g. an ``idempotent`` contract without a ``key_retention`` horizon, or a
    ``reconcilable`` contract without a ``settlement_bound`` — both would let the
    broker promise a guarantee it cannot actually keep.
    """


class PayloadConflictError(EffectBrokerError):
    """The operation key was reused with a different request hash.

    Surfaces as HTTP 409. The broker never dispatches: the same business intent
    arriving with changed arguments or a different contract version is a caller
    bug, not a duplicate to collapse onto the in-flight effect.
    """

    def __init__(self, operation_key: str, existing_hash: str, incoming_hash: str):
        super().__init__(
            f"operation_key {operation_key!r} already exists with a different "
            f"payload (existing {existing_hash[:12]}, incoming {incoming_hash[:12]})"
        )
        self.operation_key = operation_key
        self.existing_hash = existing_hash
        self.incoming_hash = incoming_hash


class InvalidTransitionError(EffectBrokerError):
    """A requested status transition is not allowed from the current status."""

    def __init__(self, current, target):  # noqa: ANN001 - EffectStatus, avoid cycle
        super().__init__(f"cannot transition from {current} to {target}")
        self.current = current
        self.target = target


class VersionConflictError(EffectBrokerError):
    """A compare-and-swap failed: another worker advanced the effect first.

    This is how a stale/zombie worker is fenced out — it may still finish network
    I/O, but its local write is rejected because the expected version is no longer
    current.
    """


class UnknownEffectError(EffectBrokerError):
    """No effect exists for the given tenant and id."""


class AdapterError(EffectBrokerError):
    """An adapter failed in a way that is a definite, proven non-commit.

    Ambiguous failures (timeout, connection reset after send) must NOT raise this
    — they are ``OUTCOME_UNKNOWN``, not failures.
    """


class AuthorizationError(EffectBrokerError):
    """The request is not authorized for the addressed tenant."""
