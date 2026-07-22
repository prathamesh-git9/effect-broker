"""Tests for the correctness-critical core: canonicalization, the state machine,
and contract validation. These are the pieces a subtle bug in would silently
break the exactly-one-effect guarantee, so they are exhaustive and pedantic.
"""

from __future__ import annotations

import math
from datetime import timedelta

import pytest

from effect_broker.canonical import canonical_json, downstream_key, request_hash
from effect_broker.contracts import validate_contract
from effect_broker.errors import (
    CanonicalizationError,
    ContractError,
    InvalidTransitionError,
)
from effect_broker.models import EffectContract, EffectStatus, SafetyClass
from effect_broker.statemachine import (
    assert_transition,
    is_allowed,
    is_terminal,
    route_unknown,
)

S = EffectStatus

# --- canonicalization --------------------------------------------------------


def test_canonical_is_key_order_independent():
    a = canonical_json({"b": 1, "a": 2, "c": {"y": 1, "x": 2}})
    b = canonical_json({"c": {"x": 2, "y": 1}, "a": 2, "b": 1})
    assert a == b == '{"a":2,"b":1,"c":{"x":2,"y":1}}'


def test_canonical_rejects_non_finite_float():
    for bad in (float("nan"), math.inf, -math.inf):
        with pytest.raises(CanonicalizationError):
            canonical_json({"x": bad})


def test_canonical_rejects_non_string_keys():
    with pytest.raises(CanonicalizationError):
        canonical_json({1: "a"})


def test_canonical_rejects_bytes_and_unknown_types():
    with pytest.raises(CanonicalizationError):
        canonical_json({"x": b"bytes"})
    with pytest.raises(CanonicalizationError):
        canonical_json({"x": object()})


def test_request_hash_stable_and_payload_sensitive():
    h1 = request_hash("charge", {"amount": 10, "cur": "usd"}, "pay", "v1")
    h2 = request_hash("charge", {"cur": "usd", "amount": 10}, "pay", "v1")
    h3 = request_hash("charge", {"amount": 11, "cur": "usd"}, "pay", "v1")
    assert h1 == h2  # key order does not change identity
    assert h1 != h3  # a different amount is a different operation
    assert request_hash("charge", {"amount": 10}, "pay", "v1") != request_hash(
        "charge", {"amount": 10}, "pay", "v2"
    )  # contract version is bound into identity


def test_downstream_key_is_deterministic_secret_and_separated():
    k = b"tenant-secret"
    assert downstream_key(k, "t1", "op:1") == downstream_key(k, "t1", "op:1")
    assert downstream_key(k, "t1", "op:1") != downstream_key(b"other", "t1", "op:1")
    # NUL separation: (a, bc) must not collide with (ab, c).
    assert downstream_key(k, "a", "bc") != downstream_key(k, "ab", "c")


# --- state machine -----------------------------------------------------------


def test_ambiguity_never_becomes_a_plain_retry():
    # The core invariant: a dispatch can go to OUTCOME_UNKNOWN, and from there it
    # must NOT jump straight to SUCCEEDED or back to DISPATCHING-as-success. The
    # only success paths out of unknown go through reconciliation.
    assert is_allowed(S.DISPATCHING, S.OUTCOME_UNKNOWN)
    assert not is_allowed(S.OUTCOME_UNKNOWN, S.SUCCEEDED)
    assert is_allowed(S.OUTCOME_UNKNOWN, S.RECONCILING)
    assert is_allowed(S.RECONCILING, S.SUCCEEDED)


def test_terminal_statuses_have_no_exits():
    for terminal in (S.SUCCEEDED, S.FAILED_FINAL, S.COMPENSATED):
        assert is_terminal(terminal)
        for target in S:
            assert not is_allowed(terminal, target)


def test_assert_transition_raises_on_forbidden():
    assert_transition(S.PREPARED, S.DISPATCHING)  # no raise
    with pytest.raises(InvalidTransitionError):
        assert_transition(S.PREPARED, S.SUCCEEDED)


def test_route_unknown_by_safety_class():
    assert route_unknown(SafetyClass.IDEMPOTENT) is S.DISPATCHING
    assert route_unknown(SafetyClass.RECONCILABLE) is S.RECONCILING
    assert route_unknown(SafetyClass.TRANSACTIONAL) is S.RECONCILING
    # The honest one: an unsafe target's ambiguity is never auto-retried.
    assert route_unknown(SafetyClass.UNSAFE) is S.MANUAL_REVIEW


# --- contract validation -----------------------------------------------------


def _contract(safety, **kw) -> EffectContract:
    base = {"name": "t", "version": "v1", "safety": safety, "retry_limit": 0}
    base.update(kw)
    return EffectContract(**base)


def test_idempotent_requires_retention():
    with pytest.raises(ContractError):
        validate_contract(_contract(SafetyClass.IDEMPOTENT))
    validate_contract(
        _contract(
            SafetyClass.IDEMPOTENT, retry_limit=3, key_retention=timedelta(hours=24)
        )
    )  # no raise


def test_reconcilable_requires_settlement_bound():
    with pytest.raises(ContractError):
        validate_contract(_contract(SafetyClass.RECONCILABLE, retry_limit=3))
    validate_contract(
        _contract(
            SafetyClass.RECONCILABLE, retry_limit=3, settlement_bound=timedelta(minutes=5)
        )
    )


def test_unsafe_must_not_allow_retries():
    with pytest.raises(ContractError):
        validate_contract(_contract(SafetyClass.UNSAFE, retry_limit=1))
    validate_contract(_contract(SafetyClass.UNSAFE, retry_limit=0))  # no raise
