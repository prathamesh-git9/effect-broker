"""Tool-contract validation and loading.

A contract is a *promise about the target*, and the broker refuses to hold a
promise the target cannot keep. So validation is where several of the guarantee
table's honesty constraints are mechanically enforced: an ``idempotent`` contract
without a retention horizon, or a ``reconcilable`` contract without a settlement
bound, is rejected rather than quietly downgraded — because either would let the
broker "guarantee" something it has no basis for.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import timedelta
from pathlib import Path
from typing import Any

import yaml

from effect_broker.errors import ContractError
from effect_broker.models import EffectContract, SafetyClass


def validate_contract(contract: EffectContract) -> EffectContract:
    """Return ``contract`` unchanged if valid, else raise :class:`ContractError`."""
    if contract.retry_limit < 0:
        raise ContractError(f"{contract.name}: retry_limit must be >= 0")

    if contract.safety is SafetyClass.IDEMPOTENT:
        if contract.key_retention is None or contract.key_retention <= timedelta(0):
            raise ContractError(
                f"{contract.name}: idempotent contracts require a positive "
                "key_retention (a redispatch after the target forgets the key "
                "would create a second effect)"
            )
    elif contract.safety is SafetyClass.RECONCILABLE:
        if contract.settlement_bound is None or contract.settlement_bound < timedelta(0):
            raise ContractError(
                f"{contract.name}: reconcilable contracts require a settlement_bound "
                "(without a maximum settlement interval a probe can only ever "
                "discover success, never prove absence)"
            )
    elif contract.safety is SafetyClass.UNSAFE and contract.retry_limit != 0:
        # An unsafe target has no way to make a second dispatch safe, so the only
        # honest retry_limit is zero: at most one dispatch, then outcome_unknown.
        raise ContractError(
            f"{contract.name}: unsafe contracts must have retry_limit=0 "
            "(there is no safe redispatch for a target that cannot dedupe or "
            "prove its state)"
        )
    return contract


def contract_from_mapping(name: str, data: Mapping[str, Any]) -> EffectContract:
    """Build and validate a contract from a config mapping.

    Durations are given in seconds in config for unambiguous, timezone-free
    round-tripping.
    """
    try:
        safety = SafetyClass(data["safety"])
    except (KeyError, ValueError) as exc:
        raise ContractError(f"{name}: missing or unknown 'safety'") from exc

    def _duration(field: str) -> timedelta | None:
        seconds = data.get(field)
        return None if seconds is None else timedelta(seconds=float(seconds))

    contract = EffectContract(
        name=name,
        version=str(data.get("version", "v1")),
        safety=safety,
        retry_limit=int(data.get("retry_limit", 0)),
        key_retention=_duration("key_retention_seconds"),
        settlement_bound=_duration("settlement_bound_seconds"),
    )
    return validate_contract(contract)


class ContractRegistry:
    """An immutable, validated set of tool contracts loaded once at startup.

    Contracts are pinned into each effect at reservation time, so a later reload
    of this registry never changes the retry semantics of an in-flight effect.
    """

    def __init__(self, contracts: Mapping[str, EffectContract]) -> None:
        self._by_tool = dict(contracts)

    def get(self, tool: str) -> EffectContract:
        try:
            return self._by_tool[tool]
        except KeyError as exc:
            raise ContractError(f"no contract registered for tool {tool!r}") from exc

    def tools(self) -> list[str]:
        return sorted(self._by_tool)

    @classmethod
    def from_mapping(cls, data: Mapping[str, Mapping[str, Any]]) -> ContractRegistry:
        return cls(
            {tool: contract_from_mapping(tool, spec) for tool, spec in data.items()}
        )

    @classmethod
    def from_yaml(cls, path: str | Path) -> ContractRegistry:
        raw = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
        return cls.from_mapping(raw.get("tools", raw))
