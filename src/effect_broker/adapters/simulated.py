"""Deterministic target adapter used by the crash-window proof tests.

The simulated target is authoritative state outside the broker. It can commit
and then lose the response, deduplicate idempotent retries by downstream key,
hide reconcilable commits until the settlement horizon, or refuse to prove
anything for unsafe effects. That lets tests exercise the real ambiguity rather
than a mocked success path.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

from effect_broker.adapters.base import AmbiguousDispatchError, EffectAdapter
from effect_broker.errors import AdapterError
from effect_broker.models import (
    DispatchResult,
    EffectContract,
    EffectRecord,
    ProbeResult,
    ProbeStatus,
    SafetyClass,
)


@dataclass(frozen=True, slots=True)
class SimulatedCharge:
    external_id: str
    downstream_key: str
    request_hash: str
    amount: object
    committed_at: datetime


class SimulatedTarget:
    """In-memory authoritative target with explicit crash-demo switches."""

    def __init__(self) -> None:
        self.charges: dict[str, SimulatedCharge] = {}
        self._by_downstream_key: dict[str, list[str]] = {}
        self._next_charge_id = 1
        self.lose_next_response_after_commit = False
        self.hide_commits_until_settled = False
        self.now = datetime.now(UTC)
        self.dispatch_count = 0

    def lose_next_response(self) -> None:
        """Make the next dispatch commit remotely and then look ambiguous."""
        self.lose_next_response_after_commit = True

    def set_now(self, now: datetime) -> None:
        self.now = now

    def commit(
        self,
        effect: EffectRecord,
        *,
        dedupe: bool,
    ) -> SimulatedCharge:
        self.dispatch_count += 1
        existing = self.first_by_downstream_key(effect.downstream_key)
        if existing is not None:
            if existing.request_hash != effect.request_hash:
                raise AdapterError("target rejected payload drift for reused key")
            if dedupe:
                return existing

        external_id = f"charge_{self._next_charge_id:06d}"
        self._next_charge_id += 1
        charge = SimulatedCharge(
            external_id=external_id,
            downstream_key=effect.downstream_key,
            request_hash=effect.request_hash,
            amount=effect.request.arguments.get("amount"),
            committed_at=self.now,
        )
        self.charges[external_id] = charge
        self._by_downstream_key.setdefault(effect.downstream_key, []).append(external_id)
        return charge

    def first_by_downstream_key(self, downstream_key: str) -> SimulatedCharge | None:
        ids = self._by_downstream_key.get(downstream_key, [])
        if not ids:
            return None
        return self.charges[ids[0]]

    def charges_for_key(self, downstream_key: str) -> list[SimulatedCharge]:
        return [
            self.charges[external_id]
            for external_id in self._by_downstream_key.get(downstream_key, [])
        ]


class SimulatedAdapter(EffectAdapter):
    """Adapter whose behavior is selected by the pinned safety contract."""

    def __init__(
        self,
        target: SimulatedTarget,
        contract: EffectContract,
    ) -> None:
        self.target = target
        self.contract = contract

    async def dispatch(
        self,
        effect: EffectRecord,
        *,
        attempt_id: str,
    ) -> DispatchResult:
        del attempt_id
        dedupe = effect.contract.safety in {
            SafetyClass.IDEMPOTENT,
            SafetyClass.TRANSACTIONAL,
        }
        charge = self.target.commit(effect, dedupe=dedupe)
        if self.target.lose_next_response_after_commit:
            self.target.lose_next_response_after_commit = False
            raise AmbiguousDispatchError("response lost after target commit")
        return DispatchResult(
            external_id=charge.external_id,
            output={"external_id": charge.external_id},
            committed=True,
        )

    async def probe(self, effect: EffectRecord) -> ProbeResult:
        if effect.contract.safety is SafetyClass.UNSAFE:
            return ProbeResult(
                status=ProbeStatus.UNKNOWN,
                evidence={"reason": "unsafe target has no authoritative lookup"},
            )

        charge = self.target.first_by_downstream_key(effect.downstream_key)
        settled = self._settlement_elapsed(effect)
        if charge is not None and (not self.target.hide_commits_until_settled or settled):
            return ProbeResult(
                status=ProbeStatus.COMMITTED,
                external_id=charge.external_id,
                output={"external_id": charge.external_id},
                evidence={"downstream_key": effect.downstream_key},
            )

        if effect.contract.safety is SafetyClass.RECONCILABLE and settled:
            return ProbeResult(
                status=ProbeStatus.NOT_COMMITTED,
                evidence={"settlement_bound_elapsed": True},
            )

        return ProbeResult(
            status=ProbeStatus.UNKNOWN,
            evidence={"settlement_bound_elapsed": settled},
        )

    def _settlement_elapsed(self, effect: EffectRecord) -> bool:
        bound = effect.contract.settlement_bound
        if bound is None:
            return False
        return self.target.now >= effect.created_at + bound


_DEFAULT_TARGET = SimulatedTarget()


def default_adapter_for(effect: EffectRecord) -> SimulatedAdapter:
    """Return a process-local simulated target adapter for docker-compose demos."""

    return SimulatedAdapter(_DEFAULT_TARGET, effect.contract)
