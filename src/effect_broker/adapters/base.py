"""Adapter protocol and conformance checks.

Adapters own target-specific truth: how to pass the downstream key, how to
recognize a committed response, and when a probe is authoritative. They do not
own retry policy. A timeout or lost response must surface as ambiguity so the
worker can persist ``OUTCOME_UNKNOWN`` instead of issuing a blind retry.
"""

from __future__ import annotations

from dataclasses import replace
from typing import Protocol

from effect_broker.errors import AdapterError
from effect_broker.models import (
    DispatchResult,
    EffectContract,
    EffectRecord,
    ProbeResult,
    ProbeStatus,
)


class AmbiguousDispatchError(RuntimeError):
    """The target may have committed, but the adapter has no authoritative proof."""


class EffectAdapter(Protocol):
    contract: EffectContract

    async def dispatch(
        self,
        effect: EffectRecord,
        *,
        attempt_id: str,
    ) -> DispatchResult:
        """Execute once using ``effect.downstream_key``; SDK retries are disabled."""

    async def probe(self, effect: EffectRecord) -> ProbeResult:
        """Return authoritative evidence, or UNKNOWN when proof is unavailable."""


class AdapterConformance:
    """Reusable checks every real adapter should be able to satisfy.

    These helpers are deliberately small and assertion-based so an adapter test
    suite can call the relevant checks with fixture-built ``EffectRecord``
    values. The helpers check behavior, not implementation: stable key reuse,
    payload drift rejection, and the three probe outcomes.
    """

    @staticmethod
    async def stable_key_reuse(
        adapter: EffectAdapter,
        effect: EffectRecord,
    ) -> None:
        first = await adapter.dispatch(effect, attempt_id="attempt-1")
        second = await adapter.dispatch(effect, attempt_id="attempt-2")
        assert first.committed
        assert second.committed
        assert first.external_id == second.external_id

    @staticmethod
    async def payload_drift_rejection(
        adapter: EffectAdapter,
        effect: EffectRecord,
    ) -> None:
        await adapter.dispatch(effect, attempt_id="attempt-1")
        drifted = replace(effect, request_hash=f"{effect.request_hash}-drift")
        try:
            await adapter.dispatch(drifted, attempt_id="attempt-2")
        except AdapterError:
            return
        raise AssertionError("adapter accepted payload drift for a reused key")

    @staticmethod
    async def positive_probe(
        adapter: EffectAdapter,
        effect: EffectRecord,
    ) -> None:
        dispatched = await adapter.dispatch(effect, attempt_id="attempt-1")
        probe = await adapter.probe(effect)
        assert dispatched.committed
        assert probe.status is ProbeStatus.COMMITTED
        assert probe.external_id == dispatched.external_id

    @staticmethod
    async def negative_probe(
        adapter: EffectAdapter,
        effect: EffectRecord,
    ) -> None:
        probe = await adapter.probe(effect)
        assert probe.status is ProbeStatus.NOT_COMMITTED

    @staticmethod
    async def unknown_probe(
        adapter: EffectAdapter,
        effect: EffectRecord,
    ) -> None:
        probe = await adapter.probe(effect)
        assert probe.status is ProbeStatus.UNKNOWN
