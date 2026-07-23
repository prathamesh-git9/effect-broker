"""MCP management and proxy surface.

The MCP server is intentionally an integration boundary, not a policy engine.
Models supply operation identity and business arguments; configured contracts
decide safety and retry behavior in the broker.
"""

from __future__ import annotations

from typing import Any

from effect_broker.engine import EffectBroker
from effect_broker.models import EffectRequest, EffectStatus


def create_mcp_server(
    broker: EffectBroker,
    *,
    tenant_id: str = "tenant-dev",
):
    """Create an MCP server with management tools and a mutating proxy concept."""
    try:
        from mcp.server.fastmcp import FastMCP
    except ImportError as exc:  # pragma: no cover - optional dependency.
        raise RuntimeError("Install effect-broker[mcp] to run the MCP server") from exc

    mcp = FastMCP("effect-broker")

    @mcp.tool()
    async def submit_effect(
        operation_key: str,
        tool: str,
        arguments: dict[str, Any],
        requested_by: str = "mcp-agent",
        trace_id: str | None = None,
    ) -> dict[str, Any]:
        """Reserve a brokered effect; map the agent run/step id to operation_key."""
        reservation = await broker.submit(
            tenant_id,
            EffectRequest(
                operation_key=operation_key,
                tool=tool,
                arguments=arguments,
                requested_by=requested_by,
                trace_id=trace_id,
            ),
        )
        return {
            "effect_id": reservation.effect.effect_id,
            "status": reservation.effect.status.value,
            "created": reservation.created,
            "replayed": reservation.replayed,
        }

    @mcp.tool()
    async def get_effect(effect_id: str) -> dict[str, Any]:
        """Inspect one tenant-scoped effect."""
        effect = await broker.get(tenant_id, effect_id)
        return _effect_dict(effect)

    @mcp.tool()
    async def reconcile_effect(effect_id: str) -> dict[str, Any]:
        """Request reconciliation through the broker's pinned safety contract."""
        effect = await broker.reconcile(tenant_id, effect_id)
        return _effect_dict(effect)

    @mcp.tool()
    async def cancel_effect(
        effect_id: str,
        expected_version: int,
        actor: str,
        reason: str,
    ) -> dict[str, Any]:
        """Cancel prepared/retryable work; fails if dispatch won the CAS race."""
        effect = await broker.cancel(
            tenant_id,
            effect_id,
            expected_version=expected_version,
            actor=actor,
            reason=reason,
        )
        return _effect_dict(effect)

    @mcp.tool()
    async def list_effects(
        status: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        """List effects; status may be outcome_unknown for operator queues."""
        parsed = EffectStatus(status) if status is not None else None
        effects = await broker.list(tenant_id, status=parsed, limit=limit, offset=offset)
        return [_effect_dict(effect) for effect in effects]

    @mcp.tool()
    async def brokered_tool_call(
        operation_key: str,
        tool: str,
        arguments: dict[str, Any],
        requested_by: str = "mcp-agent",
    ) -> dict[str, Any]:
        """Proxy convention for mutating tools: operation_key is required.

        Agents should derive it from the run and step id. The model never
        chooses safety class or retry policy; the configured contract for
        ``tool`` is pinned by the broker.
        """
        return await submit_effect(
            operation_key=operation_key,
            tool=tool,
            arguments=arguments,
            requested_by=requested_by,
        )

    return mcp


def _effect_dict(effect) -> dict[str, Any]:  # noqa: ANN001
    return {
        "effect_id": effect.effect_id,
        "status": effect.status.value,
        "operation_key": effect.request.operation_key,
        "tool": effect.request.tool,
        "contract": effect.contract.name,
        "contract_version": effect.contract.version,
        "safety": effect.contract.safety.value,
        "version": effect.version,
        "created_at": effect.created_at.isoformat(),
        "updated_at": effect.updated_at.isoformat(),
    }
