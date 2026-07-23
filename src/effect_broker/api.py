"""FastAPI surface for reserving and inspecting effects.

The HTTP process accepts tenant-scoped intents and exposes read/repair
operations. It deliberately does not dispatch work; workers own adapter I/O.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from effect_broker.auth import APIKeyAuthenticator, require_tenant
from effect_broker.config import Settings
from effect_broker.engine import EffectBroker
from effect_broker.errors import (
    CanonicalizationError,
    ContractError,
    EffectBrokerError,
    InvalidTransitionError,
    PayloadConflictError,
    UnknownEffectError,
    VersionConflictError,
)
from effect_broker.models import EffectRecord, EffectRequest, EffectStatus
from effect_broker.observability import PROMETHEUS_CONTENT_TYPE, metrics_text


class SubmitEffectBody(BaseModel):
    operation_key: str
    tool: str
    arguments: dict[str, Any]
    requested_by: str
    trace_id: str | None = None


class ContractBody(BaseModel):
    name: str
    version: str
    safety: str
    retry_limit: int
    key_retention_seconds: float | None = None
    settlement_bound_seconds: float | None = None


class EffectBody(BaseModel):
    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "effect_id": "eff_000123",
                    "status": "outcome_unknown",
                    "contract": {
                        "name": "charge",
                        "version": "v1",
                        "safety": "unsafe",
                        "retry_limit": 0,
                    },
                    "version": 3,
                    "created_at": "2026-07-22T10:00:00Z",
                    "updated_at": "2026-07-22T10:01:00Z",
                    "operation_key": "agent-run:run_789:step:charge_03",
                    "tool": "charge",
                    "requested_by": "agent",
                    "trace_id": "trace-1",
                }
            ]
        }
    )

    effect_id: str
    status: EffectStatus
    contract: ContractBody
    version: int
    created_at: datetime
    updated_at: datetime
    operation_key: str
    tool: str
    requested_by: str
    trace_id: str | None


class ReservationBody(BaseModel):
    effect_id: str
    status: EffectStatus
    created: bool
    replayed: bool


class ListEffectsBody(BaseModel):
    items: list[EffectBody]
    limit: int
    offset: int


class ResolveEffectBody(BaseModel):
    resolution: Literal["succeeded", "failed_final", "compensated"]
    evidence: dict[str, Any] = Field(default_factory=dict)


class CancelEffectBody(BaseModel):
    expected_version: int = Field(ge=0)
    actor: str = Field(min_length=1)
    reason: str = Field(min_length=1)


class ReconcileBody(BaseModel):
    effect_id: str
    status: EffectStatus


def create_app(
    broker: EffectBroker,
    authenticator: APIKeyAuthenticator | None = None,
):
    """Create a FastAPI app over an existing broker instance."""
    from fastapi import Depends, FastAPI, Query, status
    from fastapi.responses import JSONResponse, Response

    status_query = Query(default=None, alias="status")
    limit_query = Query(default=50, ge=1, le=500)
    offset_query = Query(default=0, ge=0)

    app = FastAPI(title="effect-broker")
    app.state.broker = broker
    app.state.authenticator = authenticator or APIKeyAuthenticator.from_settings(
        Settings()
    )

    @app.exception_handler(EffectBrokerError)
    async def domain_error_handler(_request, exc: EffectBrokerError):
        status_code = _status_for_error(exc)
        return JSONResponse(
            status_code=status_code,
            media_type="application/problem+json",
            content={
                "type": f"https://effect-broker.local/problems/{type(exc).__name__}",
                "title": _title_for_error(exc),
                "status": status_code,
                "detail": str(exc),
            },
        )

    @app.post(
        "/effects",
        response_model=ReservationBody,
        status_code=status.HTTP_201_CREATED,
        responses={
            409: {
                "description": "Payload conflict",
                "content": {
                    "application/problem+json": {
                        "example": {
                            "type": (
                                "https://effect-broker.local/problems/"
                                "PayloadConflictError"
                            ),
                            "title": "Payload conflict",
                            "status": 409,
                            "detail": (
                                "operation_key 'order:42:charge:v1' already "
                                "exists with a different payload"
                            ),
                        }
                    }
                },
            }
        },
    )
    async def submit_effect(
        body: SubmitEffectBody,
        tenant_id: str = Depends(require_tenant),
    ) -> ReservationBody:
        reservation = await app.state.broker.submit(
            tenant_id,
            EffectRequest(
                operation_key=body.operation_key,
                tool=body.tool,
                arguments=body.arguments,
                requested_by=body.requested_by,
                trace_id=body.trace_id,
            ),
        )
        return ReservationBody(
            effect_id=reservation.effect.effect_id,
            status=reservation.effect.status,
            created=reservation.created,
            replayed=reservation.replayed,
        )

    @app.get("/effects/{effect_id}", response_model=EffectBody)
    async def get_effect(
        effect_id: str,
        tenant_id: str = Depends(require_tenant),
    ) -> EffectBody:
        return _effect_body(await app.state.broker.get(tenant_id, effect_id))

    @app.get("/effects/{effect_id}/receipt")
    async def get_receipt(
        effect_id: str,
        tenant_id: str = Depends(require_tenant),
    ) -> dict[str, Any]:
        return dict(await app.state.broker.replay_receipt(tenant_id, effect_id))

    @app.post("/effects/{effect_id}/reconcile", response_model=ReconcileBody)
    async def reconcile_effect(
        effect_id: str,
        tenant_id: str = Depends(require_tenant),
    ) -> ReconcileBody:
        effect = await app.state.broker.reconcile(tenant_id, effect_id)
        return ReconcileBody(effect_id=effect.effect_id, status=effect.status)

    @app.get("/effects", response_model=ListEffectsBody)
    async def list_effects(
        status_filter: EffectStatus | None = status_query,
        limit: int = limit_query,
        offset: int = offset_query,
        tenant_id: str = Depends(require_tenant),
    ) -> ListEffectsBody:
        effects = await app.state.broker.list(
            tenant_id,
            status=status_filter,
            limit=limit,
            offset=offset,
        )
        return ListEffectsBody(
            items=[_effect_body(effect) for effect in effects],
            limit=limit,
            offset=offset,
        )

    @app.post("/effects/{effect_id}/resolve", response_model=EffectBody)
    async def resolve_effect(
        effect_id: str,
        body: ResolveEffectBody,
        tenant_id: str = Depends(require_tenant),
    ) -> EffectBody:
        effect = await app.state.broker.resolve(
            tenant_id,
            effect_id,
            resolution=EffectStatus(body.resolution),
            evidence=body.evidence,
        )
        return _effect_body(effect)

    @app.post("/effects/{effect_id}/cancel", response_model=EffectBody)
    async def cancel_effect(
        effect_id: str,
        body: CancelEffectBody,
        tenant_id: str = Depends(require_tenant),
    ) -> EffectBody:
        effect = await app.state.broker.cancel(
            tenant_id,
            effect_id,
            expected_version=body.expected_version,
            actor=body.actor,
            reason=body.reason,
        )
        return _effect_body(effect)

    @app.get("/healthz")
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/metrics")
    async def metrics() -> Response:
        return Response(
            content=metrics_text(),
            media_type=PROMETHEUS_CONTENT_TYPE,
        )

    return app


def _effect_body(effect: EffectRecord) -> EffectBody:
    contract = effect.contract
    return EffectBody(
        effect_id=effect.effect_id,
        status=effect.status,
        contract=ContractBody(
            name=contract.name,
            version=contract.version,
            safety=contract.safety.value,
            retry_limit=contract.retry_limit,
            key_retention_seconds=_seconds(contract.key_retention),
            settlement_bound_seconds=_seconds(contract.settlement_bound),
        ),
        version=effect.version,
        created_at=effect.created_at,
        updated_at=effect.updated_at,
        operation_key=effect.request.operation_key,
        tool=effect.request.tool,
        requested_by=effect.request.requested_by,
        trace_id=effect.request.trace_id,
    )


def _seconds(value) -> float | None:  # noqa: ANN001 - datetime.timedelta or None.
    return None if value is None else value.total_seconds()


def _status_for_error(exc: EffectBrokerError) -> int:
    if isinstance(exc, UnknownEffectError):
        return 404
    if isinstance(exc, PayloadConflictError):
        return 409
    if isinstance(exc, (InvalidTransitionError, VersionConflictError)):
        return 409
    if isinstance(exc, (ContractError, CanonicalizationError)):
        return 422
    return 500


def _title_for_error(exc: EffectBrokerError) -> str:
    if isinstance(exc, UnknownEffectError):
        return "Effect not found"
    if isinstance(exc, PayloadConflictError):
        return "Payload conflict"
    if isinstance(exc, (InvalidTransitionError, VersionConflictError)):
        return "Invalid transition"
    if isinstance(exc, ContractError):
        return "Contract error"
    if isinstance(exc, CanonicalizationError):
        return "Canonicalization error"
    return "Effect broker error"
