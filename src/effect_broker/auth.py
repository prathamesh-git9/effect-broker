"""API-key authentication with tenant identity derived from config.

Raw API keys are accepted only at the process boundary, hashed immediately, and
never stored on objects that need to be rendered or logged. Tenant identity
comes from the matched key, not from caller-controlled request JSON.
"""

from __future__ import annotations

import hashlib
import hmac
from dataclasses import dataclass

from effect_broker.config import Settings

try:  # FastAPI is an optional server dependency.
    from fastapi import Depends, HTTPException, Request, status
    from fastapi.security import APIKeyHeader, HTTPAuthorizationCredentials, HTTPBearer
except ImportError:  # pragma: no cover - exercised by minimal installs.
    Depends = HTTPException = Request = status = None  # type: ignore[assignment]
    APIKeyHeader = HTTPAuthorizationCredentials = HTTPBearer = None  # type: ignore[misc]


def hash_api_key(raw_key: str) -> str:
    """Return the SHA-256 hex digest used for API-key lookup."""
    return hashlib.sha256(raw_key.encode("utf-8")).hexdigest()


@dataclass(frozen=True, slots=True)
class APIKeyAuthenticator:
    """Tenant lookup by SHA-256 API-key digest."""

    key_hash_to_tenant: dict[str, str]

    @classmethod
    def from_settings(cls, settings: Settings) -> APIKeyAuthenticator:
        mapping = dict(settings.api_key_hashes)
        if settings.dev_api_key_enabled:
            mapping.setdefault(hash_api_key(settings.dev_api_key), settings.dev_tenant_id)
        return cls(mapping)

    def tenant_for_key(self, raw_key: str) -> str | None:
        incoming = hash_api_key(raw_key)
        for stored_hash, tenant_id in self.key_hash_to_tenant.items():
            if hmac.compare_digest(stored_hash, incoming):
                return tenant_id
        return None


if HTTPBearer is not None:
    _bearer = HTTPBearer(auto_error=False)
    _api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)
    _bearer_dependency = Depends(_bearer)
    _api_key_dependency = Depends(_api_key_header)

    async def require_tenant(
        request: Request,
        bearer: HTTPAuthorizationCredentials | None = _bearer_dependency,
        api_key: str | None = _api_key_dependency,
    ) -> str:
        """FastAPI dependency returning the authenticated tenant id."""
        raw_key = api_key
        if raw_key is None and bearer is not None:
            raw_key = bearer.credentials
        if not raw_key:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="missing API key",
            )
        authenticator = request.app.state.authenticator
        tenant_id = authenticator.tenant_for_key(raw_key)
        if tenant_id is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="invalid API key",
            )
        return tenant_id

else:

    async def require_tenant(*args, **kwargs) -> str:  # noqa: ANN002, ANN003
        raise RuntimeError("FastAPI is required for require_tenant")
