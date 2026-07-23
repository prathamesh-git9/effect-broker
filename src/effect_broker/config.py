"""Typed process configuration and broker construction.

Configuration is intentionally thin: it picks the durable ledger, loads pinned
tool contracts, and derives tenant HMAC keys from one broker secret. It does not
encode retry policy or adapter safety decisions; those stay in the core.
"""

from __future__ import annotations

import base64
import warnings
from enum import StrEnum
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from effect_broker.contracts import ContractRegistry
from effect_broker.engine import AdapterFor, EffectBroker
from effect_broker.store.base import tenant_key_provider_from_secret
from effect_broker.store.memory import InMemoryStore
from effect_broker.store.postgres import PostgresStore
from effect_broker.store.sqlite import SqliteStore

_DEV_BROKER_SECRET = b"effect-broker-dev-secret-do-not-use-in-production"


class StoreKind(StrEnum):
    MEMORY = "memory"
    SQLITE = "sqlite"
    POSTGRES = "postgres"


class Settings(BaseSettings):
    """Environment-backed settings for local operators and server processes."""

    model_config = SettingsConfigDict(
        env_prefix="EFFECT_BROKER_",
        env_nested_delimiter="__",
        extra="ignore",
    )

    broker_secret_hex: str | None = None
    broker_secret_base64: str | None = None
    store: StoreKind = StoreKind.MEMORY
    sqlite_path: Path = Path("effect-broker.sqlite3")
    postgres_dsn: str | None = None
    contracts_path: Path = Path("contracts.yaml")
    api_key_hashes: dict[str, str] = Field(default_factory=dict)
    dev_api_key_enabled: bool = True
    dev_api_key: str = "dev-effect-broker-key"
    dev_tenant_id: str = "tenant-dev"
    adapter_factory: str | None = None

    def broker_secret_bytes(self) -> bytes:
        """Decode the configured broker secret without accepting raw strings."""
        if self.broker_secret_hex:
            return bytes.fromhex(self.broker_secret_hex)
        if self.broker_secret_base64:
            return base64.b64decode(self.broker_secret_base64, validate=True)
        warnings.warn(
            "EFFECT_BROKER_BROKER_SECRET_HEX or "
            "EFFECT_BROKER_BROKER_SECRET_BASE64 is not set; using the "
            "development broker secret. Do not use this configuration for "
            "real tenants.",
            RuntimeWarning,
            stacklevel=2,
        )
        return _DEV_BROKER_SECRET


def load_contracts(path: str | Path) -> ContractRegistry:
    """Load contracts when the file exists; otherwise return an empty registry."""
    contract_path = Path(path)
    if not contract_path.exists():
        return ContractRegistry({})
    return ContractRegistry.from_yaml(contract_path)


def build_broker(
    settings: Settings,
    adapter_for: AdapterFor | None = None,
) -> EffectBroker:
    """Build an :class:`EffectBroker` from config without dispatch adapters."""
    secret = settings.broker_secret_bytes()
    tenant_keys = tenant_key_provider_from_secret(secret)
    contracts = load_contracts(settings.contracts_path)
    if settings.store is StoreKind.MEMORY:
        store = InMemoryStore(tenant_keys)
    elif settings.store is StoreKind.SQLITE:
        store = SqliteStore.open(settings.sqlite_path, secret)
    else:
        if settings.postgres_dsn is None:
            raise ValueError(
                "EFFECT_BROKER_POSTGRES_DSN is required when EFFECT_BROKER_STORE=postgres"
            )
        store = PostgresStore.connect(settings.postgres_dsn, tenant_keys)
    return EffectBroker(store, contracts, adapter_for)
