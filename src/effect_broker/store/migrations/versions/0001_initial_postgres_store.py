"""initial production Postgres store

Revision ID: 0001_initial_postgres_store
Revises:
Create Date: 2026-07-23
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0001_initial_postgres_store"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "tenants",
        sa.Column("tenant_id", sa.Text(), primary_key=True),
        sa.Column("name", sa.Text()),
        sa.Column("api_key_hash", sa.Text()),
        sa.Column("hmac_key_id", sa.Text()),
        sa.Column("disabled", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
    )
    op.create_table(
        "effect_intents",
        sa.Column("effect_id", sa.Text(), primary_key=True),
        sa.Column(
            "tenant_id",
            sa.Text(),
            sa.ForeignKey("tenants.tenant_id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column("operation_key", sa.Text(), nullable=False),
        sa.Column("tool", sa.Text(), nullable=False),
        sa.Column("arguments_json", postgresql.JSONB(), nullable=False),
        sa.Column("requested_by", sa.Text(), nullable=False),
        sa.Column("trace_id", sa.Text()),
        sa.Column("request_hash", sa.Text(), nullable=False),
        sa.Column("downstream_key", sa.Text(), nullable=False),
        sa.Column("contract_name", sa.Text(), nullable=False),
        sa.Column("contract_version", sa.Text(), nullable=False),
        sa.Column("safety_class", sa.Text(), nullable=False),
        sa.Column("retry_limit", sa.Integer(), nullable=False),
        sa.Column("key_retention_seconds", sa.BigInteger()),
        sa.Column("settlement_bound_seconds", sa.BigInteger()),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("worker_id", sa.Text()),
        sa.Column("lease_until", sa.DateTime(timezone=True)),
        sa.UniqueConstraint(
            "tenant_id",
            "operation_key",
            name="uq_effect_intents_tenant_operation_key",
        ),
    )
    op.create_index(
        "ix_effect_intents_status_lease",
        "effect_intents",
        ["status", "lease_until"],
    )
    op.create_index("ix_effect_intents_tenant_id", "effect_intents", ["tenant_id"])
    op.create_table(
        "effect_attempts",
        sa.Column("attempt_id", sa.Text(), primary_key=True),
        sa.Column(
            "effect_id",
            sa.Text(),
            sa.ForeignKey("effect_intents.effect_id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("ordinal", sa.Integer(), nullable=False),
        sa.Column("worker_id", sa.Text(), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("effect_id", "ordinal", name="uq_effect_attempts_ordinal"),
    )
    op.create_index(
        "ix_effect_attempts_effect_id",
        "effect_attempts",
        ["effect_id"],
    )
    op.create_table(
        "effect_receipts",
        sa.Column("receipt_id", sa.Text(), primary_key=True),
        sa.Column(
            "effect_id",
            sa.Text(),
            sa.ForeignKey("effect_intents.effect_id", ondelete="CASCADE"),
            nullable=False,
            unique=True,
        ),
        sa.Column("external_id", sa.Text()),
        sa.Column("contract_name", sa.Text(), nullable=False),
        sa.Column("contract_version", sa.Text(), nullable=False),
        sa.Column("downstream_key", sa.Text(), nullable=False),
        sa.Column("metadata_json", postgresql.JSONB(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index(
        "ix_effect_receipts_effect_id",
        "effect_receipts",
        ["effect_id"],
    )
    op.create_table(
        "effect_events",
        sa.Column(
            "effect_id",
            sa.Text(),
            sa.ForeignKey("effect_intents.effect_id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("sequence", sa.Integer(), nullable=False),
        sa.Column("event_type", sa.Text(), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column("at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("data_json", postgresql.JSONB(), nullable=False),
        sa.PrimaryKeyConstraint("effect_id", "sequence", name="pk_effect_events"),
    )
    op.create_index("ix_effect_events_effect_id", "effect_events", ["effect_id"])
    op.execute(
        """
        CREATE OR REPLACE FUNCTION effect_receipts_no_update()
        RETURNS trigger AS $$
        BEGIN
            RAISE EXCEPTION 'effect_receipts are immutable';
        END;
        $$ LANGUAGE plpgsql
        """
    )
    op.execute(
        """
        CREATE TRIGGER trg_effect_receipts_no_update
        BEFORE UPDATE ON effect_receipts
        FOR EACH ROW EXECUTE FUNCTION effect_receipts_no_update()
        """
    )


def downgrade() -> None:
    op.execute("DROP TRIGGER IF EXISTS trg_effect_receipts_no_update ON effect_receipts")
    op.execute("DROP FUNCTION IF EXISTS effect_receipts_no_update")
    op.drop_index("ix_effect_events_effect_id", table_name="effect_events")
    op.drop_table("effect_events")
    op.drop_index("ix_effect_receipts_effect_id", table_name="effect_receipts")
    op.drop_table("effect_receipts")
    op.drop_index("ix_effect_attempts_effect_id", table_name="effect_attempts")
    op.drop_table("effect_attempts")
    op.drop_index("ix_effect_intents_tenant_id", table_name="effect_intents")
    op.drop_index("ix_effect_intents_status_lease", table_name="effect_intents")
    op.drop_table("effect_intents")
    op.drop_table("tenants")
