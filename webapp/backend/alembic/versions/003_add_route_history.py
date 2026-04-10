"""Add route_history table for tracking route availability over time.

Revision ID: 003
Revises: 002
Create Date: 2026-04-10
"""

import sqlalchemy as sa
from alembic import op

revision = "003"
down_revision = "002"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "route_history",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("origin", sa.String(3), nullable=False),
        sa.Column("destination", sa.String(3), nullable=False),
        sa.Column("airline", sa.String(2), nullable=False),
        sa.Column("has_fares", sa.Boolean, nullable=False),
        sa.Column("fare_count", sa.Integer, nullable=False, server_default="0"),
        sa.Column("min_price", sa.Numeric(10, 2), nullable=True),
        sa.Column("earliest_flight", sa.Date, nullable=True),
        sa.Column("latest_flight", sa.Date, nullable=True),
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("ix_route_history_route", "route_history",
                    ["origin", "destination", "airline"])
    op.create_index("ix_route_history_observed", "route_history",
                    ["observed_at"])


def downgrade():
    op.drop_table("route_history")
