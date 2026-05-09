"""Add price_history table.

Revision ID: 002
Revises: 001
Create Date: 2026-04-09
"""

import sqlalchemy as sa
from alembic import op

revision = "002"
down_revision = "001"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "price_history",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("origin", sa.String(3), nullable=False),
        sa.Column("destination", sa.String(3), nullable=False),
        sa.Column("airline", sa.String(2), nullable=False),
        sa.Column("departure_date", sa.Date, nullable=False),
        sa.Column("price", sa.Numeric(10, 2), nullable=False),
        sa.Column("currency", sa.String(3), nullable=False),
        sa.Column("flight_number", sa.String(32), nullable=True),
        sa.Column("scraped_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("ix_price_history_route", "price_history",
                    ["origin", "destination", "airline"])
    op.create_index("ix_price_history_date", "price_history",
                    ["departure_date"])
    op.create_index("ix_price_history_scraped", "price_history",
                    ["scraped_at"])


def downgrade():
    op.drop_table("price_history")
