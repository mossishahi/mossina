"""Add departure_time and arrival_time columns to fares.

Lets the Ryanair fares scraper (cheapestPerDay endpoint) store flight times
directly on the fare row, so the UI no longer has to join against schedules
by flight_number -- which breaks whenever the cheapest-fare API returns just
the airline code instead of a real flight number.

Revision ID: 004
Revises: 003
Create Date: 2026-05-17
"""

import sqlalchemy as sa
from alembic import op

revision = "004"
down_revision = "003"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("fares", sa.Column("departure_time", sa.Time(), nullable=True))
    op.add_column("fares", sa.Column("arrival_time", sa.Time(), nullable=True))


def downgrade():
    op.drop_column("fares", "arrival_time")
    op.drop_column("fares", "departure_time")
