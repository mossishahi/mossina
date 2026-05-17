"""Add airport_distances table (the precomputed ground graph).

Stores great-circle distances between every pair of airports within
MAX_GROUND_DISTANCE_KM (200km), in canonical order (a < b). Populated
by scripts/compute_airport_distances.py.

Revision ID: 005
Revises: 004
Create Date: 2026-05-17
"""

import sqlalchemy as sa
from alembic import op

revision = "005"
down_revision = "004"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "airport_distances",
        sa.Column(
            "a",
            sa.String(3),
            sa.ForeignKey("airports.iata_code", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "b",
            sa.String(3),
            sa.ForeignKey("airports.iata_code", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("distance_km", sa.Float(), nullable=False),
        sa.PrimaryKeyConstraint("a", "b"),
        sa.CheckConstraint("a < b", name="ck_airport_distances_canonical_order"),
    )
    op.create_index(
        "ix_airport_distances_a_dist", "airport_distances", ["a", "distance_km"],
    )
    op.create_index(
        "ix_airport_distances_b_dist", "airport_distances", ["b", "distance_km"],
    )


def downgrade():
    op.drop_index("ix_airport_distances_b_dist", table_name="airport_distances")
    op.drop_index("ix_airport_distances_a_dist", table_name="airport_distances")
    op.drop_table("airport_distances")
