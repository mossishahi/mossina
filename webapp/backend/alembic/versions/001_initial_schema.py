"""Initial schema.

Revision ID: 001
Revises: -
Create Date: 2026-03-31
"""

import sqlalchemy as sa
from alembic import op

revision = "001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    # --- countries ---
    op.create_table(
        "countries",
        sa.Column("code", sa.String(2), primary_key=True),
        sa.Column("name", sa.String(255), nullable=True),
        sa.Column("currency", sa.String(3), nullable=True),
    )

    # --- airports ---
    op.create_table(
        "airports",
        sa.Column("iata_code", sa.String(3), primary_key=True),
        sa.Column("name", sa.String(512), nullable=True),
        sa.Column("city", sa.String(255), nullable=True),
        sa.Column(
            "country_code",
            sa.String(2),
            sa.ForeignKey("countries.code", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("latitude", sa.Float, nullable=True),
        sa.Column("longitude", sa.Float, nullable=True),
        sa.Column("timezone", sa.String(64), nullable=True),
    )
    op.create_index("ix_airports_country_code", "airports", ["country_code"])

    # --- routes ---
    op.create_table(
        "routes",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column(
            "origin",
            sa.String(3),
            sa.ForeignKey("airports.iata_code"),
            nullable=False,
        ),
        sa.Column(
            "destination",
            sa.String(3),
            sa.ForeignKey("airports.iata_code"),
            nullable=False,
        ),
        sa.Column("airline", sa.String(2), nullable=False),
        sa.Column(
            "is_connecting", sa.Boolean, nullable=False, server_default="false",
        ),
        sa.Column(
            "new_route", sa.Boolean, nullable=False, server_default="false",
        ),
        sa.Column("last_seen", sa.DateTime(timezone=True), nullable=True),
        sa.UniqueConstraint(
            "origin", "destination", "airline",
            name="uq_routes_origin_destination_airline",
        ),
    )
    op.create_index("ix_routes_origin", "routes", ["origin"])
    op.create_index("ix_routes_destination", "routes", ["destination"])
    op.create_index("ix_routes_airline", "routes", ["airline"])

    # --- schedules ---
    op.create_table(
        "schedules",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column(
            "origin",
            sa.String(3),
            sa.ForeignKey("airports.iata_code"),
            nullable=False,
        ),
        sa.Column(
            "destination",
            sa.String(3),
            sa.ForeignKey("airports.iata_code"),
            nullable=False,
        ),
        sa.Column("flight_number", sa.String(32), nullable=True),
        sa.Column("departure_date", sa.Date, nullable=False),
        sa.Column("departure_time", sa.Time, nullable=True),
        sa.Column("arrival_time", sa.Time, nullable=True),
        sa.Column("airline", sa.String(2), nullable=False),
        sa.Column("scraped_at", sa.DateTime(timezone=True), nullable=True),
        sa.UniqueConstraint(
            "origin", "destination", "departure_date",
            "flight_number", "airline",
            name="uq_schedules_route_date_flight",
        ),
    )

    # --- fares ---
    op.create_table(
        "fares",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column(
            "origin",
            sa.String(3),
            sa.ForeignKey("airports.iata_code"),
            nullable=False,
        ),
        sa.Column(
            "destination",
            sa.String(3),
            sa.ForeignKey("airports.iata_code"),
            nullable=False,
        ),
        sa.Column("departure_date", sa.Date, nullable=False),
        sa.Column("arrival_date", sa.Date, nullable=True),
        sa.Column("price", sa.Numeric(10, 2), nullable=False),
        sa.Column("currency", sa.String(3), nullable=False),
        sa.Column("flight_number", sa.String(32), nullable=True),
        sa.Column("airline", sa.String(2), nullable=False),
        sa.Column("scraped_at", sa.DateTime(timezone=True), nullable=True),
        sa.UniqueConstraint(
            "origin", "destination", "departure_date",
            "flight_number", "airline",
            name="uq_fares_route_date_flight",
        ),
    )
    op.create_index(
        "ix_fares_origin_destination_departure_date_airline",
        "fares",
        ["origin", "destination", "departure_date", "airline"],
    )
    op.create_index("ix_fares_departure_date", "fares", ["departure_date"])

    # --- exchange_rates ---
    op.create_table(
        "exchange_rates",
        sa.Column("currency", sa.String(3), primary_key=True),
        sa.Column("rate_to_eur", sa.Numeric(12, 6), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
    )


def downgrade():
    op.drop_table("exchange_rates")
    op.drop_table("fares")
    op.drop_table("schedules")
    op.drop_table("routes")
    op.drop_table("airports")
    op.drop_table("countries")
