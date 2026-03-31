"""Synchronous SQLAlchemy engine, session factory, and ORM models.

Models mirror the webapp definitions so both services share the same
PostgreSQL schema.  The scraper adds UniqueConstraints on schedules and
fares that the migration also creates -- these are needed for INSERT ...
ON CONFLICT DO UPDATE upserts.
"""

from datetime import date, datetime, time
from decimal import Decimal

from sqlalchemy import (
    Boolean,
    Date,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    Time,
    UniqueConstraint,
    create_engine,
)
from sqlalchemy.orm import Mapped, declarative_base, mapped_column, sessionmaker

from src.config import DATABASE_URL_SYNC

Base = declarative_base()

engine = create_engine(DATABASE_URL_SYNC, echo=False, pool_pre_ping=True)
SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)


# ------------------------------------------------------------------
# Models
# ------------------------------------------------------------------

class Country(Base):
    __tablename__ = "countries"

    code: Mapped[str] = mapped_column(String(2), primary_key=True)
    name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    currency: Mapped[str | None] = mapped_column(String(3), nullable=True)


class Airport(Base):
    __tablename__ = "airports"

    iata_code: Mapped[str] = mapped_column(String(3), primary_key=True)
    name: Mapped[str | None] = mapped_column(String(512), nullable=True)
    city: Mapped[str | None] = mapped_column(String(255), nullable=True)
    country_code: Mapped[str | None] = mapped_column(
        String(2),
        ForeignKey("countries.code", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    latitude: Mapped[float | None] = mapped_column(Float, nullable=True)
    longitude: Mapped[float | None] = mapped_column(Float, nullable=True)
    timezone: Mapped[str | None] = mapped_column(String(64), nullable=True)


class Route(Base):
    __tablename__ = "routes"
    __table_args__ = (
        UniqueConstraint(
            "origin", "destination", "airline",
            name="uq_routes_origin_destination_airline",
        ),
        Index("ix_routes_origin", "origin"),
        Index("ix_routes_destination", "destination"),
        Index("ix_routes_airline", "airline"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    origin: Mapped[str] = mapped_column(
        String(3), ForeignKey("airports.iata_code"), nullable=False,
    )
    destination: Mapped[str] = mapped_column(
        String(3), ForeignKey("airports.iata_code"), nullable=False,
    )
    airline: Mapped[str] = mapped_column(String(2), nullable=False)
    is_connecting: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    new_route: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    last_seen: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True,
    )


class Schedule(Base):
    __tablename__ = "schedules"
    __table_args__ = (
        UniqueConstraint(
            "origin", "destination", "departure_date",
            "flight_number", "airline",
            name="uq_schedules_route_date_flight",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    origin: Mapped[str] = mapped_column(
        String(3), ForeignKey("airports.iata_code"), nullable=False,
    )
    destination: Mapped[str] = mapped_column(
        String(3), ForeignKey("airports.iata_code"), nullable=False,
    )
    flight_number: Mapped[str | None] = mapped_column(String(32), nullable=True)
    departure_date: Mapped[date] = mapped_column(Date, nullable=False)
    departure_time: Mapped[time | None] = mapped_column(Time, nullable=True)
    arrival_time: Mapped[time | None] = mapped_column(Time, nullable=True)
    airline: Mapped[str] = mapped_column(String(2), nullable=False)
    scraped_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True,
    )


class Fare(Base):
    __tablename__ = "fares"
    __table_args__ = (
        UniqueConstraint(
            "origin", "destination", "departure_date",
            "flight_number", "airline",
            name="uq_fares_route_date_flight",
        ),
        Index(
            "ix_fares_origin_destination_departure_date_airline",
            "origin", "destination", "departure_date", "airline",
        ),
        Index("ix_fares_departure_date", "departure_date"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    origin: Mapped[str] = mapped_column(
        String(3), ForeignKey("airports.iata_code"), nullable=False,
    )
    destination: Mapped[str] = mapped_column(
        String(3), ForeignKey("airports.iata_code"), nullable=False,
    )
    departure_date: Mapped[date] = mapped_column(Date, nullable=False)
    arrival_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    price: Mapped[Decimal] = mapped_column(Numeric(10, 2), nullable=False)
    currency: Mapped[str] = mapped_column(String(3), nullable=False)
    flight_number: Mapped[str | None] = mapped_column(String(32), nullable=True)
    airline: Mapped[str] = mapped_column(String(2), nullable=False)
    scraped_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True,
    )


class ExchangeRate(Base):
    __tablename__ = "exchange_rates"

    currency: Mapped[str] = mapped_column(String(3), primary_key=True)
    rate_to_eur: Mapped[Decimal] = mapped_column(Numeric(12, 6), nullable=False)
    updated_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True,
    )
