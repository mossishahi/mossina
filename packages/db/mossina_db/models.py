"""Single source of truth for all SQLAlchemy ORM models.

Uses scraper model definitions as the canonical source because they include
the UniqueConstraint entries on schedules and fares required for
INSERT ... ON CONFLICT DO UPDATE upserts.  Adds ORM relationships used by
the backend (Country.airports / Airport.country).
"""

from __future__ import annotations

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
)
from sqlalchemy.orm import Mapped, declarative_base, mapped_column, relationship

Base = declarative_base()


class Country(Base):
    __tablename__ = "countries"

    code: Mapped[str] = mapped_column(String(2), primary_key=True)
    name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    currency: Mapped[str | None] = mapped_column(String(3), nullable=True)

    airports: Mapped[list[Airport]] = relationship("Airport", back_populates="country")


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

    country: Mapped[Country | None] = relationship(
        "Country",
        back_populates="airports",
        foreign_keys=[country_code],
    )


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


class PriceHistory(Base):
    __tablename__ = "price_history"
    __table_args__ = (
        Index("ix_price_history_route", "origin", "destination", "airline"),
        Index("ix_price_history_date", "departure_date"),
        Index("ix_price_history_scraped", "scraped_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    origin: Mapped[str] = mapped_column(String(3), nullable=False)
    destination: Mapped[str] = mapped_column(String(3), nullable=False)
    airline: Mapped[str] = mapped_column(String(2), nullable=False)
    departure_date: Mapped[date] = mapped_column(Date, nullable=False)
    price: Mapped[Decimal] = mapped_column(Numeric(10, 2), nullable=False)
    currency: Mapped[str] = mapped_column(String(3), nullable=False)
    flight_number: Mapped[str | None] = mapped_column(String(32), nullable=True)
    scraped_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False,
    )


class RouteHistory(Base):
    __tablename__ = "route_history"
    __table_args__ = (
        Index("ix_route_history_route", "origin", "destination", "airline"),
        Index("ix_route_history_observed", "observed_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    origin: Mapped[str] = mapped_column(String(3), nullable=False)
    destination: Mapped[str] = mapped_column(String(3), nullable=False)
    airline: Mapped[str] = mapped_column(String(2), nullable=False)
    has_fares: Mapped[bool] = mapped_column(Boolean, nullable=False)
    fare_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    min_price: Mapped[Decimal | None] = mapped_column(Numeric(10, 2), nullable=True)
    earliest_flight: Mapped[date | None] = mapped_column(Date, nullable=True)
    latest_flight: Mapped[date | None] = mapped_column(Date, nullable=True)
    observed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False,
    )


class ExchangeRate(Base):
    __tablename__ = "exchange_rates"

    currency: Mapped[str] = mapped_column(String(3), primary_key=True)
    rate_to_eur: Mapped[Decimal] = mapped_column(Numeric(12, 6), nullable=False)
    updated_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True,
    )
