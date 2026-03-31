"""Schedule and fare ORM models."""

from datetime import date, datetime, time
from decimal import Decimal

from sqlalchemy import Date, DateTime, ForeignKey, Index, Integer, Numeric, String, Time
from sqlalchemy.orm import Mapped, mapped_column

from app.database import Base


class Schedule(Base):
    __tablename__ = "schedules"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    origin: Mapped[str] = mapped_column(String(3), ForeignKey("airports.iata_code"), nullable=False)
    destination: Mapped[str] = mapped_column(
        String(3),
        ForeignKey("airports.iata_code"),
        nullable=False,
    )
    flight_number: Mapped[str | None] = mapped_column(String(32), nullable=True)
    departure_date: Mapped[date] = mapped_column(Date, nullable=False)
    departure_time: Mapped[time | None] = mapped_column(Time, nullable=True)
    arrival_time: Mapped[time | None] = mapped_column(Time, nullable=True)
    airline: Mapped[str] = mapped_column(String(2), nullable=False)
    scraped_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class Fare(Base):
    __tablename__ = "fares"
    __table_args__ = (
        Index(
            "ix_fares_origin_destination_departure_date_airline",
            "origin",
            "destination",
            "departure_date",
            "airline",
        ),
        Index("ix_fares_departure_date", "departure_date"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    origin: Mapped[str] = mapped_column(String(3), ForeignKey("airports.iata_code"), nullable=False)
    destination: Mapped[str] = mapped_column(
        String(3),
        ForeignKey("airports.iata_code"),
        nullable=False,
    )
    departure_date: Mapped[date] = mapped_column(Date, nullable=False)
    arrival_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    price: Mapped[Decimal] = mapped_column(Numeric(10, 2), nullable=False)
    currency: Mapped[str] = mapped_column(String(3), nullable=False)
    flight_number: Mapped[str | None] = mapped_column(String(32), nullable=True)
    airline: Mapped[str] = mapped_column(String(2), nullable=False)
    scraped_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
