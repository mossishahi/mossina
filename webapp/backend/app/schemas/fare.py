"""Fare API schemas."""

from datetime import date
from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field


class FareOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    departure_date: date
    price: Decimal
    price_eur: Decimal | None = None
    currency: str
    airline: str
    flight_number: str | None = None


class RouteFaresOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    origin: str
    destination: str
    fares: list[FareOut] = Field(default_factory=list)
    cheapest_eur: Decimal | None = None
