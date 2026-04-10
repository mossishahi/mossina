"""Fare-related response models."""

from pydantic import BaseModel

from src.api.models.common import PaginationMeta

__all__ = [
    "FareResponse",
    "FareListResponse",
]


class FareResponse(BaseModel):
    id: int
    origin: str
    origin_name: str | None = None
    destination: str
    destination_name: str | None = None
    airline: str
    departure_date: str | None = None
    arrival_date: str | None = None
    price: float | None = None
    currency: str | None = None
    flight_number: str | None = None
    scraped_at: str | None = None


class FareListResponse(BaseModel):
    fares: list[FareResponse]
    meta: PaginationMeta
