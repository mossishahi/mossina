"""Airport-related response models."""

from pydantic import BaseModel

from src.api.models.common import PaginationMeta

__all__ = [
    "AirportResponse",
    "AirportListResponse",
    "AirportDetail",
    "AirportSearchResult",
]


class AirportResponse(BaseModel):
    iata_code: str
    name: str
    city: str | None = None
    country_code: str | None = None
    country_name: str | None = None
    latitude: float | None = None
    longitude: float | None = None
    timezone: str | None = None


class AirportListResponse(BaseModel):
    airports: list[AirportResponse]
    meta: PaginationMeta | None = None


class ConnectedRoute(BaseModel):
    destination: AirportResponse
    airline: str
    has_fares: bool = False
    min_price: float | None = None
    currency: str | None = None


class AirportDetail(BaseModel):
    airport: AirportResponse
    outbound_routes: list[ConnectedRoute]
    inbound_route_count: int


class AirportSearchResult(BaseModel):
    iata_code: str
    name: str
    city: str | None = None
    country_code: str | None = None
    country_name: str | None = None
    match_field: str
