"""Shared response models."""

from pydantic import BaseModel

__all__ = [
    "PaginationMeta",
    "CountryResponse",
    "CountryListResponse",
    "AirlineResponse",
    "AirlineListResponse",
    "StatsResponse",
    "TableCount",
    "AirlineStat",
]


class PaginationMeta(BaseModel):
    page: int
    per_page: int
    total: int
    total_pages: int


class CountryResponse(BaseModel):
    code: str
    name: str
    currency: str | None = None


class CountryListResponse(BaseModel):
    countries: list[CountryResponse]


class AirlineResponse(BaseModel):
    code: str
    name: str


class AirlineListResponse(BaseModel):
    airlines: list[AirlineResponse]


class TableCount(BaseModel):
    table: str
    count: int


class AirlineStat(BaseModel):
    airline: str
    route_count: int


class StatsResponse(BaseModel):
    tables: list[TableCount]
    airlines: list[AirlineStat]
    last_updated: str | None = None
    db_size_mb: float
