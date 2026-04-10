"""Path and cycle search request and response schemas."""

from datetime import date

from pydantic import BaseModel, ConfigDict


class HopConstraint(BaseModel):
    """Filter for a specific hop position (0 = origin, N = destination)."""
    min_stay_days: int | None = None
    max_stay_days: int | None = None
    include_cities: list[str] | None = None
    exclude_cities: list[str] | None = None


class LegConstraint(BaseModel):
    """Filter for the connection between hop i and hop i+1."""
    airline: str | None = None


class PathSearchRequest(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    origins: list[str]
    destinations: list[str] | None = None
    max_hops: int
    date_from: date | None = None
    date_to: date | None = None
    only_selected: bool
    airline: str | None = None
    hop_filters: list[HopConstraint] | None = None
    leg_filters: list[LegConstraint] | None = None


class CycleSearchRequest(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    origins: list[str]
    max_hops: int
    date_from: date | None = None
    date_to: date | None = None
    only_selected: bool
    hop_filters: list[HopConstraint] | None = None
    leg_filters: list[LegConstraint] | None = None


class PathLeg(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    origin: str
    destination: str
    airline: str
    cost_eur: float | None = None
    best_date: str | None = None


class PathResult(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    path: list[str]
    legs: list[PathLeg]
    total_cost_eur: float | None = None
    is_partial: bool = False


class SearchResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    results: list[PathResult]
    count: int
    search_time_ms: float
