"""Path and cycle search request and response schemas."""

from datetime import date
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from mossina_db.models import MAX_GROUND_DISTANCE_KM

LegKind = Literal["flight", "ground"]


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
    # max_stops counts CITIES visited (origin + intermediates + dest).
    # Internally this maps to legs = max_stops - 1 edges in the DFS.
    # A ground transfer to a different city also consumes a stop.
    max_stops: int = Field(ge=2)
    date_from: date | None = None
    date_to: date | None = None
    only_selected: bool
    airline: str | None = None
    hop_filters: list[HopConstraint] | None = None
    leg_filters: list[LegConstraint] | None = None
    # Max ground-transfer distance (km). null / 0 disables ground transfers
    # entirely (default). Bounded by MAX_GROUND_DISTANCE_KM because anything
    # larger would query an incomplete precomputed graph.
    ground_distance_km: float | None = Field(
        default=None, ge=0, le=MAX_GROUND_DISTANCE_KM,
    )


class CycleSearchRequest(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    origins: list[str]
    # For cycles, max_stops counts distinct cities visited; the closing
    # return to the origin doesn't add a new stop. Minimum 3 (origin +
    # at least one intermediate + return to origin).
    max_stops: int = Field(ge=3)
    date_from: date | None = None
    date_to: date | None = None
    only_selected: bool
    hop_filters: list[HopConstraint] | None = None
    leg_filters: list[LegConstraint] | None = None
    ground_distance_km: float | None = Field(
        default=None, ge=0, le=MAX_GROUND_DISTANCE_KM,
    )


class PathLeg(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    origin: str
    destination: str
    # For flights this is the IATA airline code (e.g. "FR"); for ground
    # transfers it's the sentinel "GROUND".
    airline: str
    # "flight" by default for backwards compatibility with existing clients.
    kind: LegKind = "flight"
    cost_eur: float | None = None
    best_date: str | None = None
    # Populated only when kind == "ground".
    ground_distance_km: float | None = None


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
