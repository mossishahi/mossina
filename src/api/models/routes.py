"""Route-related response models."""

from pydantic import BaseModel

from src.api.models.common import PaginationMeta

__all__ = [
    "RouteResponse",
    "RouteListResponse",
    "GraphNode",
    "GraphEdge",
    "GraphResponse",
]


class RouteResponse(BaseModel):
    id: int
    origin: str
    origin_name: str | None = None
    origin_city: str | None = None
    origin_country: str | None = None
    destination: str
    destination_name: str | None = None
    destination_city: str | None = None
    destination_country: str | None = None
    airline: str
    is_connecting: bool = False
    new_route: bool = False
    seasonal_route: bool = False
    last_seen: str | None = None
    min_price: float | None = None
    currency: str | None = None


class RouteListResponse(BaseModel):
    routes: list[RouteResponse]
    meta: PaginationMeta


class GraphNode(BaseModel):
    iata_code: str
    name: str
    city: str | None = None
    country_code: str | None = None
    country_name: str | None = None
    latitude: float | None = None
    longitude: float | None = None
    route_count: int = 0


class GraphEdge(BaseModel):
    origin: str
    destination: str
    airline: str
    min_price: float | None = None
    currency: str | None = None


class GraphResponse(BaseModel):
    nodes: list[GraphNode]
    edges: list[GraphEdge]
