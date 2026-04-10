"""Network graph endpoint: nodes (airports) and edges (routes) for map rendering."""

import sqlite3

from fastapi import APIRouter, Depends, Query

from src.api.dependencies import get_db
from src.api.models.routes import GraphEdge, GraphNode, GraphResponse

router = APIRouter(prefix="/graph", tags=["graph"])


@router.get("", response_model=GraphResponse)
def get_graph(
    airline: str | None = Query(None, description="Filter by airline code"),
    country: str | None = Query(None, description="Filter by country code"),
    db: sqlite3.Connection = Depends(get_db),
):
    """Return the full network graph for map visualization."""
    route_where, route_params = [], []
    if airline:
        route_where.append("r.airline = ?")
        route_params.append(airline.upper())

    route_filter = f"WHERE {' AND '.join(route_where)}" if route_where else ""

    edges_raw = db.execute(
        f"""SELECT r.origin, r.destination, r.airline,
                   MIN(f.price) AS min_price, f.currency
            FROM routes r
            LEFT JOIN fares f ON f.origin = r.origin
                             AND f.destination = r.destination
                             AND f.airline = r.airline
                             AND f.departure_date >= date('now')
            {route_filter}
            GROUP BY r.origin, r.destination, r.airline""",
        route_params,
    ).fetchall()

    airport_codes = set()
    edges = []
    for r in edges_raw:
        airport_codes.add(r["origin"])
        airport_codes.add(r["destination"])
        edges.append(GraphEdge(
            origin=r["origin"],
            destination=r["destination"],
            airline=r["airline"],
            min_price=r["min_price"],
            currency=r["currency"],
        ))

    if not airport_codes:
        return GraphResponse(nodes=[], edges=[])

    placeholders = ",".join("?" for _ in airport_codes)
    node_where = f"a.iata_code IN ({placeholders})"
    node_params = list(airport_codes)
    if country:
        node_where += " AND a.country_code = ?"
        node_params.append(country.upper())

    nodes_raw = db.execute(
        f"""SELECT a.iata_code, a.name, a.city, a.country_code,
                   COALESCE(c.name, '') AS country_name,
                   a.latitude, a.longitude,
                   COUNT(DISTINCT r.destination) AS route_count
            FROM airports a
            LEFT JOIN countries c ON a.country_code = c.code
            LEFT JOIN routes r ON r.origin = a.iata_code
            WHERE {node_where}
            GROUP BY a.iata_code""",
        node_params,
    ).fetchall()

    if country:
        valid_codes = {r["iata_code"] for r in nodes_raw}
        edges = [e for e in edges
                 if e.origin in valid_codes or e.destination in valid_codes]

    nodes = [
        GraphNode(
            iata_code=r["iata_code"],
            name=r["name"],
            city=r["city"],
            country_code=r["country_code"],
            country_name=r["country_name"],
            latitude=r["latitude"],
            longitude=r["longitude"],
            route_count=r["route_count"],
        )
        for r in nodes_raw
    ]

    return GraphResponse(nodes=nodes, edges=edges)
