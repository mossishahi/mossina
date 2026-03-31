"""DFS path and cycle search over route graph with fare-based cost in EUR."""

from __future__ import annotations

import time
from collections import defaultdict
from datetime import date

from sqlalchemy import select, tuple_
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.fare import Fare
from app.schemas.search import (
    CycleSearchRequest,
    PathLeg,
    PathResult,
    PathSearchRequest,
    SearchResponse,
)
from app.services.exchange_rates import convert_to_eur, get_rates
from app.services.graph import fetch_filtered_route_edges

AIRLINE_META = {
    "FR": {"name": "Ryanair", "color": "#0b4ea2"},
    "W6": {"name": "Wizz Air", "color": "#e500a4"},
}

TIME_BUDGET_S = 10.0
NODE_CHECK_INTERVAL = 5000


def _normalize_date_range(
    date_from: date | None, date_to: date | None
) -> tuple[date | None, date | None]:
    if date_from is None and date_to is None:
        return None, None
    if date_from is None:
        return date_to, date_to
    if date_to is None:
        return date_from, date_from
    return date_from, date_to


def _sort_key_cost(result: PathResult) -> tuple[int, float]:
    if result.total_cost_eur is None:
        return (1, 0.0)
    return (0, result.total_cost_eur)


def _cycle_dedup_key(airports: list[str]) -> tuple[str, ...]:
    """Normalize cycle by sorting unique airports (drop duplicate closing node)."""
    if len(airports) < 2:
        return tuple(airports)
    if airports[0] == airports[-1]:
        core = airports[:-1]
    else:
        core = airports
    return tuple(sorted(core))


def _build_path_result(
    airports: list[str],
    edges: list[tuple[str, str, str]],
    leg_costs: dict[tuple[str, str, str], float | None],
) -> PathResult:
    legs: list[PathLeg] = []
    partial = False
    total: float | None = 0.0
    for o, d, airline in edges:
        key = (o, d, airline)
        c = leg_costs.get(key)
        legs.append(PathLeg(origin=o, destination=d, airline=airline, cost_eur=c))
        if c is None:
            partial = True
            total = None
        elif total is not None:
            total += c
    return PathResult(
        path=airports,
        legs=legs,
        total_cost_eur=total,
        is_partial=partial,
    )


async def _load_min_leg_costs_eur(
    db: AsyncSession,
    legs: set[tuple[str, str, str]],
    date_from: date | None,
    date_to: date | None,
    rates: dict[str, float],
) -> dict[tuple[str, str, str], float | None]:
    if not legs:
        return {}
    leg_list = list(legs)
    stmt = select(
        Fare.origin,
        Fare.destination,
        Fare.airline,
        Fare.price,
        Fare.currency,
    ).where(tuple_(Fare.origin, Fare.destination, Fare.airline).in_(leg_list))
    df, dt = _normalize_date_range(date_from, date_to)
    if df is not None and dt is not None:
        stmt = stmt.where(Fare.departure_date >= df, Fare.departure_date <= dt)
    result = await db.execute(stmt)
    best: dict[tuple[str, str, str], float | None] = {k: None for k in legs}
    for o, d, airline, price, currency in result.all():
        key = (o, d, airline)
        eur = convert_to_eur(float(price), str(currency), rates)
        prev = best.get(key)
        if eur is None:
            continue
        if prev is None or eur < prev:
            best[key] = eur
    return best


def _build_adjacency_with_airlines(
    edges: list[tuple[str, str, str]],
) -> dict[str, list[tuple[str, str]]]:
    adj: dict[str, list[tuple[str, str]]] = defaultdict(list)
    for o, d, a in edges:
        adj[o].append((d, a))
    for o in adj:
        adj[o].sort(key=lambda t: (t[0], t[1]))
    return adj


async def find_paths(db: AsyncSession, request: PathSearchRequest) -> SearchResponse:
    t_start = time.perf_counter()
    edges = await fetch_filtered_route_edges(
        db, request.airline, request.date_from, request.date_to
    )
    rates = await get_rates(db)
    leg_keys = {(o, d, a) for o, d, a in edges}
    leg_costs = await _load_min_leg_costs_eur(
        db, leg_keys, request.date_from, request.date_to, rates
    )

    all_nodes: set[str] = set()
    for o, d, _ in edges:
        all_nodes.add(o)
        all_nodes.add(d)

    dest_set = set(request.destinations) if request.destinations is not None else all_nodes
    origins_set = set(request.origins)
    allowed_intermediate = origins_set | dest_set

    adj = _build_adjacency_with_airlines(edges)

    results: list[PathResult] = []
    node_visits = 0
    timed_out = False

    def over_budget() -> bool:
        nonlocal timed_out
        if timed_out:
            return True
        if node_visits % NODE_CHECK_INTERVAL == 0 and node_visits > 0:
            if time.perf_counter() - t_start > TIME_BUDGET_S:
                timed_out = True
                return True
        return False

    def dfs_path(
        current: str,
        path_nodes: list[str],
        path_edges: list[tuple[str, str, str]],
        visited: set[str],
    ) -> None:
        nonlocal node_visits
        node_visits += 1
        if over_budget():
            return
        if current in dest_set and len(path_edges) >= 1:
            interiors_ok = (not request.only_selected) or all(
                x in allowed_intermediate for x in path_nodes[1:-1]
            )
            if interiors_ok:
                results.append(_build_path_result(path_nodes, path_edges, leg_costs))
        if len(path_edges) >= request.max_hops:
            return
        for nxt, airline in adj.get(current, []):
            if over_budget():
                return
            if nxt in visited:
                continue
            next_nodes = path_nodes + [nxt]
            if request.only_selected:
                interiors = next_nodes[1:-1]
                if not all(x in allowed_intermediate for x in interiors):
                    continue
            next_edges = path_edges + [(current, nxt, airline)]
            visited.add(nxt)
            dfs_path(nxt, next_nodes, next_edges, visited)
            visited.remove(nxt)

    for origin in sorted(origins_set):
        if over_budget():
            break
        if origin not in adj and origin not in all_nodes:
            continue
        dfs_path(origin, [origin], [], {origin})

    results.sort(key=_sort_key_cost)
    elapsed_ms = (time.perf_counter() - t_start) * 1000
    return SearchResponse(
        results=results,
        count=len(results),
        search_time_ms=elapsed_ms,
    )


async def find_cycles(db: AsyncSession, request: CycleSearchRequest) -> SearchResponse:
    t_start = time.perf_counter()
    edges = await fetch_filtered_route_edges(
        db, None, request.date_from, request.date_to
    )
    rates = await get_rates(db)
    leg_keys = {(o, d, a) for o, d, a in edges}
    leg_costs = await _load_min_leg_costs_eur(
        db, leg_keys, request.date_from, request.date_to, rates
    )

    origins_set = set(request.origins)
    allowed_intermediate = origins_set

    adj = _build_adjacency_with_airlines(edges)

    seen_cycle_keys: set[tuple[str, ...]] = set()
    results: list[PathResult] = []
    node_visits = 0
    timed_out = False

    def over_budget() -> bool:
        nonlocal timed_out
        if timed_out:
            return True
        if node_visits % NODE_CHECK_INTERVAL == 0 and node_visits > 0:
            if time.perf_counter() - t_start > TIME_BUDGET_S:
                timed_out = True
                return True
        return False

    def dfs_cycle(
        start: str,
        current: str,
        path_nodes: list[str],
        path_edges: list[tuple[str, str, str]],
    ) -> None:
        nonlocal node_visits
        node_visits += 1
        if over_budget():
            return
        for nxt, airline in adj.get(current, []):
            if over_budget():
                return
            if nxt == start:
                if len(path_edges) < 1:
                    continue
                full_nodes = path_nodes + [start]
                if request.only_selected:
                    interiors = full_nodes[1:-1]
                    if not all(x in allowed_intermediate for x in interiors):
                        continue
                key = _cycle_dedup_key(full_nodes)
                if key in seen_cycle_keys:
                    continue
                seen_cycle_keys.add(key)
                full_edges = path_edges + [(current, nxt, airline)]
                results.append(_build_path_result(full_nodes, full_edges, leg_costs))
                continue
            if nxt in path_nodes[1:]:
                continue
            next_nodes = path_nodes + [nxt]
            if request.only_selected:
                interiors = next_nodes[1:-1]
                if not all(x in allowed_intermediate for x in interiors):
                    continue
            next_edges = path_edges + [(current, nxt, airline)]
            dfs_cycle(start, nxt, next_nodes, next_edges)

    for start in sorted(origins_set):
        if over_budget():
            break
        if start not in adj:
            continue
        dfs_cycle(start, start, [start], [])

    results.sort(key=_sort_key_cost)
    elapsed_ms = (time.perf_counter() - t_start) * 1000
    return SearchResponse(
        results=results,
        count=len(results),
        search_time_ms=elapsed_ms,
    )
